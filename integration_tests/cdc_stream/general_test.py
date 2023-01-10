# Copyright 2022 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# Lint as: python3
"""cdc_stream general test."""

import os
import posixpath
import shutil

from integration_tests.framework import utils
from integration_tests.cdc_stream import test_base


class GeneralTest(test_base.CdcStreamTest):
  """cdc_stream general test class."""

  def test_stream(self):
    """Stream an existing directory."""
    files = [
        'dir1\\file1_1.txt', 'dir1\\file1_2.txt', 'dir2\\file2_1.txt',
        'file0.txt'
    ]
    dirs = ['dir1\\emptydir2\\', 'emptydir1\\', 'dir1\\', 'dir2\\']
    self._create_test_data(files, dirs)
    self._start()
    self._test_dir_content(files=files, dirs=dirs)
    self._assert_cache()
    self._assert_cdc_fuse_mounted()

  def test_update_file(self):
    """File updates are visible on remote instance."""
    filename = 'file1.txt'
    utils.create_test_file(os.path.join(self.local_base_dir, filename), 1024)
    self._start()
    self._test_dir_content(files=[filename], dirs=[])
    cache_size = self._get_cache_size_in_bytes()
    original = utils.get_ssh_command_output(self.ls_cmd)

    # Modify the file, cache should become larger.
    utils.create_test_file(os.path.join(self.local_base_dir, filename), 2048)

    self.assertTrue(self._wait_until_remote_dir_changed(original))
    self._test_dir_content(files=[filename], dirs=[])
    self.assertGreater(self._get_cache_size_in_bytes(), cache_size)

  def test_add_file(self):
    """New file is visible on remote instance."""
    self._start()
    self._test_dir_content(files=[], dirs=[])
    cache_size = self._get_cache_size_in_bytes()
    # Create a file, cache should become larger.
    filename = 'file1.txt'
    original = utils.get_ssh_command_output(self.ls_cmd)

    utils.create_test_file(os.path.join(self.local_base_dir, filename), 1024)

    self.assertTrue(self._wait_until_remote_dir_changed(original))
    self._test_dir_content(files=[filename], dirs=[])
    self.assertGreater(self._get_cache_size_in_bytes(), cache_size)

  def test_change_mtime(self):
    """Change of mtime is visible on remote instance."""
    filename = 'file1.txt'
    file_local_path = os.path.join(self.local_base_dir, filename)
    utils.create_test_file(file_local_path, 1024)
    self._start()
    mtime = os.path.getmtime(file_local_path)
    self._test_dir_content(files=[filename], dirs=[])
    cache_size = self._get_cache_size_in_bytes()
    original = utils.get_ssh_command_output(self.ls_cmd)

    # Change mtime of the file, a new manifest should be created.
    utils.change_modified_time(file_local_path)

    self.assertTrue(self._wait_until_remote_dir_changed(original))

    # Cache should become larger.
    self._test_dir_content(files=[filename], dirs=[])
    self.assertNotEqual(os.path.getmtime(file_local_path), mtime)
    self.assertGreater(self._get_cache_size_in_bytes(), cache_size)

  def test_remove_file(self):
    """File removal is visible on remote instance."""
    filename = 'file1.txt'
    file_local_path = os.path.join(self.local_base_dir, filename)
    utils.create_test_file(file_local_path, 1024)
    self._start()
    self._test_dir_content(files=[filename], dirs=[])
    cache_size = self._get_cache_size_in_bytes()
    original = utils.get_ssh_command_output(self.ls_cmd)

    # After removing a file, the manifest is updated.
    utils.remove_test_file(file_local_path)

    self.assertTrue(self._wait_until_remote_dir_changed(original))

    self._test_dir_content(files=[], dirs=[])
    self.assertGreater(self._get_cache_size_in_bytes(), cache_size)

    filename = 'file1.txt'
    file_local_path = os.path.join(self.local_base_dir, filename)
    utils.create_test_file(file_local_path, 1024)
    self._start()
    self._test_dir_content(files=[filename], dirs=[])
    cache_size = self._get_cache_size_in_bytes()

    # After a file is renamed, the manifest is updated.
    renamed_filename = 'file2.txt'
    os.rename(file_local_path,
              os.path.join(self.local_base_dir, renamed_filename))

    self.assertTrue(self._wait_until_remote_dir_changed(original))

    self._test_dir_content(files=[renamed_filename], dirs=[])
    self.assertGreater(self._get_cache_size_in_bytes(), cache_size)

  def test_add_directory(self):
    """A new directory is visible on remote instance."""
    self._start()
    self._test_dir_content(files=[], dirs=[])
    cache_size = self._get_cache_size_in_bytes()
    original = utils.get_ssh_command_output(self.ls_cmd)

    # Create a directory, cache becomes larger as a new manifest arrived.
    directory = 'dir1\\'
    dir_local_path = os.path.join(self.local_base_dir, directory)
    utils.create_test_directory(dir_local_path)

    self.assertTrue(self._wait_until_remote_dir_changed(original))
    self._test_dir_content(files=[], dirs=[directory])
    self.assertGreater(self._get_cache_size_in_bytes(), cache_size)

  def test_remove_directory(self):
    """A directory removal is visible on remote instance."""
    directory = 'dir1\\'
    dir_local_path = os.path.join(self.local_base_dir, directory)

    utils.create_test_directory(dir_local_path)
    self._start()
    self._test_dir_content(files=[], dirs=[directory])
    cache_size = self._get_cache_size_in_bytes()
    original = utils.get_ssh_command_output(self.ls_cmd)

    # After removing a file, the manifest is updated.
    utils.remove_test_directory(dir_local_path)

    self.assertTrue(self._wait_until_remote_dir_changed(original))

    self._test_dir_content(files=[], dirs=[])
    self.assertGreater(self._get_cache_size_in_bytes(), cache_size)

  def test_rename_directory(self):
    """A renamed directory is visible on remote instance."""
    directory = 'dir1\\'
    dir_local_path = os.path.join(self.local_base_dir, directory)

    utils.create_test_directory(dir_local_path)
    self._start()
    self._test_dir_content(files=[], dirs=[directory])
    cache_size = self._get_cache_size_in_bytes()
    original = utils.get_ssh_command_output(self.ls_cmd)

    # After removing a file, the manifest us updated.
    renamed_directory = 'dir2\\'
    os.rename(dir_local_path,
              os.path.join(self.local_base_dir, renamed_directory))

    self.assertTrue(self._wait_until_remote_dir_changed(original))

    self._test_dir_content(files=[], dirs=[renamed_directory])
    self.assertGreater(self._get_cache_size_in_bytes(), cache_size)

  def test_detect_executables(self):
    """Executable bits are propagated to remote instance."""
    # Add an .exe, an ELF file and a .sh file to the streamed directory.
    cdc_stream_dir = os.path.dirname(utils.CDC_STREAM_PATH)
    exe_filename = os.path.basename(utils.CDC_STREAM_PATH)
    elf_filename = 'cdc_fuse_fs'
    sh_filename = 'script.sh'

    shutil.copyfile(
        os.path.join(cdc_stream_dir, exe_filename),
        os.path.join(self.local_base_dir, exe_filename))
    shutil.copyfile(
        os.path.join(cdc_stream_dir, elf_filename),
        os.path.join(self.local_base_dir, elf_filename))
    with open(os.path.join(self.local_base_dir, sh_filename), 'w') as f:
      f.write('#!/path/to/bash\n\nls -al')

    files = [exe_filename, elf_filename, sh_filename]
    self._start()
    self._test_dir_content(files=files, dirs=[], is_exe=True)
    self._assert_cache()

  def test_resend_corrupted_chunks(self):
    """Corrupted chunks are recovered."""
    filename = 'file1.txt'
    remote_file_path = posixpath.join(self.remote_base_dir, filename)
    utils.create_test_file(os.path.join(self.local_base_dir, filename), 1024)
    self._start()

    manifest_chunk = utils.get_ssh_command_output('find %s -type f' %
                                                  self.cache_dir).rstrip('\r\n')

    # Read the file without caching.
    utils.get_ssh_command_output('dd if=%s bs=1K of=/dev/null iflag=direct' %
                                 remote_file_path)

    # Find any data chunk.
    data_chunks = utils.get_ssh_command_output('find %s -type f' %
                                               self.cache_dir)
    chunk_path = manifest_chunk
    for chunk in data_chunks.splitlines():
      if manifest_chunk not in chunk:
        chunk_path = chunk.rstrip('\r\n')
        break
    chunk_data = utils.get_ssh_command_output('cat %s' % chunk_path)

    # Modify the chosen data chunk.
    utils.get_ssh_command_output('dd if=/dev/zero of=%s bs=1 count=3' %
                                 chunk_path)
    self.assertNotEqual(chunk_data,
                        utils.get_ssh_command_output('cat %s' % chunk_path))

    # Read the file again, the chunk should be recovered.
    self._test_dir_content(files=[filename], dirs=[])
    self.assertEqual(chunk_data,
                     utils.get_ssh_command_output('cat %s' % chunk_path),
                     'The corrupted chunk was not recreated')

  def test_unicode(self):
    """Stream a directory with non-ASCII Unicode paths."""
    streamed_dir = '⛽⛽⛽'
    filename = '⛽⛽⛽⛽⛽⛽⛽⛽.dat'
    nonascii_local_data_path = os.path.join(self.local_base_dir, streamed_dir,
                                            filename)
    nonascii_remote_data_path = posixpath.join(self.remote_base_dir, filename)
    utils.create_test_file(nonascii_local_data_path, 1024)
    self._start(os.path.join(self.local_base_dir, streamed_dir))
    self._assert_cache()
    self.assertTrue(
        utils.sha1_matches(nonascii_local_data_path, nonascii_remote_data_path))

  def test_recovery(self):
    """Remount succeeds also if FUSE was killed at the previous execution."""
    files = [
        'dir1\\file1_1.txt', 'dir1\\file1_2.txt', 'dir2\\file2_1.txt',
        'file0.txt'
    ]
    dirs = ['dir1\\emptydir2\\', 'emptydir1\\', 'dir1\\', 'dir2\\']
    self._create_test_data(files, dirs)
    self._start()
    self._test_dir_content(files=files, dirs=dirs)
    self._assert_cache()
    self._assert_cdc_fuse_mounted()
    utils.get_ssh_command_output('killall cdc_fuse_fs')
    self._test_dir_content(files=[], dirs=[])
    self._start()
    self._test_dir_content(files=files, dirs=dirs)


if __name__ == '__main__':
  test_base.test_base.main()
