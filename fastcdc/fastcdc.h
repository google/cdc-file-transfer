/*
 * Copyright 2022 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#ifndef FASTCDC_FASTCDC_H_
#define FASTCDC_FASTCDC_H_

#include <string.h>

#include <algorithm>
#include <cassert>
#include <cstdio>
#include <functional>
#include <iostream>
#include <vector>

namespace cdc_ft {
namespace fastcdc {

// Configures the chunk sizes that the ChunkerTmpl class produces. All sizes are
// given in bytes.
struct Config {
  Config(size_t min_size, size_t avg_size, size_t max_size)
      : min_size(min_size), avg_size(avg_size), max_size(max_size) {}
  // The minimum chunk size defines the lower bound for generated chunks. Note
  // that this size can still be undercut for the last chunk after processing
  // the input data.
  size_t min_size;
  // The average chunk size is the target size for chunks, not including the
  // effects of max_size regression. Before regression, sizes will show an
  // offset exponential distribution decaying after min_size with the desired
  // average size. Regression will "reflect-back" the exponential
  // distribution past max_size, which reduces the actual average size and
  // gives a very flat distribution when max_size is small.
  size_t avg_size;
  // The maximum size is the upper bound for generating chunks. This limit is
  // never exceeded. If a chunk boundary was not detected based on the content
  // when this limit is hit, a new boundary is forced.
  size_t max_size;
};

// Callback function for handling the cut-point of a new chunk.
using ChunkFoundHandler = std::function<void(const uint8_t* data, size_t len)>;

// Implements a very fast content-defined chunking algorithm.
//
// FastCDC [1] identifies chunk boundaries based on a simple yet efficient
// "gear" rolling hash, a "normalized chunking" algorithm using a stepped
// chunk probability with a pair spread-out bitmasks for the '!(hash&mask)'
// "hash criteria".
//
// This library implements a modified version based on rollsum-chunking [2]
// tests and analysis that showed simple "exponential chunking" gives better
// deduplication, and a 'hash<=threshold' "hash criteria" works better for
// the gear rollsum and can support arbitrary non-power-of-two sizes.
//
// For limiting block sizes it uses a modified version of "Regression
// Chunking"[3] with an arbitrary number of regressions using power-of-2
// target block lengths (not multiples of the target block length, which
// doesn't have to be a power-of-2). This means we can use a bitmask for the
// most significant bits for the regression hash criteria.
//
// The Config struct passed in during construction defines the minimum, average,
// and maximum allowed chunk sizes. Those are runtime parameters.
//
// The template allows additional compile-time configuration:
//
// - T : The type used for the hash. Should be an unsigned integer type,
// ideally uint32_t or uint64_t. The number of bits of this type determines
// the "sliding window" size of the gear hash. A smaller type is likely to be
// faster at the expense of reduced deduplication.
//
// - gear: an array of random numbers that serves as a look-up table to
// modify be added to the rolling hash in each round based on the input data.
// This library comes with two different tables, one of type uint32_t and one
// of uint64_t. Both showed good results in our experiments, yet the 64-bit
// version provided slightly better deduplication.
//
// [1] https://www.usenix.org/system/files/conference/atc16/atc16-paper-xia.pdf.
// [2] https://github.com/dbaarda/rollsum-chunking/blob/master/RESULTS.rst
// [3] https://www.usenix.org/system/files/conference/atc12/atc12-final293.pdf
//
// TODO: Remove template parameters.
template <typename T, const T gear[256]>
class ChunkerTmpl {
 public:
  // Constructor.
  ChunkerTmpl(const Config& cfg, ChunkFoundHandler handler)
      : cfg_(cfg), handler_(handler) {
    assert(cfg_.avg_size >= 1);
    assert(cfg_.min_size <= cfg_.avg_size);
    assert(cfg_.avg_size <= cfg_.max_size);

    // Calculate the threshold the hash must be <= to for a 1/(avg-min+1)
    // chance of a chunk boundary.
    kthreshold_ = (T)(-1) / (cfg_.avg_size - cfg_.min_size + 1);
    data_.reserve(cfg_.max_size << 1);
  }

  // Slices the given data block into chunks and calls the specified handler
  // function for each chunk cut-point. The remaining data is buffered and used
  // in the next call to Process() or Finalize().
  void Process(const uint8_t* data, size_t size) {
    bool eof = data == nullptr || size == 0;
    if (!eof) {
      // Append new data.
      assert(size > 0);
      data_.insert(data_.end(), data, data + size);
    }

    if (data_.empty()) {
      return;
    }

    assert(handler_ != nullptr);

    const uint8_t* data_ptr = &data_[0];
    size_t bytes_left = data_.size();
    while (bytes_left >= cfg_.max_size || (bytes_left > 0 && eof)) {
      const size_t chunk_size = FindChunkBoundary(data_ptr, bytes_left);

      handler_(data_ptr, chunk_size);

      assert(bytes_left >= chunk_size);
      bytes_left -= chunk_size;
      data_ptr += chunk_size;
    }

    // Copy remaining data to the beginning of the array. Using memmove/resize
    // is not slow when shrinking a vector.
    memmove(&data_[0], data_ptr, bytes_left);
    data_.resize(bytes_left);
  }

  // Slices all remaining buffered data into chunks, where the last chunk might
  // be smaller than the specified minimum chunk size.
  void Finalize() { Process(nullptr, 0); }

  // Returns the threshold for the hash <= threshold chunk boundary.
  T Threshold() { return kthreshold_; }

 private:
  size_t FindChunkBoundary(const uint8_t* data, size_t len) {
    if (len <= cfg_.min_size) {
      return len;
    }
    if (len > cfg_.max_size) {
      len = cfg_.max_size;
    }

    // Initialize the regression length to max_size and the regression mask
    // to an empty bitmask (match any hash).
    size_t rc_len = cfg_.max_size;
    T rc_mask = 0;

    // Init hash to all 1's to avoid zero-length chunks with min_size=0.
    T hash = (T)-1;
    // Skip the first min_size bytes, but "warm up" the rolling hash for enough
    // rounds to make sure the hash has gathered full "content history".
    size_t i = cfg_.min_size > khashbits_ ? cfg_.min_size - khashbits_ : 0;
    for (/*empty*/; i < cfg_.min_size; ++i) {
      hash = (hash << 1) + gear[data[i]];
    }
    for (/*empty*/; i < len; ++i) {
      if (!(hash & rc_mask)) {
        if (hash <= kthreshold_) {
          // This hash matches the target length hash criteria, return it.
          return i;
        } else {
          // This is a better regression point. Set it as the new rc_len and
          // update rc_mask to check as many MSBits as this hash would pass.
          rc_len = i;
          for (rc_mask = (T)-1; hash & rc_mask; rc_mask <<= 1)
            ;
        }
      }
      hash = (hash << 1) + gear[data[i]];
    }
    // Return best regression point we found or the end if it's better.
    return (hash & rc_mask) ? rc_len : i;
  }

  static constexpr size_t khashbits_ = sizeof(T) * 8;
  const Config cfg_;
  const ChunkFoundHandler handler_;
  T kthreshold_;
  std::vector<uint8_t> data_;
};

namespace internal {

static constexpr uint32_t gear32[256] = {
    0xcab06edf, 0xb2718138, 0x3c224673, 0x3b9cf4f3, 0x99309a2f, 0x4cae6426,
    0x5cd1268b, 0xfa8d5e6e, 0x3dce9096, 0x03f6d1ba, 0x10cbd5c6, 0x7a32df70,
    0x5caaf980, 0x1ee50161, 0xdb3e2adf, 0xdaa1b79b, 0x8a876bdb, 0x55214dcf,
    0x033ce45c, 0x93da2d58, 0x2c897e9b, 0x7ca38bce, 0x6ba9c6df, 0x644f3827,
    0x17919e09, 0x98991c4f, 0xb022e20c, 0xaeed89e5, 0xac46f0a2, 0x77e8ab7c,
    0x80cdb866, 0x1cf8a455, 0x342e8a7c, 0x82307545, 0x685c10bf, 0xf4b4db0d,
    0xd583f695, 0xef3be7f8, 0x6f443b74, 0xfb536307, 0xd1eebf07, 0x3fc4cbff,
    0x9c56a01f, 0x0c876401, 0x7582b5a4, 0xb67e02d9, 0xf31f1d4a, 0x308e0bfc,
    0xc2fbe865, 0x189ff266, 0xe9301f82, 0x0c99f8f2, 0xb536b229, 0xf176078b,
    0x7e638b7f, 0xb1b17b3b, 0xdc699078, 0xee113abe, 0xe05387c9, 0x834b5fb3,
    0x6577e854, 0x46310ed6, 0xe9095a8f, 0x0666ba24, 0x6f3e64d9, 0x60a137c6,
    0x00a3fe71, 0x252827d0, 0xc968a79d, 0x71adf1c7, 0xb90b26df, 0xc0b76174,
    0x53a4a968, 0x1d8cde87, 0xee076527, 0x78ada3ed, 0x2222a4cf, 0x0f20e8b1,
    0x52661029, 0x4ee67246, 0x22f83593, 0xc06b6d72, 0xe9780131, 0x46aa9013,
    0xb0192122, 0xa88b381f, 0x3b884ca7, 0x9e1188b8, 0x28e02253, 0xa19d3fc6,
    0xea459915, 0xb5b9a788, 0x96428060, 0x753524b8, 0x61c9c992, 0x6ba735d4,
    0x66ab303e, 0xbcbdd2c2, 0xe3df7ac9, 0x2f0cf65d, 0xcdf98e52, 0xb64160e8,
    0x6b8be972, 0x45602f72, 0xcbeb420e, 0xd9a2bd46, 0xb615d4a4, 0x1cfc7f69,
    0x603689d5, 0xc3bcd0d8, 0xc4d8da81, 0xa700392a, 0x27e3a0be, 0x3e7122fa,
    0x9f4ff2d6, 0x3ab159c1, 0xa3b1cc44, 0x54d2060c, 0x9f664a53, 0xb7933a53,
    0x17e0a83d, 0xab53f0f6, 0xfb54c682, 0xc2dce1fe, 0xb728b96c, 0x27a24073,
    0x35cd89cd, 0x1626c9a9, 0x9dcf73fd, 0x2a40ad38, 0x321c7bf2, 0x859f9ad2,
    0xd12d993f, 0xcb56ee3c, 0xf95e36dc, 0x8ada584b, 0x2868e9bc, 0xe2f137ee,
    0xa7ba3cae, 0xeb331d08, 0x2a2e1fc3, 0x13ed8950, 0x707abf0e, 0xf6c84db8,
    0xbe1b3e9f, 0x8a98a6ef, 0xa829daf1, 0x8f9fd9f8, 0x1d8002fb, 0xe07544a4,
    0xd69cb989, 0x030c29c2, 0x4f0e4227, 0x2b843c5a, 0x61d649fa, 0x24a23275,
    0x29ab7954, 0x1a977796, 0xafc840bb, 0x68ea74e9, 0x51e18221, 0x7e7aacb9,
    0xd83aac74, 0x16f3ffb4, 0xa1822460, 0x796e4267, 0xce57a57f, 0xdf15a7ee,
    0xf6098f14, 0x6bb45abd, 0x51933c35, 0x792d3f18, 0x4872d2de, 0xe66a579c,
    0x5750ffa9, 0x149d5472, 0x57d2e4ac, 0x9b2030bd, 0xa6befac0, 0x7eb0fa7d,
    0x5288b8de, 0xfd749b9c, 0x5389ae25, 0x90a31d56, 0x07acafbe, 0x9ffa7e2c,
    0x19a42631, 0xbc581a52, 0xc2517ad6, 0xe437de30, 0xd75eafd7, 0x8397f5ef,
    0x894d0064, 0xeae51be9, 0xa0973cf4, 0xd09dd0df, 0x654de33c, 0x99698bf2,
    0xb2be2b5c, 0x7df281a9, 0xdc5bdac7, 0xb8bc6817, 0xc2b8ac02, 0x6755088b,
    0x42fdf274, 0xd758e0a0, 0x0fe0775a, 0x3b089ae3, 0x1302b17c, 0xbbf11915,
    0x30f3ad8f, 0x8a38175b, 0x05ddabe9, 0x6647ac44, 0x49570ac5, 0x6ad85643,
    0x6062344e, 0xf9515337, 0x3ff407ae, 0x8ff0dc25, 0x2e047222, 0x3dab32fe,
    0x70899f3f, 0x594402c4, 0x7bdb81fd, 0xb93110d4, 0xe15de0ff, 0x7265b35e,
    0x0ffbffbd, 0x234ab621, 0x1ea74ed8, 0x82caa7b4, 0x3fe7fa4f, 0xa9ab690b,
    0x82e8993e, 0xa2d35adf, 0xf87827c5, 0x00172b3e, 0xa284d80b, 0x8d536c67,
    0xd63cb52d, 0xc6db6dbb, 0x523e1ba5, 0x557c6536, 0x4168f166, 0xd7acfd41,
    0xde089e30, 0xbf167903, 0x551a3200, 0xa330b700, 0x917e3ebf, 0x5a794e62,
    0xe44d3356, 0x9fcd9417, 0x30eb9b8b, 0x6e33ef51};

static constexpr uint64_t gear64[256] = {
    0x651748f5a15f8222, 0xd6eda276c877d8ea, 0x66896ef9591b326b,
    0xcd97506b21370a12, 0x8c9c5c9acbeb2a05, 0xb8b9553ee17665ef,
    0x1784a989315b1de6, 0x947666c9c50df4bd, 0xb3f660ea7ff2d6a4,
    0xbcd6adb8d6d70eb5, 0xb0909464f9c63538, 0xe50e3e46a8e1b285,
    0x21ed7b80c0163ce0, 0xf209acd115f7b43b, 0xb8c9cb07eaf16a58,
    0xb60478aa97ba854c, 0x8fb213a0b5654c3d, 0x42e8e7bd9fb03710,
    0x737e3de60a90b54f, 0x9172885f5aa79c8b, 0x787faae7be109c36,
    0x86ad156f5274cb9f, 0x6ac0a8daa59ee1ab, 0x5e55bc229d5c618e,
    0xa54fb69a5f181d41, 0xc433d4cf44d8e974, 0xd9efe85b722e48a3,
    0x7a5e64f9ea3d9759, 0xba3771e13186015d, 0x5d468c5fad6ef629,
    0x96b1af02152ebfde, 0x63706f4aa70e0111, 0xe7a9169252de4749,
    0xf548d62570bc8329, 0xee639a9117e8c946, 0xd31b0f46f3ff6847,
    0xfed7938495624fc5, 0x1ef2271c5a28122e, 0x7fd8e0e95eac73ef,
    0x920558e0ee131d4c, 0xce2e67cb1034bcd1, 0x6f4b338d34b004ae,
    0x92f5e7271cf95c9a, 0x12e1305a9c558342, 0x1e30d88013ad77ae,
    0x09acc1a57bbb604e, 0xaf187082c6f56192, 0xd2e5d987f04ac6f0,
    0x3b22fca40423da70, 0x7dfba8ce699a9a87, 0xe8b15f90ea96bd2a,
    0xcda1a1089cc2cbe7, 0x72f70448459de898, 0x1ab992dbb61cd46e,
    0x912ad04becbb29da, 0x98c6bb3aa3ce09ed, 0x6373bd2e7a041f3a,
    0x1f98f28bd178c53a, 0xe6adbc82ba5d9f96, 0x7456da7d805cbe01,
    0xd673662dcc135eeb, 0xb299e26eaadcb311, 0x2c2582172f8114af,
    0xeded114d7f623da6, 0xb3462a0e623276e4, 0x3af752be3d34bfaa,
    0x1311ccc0a1855a89, 0x0812bbcecc92b2e4, 0x9974b5747289f2f5,
    0x3a030eff770f2026, 0x52462b2aa42a847a, 0x2beaa107d15a012b,
    0x0c0035e0fe073398, 0x4f2f9de2ac206766, 0x5dd51a617c291deb,
    0x1ac66905652cc03b, 0x11067b0947fc07a1, 0x02b5fcd96ad06d52,
    0x74244ec1aa2821fd, 0xf6089e32060e9439, 0xd8f076a33bcbf1a7,
    0x5162743c755d8d5e, 0x8d34fc683e4e3d06, 0x46efe9b21a0252a3,
    0x4631e8d0109c6145, 0xfdf7a14bc0223957, 0x750934b3d0b8bb1e,
    0x2ecd1b3efed5ddb9, 0x2bcbd89a83ccfbce, 0x3507c79e58dd5886,
    0x5476a67ecd4a772f, 0xaa0be3856dd76405, 0x22289a358a4dd421,
    0xf570433f14503ad1, 0x8a9f440251a722c3, 0x77dd711752b4398c,
    0xbbd9edf9c6160a31, 0xb94b59220b23f079, 0xfdca3d75d2f33ccf,
    0xb29452c460c9e977, 0xe89afe2dd4bf3b02, 0x47ec6f32c91bfee4,
    0x1aab5ec3445706b8, 0x588bf4fa55334006, 0xe2290ca1e29acd96,
    0x3c49e189f831c37c, 0x6448c973b5177498, 0x556a6e09ba158de7,
    0x90b25013a8d9a067, 0xa4f2f7a50c58e1c4, 0x5e765e871008700e,
    0x242f5ae7738327af, 0xc1e6a2819cc5a219, 0xcb48d801fd6a5449,
    0xa208de2301931383, 0xde3c143fe44e39b0, 0x6bb74b09c73e4133,
    0xb5b1ed1b63d54c11, 0x587567d454ce7716, 0xf47ddbc987cb0392,
    0x87b19254448f03f1, 0x985fd00ec372fafa, 0x64b92ba521aa46e4,
    0xce63f4013d587b0f, 0xa691ae698726030e, 0xeaefbf690264e9aa,
    0x68edd400523eb152, 0x35d9353aa1957c60, 0x2e2c2d7a9cb68385,
    0xfc7549edaf43bf9e, 0x48b2adb23026e2c7, 0x3777cb79a024bcf9,
    0x644128f7c184102d, 0x70189d3ca4390de9, 0x085fea7986d4cd34,
    0x6dbe7626c8457464, 0x9fa41cfa9c4265eb, 0xdaa163a641946463,
    0x02f5c4bd9efa2074, 0x783201871822c3c9, 0xb0dfec499202bce0,
    0x1f1c9c12d84dccab, 0x1596f8819f2ed68e, 0xb0352c3e9fc84468,
    0x24a6673db9122956, 0x84f5b9e60b274739, 0x7216b28a0b54ac46,
    0xc7789de20e9cdca4, 0x903db5d289dd6563, 0xce66a947f7033516,
    0x3677dbc62307b2ca, 0x8d8e9d5530eb46ac, 0x79c4bad281bd93e2,
    0x287d942042068c36, 0xde4b98e5464b6ad5, 0x612534b97d1d21bf,
    0xdf98659772d822a1, 0x93053df791aa6264, 0x2254a8a2d54528ba,
    0x2301164aeb69c43d, 0xf56863474ac2417f, 0x6136b73e1b75de42,
    0xc7c3bd487e06b532, 0x7232fbed1eb9be85, 0x36d60f0bd7909e43,
    0xe08cbf774a4ce1f2, 0xf75fbc0d97cb8384, 0xa5097e5af367637b,
    0x7bce2dcfa856dbb2, 0xfbfb729dd808c894, 0x3dc8eba10ad7112e,
    0xf2d1854eedce4928, 0xb705f5c1aebd2104, 0x78fa4d004417d956,
    0x9e5162660729f858, 0xda0bcd5eb9f91f0e, 0x748d1be11e06b362,
    0xf4c2be9a04547734, 0x6f2bcd7c88abdf9a, 0x50865dafdfd8a404,
    0x9d820665691728f0, 0x59fe7a56aa07118e, 0x4df1d768c23660ec,
    0xab6310b8edfb8c5e, 0x029b47623fc9ffe4, 0x50c2cca231374860,
    0x0561505a8dbbdc69, 0x8d07fe136de385f3, 0xc7fb6bb1731b1c1c,
    0x2496d1256f1fac7a, 0x79508cee90d84273, 0x09f51a2108676501,
    0x2ef72d3dc6a50061, 0xe4ad98f5792dd6d6, 0x69fa05e609ae7d33,
    0xf7f30a8b9ae54285, 0x04a2cb6a0744764b, 0xc4b0762f39679435,
    0x60401bc93ef6047b, 0x76f6aa76e23dbe0c, 0x8a209197811e39da,
    0x4489a9683fa03888, 0x2604ad5741a6f8d8, 0x7faa9e0c64a94532,
    0x0dbfee8cdae8f54e, 0x0a7c5885f0b76d4a, 0x55dfb1ac12e83645,
    0xedc967651c4938cc, 0x4e006ab71a48b85e, 0x193f621602de413c,
    0xb56458b71d56944f, 0xf2b639509a2fa5da, 0xb4a76f284c365450,
    0x4d3b65d2d2ae22f7, 0xbcc5f8303efca485, 0x8a044f312671aaea,
    0x688d69e89af0f57a, 0x229957dc1facede8, 0x2ed75c321073da13,
    0xf199e7ece5fcefef, 0x50c85b5c837a6c64, 0x71703c6e676bf698,
    0xc1b4eb52b1e5a518, 0x0f46a5e6c9cb68ca, 0xebb933688d69d7f7,
    0x5ab7404b8d1e3ef4, 0x261acc20c5a64a90, 0xb88788798adc718a,
    0x3e44e9b6bad5bc15, 0xf6bb456f086346bc, 0xd66e17e5734cbde1,
    0x392036dae96e389d, 0x4a62ceac9d4202de, 0x9d55f412f32e5f6e,
    0x0e1d841509d9ee9d, 0xc3130bdc638ed9e2, 0x0cd0e82af24964d9,
    0x3ec4c59463ba9b50, 0x055bc4d8685ab1bc, 0xb9e343c96a3a4253,
    0x8eba190d8688f7f9, 0xd31df36c792c629b, 0xddf82f659b127104,
    0x6f12dc8ba930fbb7, 0xa0aee6bb7e81a7f0, 0x8c6ba78747ae8777,
    0x86f00167eda1f9bc, 0x3a6f8b8f8a3790c9, 0x7845bb4a1c3bfbbb,
    0xc875ab077f66cf23, 0xa68b83d8d69b97ee, 0xb967199139f9a0a6,
    0x8a3a1a4d3de036b7, 0xdf3c5c0c017232a4, 0x8e60e63156990620,
    0xd31b4b03145f02fa};

};  // namespace internal

// Chunker template with a 32-bit gear table.
template <const uint32_t gear[256] = internal::gear32>
using Chunker32 = ChunkerTmpl<uint32_t, gear>;

// Chunker template with a 64-bit gear table.
template <const uint64_t gear[256] = internal::gear64>
using Chunker64 = ChunkerTmpl<uint64_t, gear>;

// Default chunker class using params that are known to work well.
using Chunker = Chunker64<>;

}  // namespace fastcdc
}  // namespace cdc_ft

#endif  // FASTCDC_FASTCDC_H_
