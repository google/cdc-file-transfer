# CDC Indexer

This directory contains a CDC indexer based on our implementation of
[FastCDC](https://www.usenix.org/system/files/conference/atc16/atc16-paper-xia.pdf).

Run the sample with Bazel:

```
bazel run -c opt //cdc_indexer -- --inputs '/path/to/files'
```

The CDC algorithm can be tweaked with a few compile-time constants for
experimentation. See the file `indexer.h` for preprocessor macros that can be
enabled, for example:

```
bazel build -c opt --copt=-DCDC_GEAR_BITS=32 //cdc_indexer
```

At the end of the operation, the indexer outputs a summary of the results such
as the following:

```
00:02   7.44 GB in 2 files processed at 3.1 GB/s, 50% deduplication
Operation succeeded.

Chunk size (min/avg/max): 128 KB / 256 KB / 1024 KB  |  Threads: 12
gear_table: 64 bit  |  threshold: 0x7fffc0001fff
           Duration:           00:03
        Total files:               2
       Total chunks:           39203
      Unique chunks:           20692
         Total data:         9.25 GB
        Unique data:         4.88 GB
         Throughput:       3.07 GB/s
    Avg. chunk size:          247 KB
      Deduplication:           47.2%

 160 KB #########                                      1419 ( 7%)
 192 KB ########                                       1268 ( 6%)
 224 KB ###################                            2996 (14%)
 256 KB ########################################       6353 (31%)
 288 KB ######################                         3466 (17%)
 320 KB ##########################                     4102 (20%)
 352 KB ######                                          946 ( 5%)
 384 KB                                                  75 ( 0%)
 416 KB                                                  27 ( 0%)
 448 KB                                                   7 ( 0%)
 480 KB                                                   5 ( 0%)
 512 KB                                                   1 ( 0%)
 544 KB                                                   4 ( 0%)
 576 KB                                                   2 ( 0%)
 608 KB                                                   3 ( 0%)
 640 KB                                                   3 ( 0%)
 672 KB                                                   3 ( 0%)
 704 KB                                                   2 ( 0%)
 736 KB                                                   0 ( 0%)
 768 KB                                                   0 ( 0%)
 800 KB                                                   1 ( 0%)
 832 KB                                                   0 ( 0%)
 864 KB                                                   0 ( 0%)
 896 KB                                                   0 ( 0%)
 928 KB                                                   0 ( 0%)
 960 KB                                                   0 ( 0%)
 992 KB                                                   0 ( 0%)
1024 KB                                                   9 ( 0%)
```

For testing multiple combinations and comparing the results, the indexer also
features a flag `--results_file="results.csv"` which appends the raw data to the
given file in CSV format. Combine this flag with `--description` to label each
experiment with additional columns.
