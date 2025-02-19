# iobench - filesystem read performance benchmark

This is a simple benchmark to measure filesystem read performance. It reports the performance in files/second and in bytes/second, as well as configurable parallelism.

Example (parallelism of 16):

    $ iobench read-tree -j16
    -- reading ["/home/cdb/iobench"] using 16 threads
    -- list: 839533 files/s  (2767 files in 0.003295879 s)
    -- read: 21425 MB/s   101436 files/s  (584.443475 MB in 0.027278376 s)

Compare this with a non-parallel process (concurrency=1):

    $ iobench read-tree -j1
    -- reading ["/home/cdb/iobench"] using 1 threads
    -- list: 367967 files/s  (2767 files in 0.007519689 s)
    -- read: 6879 MB/s   32569 files/s  (584.443475 MB in 0.084956955 s)

It can be helpful to identify bottlenecks caused by EDR (endpoint detection
and response), antivirus software, and other 3rd party software
that can degrade the performance of the operating system and hardware.
