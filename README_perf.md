
```
$ cd build_dir
$ mkdir perf
$ cd perf
$ sudo bash
# source ../../setup_env.source 
# perf record -F max -o perf.out  -- ../bin/run_redis_workload -t 2 -f ../../data/redis_query_sample-10000.csv

```
