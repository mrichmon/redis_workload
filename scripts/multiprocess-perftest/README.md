# Multiprocess and Multithread testing of redis++

These scripts may be used to perform both multi-threaded and multi-processing tests.

* multiprocess tests -- run n processes in parallel, 1 thread per process
* multithread tests -- run 1 process, with n threads per process

## Multiprocess testing

* edit `do_parallel_test.sh` to specify list of process counts. 
* edit  `run_tests.sh` to specify per process thread count=1, input datafile, and `run_redis_workload` operating mode
* run `./do_parallel_test.sh` resulting test output is produced in `count*` folders where the folder number is the number of
  processes in the test.
* collect the run2 percentile values for each test using:

```
### Collect p50 timings for 4 process test run
$ find count4 -type f -print0 | xargs -0 -I % ./extract.sh % p50
```

## Multithread testing

* edit `do_parallel_test.sh` to specify process count list = 1 
* edit  `run_tests.sh` to specify per process thread count="1 2 4 8 16 32 64" (or whichever thread counts you need),
  input datafile, and `run_redis_workload` operating mode
* run `./do_parallel_test.sh` resulting test output is produced in `count-1/data-1/threads-*.txt` file where the file
  number is the number of threads in the test.
* collect the run2 percentile values for each test using:

```
### Collect p50 timings for 4 thread test run
$ find count1/data-1/threads-4.txt -type f -print0 | xargs -0 -I % ./extract.sh % p50
```

