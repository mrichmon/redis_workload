# redis_workload
Code to exercise redis for multithreading and multiprocessing.

The `redis_workload` executable replays a log of mget(key) requests.

The input datafile should be a CSV formated file. Each line in the CSV file is interpreted as a single mget call to Redis. Each comma separated
on the line is used as a separate redis key.

For example, a CSV data file containing:
```
test.datastore:v1:{144150080724670024}
test.datastore:v1:{162251909695013772},test.datastore:v1:{162251909695013766},test.datastore:v1:{171279780761569308}
test.datastore:v1:{175783380971950953}
```
Will issue 3 mget calls to redis:
* `mget(["test.datastore:v1:{144150080724670024}"])`
* `mget(["test.datastore:v1:{162251909695013772}","test.datastore:v1:{162251909695013766}","test.datastore:v1:{171279780761569308}"])`
* `mget(["test.datastore:v1:{175783380971950953}"])`

These calls will be distributed across the worker threads/processes based on the command line options provided.
