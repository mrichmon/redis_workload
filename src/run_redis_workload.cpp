#include <getopt.h>
#include <redis_workload/query_runner.h>
#include <redis_workload/redis_store.h>
#include <redis_workload/run_redis_workload.h>
#include <redis_workload/util.h>
#include <sys/stat.h>

#include <boost/thread.hpp>
#include <iostream>
#include <redis_workload/remove_duplicates.hpp>
#include <string>
#include <vector>

using redis_store::bool_to_string;
using redis_store::findAverage;
using redis_store::findPercentile;
using redis_store::RedisDataStore;
using redis_store::removeDuplicates;

using query_runner::OperationMode;
using query_runner::QueryListCollector;
using query_runner::QueryRunner;

inline bool fileExists(const std::string& filename) {
    struct stat buffer;
    return (stat(filename.c_str(), &buffer) == 0);
}

void doTestRun(std::string& testName, unsigned int threadCount, QueryListCollector& collector, std::shared_ptr<RedisDataStore>& dataStore) {
    std::vector<QueryRunner*> runners;
    runners.reserve(threadCount);
    for (unsigned int i = 0; i < threadCount; i++) {
        runners[i] = new QueryRunner(testName, i, dataStore, collector.getBucket(i));
    }

    std::cout << "All runners initialized" << std::endl;

    std::vector<boost::thread> threads;
    threads.reserve(threadCount);
    for (unsigned int i = 0; i < threadCount; i++) {
        if (runners[i]->readyToRun()) {
            std::cout << "  runner: " << std::to_string(i) << std::endl;
            threads.push_back(runners[i]->spawn());
        }
    }

    std::cout << std::endl;
    std::cout << "All runners spawned" << std::endl;

    for (auto& t : threads) {
        t.join();
    }

    std::cout << std::endl;
    std::cout << "All runners complete" << std::endl;

    std::cout << std::endl;
    for (unsigned int i = 0; i < threadCount; i++) {
        if (runners[i]->runComplete()) {
            std::cout << runners[i]->getReport();
        }
        else {
            std::cerr << "error: runner report " << std::to_string(i) << " not ready" << std::endl;
        }
        std::cout << std::endl;
    }
}

void usage(const std::string& appName) {
    std::cout << appName << std::endl;
    std::cout << "Run redis workload test using data.csv file as input mget query operations." << std::endl
              << "Default operation evenly divides the input data into separate buckets, one " << std::endl
              << "bucket per thread." << std::endl;

    std::cout << std::endl;
    std::cout << "usage: " << appName << " -t <n> -f <data.csv>" << std::endl;
    std::cout << "where:" << std::endl;
    std::cout << "    -t <n>           number of threads to use" << std::endl;
    std::cout << "    -f <filename>    data file to use (csv format)" << std::endl;
    std::cout << "    -r               replicate the data across threads (instead of dividing the data)" << std::endl;
}

int main(int argc, char* argv[]) {
    const std::string appName(basename(*argv));

    int threadCount = 0;
    std::string datafileName;
    OperationMode mode = OperationMode::divide;

    std::string argumentTemplate = "t:f:hr";

    // TODO: Add input datafile argument
    int ch;
    while ((ch = getopt(argc, argv, argumentTemplate.c_str())) != -1) {
        switch (ch) {
            case 't':
                try {
                    threadCount = std::stoi(optarg);
                }
                catch (std::invalid_argument& e) {
                    std::cerr << "error: thread count argument invalid" << std::endl;
                    exit(1);
                };
                break;
            case 'f':
                datafileName = std::string(optarg);
                break;
            case 'r':
                mode = OperationMode::replicate;
                break;
            case 'h':
                usage(appName);
                exit(0);
            case '?':
            default: {
                usage(appName);
                exit(0);
            }
        }
    }

    if (threadCount < 1) {
        std::cerr << "error: invalid thread count requested" << std::endl;
        exit(1);
    }

    if (!fileExists(datafileName)) {
        std::cerr << "error: data file does not exist at: " << datafileName << std::endl;
        exit(1);
    }

    std::cout << std::endl;
    std::cout << "Running test with:" << std::endl;
    std::cout << "    datafile: " << datafileName << std::endl;
    std::cout << "    threadCount: " << std::to_string(threadCount) << std::endl;
    switch (mode) {
        case OperationMode::divide:
            std::cout << "    mode: divide" << std::endl;
            break;
        case OperationMode::replicate:
            std::cout << "    mode: replicate" << std::endl;
            break;
    }
    std::cout << std::endl;

    QueryListCollector collector(threadCount, mode);
    collector.parseCsvIntoBuckets(datafileName);

    std::cout << std::endl;

    std::shared_ptr<RedisDataStore> redisStore = RedisDataStore::factory();

    // Testing Cluster Slots get
    // redisStore->getClusterSlots();
    // exit(0);

    std::string testName = "run1";
    doTestRun(testName, threadCount, collector, redisStore);

    std::cout << std::endl;
    std::cout << std::endl;

    testName = "run2";
    doTestRun(testName, threadCount, collector, redisStore);

    std::cout << "Tests complete" << std::endl;
    return 0;
}
