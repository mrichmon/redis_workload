#include <redis_workload/query_runner.h>
#include <redis_workload/util.h>

#include <boost/chrono.hpp>
#include <fstream>
#include <iostream>
#include <string>

using redis_store::findPercentile;
using redis_store::multiget_result_map_t;
using redis_store::RedisDataStore;
using redis_store::vector_keys_t;

namespace query_runner {

inline vector_keys_t parseLine(std::string& line) {
    vector_keys_t keyList;

    std::stringstream ss(line);
    while (ss.good()) {
        std::string k;
        getline(ss, k, ',');
        keyList.push_back(k);
    }
    return keyList;
}

inline boost::chrono::high_resolution_clock::time_point createTimer() {
    return boost::chrono::high_resolution_clock::now();
}

inline long long readTimerMicroseconds(boost::chrono::high_resolution_clock::time_point t) {
    boost::chrono::microseconds duration = boost::chrono::duration_cast<boost::chrono::microseconds>(boost::chrono::high_resolution_clock::now() - t);
    return duration.count();
}

QueryListCollector::QueryListCollector(unsigned int bucketCount, OperationMode mode) : bucketCount_(bucketCount), mode_(mode) {
    initializeBuckets();
}

void QueryListCollector::initializeBuckets() {
    queryBuckets_ = std::unordered_map<unsigned int, std::vector<vector_keys_t>>();
    queryBuckets_.reserve(bucketCount_);
    for (unsigned int i = 0; i < bucketCount_; i++) {
        queryBuckets_[i] = std::vector<vector_keys_t>();
    }
}

void QueryListCollector::parseCsvIntoBuckets(const std::string& filename) {
    std::ifstream csvFile(filename);
    if (!csvFile.is_open()) {
        throw std::runtime_error("Could not open file: " + filename);
    }

    std::string line;
    unsigned long long lineCounter = 0;
    while (std::getline(csvFile, line)) {
        if (line.empty()) {
            continue;
        }

        vector_keys_t keyList = parseLine(line);
        unsigned int bucket = lineCounter % bucketCount_;
        switch (mode_) {
            case OperationMode::divide:
                // Divide the queries across the buckets
                queryBuckets_[bucket].push_back(keyList);
                break;
            case OperationMode::replicate:
                // Push the query into all buckets
                for (auto& i : queryBuckets_) {
                    i.second.push_back(keyList);
                }
                break;
        }
        lineCounter++;
    }

    std::cout << "After parse, bucket sizes: " << std::endl;
    for (unsigned int i = 0; i < bucketCount_; i++) {
        // std::cout << "bucket: " << std::to_string(i) << "  -- " <<
        // std::to_string(queryBuckets_[i].size()) << "  " << queryBuckets_[i][0][0]
        // << std::endl;
        std::cout << "    bucket: " << std::to_string(i) << "  -- " << std::to_string(queryBuckets_[i].size()) << std::endl;
    }
}

std::vector<vector_keys_t> QueryListCollector::getBucket(unsigned int bucketId) {
    return queryBuckets_[bucketId];
}

QueryRunner::QueryRunner(std::string& name, unsigned int id, const std::shared_ptr<RedisDataStore>& dataStore, std::vector<vector_keys_t> queryList) :
    id_(id),
    dataStore_(dataStore) {
    setName(name);
    setQueryList(queryList);
}

size_t QueryRunner::getMaxKeyLength() {
    size_t maxLength = 0;

    for (auto& q : queryList_) {
        size_t currLength = q.size();
        if (currLength > maxLength) {
            maxLength = currLength;
        }
    }
    return maxLength;
}

std::string QueryRunner::getName() {
    return (name_ + "." + std::to_string(id_));
}

void QueryRunner::setName(const std::string& n) {
    name_ = n;
}

size_t QueryRunner::getQueryCount() {
    return queryList_.size();
}

size_t QueryRunner::getQueryListKeysTotal() {
    size_t total = 0;

    for (auto& q : queryList_) {
        total += q.size();
    }

    return total;
}

void QueryRunner::setQueryList(std::vector<vector_keys_t>& queryList) {
    queryList_ = queryList;
    queryListKeysTotal_ = getQueryListKeysTotal();
    maxKeyLength_ = getMaxKeyLength();
}

bool QueryRunner::readyToRun() {
    if (queryList_.empty()) {
        return false;
    }
    return true;
}

void QueryRunner::run() {
    unsigned long querySuccessCount = 0;
    unsigned long totalFetchedObjectCount = 0;

    std::vector<long long> individualQueryTimesMicro;
    individualQueryTimesMicro.reserve(queryList_.size());

    std::cout << "  " << getName() << " starting runner " << std::to_string(id_) << std::endl;

    auto totalTimer = createTimer();

    for (auto& k : queryList_) {
        vector_keys_t sharedKeys = k;
        std::shared_ptr<multiget_result_map_t> results = std::make_shared<multiget_result_map_t>();
        // TODO add in collection of timing for each query
        auto timer = createTimer();
        dataStore_->fetchByFeatureKeys(sharedKeys, results, false);
        long long runtime = readTimerMicroseconds(timer);
        individualQueryTimesMicro.push_back(runtime);
        querySuccessCount++;
        totalFetchedObjectCount += results->size();
    }

    long totalRuntimeMicroseconds = readTimerMicroseconds(totalTimer);
    long totalRuntimeMilliseconds = totalRuntimeMicroseconds / 1000;

    makeReport(totalRuntimeMilliseconds, queryList_.size(), querySuccessCount, totalFetchedObjectCount, individualQueryTimesMicro);
    runComplete_ = true;
}

bool QueryRunner::runComplete() {
    return runComplete_;
}

boost::thread QueryRunner::spawn() {
    return {&QueryRunner::run, this};
}

void QueryRunner::makeReport(long runtime,
                             size_t queryCount,
                             unsigned long querySuccessCount,
                             unsigned long totalFetchedObjects,
                             [[maybe_unused]] std::vector<long long> individualQueryTimesMicro) {
    std::stringstream sstream;
    sstream << "Runner report: " << std::to_string(id_) << std::endl;
    sstream << "  Input query count: " << std::to_string(queryCount) << std::endl;
    sstream << "  Query successes count: " << std::to_string(querySuccessCount) << std::endl;
    sstream << "  Input qeoId count: " << std::to_string(queryListKeysTotal_) << std::endl;
    sstream << "  Max key length: " << std::to_string(maxKeyLength_) << std::endl;
    sstream << "  Fetched object count: " << std::to_string(totalFetchedObjects) << std::endl;
    sstream << "  Total runtime: " << std::to_string(runtime) << " milliseconds" << std::endl;

    std::vector percentiles {50, 90, 95, 99, 100};
    for (auto p : percentiles) {
        std::string percentilePrefix;
        if (p < 100) {
            percentilePrefix = " p";
        }
        else {
            percentilePrefix = "p";
        }
        sstream << "      " << getName() << " query times elapsed(microseconds)   " << percentilePrefix << std::to_string(p) << ": "
                << std::to_string(findPercentile(p, &individualQueryTimesMicro)) << std::endl;
    }

    report_ = sstream.str();
}

std::string QueryRunner::getReport() {
    return report_;
}

}  // namespace query_runner
