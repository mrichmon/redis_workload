#pragma once

#include <redis_workload/datatypes.h>
#include <redis_workload/redis_store.h>

#include <cstddef>

using redis_store::fetch_by_string_map_t;
using redis_store::RedisDataStore;
using redis_store::vector_keys_t;

namespace query_runner {

enum class OperationMode
{
    divide,
    replicate
};

class QueryListCollector {
private:
    std::unordered_map<unsigned int, std::vector<vector_keys_t>> queryBuckets_;
    unsigned int bucketCount_;
    OperationMode mode_;

    void initializeBuckets();

public:
    QueryListCollector() = delete;

    QueryListCollector(unsigned int bucketCount, OperationMode mode);

    void parseCsvIntoBuckets(const std::string& filename);

    std::vector<vector_keys_t> getBucket(unsigned int bucketId);
};

class QueryRunner {
private:
    unsigned int id_;
    std::shared_ptr<RedisDataStore> dataStore_;
    std::vector<vector_keys_t> queryList_;

    size_t queryListKeysTotal_ = -1;
    size_t maxKeyLength_ = -1;

    std::string report_;

    bool runComplete_ = false;

    std::string name_;

public:
    QueryRunner() = delete;

    QueryRunner(std::string& name, unsigned int id, const std::shared_ptr<RedisDataStore>& dataStore, std::vector<vector_keys_t> queryList);

    size_t getMaxKeyLength();

    std::string getName();

    void setName(const std::string& n);

    size_t getQueryCount();

    size_t getQueryListKeysTotal();

    void setQueryList(std::vector<vector_keys_t>& queryList);

    bool readyToRun();

    void run();

    bool runComplete();

    boost::thread spawn();

    void makeReport(long runtime,
                    size_t queryCount,
                    unsigned long querySuccessCount,
                    unsigned long totalFetchedObjects,
                    std::vector<long long> individualQueryTimes);

    std::string getReport();
};

}  // namespace query_runner