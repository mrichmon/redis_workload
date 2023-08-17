#include <json/json.h>
#include <redis_workload/datatypes.h>
#include <redis_workload/redis_store.h>
#include <redis_workload/redis_store_params.h>
#include <redis_workload/util.h>

#include <redis_workload/remove_duplicates.hpp>

#ifdef USE_BOOST_FUTURE
    #include <boost/stacktrace.hpp>
#endif  // USE_BOOST_FUTURE
#include <chrono>
#include <iostream>
#include <string>
#include <thread>
#include <utility>
#include <vector>

namespace redis_store {

std::shared_ptr<RedisDataStore> RedisDataStore::redisDataStore_ = nullptr;

std::shared_ptr<RedisDataStore> RedisDataStore::factory() {
    if (redisDataStore_ != nullptr) {
        std::cout << "warn: ignoring factory arguments because the "
                     "RedisDataStore already exists"
                  << std::endl;
        return redisDataStore_;
    }
    /** Hard-code values to disconnect from config parsing */
    std::pair<std::string, std::string> credentials = getRedisCredentialsFromEnv();
    std::pair<std::string, int> host = getRedisHostFromEnv();

    RedisStoreParams params;
    params.redisHost = host.first;
    params.redisPort = host.second;
    params.redisUser = credentials.first;
    params.redisPassword = credentials.second;

    params.maxMultiKeyBatchSize = 40;
    params.redisKeyPrefix = "test.datastore:v1:{";
    params.redisKeySuffix = "}";
    params.preferReadReplicas = true;
    params.poolSize = 1000;
    params.poolWaitTimeout = 0;
    params.poolConnectionLifetime = 0;
    params.poolConnectionMaxIdle = 0;

    redisDataStore_ = std::make_shared<RedisDataStore>(params);

    std::cout << "RedisDataStore connected to Redis server version: " << redisDataStore_->getRedisServerVersion() << std::endl;
    std::cout << "RedisDataStore dataset version: " << redisDataStore_->getDatasetVersion() << std::endl;

    return redisDataStore_;
}

static const int REDIS_CONNECTION_RETRY_DELAY_MS = 10;
static const int REDIS_CONNECTION_RETRY_COUNT = 3;

RedisDataStore::RedisDataStore(const RedisStoreParams& params) :
    params_(params),
    maxMultiKeyBatchCount_(params.maxMultiKeyBatchSize),
    redisKeyPrefix_(params.redisKeyPrefix),
    redisKeySuffix_(params.redisKeySuffix),
    datasetMetaDataKey(params.redisKeyPrefix + "dataset_metadata" + params.redisKeySuffix) {
    sw::redis::ConnectionOptions connectionOptions;
    connectionOptions.host = params_.redisHost;
    connectionOptions.port = params_.redisPort;

    connectionOptions.user = params_.redisUser;
    connectionOptions.password = params_.redisPassword;

    connectionOptions.readonly = params_.preferReadReplicas;

    sw::redis::ConnectionPoolOptions poolOptions;
    poolOptions.size = params_.poolSize;
    poolOptions.wait_timeout = std::chrono::milliseconds(params_.poolWaitTimeout);
    poolOptions.connection_lifetime = std::chrono::minutes(params_.poolConnectionLifetime);
    poolOptions.connection_idle_time = std::chrono::milliseconds(params_.poolConnectionMaxIdle);

    std::stringstream connectionValues;
    connectionValues << "host:" << params_.redisHost << ", ";
    connectionValues << "port:" << std::to_string(params_.redisPort) << ", ";
    connectionValues << "user:" << params_.redisUser << ", ";
    connectionValues << "preferReadReplicas:" << (params_.preferReadReplicas ? "true" : "false") << ", ";
    connectionValues << "poolSize:" << std::to_string(params_.poolSize) << ", ";
    connectionValues << "waitTimeout:" << std::to_string(params_.poolWaitTimeout) << ", ";
    connectionValues << "connectionLifetime:" << std::to_string(params_.poolConnectionLifetime) << ", ";
    connectionValues << "maxIdleTime:" << std::to_string(params_.poolConnectionMaxIdle) << ", ";
    connectionValues << "maxMultiKeyBatchCount:" << std::to_string(maxMultiKeyBatchCount_);

    std::string message = "Creating RedisDataStore with redis connection options:" + connectionValues.str();
    std::cout << message << std::endl;

    // Note that the RedisCluster class is movable but not copyable
    try {
        auto redisCluster = sw::redis::AsyncRedisCluster(connectionOptions, poolOptions);
        redisConnection_ = std::make_unique<sw::redis::AsyncRedisCluster>(std::move(redisCluster));
    }
    catch (sw::redis::Error& e) {
        std::cerr << "error: Caught exception connecting to redis cluster: " << e.what() << std::endl;
    }
    catch (...) {
        std::cout << "Caught exception during AsyncRedisCluster create" << std::endl;
#ifdef USE_BOOST_FUTURE
        std::cout << boost::stacktrace::stacktrace();
#endif  // USE_BOOST_FUTURE
    }

    bool operationSuccess = false;
    int operationRetry = REDIS_CONNECTION_RETRY_COUNT;
    int retryDelayMs = REDIS_CONNECTION_RETRY_DELAY_MS;

    /*
     * TODO: How to handle nominal exceptions caught during start-up so log
     *       messages do not result in unintended concern?
     *       Ensure AsyncRedisCluster object is fully initialized and able to
     *       round-trip operations to the redis cluster.
     */
    while ((!operationSuccess) && (operationRetry > 0)) {
        operationRetry -= 1;
        std::string clusterInfo;
        try {
            clusterInfo = issueSynchronousRedisClusterStringCommand({"CLUSTER", "INFO"});
        }
        catch (sw::redis::Error& e) {
            message = std::string("caught redis connection exception: ").append(e.what());
            std::cout << message << std::endl;
        }
        catch (...) {
            std::cout << "Caught exception during issueSynchronousRedisClusterStringCommand" << std::endl;
#ifdef USE_BOOST_FUTURE
            std::cout << boost::stacktrace::stacktrace();
#endif  // USE_BOOST_FUTURE
        }

        if (!clusterInfo.empty()) {
            operationSuccess = true;
            std::cout << "Redis Cluster Info:" << std::endl << clusterInfo << std::endl;
        }
        else {
            message = ("Redis Cluster connection not ready, sleeping for " + std::to_string(retryDelayMs) + "ms");
            std::cout << message << std::endl;
            std::this_thread::sleep_for(std::chrono::milliseconds(retryDelayMs));
        }
    }
}

std::string RedisDataStore::getDatasetVersionFromDatasetMeta(const std::string& meta_string) {
    std::string data_version;

    JSONCPP_STRING err;
    Json::Value root;

    Json::CharReaderBuilder builder;
    const std::unique_ptr<Json::CharReader> reader(builder.newCharReader());
    if (!reader->parse(meta_string.c_str(), meta_string.c_str() + meta_string.length(), &root, &err)) {
        std::string message = "error: Unable to parse dataset metadata string as JSON";
        std::cout << message << std::endl;

        data_version = "unknown";
        return data_version;
    }

    const std::string data_bundle_name = root["data_bundle"][0]["name"].asString();
    const std::string data_bundle_version = root["data_bundle"][0]["version"].asString();
    const std::string data_source_key = data_bundle_name + " " + data_bundle_version;

    data_version = root["data_sources"][data_source_key]["basemap"]["id"].asString();

    return data_version;
}

std::string RedisDataStore::getRedisServerVersionFromInfo(const std::string& infoString) {
    const std::string REDIS_VERSION = "redis_version:";

    std::size_t start_index = infoString.find(REDIS_VERSION);
    if (start_index == std::string::npos) {
        return "UNKNOWN VERSION";
    }

    start_index = start_index + REDIS_VERSION.size();
    std::size_t end_index = infoString.find("\r\n", start_index) - start_index;
    std::string serverVersion = infoString.substr(start_index, end_index);

    return serverVersion;
}

void RedisDataStore::zipResultObjects(const std::shared_ptr<vector_keys_t>& keys,
                                      const std::shared_ptr<vector_results_t>& dataObjects,
                                      const std::shared_ptr<multiget_result_map_t>& zippedData) {
    size_t keysCount = keys->size();
    size_t redisResultsCount = dataObjects->size();

    if (redisResultsCount != keysCount) {
        std::string message = ("warn: Redis Data Store zip requested with dataObjects count: " + std::to_string(redisResultsCount)
                               + " != keys count: " + std::to_string(keysCount));
        std::cout << message << std::endl;
    }

    for (size_t i = 0; i < redisResultsCount; i++) {
        std::string key = keys->at(i);
        sw::redis::OptionalString blob = dataObjects->at(i);

        zippedData->insert({key, blob});
    }
}

std::string RedisDataStore::issueSynchronousRedisClusterStringCommand(const std::initializer_list<const char*>& command) {
    sw::redis::OptionalString result;
    std::string hashtag = "0";
    result = redisConnection_->command<sw::redis::OptionalString>(command.begin(), command.end()).get();

    return (result.has_value() ? result.value() : "");
}

std::string RedisDataStore::issueSynchronousRedisStringCommand(const std::initializer_list<const char*>& command, std::string hashtag) {
    sw::redis::OptionalString result;

    auto redis = redisConnection_->redis(hashtag);
    result = redis.command<sw::redis::OptionalString>(command.begin(), command.end()).get();

    return (result.has_value() ? result.value() : "");
}

// TODO: Array unpacking seems to be broken for redis++ ASync interface
//[[maybe_unused]] void RedisDataStore::issueSynchronousRedisArrayCommand(const std::initializer_list<const char*>& command) {
//    std::vector<std::string> result;
//
//    std::cout << "requested command length: " << command.size() << std::endl;
//
//    std::cout << "example from github.com" << std::endl;
//    auto r = redisConnection_->redis("0");
//    std::cout << "got AsyncRedis instance" << std::endl;
//    using SlotInfo = std::tuple<std::string, long long, std::string>;
//    using SlotMap = std::vector<std::variant<long long, SlotInfo>>;
//    auto slots = r.command<std::vector<SlotMap>>("CLUSTER", "SLOTS");
//
//    std::cout << "CLUSTER SLOTS complete" << std::endl;
//}


void RedisDataStore::redisGet(const std::string& key, std::string* result) {
    sw::redis::OptionalString data;
    data = redisConnection_->get(key).get();

    *result = (data.has_value() ? data.value() : "");
}

/**
 * Perform MGET operations against a Redis Cluster for given keys. key values
 * provided may hash to multiple hashslots.
 *
 * This implementation divides the keys into groups where all keys in each group
 * hash to a common hashslot. Separate mget calls are issued asynchronously for
 * each group. After all calls are issued, this method iterates over the futures
 * to collect the result data.
 *
 * @param keys a vector of Redis keys that hash to a single Redis hashslot.
 * @param results a map of key:value pairs retrieved from Redis.
 * @param indexByHashtag boolean indication whether the results should be
 * indexed by Redis key or geoID int
 */
void RedisDataStore::crossslotRedisMget(vector_keys_t& keys, const std::shared_ptr<multiget_result_map_t>& results, bool indexByHashtag) {
    hashslot_key_groups_t hashslotGroups;
    groupKeysByRedisHashslot(keys, hashslotGroups);

    auto futuresMap = std::make_shared<mgetFutureResultMap>();

    // Issue all MGET operations to Redis and collect Futures
    for (const auto& group : hashslotGroups) {
        // uint16_t hashslot = group.first;
        vector_keys_t hashslotKeys = group.second;
        redisMget(hashslotKeys, futuresMap);
    }

    // Iterate over Futures and map result data to keys
    for (auto&& p : *futuresMap) {
        std::shared_ptr<vector_keys_t> sliceKeys = std::make_shared<vector_keys_t>(p.first);

        if (indexByHashtag) {
            std::for_each(sliceKeys->begin(), sliceKeys->end(), [this](std::string& key) {
                key = getFeatureIDFromKey(redisKeyPrefix_, redisKeySuffix_, key);
            });
        }

        auto sliceResults = std::make_shared<vector_results_t>(p.second->get());
        auto mappedSliceObjects = std::make_shared<multiget_result_map_t>();

        zipResultObjects(sliceKeys, sliceResults, mappedSliceObjects);
        results->merge(*mappedSliceObjects);
    }
}

/**
 * Perform an MGET operation against a Redis Cluster.
 * Precondition: all elements in keys hash to a single Redis hashslot.
 * This implementation returns a map of <std::string>:Future<OptionalString>
 * after using the Async Redis++ API. After all required redisMget calls
 * have been made, the caller should iterate over the futures to unpack
 * the result data.
 *
 * @param keys a vector of Redis keys that hash to a single Redis hashslot.
 * @param results a map of key:value pairs retrieved from Redis.
 */
void RedisDataStore::redisMget(vector_keys_t& keys, const std::shared_ptr<mgetFutureResultMap>& results) {
    mgetFutureResultMap redisResults;
    size_t keysCount = keys.size();

    if (maxMultiKeyBatchCount_ > 0) {
        // If there is a maximum batch size defined, divide sliceKeys into segments
        // and issue multiple redis mget calls
        for (size_t i = 0; i < keysCount; i += maxMultiKeyBatchCount_) {
            size_t last = std::min(keysCount, i + maxMultiKeyBatchCount_);
            vector_keys_t sliceKeys(keys.begin() + (long)i, keys.begin() + (long)last);

            async_multiget_future_result_t tmpMgetFutures = redisConnection_->mget<vector_results_t>(sliceKeys.begin(), sliceKeys.end());
            std::shared_ptr<async_multiget_future_result_t> futuresResults = std::make_shared<async_multiget_future_result_t>(
                std::move(tmpMgetFutures));
            vector_keys_t hashableKeys = vector_keys_t(keys);

            results->insert({keys, futuresResults});
        }
    }
    else {
        async_multiget_future_result_t tmpMgetFutures = redisConnection_->mget<vector_results_t>(keys.begin(), keys.end());
        std::shared_ptr<async_multiget_future_result_t> futuresResults = std::make_shared<async_multiget_future_result_t>(std::move(tmpMgetFutures));
        vector_keys_t hashableKeys = vector_keys_t(keys);

        results->insert({keys, futuresResults});
    }
}

void RedisDataStore::fetchByFeatureKeys(vector_keys_t& keys, const std::shared_ptr<multiget_result_map_t>& results, bool indexByHashtag) {
    hashslot_key_groups_t hashslotGroups;

    // TODO: keys vector is mutated by this function. Measure impact of using a
    // copy of the vector. (i.e. pass by copy rather than reference.)
    // vector_keys_t localKeys = keys;
    // removeDuplicates(localKeys);

    removeDuplicates(keys);

    size_t keysCount = keys.size();
    std::shared_ptr<multiget_result_map_t> retrievedObjects = std::make_shared<multiget_result_map_t>();
    retrievedObjects->reserve(keysCount);

    crossslotRedisMget(keys, results, indexByHashtag);

    size_t resultCount = results->size();
    if (resultCount != keysCount) {
        std::string message =
            ("warn: Redis Data Store result count mismatch. "
             "crossslot mget retrieved "
             + std::to_string(resultCount) + " objects for fetchByFeatureKeys request with " + std::to_string(keysCount) + " keys");
        std::cout << message << std::endl;
    }
}

int RedisDataStore::getMultiKeyBatchCount() const {
    return maxMultiKeyBatchCount_;
}

std::string RedisDataStore::getRedisServerInfo(std::string_view hashtag) {
    std::string infoString;
    infoString = issueSynchronousRedisStringCommand({"INFO"}, std::string(hashtag));

    return infoString;
}

std::string RedisDataStore::getRedisServerVersion(std::string_view hashtag) {
    std::string infoString = this->getRedisServerInfo(hashtag);
    return this->getRedisServerVersionFromInfo(infoString);
}

std::string RedisDataStore::getDatasetMeta() {
    static std::string datasetMetaNotFound = "DATASET_META_NOT_FOUND";

    std::string datasetMetadata;
    redisGet(datasetMetaDataKey, &datasetMetadata);
    return ((not datasetMetadata.empty()) ? datasetMetadata : datasetMetaNotFound);
}

std::string RedisDataStore::getDatasetVersion() {
    std::string metaString = this->getDatasetMeta();

    return this->getDatasetVersionFromDatasetMeta(metaString);
}

//void RedisDataStore::getClusterSlots() {
//    issueSynchronousRedisArrayCommand({"CLUSTER", "INFO"});
//}

}  // namespace redis_store
