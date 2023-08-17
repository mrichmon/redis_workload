#pragma once
#include <redis_workload/datatypes.h>
#include <redis_workload/redis_store_params.h>
#include <sw/redis++/async_redis++.h>
#include <sw/redis++/async_redis.h>
#include <sw/redis++/async_redis_cluster.h>

#include <algorithm>
#include <boost/functional/hash.hpp>
#include <optional>
#include <string>
#include <unordered_set>

using redis_store::vector_keys_t;

namespace redis_store {

/**
 * Low-level handling of data store communication with Redis Cluster.
 *
 * The RedisDataStore should only be instantiated by calling the appropriate
 * static factory method. The underlying objects used by RedisDataStore are
 * heavyweight that internally use multiple threads and maintain a pool of
 * connections to the Redis Cluster.
 * This implementation assumes that a process using RedisDataStore will only
 * require a connection to a single Redis Cluster.
 */
class RedisDataStore {
private:
    static std::shared_ptr<RedisDataStore> redisDataStore_;
    RedisStoreParams params_;

    int maxMultiKeyBatchCount_ = 0;
    std::string redisKeyPrefix_;
    std::string redisKeySuffix_;
    std::string datasetMetaDataKey;

    std::unique_ptr<sw::redis::AsyncRedisCluster> redisConnection_;

    // /**
    //  * Establish network connections to all Redis shards.
    //  */
    // void warmRedisConnections();

    /**
     * Extract the Redis server version string from the info string
     * returned from sending an INFO command to a Redis server.
     *
     * @param infoString the full string response from the INFO command.
     *
     * @return the version string.
     */
    std::string getRedisServerVersionFromInfo(const std::string& infoString);

    /**
     * Extract the dataset version string from the dataset metadata string
     * for the dataset.
     *
     * @param metaString the full JSON structure stored in Redis as the dataset metadata.
     *
     * @return the version string.
     */
    std::string getDatasetVersionFromDatasetMeta(const std::string& metaString);

    /**
     * Zip the keys and dataObjects together.
     * Precondition: keys and dataObjects are assumed to be in corresponding
     * order.
     *
     * @param keys a vector of the Redis key strings
     * @param dataObjects vector of data results from Redis
     * @param zippedData unordered_map of key:dataObject values
     */
    void zipResultObjects(const std::shared_ptr<vector_keys_t>& keys,
                          const std::shared_ptr<vector_results_t>& dataObjects,
                          const std::shared_ptr<multiget_result_map_t>& zippedData);

    /**
     * Perform GET operation against a Redis Cluster for the given key.
     *
     * @param key a std::string holds a single Redis key
     * @param result the object data retrieved from the Redis Cluster
     */
    void redisGet(const std::string& key, std::string* result);

    void crossslotRedisMget(vector_keys_t& keys, const std::shared_ptr<multiget_result_map_t>& results, bool indexByHashtag = false);

    void redisMget(vector_keys_t& keys, const std::shared_ptr<mgetFutureResultMap>& results);

    /**
     * Perform the Redis Cluster command that returns a string synchronously.
     *
     * The command provided must produce a String or Status result.
     *
     * @param command the requested command stored in a vector. For example, the redis-cli command "CLUSTER INFO" should be
     *                represented as {"CLUSTER", "INFO"}
     * @return the string returned from the Redis cluster
     */
    std::string issueSynchronousRedisClusterStringCommand(const std::initializer_list<const char*>& command);

    /**
     * Perform the Redis command that returns a string synchronously.
     * The command is issued to the redis node that owns the shard containing the hashtag.
     * If the hashtag parameter is not provided the command is issued to the node that owns
     * hashtag "0".
     *
     * The command provided must produce a String or Status result.
     *
     * @param command the requested command stored in a vector. For example, the redis-cli command "INFO" should be
     *                represented as {"INFO"}
     * @return the string returned from the Redis node
     */
    std::string issueSynchronousRedisStringCommand(const std::initializer_list<const char*>& command, std::string hashtag = "0");

    /**
     * Perform the Redis command that returns an array synchronously.
     *
     * The command provided must produce an Array result.
     *
     * @param command the requested command stored in a vector. For example, the redis-cli command "CLUSTER INFO" should be
     *                represented as {"CLUSTER", "SLOTS"}
     * @return the result returned from the Redis cluster
     */
//    [[maybe_unused]] void issueSynchronousRedisArrayCommand(const std::initializer_list<const char*>& command);

public:
    /**
     * Construct an RedisDataStore instance using the supplied.
     * This constructor should only be called from the
     * RedisDataStore::factory(...) method. Client code should use the appropriate
     * RedisDataStore::factory(...) method to instantiate this class.
     *
     * @param params the configuration parameters for the Redis Data Store
     */
    explicit RedisDataStore(const RedisStoreParams& params);

    /**
     * Delete the copy constructor to avoid bypassing the factory method.
     */
    RedisDataStore& operator=(const RedisDataStore&) = delete;

    /**
     * Static factory method to instantiate the RedisDataStore singleton using
     * host, port and password arguments for Redis server.
     *
     * @param params the configuration parameters for the Redis Data Store
     *
     * @return the RedisDataStore instance.
     */
    static std::shared_ptr<RedisDataStore> factory();

    /**
     * Retrieve the features identified by Redis key values from the
     * Data Store.
     *
     * Call the underlying Redis Cluster data repository to obtain the objects
     * identified by the keys. Redis keys may hash to multiple Redis hashslots.
     *
     * @param keys a vector of the requested Redis keys as std:string.
     * @param result an empty collection used to hold the retrieved features.
     * @param indexByHashtag boolean indicating whether the result map should be index by key value or hashtag value.
     */
    void fetchByFeatureKeys(vector_keys_t& keys, const std::shared_ptr<multiget_result_map_t>& results, bool indexByHashtag = false);

    /**
     * Return the configured maximum number of keys
     * issued to Redis in a multikey call.
     *
     * @return the maximum batch size
     */
    [[nodiscard]] int getMultiKeyBatchCount() const;

    /**
     * Return the full INFO string reported by a single Redis server.
     *
     * When called without a hashtag argument the INFO data for the 0th
     * node is returned.
     *
     * When operating in async Redis Cluster mode this does not provide
     * an implementation.
     *
     * @return the INFO string
     */
    std::string getRedisServerInfo(std::string_view hashtag = "0");

    /**
     * Get the Redis server version string from a Redis server
     * that is in the Redis Cluster.
     *
     * If no hashtag is provided, return the version reported
     * by the Redis node that owns hashtag 0.
     *
     * @return the version string.
     */
    std::string getRedisServerVersion(std::string_view hashtag = "0");

    /**
     * Get the Redis dataset version string.
     *
     * @return the version string.
     */
    std::string getDatasetVersion();

    /**
     * Retrieve the dataset metadata from Orion server.
     *
     * @return the metadata string.
     */
    std::string getDatasetMeta();

    /**
     *
     */
//    void getClusterSlots();
};

}  // namespace redis_store
