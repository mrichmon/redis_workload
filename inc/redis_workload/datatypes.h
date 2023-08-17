/**
 * @file redis_workload/datatypes.h
 *
 * @brief Core redis data store datatypes
 */
#pragma once

#include <sw/redis++/async_redis++.h>
#include <sw/redis++/async_redis.h>
#include <sw/redis++/async_redis_cluster.h>

#include <algorithm>
#include <boost/functional/hash.hpp>
#include <map>
#include <optional>
#include <string>
#include <unordered_map>
#include <vector>

namespace redis_store {

/**
 * Redis Data Store hashmap type to allow easy substitution of alternate
 * hashmap implementations that conform to std:unordered_map interface.
 */
template <typename key, typename value>
using as_hashmap_t = typename std::unordered_map<key, value>;

/**
 * Data type returned from methods that identify the requested features by
 * std::string identifiers.
 * These methods are:
 *     fetchByFeatureKeys(std::vector<string>, ...)
 *     fetchByFeatureID(std::vector<string>, ...)
 */
typedef as_hashmap_t<std::string, std::shared_ptr<std::string>> fetch_by_string_map_t;

/**
 * Vector of object keys.
 */
typedef std::vector<std::string> vector_keys_t;

/**
 * Vector of redis result objects
 */
typedef std::vector<sw::redis::OptionalString> vector_results_t;

/**
 * Map of Redis hashslot ID to redis key strings
 */
typedef std::map<uint16_t, vector_keys_t> hashslot_key_groups_t;

/**
 * Map of Redis key to optional string for raw results obtained from Redis
 */
typedef as_hashmap_t<std::string, sw::redis::OptionalString> multiget_result_map_t;

/*
 * For Async operations, the map contains the pair
 * vector<keys>:future<vector<optionalstring>> The order of keys corresponds to
 * the order of optionalstrings. Using a vector as the key to an unordered_map
 * requires a custom hash function to be defined.
 */
typedef sw::redis::Future<std::vector<sw::redis::OptionalString>> async_multiget_future_result_t;
class VectorStringHasher {
public:
    size_t operator()(const vector_keys_t& v) const {
        return boost::hash_value(v);
    }
};
typedef std::unordered_map<vector_keys_t, std::shared_ptr<async_multiget_future_result_t>, VectorStringHasher> mgetFutureResultMap;

}  // namespace redis_store
