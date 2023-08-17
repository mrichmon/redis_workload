#pragma once

#include <redis_workload/datatypes.h>

#include <boost/crc.hpp>      // for boost::crc_basic, boost::crc_optimal
#include <boost/cstdint.hpp>  // for boost::uint16_t
#include <mutex>
#include <string>
#include <vector>

namespace redis_store {

// TODO debug helper routine -- remove
inline std::string bool_to_string(bool value) {
    return (value ? "T" : "F");
}

std::pair<std::string, std::string> getRedisCredentialsFromEnv();

std::pair<std::string, int> getRedisHostFromEnv();

/**
 * Divide the keys into hashslot groups and return the resulting groups.
 *
 * @param keys the Redis key strings
 * @param hashslotGroups the resulting groups of keys
 */
void groupKeysByRedisHashslot(vector_keys_t& keys, hashslot_key_groups_t& hashslotGroups);

/**
 * Get the Redis key string corresponding to the provided featureID.
 *
 * @param featureID a featureID integer passed as a string
 * @param redisKeyPrefix the Redis key prefix in use
 * @param refisKeySuffix the Redis key suffix in use
 * @return the Redis key string for the feature
 */
std::string getKeyForFeatureID(const std::string& redisKeyPrefix, const std::string& redisKeySuffix, const std::string& featureID);

/**
 * Get the Redis key string corresponding to the provided featureID.
 *
 * @param featureID a featureID integer passed as an int64_t
 * @param redisKeyPrefix the Redis key prefix in use
 * @param refisKeySuffix the Redis key suffix in use
 * @return the Redis key string for the feature
 */
std::string getKeyForFeatureID(const std::string& redisKeyPrefix, const std::string& redisKeySuffix, uint64_t featureID);

/**
 * Get the Redis key string corresponding to the provided featureID.
 *
 * @param featureID the Redis key string for the feature
 * @param redisKeyPrefix the Redis key prefix in use
 * @param refisKeySuffix the Redis key suffix in use
 * @return the int64 featureID portion of the key
 */
std::string getFeatureIDFromKey(const std::string& redisKeyPrefix, const std::string& redisKeySuffix, const std::string& featureID);

/*
 * CRC16 implementation configured to match Redis CRC16 calculation
 * used to calculate hashslot for key.
 */
static const size_t redisHashslotCRCBitWidth = 16;
typedef typename boost::uint_t<redisHashslotCRCBitWidth>::fast crc_value_type;
static const crc_value_type redisHashslotCRCPoly = 0x1021;
static const crc_value_type redisHashslotInitialRem = 0x0;
static const crc_value_type redisHashslotCRCFinalXOR = 0x0;
static const bool redisHashslotCRCReflectInputByte = false;
static const bool redisHashslotReflectOutput = false;

/**
 * Calculate Redis Hash Slot ID.
 *
 * Calculation of Redis Hash Slot Identifiers relies on a CRC16
 * generator and key parsing logic to extract redis hashtags
 * from provided keys.
 * Both the CRC generator and the hashtag parsing logic must
 * match the logic implemented in the Redis Cluster implementation.
 */
class RedisHashSlotGenerator {
public:
    ~RedisHashSlotGenerator() = default;
    virtual uint16_t crc16(const std::string& data) = 0;
    std::string getRedisHashtag(const std::string& key);
    uint16_t getHashslotForKey(const std::string& key);
};

/**
 * Singleton implementation that reuses a single CRC engine, gated
 * by a mutex.
 */
class SingletonRedisHashSlotGenerator : public RedisHashSlotGenerator {
private:
    // CRC generator configured to implement CRC16 ccitt
    boost::crc_optimal<redisHashslotCRCBitWidth,
                       redisHashslotCRCPoly,
                       redisHashslotInitialRem,
                       redisHashslotCRCFinalXOR,
                       redisHashslotCRCReflectInputByte,
                       redisHashslotReflectOutput>
        crc_ccitt;
    std::mutex mutex;

public:
    SingletonRedisHashSlotGenerator() = default;

    uint16_t crc16(const std::string& data) override;
};

/**
 * Ephemeral implementation allocates a new CRC engine for each invocation.
 */
class EphemeralRedisHashSlotGenerator : public RedisHashSlotGenerator {
private:
public:
    EphemeralRedisHashSlotGenerator() = default;

    uint16_t crc16(const std::string& data) override;
};

/**
 * CRC 16 generator from redis++ library.
 */
class RedisPlusPlusHashSlotGenerator : public RedisHashSlotGenerator {
private:
public:
    RedisPlusPlusHashSlotGenerator() = default;

    uint16_t crc16(const std::string& data) override;
};

RedisHashSlotGenerator* getRedisHashslotGenerator();

std::string makeRedisAddressString(const std::string& host, int port);

/**
 * Calculate the defined percentile value for the provided data values.
 * This function is used by the development clients to calculate the
 * percentile duration over multiple test operations.
 * 50, 95, 99, 100
 * @param percentile the integer percentile to be calculated. For example, to calculate p95 this
 *        this argument would have the value 95.
 * @param data a vector of the raw data values.
 *
 * @return the calculated percentile value.
 */
long long findPercentile(int percentile, std::vector<long long>* data);

/**
 * Calculate the average value for the provided data values.
 * This function is used by the development clients to calculate the average
 * duration over multiple test operations.
 *
 * @param data a vector of the raw data values.
 *
 * @return the calculated average value.
 */
double findAverage(std::vector<long long>* data);

}  // namespace redis_store