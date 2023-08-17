#include <redis_workload/util.h>

#include <iostream>
#include <redis_workload/remove_duplicates.hpp>
#include <string>

namespace redis_store {

std::string getEnvVar(const std::string& key) {
    char* val = getenv(key.c_str());
    return val == NULL ? std::string("") : std::string(val);
}

std::pair<std::string, std::string> getRedisCredentialsFromEnv() {
    std::string user = getEnvVar("REDIS_USER");
    std::string password = getEnvVar("REDIS_PASS");

    if (user.empty()) {
        std::cerr << "error: REDIS_USER not set" << std::endl;
        exit(1);
    }
    if (password.empty()) {
        std::cerr << "error: REDIS_PASS not set" << std::endl;
        exit(1);
    }

    return std::make_pair(user, password);
}

std::pair<std::string, int> getRedisHostFromEnv() {
    std::string host = getEnvVar("REDIS_HOST");
    std::string portStr = getEnvVar("REDIS_PORT");

    if (host.empty()) {
        std::cerr << "error: REDIS_HOST not set" << std::endl;
        exit(1);
    }
    if (portStr.empty()) {
        std::cerr << "error: REDIS_PORT not set" << std::endl;
        exit(1);
    }

    int port = stoi(portStr);

    return std::make_pair(host, port);
}

void groupKeysByRedisHashslot(vector_keys_t& keys, hashslot_key_groups_t& hashslotGroups) {
    RedisHashSlotGenerator* hashSlotGenerator = getRedisHashslotGenerator();

    removeDuplicates(keys);

    for (const std::string& k : keys) {
        uint16_t hashSlot = hashSlotGenerator->getHashslotForKey(k);

        if (auto it {hashslotGroups.find(hashSlot)}; it != std::end(hashslotGroups)) {
            // hashslotGroup already exists, so just append the new key to the
            // existing hashslot vector
            it->second.push_back(k);
        }
        else {
            // This is a new hashslot so extend the hashslotGroups map by adding a new
            // vector
            vector_keys_t newHashslotKeys = {k};
            hashslotGroups.insert(std::pair<uint16_t, vector_keys_t>(hashSlot, newHashslotKeys));
        }
    }
}

std::string getKeyForFeatureID(const std::string& redisKeyPrefix, const std::string& redisKeySuffix, const std::string& featureID) {
    return (redisKeyPrefix + featureID + redisKeySuffix);
}

std::string getKeyForFeatureID(const std::string& redisKeyPrefix, const std::string& redisKeySuffix, uint64_t featureID) {
    return getKeyForFeatureID(redisKeyPrefix, redisKeySuffix, std::to_string(featureID));
}

std::string getFeatureIDFromKey(const std::string& redisKeyPrefix, const std::string& redisKeySuffix, const std::string& key) {
    static const size_t prefix_length = redisKeyPrefix.size();
    static const size_t suffix_length = redisKeySuffix.size();
    return key.substr(prefix_length, key.size() - prefix_length - suffix_length);
}

/**
 * Calculate a CCITT CRC16 hash.
 *
 * @param data the data to calculate the CRC for.
 *
 * @return the generated checksum value.
 */
uint16_t SingletonRedisHashSlotGenerator::crc16(const std::string& data) {
    // Treat this entire method as a critical section
    std::lock_guard<std::mutex> guard(this->mutex);
    // Ensure crc generator is reset to initial state
    crc_ccitt.reset(redisHashslotInitialRem);
    crc_ccitt.process_bytes(data.c_str(), data.length());

    return crc_ccitt.checksum();
}

/**
 * Calculate a CCITT CRC16 hash.
 *
 * @param data the data to calculate the CRC for.
 *
 * @return the generated checksum value.
 */
uint16_t EphemeralRedisHashSlotGenerator::crc16(const std::string& data) {
    boost::crc_optimal<redisHashslotCRCBitWidth,
                       redisHashslotCRCPoly,
                       redisHashslotInitialRem,
                       redisHashslotCRCFinalXOR,
                       redisHashslotCRCReflectInputByte,
                       redisHashslotReflectOutput>
        crc_ccitt;

    crc_ccitt.process_bytes(data.c_str(), data.length());
    return crc_ccitt.checksum();
}

/**
 * Calculate a CCITT CRC16 hash using redis++ library implementation.
 *
 * @param data the data to calculate the CRC for.
 *
 * @return the generated checksum value.
 */
uint16_t RedisPlusPlusHashSlotGenerator::crc16(const std::string& data) {
    return sw::redis::crc16(data.c_str(), data.length());
}

/**
 * Calculate the hashtag string to use when calculating the Redis
 * hashslot. If key does not contain a valid hashtag, return the
 * full key.
 *
 * Identification of a valid hashtag follows the hashtag algorithm
 * described at https://redis.io/docs/reference/cluster-spec/
 *
 * @param key Redis key string.
 *
 * @return the string to be used to calculate the corresponding hashslot.
 */
std::string RedisHashSlotGenerator::getRedisHashtag(const std::string& key) {
    if (key == "") {
        throw std::invalid_argument("key must be a non-empty string");
    }

    std::size_t openBracePos = key.find('{');
    if (openBracePos == std::string::npos) {
        /*
         * No opening brace found so there is no hashtag in the key.
         */
        return key;
    }

    /*
     * Opening brace found in key. Follow Redis cluster algorithm
     * for finding the redis hashtag.
     */

    std::size_t closeBracePos = key.find('}', openBracePos) - openBracePos;

    std::string hashtag;
    if (closeBracePos != std::string::npos) {
        hashtag = key.substr(openBracePos + 1, closeBracePos - 1);
        if (hashtag.length() > 0)
            return hashtag;
    }
    /*
     * No closing brace found, or nothing between the braces, so
     * hash the whole key.
     */
    return key;
}

static const uint16_t redisHashslotMax = 16383;

/**
 * Calculate the hashslot identifier for the provided Redis key.
 * string to use when calculating the Redis hashslot.
 *
 * Hashslot calculation follows the specification described at
 * https://redis.io/docs/reference/cluster-spec/
 *
 * @param key Redis key string.
 *
 * @return the hashslot identifier.
 */
uint16_t RedisHashSlotGenerator::getHashslotForKey(const std::string& key) {
    /*
     * Calculation of the hashslot should follow the hashtag algorithm
     * described at https://redis.io/docs/reference/cluster-spec/
     *
     * Note that CRC16 calculation has several standard variants
     * depending on how the algorithm is parameterized. The crc16()
     * method in this file is configured to match the parameters
     * used by Redis.
     *
     * Redis hashslot is determined by CRC16(hashtag) mod redisHashslotMax
     */
    return this->crc16(getRedisHashtag(key)) & redisHashslotMax;
}

/**
 * Factory to return appropriate hashslot generator based on compile-time
 * settings
 * @return the hashslot generator instance
 */
RedisHashSlotGenerator* getRedisHashslotGenerator() {
    RedisHashSlotGenerator* generator;
#if defined(USE_REDIS_PLUS_PLUS_CRC_ENGINE)
    generator = new RedisPlusPlusHashSlotGenerator();
#elif defined(USE_EPHEMERAL_CRC_ENGINE)
    generator = new EphemeralRedisHashSlotGenerator();
#else
    generator = new SingletonRedisHashSlotGenerator();
#endif
    return generator;
}

/**
 * Calculate the percentile value found in the provided data vector.
 * e.g. to obtain the p95 value, use the following call:
 *     findPercentile(95, data);
 *
 * @param percentile the percentile value to find.
 * @param data a vector holding the dataset.
 *
 * @return the percentile-th data value in the dataset provided in data
 * parameter.
 */
// TODO: templatize
long long findPercentile(int percentile, std::vector<long long>* data) {
    if ((percentile < 1) || (percentile > 100)) {
        throw std::invalid_argument("percentile must be an integer between 1 and 100");
    }

    auto count = static_cast<unsigned long long>(data->size());
    if (count == 0) {
        throw std::invalid_argument("data cannot be empty");
    }

    if (count < 20) {
        std::cout << "warn: percentile values can be misleading for small datasets" << std::endl;
    }

    if ((percentile < 0.0) || (percentile > 100.0)) {
        throw std::invalid_argument("percentile must be between 0.0 and 100.0");
    }

    // Using the nearest-rank method, so take the ceiling and offset since vectors
    // are 0-indexed.
    auto index = ((int)(ceil((double)count * (double)(percentile / 100.0)))) - 1;
    std::sort(data->begin(), data->end());

    auto result = data->at(index);
    return result;
}

/**
 * Calculate the average value found in the provided data vector.
 *
 * @param data a vector holding the dataset.
 *
 * @return the average of the values found in the dataset provided in data
 * parameter.
 */
// TODO: templatize
double findAverage(std::vector<long long>* data) {
    if (data->empty()) {
        throw std::invalid_argument("data cannot be empty");
    }

    auto count = static_cast<unsigned long long>(data->size());
    auto average = std::accumulate(data->begin(), data->end(), 0.0) / count;
    return average;
}

}  // namespace redis_store