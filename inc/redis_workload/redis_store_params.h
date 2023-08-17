#pragma once

#include <string>

namespace redis_store {

class RedisStoreParams {
private:
    const int Min_Port_Number_ = 1024;

public:
    std::string redisHost;
    int redisPort = 6379;
    std::string redisUser = "default";
    std::string redisPassword;
    std::string redisKeyPrefix;
    std::string redisKeySuffix;
    bool preferReadReplicas = true;
    int poolSize = 3;
    int poolWaitTimeout = 0;
    int poolConnectionLifetime = 10;
    int poolConnectionMaxIdle = 0;
    int maxMultiKeyBatchSize = 10;

    /**
     * Validate the RedisStoreParam field values.
     *
     * @throws ConfigParamError for bad parameter values.
     *
     * @return true if the parameters are valid.
     */
    // TODO: Add validation checks for all config values
    bool valid() const;
};

inline static const std::string kConfigRedisClusterHost = "redis_cluster_address";
inline static const std::string kConfigRedisClusterPort = "redis_cluster_port";
inline static const std::string kConfigRedisClusterUser = "redis_cluster_user";
inline static const std::string kConfigRedisClusterPassword = "redis_cluster_password";

}  // namespace redis_store