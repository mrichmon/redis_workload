#include <redis_workload/redis_store_exceptions.h>
#include <redis_workload/redis_store_params.h>

#include <string>

namespace redis_store {

// TODO: Add validation checks for all config values
bool RedisStoreParams::valid() const {
    if (redisHost.empty()) {
        throw ConfigParamError("config error: " + kConfigRedisClusterHost);
    }

    if (redisUser.empty()) {
        throw ConfigParamError("config error: " + kConfigRedisClusterUser);
    }

    if (redisPassword.empty()) {
        throw ConfigParamError("config error: " + kConfigRedisClusterPassword + " not set");
    }

    if (redisKeyPrefix.empty()) {
        throw ConfigParamError("config error: " + redisKeyPrefix);
    }

    if (redisKeySuffix.empty()) {
        throw ConfigParamError("config error: " + redisKeySuffix);
    }

    if (redisPort <= Min_Port_Number_) {
        throw ConfigParamError("config error: " + kConfigRedisClusterPort);
    }

    return true;
}

}  // namespace redis_store
