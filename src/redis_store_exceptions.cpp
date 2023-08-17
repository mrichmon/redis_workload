#include <redis_workload/redis_store_exceptions.h>

#include <string>
#include <utility>

namespace redis_store {

ConfigParamError::ConfigParamError(std::string error) : error_(std::move(error)), what_(error_.c_str()) {}

ConfigParamError::~ConfigParamError() noexcept = default;
const char* ConfigParamError::what() const noexcept {
    return what_;
}

}  // namespace redis_store