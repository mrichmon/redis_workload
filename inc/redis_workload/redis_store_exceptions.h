#pragma once

#include <exception>
#include <string>

namespace redis_store {

/**
 * Exception raised for an configuration parameter error
 */
class ConfigParamError : public std::exception {
public:
    /**
     * @brief Constructor takes an error message
     *
     * @param error - Error message
     */
    explicit ConfigParamError(std::string error);

    /**
     * @brief Destructor reflects no-throw base
     */
    ~ConfigParamError() noexcept override;

    /**
     * @brief Returns c string error message
     *
     * @return c string error message
     */
    const char* what() const noexcept override;

private:
    const std::string error_;
    const char* const what_;
};

}  // namespace redis_store