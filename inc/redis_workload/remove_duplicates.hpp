/**
 * @file basic/remove_duplicates.hpp
 *
 * @brief Helper utility function to remove duplicates from vector.
 *
 * Method signatures for template functions must be defined with the
 * implementation of the method. (Otherwise, link issues occur with BUILD_TYPE=Release.)
 */
#pragma once

#include <algorithm>
#include <string>
#include <vector>

namespace redis_store {
/**
 * Remove duplicate entries from the vector v.
 *
 * @tparam T type of objects stored in vector
 * @param v vector of objects to be deduplicated
 */
template <typename T>
inline void removeDuplicates(std::vector<T>& v) {
    std::sort(v.begin(), v.end());
    v.erase(std::unique(v.begin(), v.end()), v.end());
}

}  // namespace redis_store
