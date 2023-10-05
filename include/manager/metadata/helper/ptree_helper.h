/*
 * Copyright 2022-2023 Project Tsurugi.
 *
 * Licensed under the Apache License, version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
#pragma once

#include <map>
#include <string>
#include <string_view>
#include <vector>

#include <boost/property_tree/ptree.hpp>

#include "manager/metadata/error_code.h"

namespace ptree_helper {

/**
 * @brief Make a ptree object from a std::vector object.
 * @param vc  [in]  Vector object to be converted.
 * @return Converted object.
 */
boost::property_tree::ptree make_array_ptree(const std::vector<int64_t>& vc);

/**
 * @brief Make a std::vector object from a ptree object.
 * @param pt  [in]  property_tree object to be converted.
 * @param key [in]  Key name of ptree.
 * @return Converted object. If there is no value corresponding to the key,
 *  empty is returned.
 */
std::vector<int64_t> make_vector_int(const boost::property_tree::ptree& pt,
                                     std::string_view key);

/**
 * @brief Converts a JSON string to a property_tree.
 * @param json  [in]  JSON string to be converted to a property_tree.
 * @param pt    [out] The converted property_tree..
 * @return ErrorCode::OK if success, otherwise an error code.
 */
manager::metadata::ErrorCode json_to_ptree(std::string_view json,
                                           boost::property_tree::ptree& pt);

/**
 * @brief Converts a property_tree to a JSON string.
 * @param pt    [in]  property_tree to be converted to a JSON string.
 * @param json  [out] The converted JSON string.
 * @return ErrorCode::OK if success, otherwise an error code.
 */
manager::metadata::ErrorCode ptree_to_json(
    const boost::property_tree::ptree& pt, std::string& json);

/**
 * @brief Converts a property_tree to a JSON string.
 * @param pt  [in]  property_tree to be converted to a JSON string.
 * @return The converted JSON string.
 */
std::string ptree_to_json(const boost::property_tree::ptree& pt);

/**
 * @brief Convert a property_tree object to a std::vector object.
 * @param pt  [in]  property_tree object.
 * @return converted object.
 */
std::vector<boost::property_tree::ptree> array_to_vector(
    const boost::property_tree::ptree& pt);

/**
 * @brief Convert a std::vector object to a property_tree object.
 * @tparam T type
 * @param vc  [in]  vector object to be converted.
 * @return converted object.
 */
template <typename T>
boost::property_tree::ptree vector_to_array(const std::vector<T>& vc);

/**
 * @brief The value for a key is extracted from the ptree and returned
 *   as a string.
 * @param pt   [in]  property_tree object.
 * @param key  [in]  key name.
 * @return String of extracted values.
 */
template <typename T>
[[nodiscard]] std::string ptree_value_to_string(
    const boost::property_tree::ptree& pt, std::string_view key) {
  std::string result_value;

  auto value = pt.get_optional<T>(key.data());
  if (value) {
    if constexpr (std::is_integral_v<T>) {
      result_value = std::to_string(value.value());
    } else {
      result_value = value.value();
    }
  }
  return result_value;
}

/**
 * @brief Returns whether the ptree object is an array.
 * @param pt  [in]  property_tree object.
 * @retval true: array.
 * @retval false: non-array.
 */
bool is_array(const boost::property_tree::ptree& pt);

/**
 * @brief Returns whether the object matches the key.
 * @param pt    [in]  property_tree object.
 * @param keys  [in]  key name and value of property_tree object.
 * @retval true: match.
 * @retval false: mismatch.
 */
bool is_match(const boost::property_tree::ptree& pt,
              const std::map<std::string_view, std::string_view>& keys);

}  // namespace ptree_helper
