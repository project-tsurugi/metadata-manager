/*
 * Copyright 2022 tsurugi project.
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

#include <string>
#include <string_view>
#include <type_traits>
#include <vector>

#include <boost/property_tree/ptree.hpp>

#include "manager/metadata/error_code.h"

namespace ptree_helper {

boost::property_tree::ptree make_array_ptree(const std::vector<int64_t>& v);
std::vector<int64_t> make_vector_int(const boost::property_tree::ptree& pt,
                                     std::string_view key);

manager::metadata::ErrorCode json_to_ptree(std::string_view json,
                                           boost::property_tree::ptree& ptree);
manager::metadata::ErrorCode ptree_to_json(
    const boost::property_tree::ptree& ptree, std::string& json);

/**
 * @brief The value for a key is extracted from the ptree and returned
 *   as a string.
 * @param ptree  [in]  ptree object.
 * @param key    [in]  key name.
 * @return String of extracted values.
 */
template <typename T>
[[nodiscard]] std::string ptree_value_to_string(const boost::property_tree::ptree& ptree,
                                  const char* key) {
  std::string result_value;

  auto value = ptree.get_optional<T>(key);
  if (value) {
    if constexpr (std::is_integral_v<T>) {
      result_value = std::to_string(value.value());
    } else {
      result_value = value.value();
    }
  }
  return result_value;
}

}  // namespace ptree_helper
