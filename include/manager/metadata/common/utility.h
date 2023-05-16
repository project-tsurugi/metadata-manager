/*
 * Copyright 2020-2021 tsurugi project.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
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
#ifndef MANAGER_METADATA_COMMON_UTILITY_H_
#define MANAGER_METADATA_COMMON_UTILITY_H_

#include <string>
#include <vector>

#include <boost/property_tree/ptree.hpp>

#include "manager/metadata/error_code.h"

namespace manager::metadata {

class Utility {
 public:
  /**
   * @brief Converts a string to a numeric.
   * @tparam T Supported types are integer (int32_t, int64_t) or
   *   floating point (float).
   * @param str    [in]  String to be converted to a floating point.
   * @param value  [out] The converted value.
   * @return ErrorCode::OK if success, otherwise an error code.
   */
  template <typename T>
  static manager::metadata::ErrorCode str_to_numeric(std::string_view str,
                                                     T& value);

  /**
   * @brief Converts boolean expression in metadata repository to boolean value
   * in application.
   * @param bool_alpha  [in]  boolean string.
   * @return Converted boolean value of the string.
   */
  static bool str_to_boolean(std::string_view bool_alpha);

  /**
   * @brief Converts boolean value in application to boolean expression in
   * metadata repository.
   * @param value  [in]  boolean expression.
   * @return Converted string of the boolean value.
   */
  static std::string boolean_to_str(const bool value);

  /**
   * @brief Split a string with a specified delimiter.
   * @param source    [in]  Source string to be split.
   * @param delimiter [in]  Delimiter to split.
   * @return Vector of the result of the split.
   */
  static std::vector<std::string> split(std::string_view source,
                                        const char& delimiter);
};  // class Utility

}  // namespace manager::metadata

#endif  // MANAGER_METADATA_COMMON_UTILITY_H_
