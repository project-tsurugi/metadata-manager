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
#include "manager/authentication/common/utilitys.h"

#include <string>

// =============================================================================
namespace manager::authentication {

/**
 * @brief Explicit Template Instantiation for str_to_numeric(float type).
 */
template ErrorCode Utilitys::str_to_numeric(std::string_view str,
                                            float& return_value);
/**
 * @brief Explicit Template Instantiation for str_to_numeric(int32_t type).
 */
template ErrorCode Utilitys::str_to_numeric(std::string_view str,
                                            std::int32_t& return_value);
/**
 * @brief Explicit Template Instantiation for str_to_numeric(int64_t type).
 */
template ErrorCode Utilitys::str_to_numeric(std::string_view str,
                                            std::int64_t& return_value);

/**
 * @brief Converts a string to a numeric.
 * @param (str)           [in]  String to be converted to a numeric.
 * @param (return_value)  [out] The converted numeric.
 * @return ErrorCode::OK if success, otherwise an error code.
 */
template <typename T>
[[nodiscard]] ErrorCode Utilitys::str_to_numeric(std::string_view str,
                                                 T& return_value) {
  try {
    return_value = convert_to_numeric<T>(str);
  } catch (...) {
    return ErrorCode::INTERNAL_ERROR;
  }

  return ErrorCode::OK;
}

/* =============================================================================
 * Private method area
 */

/**
 * @brief Converts a string to a numeric.
 * @param (str)  [in]  String to be converted to a numeric.
 * @return The converted numeric as a float type value.
 */
template <>
float Utilitys::convert_to_numeric<float>(std::string_view str) {
  return std::stof(str.data());
}

/**
 * @brief Converts a string to a numeric.
 * @param (str)  [in]  String to be converted to a numeric.
 * @return The converted numeric as a int32_t type value.
 */
template <>
std::int32_t Utilitys::convert_to_numeric<std::int32_t>(std::string_view str) {
  return std::stoi(str.data());
}

/**
 * @brief Converts a string to a numeric.
 * @param (str)  [in]  String to be converted to a numeric.
 * @return The converted numeric as a int64_t type value.
 */
template <>
std::int64_t Utilitys::convert_to_numeric<std::int64_t>(std::string_view str) {
  return std::stol(str.data());
}

}  // namespace manager::authentication
