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
#include "manager/metadata/common/utility.h"

#include <charconv>
#include <type_traits>

#include "manager/metadata/common/message.h"
#include "manager/metadata/helper/logging_helper.h"

// =============================================================================
namespace {

using manager::metadata::ErrorCode;
using manager::metadata::Message;

/**
 * @brief Converts a string to a numeric.
 * @param str    [in]  String to be converted to a floating point.
 * @param value  [out] The converted value.
 * @return ErrorCode::OK if success, otherwise an error code.
 */
template <typename T, std::enable_if_t<std::is_floating_point_v<T>,
                                       std::nullptr_t> = nullptr>
ErrorCode convert_to_numeric(std::string_view str, T& value) {
  try {
    value = std::stof(str.data());
    LOG_DEBUG << "Convert string to floating point: \"" << str << "\"";
  } catch (...) {
    LOG_ERROR << Message::CONVERT_STRING_TO_FLOAT_FAILURE << "\"" << str
              << "\"";
    return ErrorCode::INTERNAL_ERROR;
  }

  return ErrorCode::OK;
}

/**
 * @brief Converts a string to a numeric.
 * @param str    [in]  String to be converted to a integral.
 * @param value  [out] The converted value.
 * @return ErrorCode::OK if success, otherwise an error code.
 */
template <typename T,
          std::enable_if_t<std::is_integral_v<T>, std::nullptr_t> = nullptr>
ErrorCode convert_to_numeric(std::string_view str, T& value) {
  ErrorCode error = ErrorCode::UNKNOWN;
  T converted_value{};

  const auto [ptr, err_code] =
      std::from_chars(std::begin(str), std::end(str), converted_value);
  if (err_code == std::errc{}) {
    value = converted_value;
    error = ErrorCode::OK;
  } else {
    LOG_ERROR << Message::CONVERT_STRING_TO_INT_FAILURE
              << "error: " << static_cast<int>(err_code) << ", value: \"" << str
              << "\"";
    error = ErrorCode::INTERNAL_ERROR;
  }

  return error;
}

}  // namespace

// =============================================================================
namespace manager::metadata {

/**
 * @brief Explicit Template Instantiation for str_to_numeric(float type).
 */
template ErrorCode Utility::str_to_numeric(std::string_view str,
                                           float& return_value);
/**
 * @brief Explicit Template Instantiation for str_to_numeric(int32_t type).
 */
template ErrorCode Utility::str_to_numeric(std::string_view str,
                                           std::int32_t& return_value);
/**
 * @brief Explicit Template Instantiation for str_to_numeric(uint64_t type).
 */
template ErrorCode Utility::str_to_numeric(std::string_view str,
                                           std::uint64_t& return_value);
/**
 * @brief Explicit Template Instantiation for str_to_numeric(int64_t type).
 */
template ErrorCode Utility::str_to_numeric(std::string_view str,
                                           std::int64_t& return_value);

template <typename T>
ErrorCode Utility::str_to_numeric(std::string_view str, T& return_value) {
  return convert_to_numeric(str, return_value);
}

bool Utility::str_to_boolean(std::string_view bool_alpha) {
  bool result = false;

  // Convert to lowercase.
  std::string bool_alpha_lower = std::string(bool_alpha);
  std::transform(bool_alpha_lower.begin(), bool_alpha_lower.end(),
                 bool_alpha_lower.begin(), ::tolower);

  // Convert to Boolean value.
  std::istringstream is(bool_alpha_lower.c_str());
  is >> std::boolalpha >> result;

  return result;
}

std::string Utility::boolean_to_str(const bool value) {
  std::stringstream ss;
  ss << std::boolalpha << value;
  return ss.str();
}

std::vector<std::string> Utility::split(std::string_view source,
                                        const char& delimiter) {
  std::vector<std::string> result;
  std::stringstream ss{source.data()};
  std::string buffer;

  while (std::getline(ss, buffer, delimiter)) {
    result.push_back(buffer);
  }

  return result;
}

}  // namespace manager::metadata
