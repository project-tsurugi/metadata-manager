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

#include <iostream>

#include <boost/property_tree/json_parser.hpp>

#include "manager/metadata/common/message.h"

// =============================================================================
namespace manager::metadata {

namespace json_parser = boost::property_tree::json_parser;
using boost::property_tree::json_parser_error;

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
 * @brief Explicit Template Instantiation for str_to_numeric(int64_t type).
 */
template ErrorCode Utility::str_to_numeric(std::string_view str,
                                            std::int64_t& return_value);

/**
 * @brief Converts a string to a numeric.
 * @param (str)           [in]  String to be converted to a numeric.
 * @param (return_value)  [out] The converted numeric.
 * @return ErrorCode::OK if success, otherwise an error code.
 */
template <typename T>
[[nodiscard]] ErrorCode Utility::str_to_numeric(std::string_view str,
                                                 T& return_value) {
  try {
    return_value = convert_to_numeric<T>(str);
  } catch (...) {
    return ErrorCode::INTERNAL_ERROR;
  }

  return ErrorCode::OK;
}

/**
 * @brief Converts a JSON string to a property_tree.
 * @param (json)   [in]  JSON string to be converted to a property_tree.
 * @param (ptree)  [out] The converted property_tree..
 * @return ErrorCode::OK if success, otherwise an error code.
 */
[[nodiscard]] ErrorCode Utility::json_to_ptree(
    std::string_view json, boost::property_tree::ptree& ptree) {
  ErrorCode error = ErrorCode::UNKNOWN;

  if (!json.empty()) {
    std::stringstream ss;
    ss << json;
    try {
      json_parser::read_json(ss, ptree);
    } catch (json_parser_error& e) {
      std::cerr << Message::READ_JSON_FAILURE << e.what() << std::endl;
      error = ErrorCode::INTERNAL_ERROR;
      return error;
    } catch (...) {
      std::cerr << Message::READ_JSON_FAILURE << std::endl;
      error = ErrorCode::INTERNAL_ERROR;
      return error;
    }
  }

  return ErrorCode::OK;
}

/**
 * @brief Converts a property_tree to a JSON string.
 * @param (ptree)  [in]  property_tree to be converted to a JSON string.
 * @param (json)   [out] The converted JSON string.
 * @return ErrorCode::OK if success, otherwise an error code.
 */
[[nodiscard]] ErrorCode Utility::ptree_to_json(
    const boost::property_tree::ptree& ptree, std::string& json) {
  ErrorCode error = ErrorCode::UNKNOWN;

  if (!ptree.empty()) {
    std::stringstream ss;
    try {
      json_parser::write_json(ss, ptree, false);
    } catch (json_parser_error& e) {
      std::cerr << Message::WRITE_JSON_FAILURE << e.what() << std::endl;
      error = ErrorCode::INVALID_PARAMETER;
      return error;
    } catch (...) {
      std::cerr << Message::WRITE_JSON_FAILURE << std::endl;
      error = ErrorCode::INVALID_PARAMETER;
      return error;
    }
    json = ss.str();
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
float Utility::convert_to_numeric<float>(std::string_view str) {
  return std::stof(str.data());
}

/**
 * @brief Converts a string to a numeric.
 * @param (str)  [in]  String to be converted to a numeric.
 * @return The converted numeric as a int32_t type value.
 */
template <>
std::int32_t Utility::convert_to_numeric<std::int32_t>(std::string_view str) {
  return std::stoi(str.data());
}

/**
 * @brief Converts a string to a numeric.
 * @param (str)  [in]  String to be converted to a numeric.
 * @return The converted numeric as a int64_t type value.
 */
template <>
std::int64_t Utility::convert_to_numeric<std::int64_t>(std::string_view str) {
  return std::stol(str.data());
}

}  // namespace manager::metadata
