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
#include "manager/metadata/helper/ptree_helper.h"

#include <boost/property_tree/json_parser.hpp>

#include "manager/metadata/common/message.h"
#include "manager/metadata/helper/logging_helper.h"

namespace ptree_helper {

namespace json_parser = boost::property_tree::json_parser;

using boost::property_tree::json_parser_error;
using boost::property_tree::ptree;
using manager::metadata::ErrorCode;
using manager::metadata::Message;

// ==========================================================================
// ptree_helper functions.

/**
 * @brief Make a ptree object from a std::vector object.
 * @param v  [in]  Vector object to be converted.
 * @return Converted object.
 */
boost::property_tree::ptree make_array_ptree(const std::vector<int64_t>& v) {
  ptree child;

  for (const auto& value : v) {
    ptree pt;
    pt.put("", value);
    child.push_back(std::make_pair("", pt));
  }

  return child;
}

/**
 * @brief Make a std::vector object from a ptree object.
 * @param pt  [in]  ptree object to be converted.
 * @param key [in]  Key name of ptree.
 * @return Converted object. If there is no value corresponding to the key,
 *  empty is returned.
 */
std::vector<int64_t> make_vector_int(const boost::property_tree::ptree& pt,
                                     std::string_view key) {
  std::vector<int64_t> v = {};

  auto opt_child = pt.get_child_optional(key.data());
  if (opt_child) {
    std::transform(opt_child.get().begin(), opt_child.get().end(),
                   std::back_inserter(v),
                   [](boost::property_tree::ptree::value_type vt) {
                     return vt.second.get<int64_t>("");
                   });
  }

  return v;
}

/**
 * @brief Converts a JSON string to a property_tree.
 * @param json   [in]  JSON string to be converted to a property_tree.
 * @param ptree  [out] The converted property_tree..
 * @return ErrorCode::OK if success, otherwise an error code.
 */
manager::metadata::ErrorCode json_to_ptree(
    std::string_view json, boost::property_tree::ptree& ptree) {
  ErrorCode error = ErrorCode::UNKNOWN;

  if (!json.empty()) {
    std::stringstream ss;
    ss << json;
    try {
      json_parser::read_json(ss, ptree);
    } catch (json_parser_error& e) {
      LOG_ERROR << Message::READ_JSON_FAILURE << e.what();
      error = ErrorCode::INTERNAL_ERROR;
      return error;
    } catch (...) {
      LOG_ERROR << Message::READ_JSON_FAILURE;
      error = ErrorCode::INTERNAL_ERROR;
      return error;
    }
  }

  return ErrorCode::OK;
}

/**
 * @brief Converts a property_tree to a JSON string.
 * @param ptree  [in]  property_tree to be converted to a JSON string.
 * @param json   [out] The converted JSON string.
 * @return ErrorCode::OK if success, otherwise an error code.
 */
manager::metadata::ErrorCode ptree_to_json(
    const boost::property_tree::ptree& ptree, std::string& json) {
  ErrorCode error = ErrorCode::UNKNOWN;

  if (!ptree.empty()) {
    std::stringstream ss;
    try {
      json_parser::write_json(ss, ptree, false);
    } catch (json_parser_error& e) {
      LOG_ERROR << Message::WRITE_JSON_FAILURE << e.what();
      error = ErrorCode::INVALID_PARAMETER;
      return error;
    } catch (...) {
      LOG_ERROR << Message::WRITE_JSON_FAILURE;
      error = ErrorCode::INVALID_PARAMETER;
      return error;
    }
    json = ss.str();
  }

  return ErrorCode::OK;
}

}  // namespace ptree_helper
