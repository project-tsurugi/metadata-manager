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
using manager::metadata::ErrorCode;
using manager::metadata::Message;

// ==========================================================================
// ptree_helper functions.

boost::property_tree::ptree make_array_ptree(const std::vector<int64_t>& vc) {
  boost::property_tree::ptree child;

  for (const auto& value : vc) {
    boost::property_tree::ptree pt;
    pt.put("", value);
    child.push_back(std::make_pair("", pt));
  }

  return child;
}

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

manager::metadata::ErrorCode json_to_ptree(std::string_view json,
                                           boost::property_tree::ptree& pt) {
  ErrorCode error = ErrorCode::UNKNOWN;

  if (!json.empty()) {
    std::stringstream ss;
    ss << json;
    try {
      json_parser::read_json(ss, pt);
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

manager::metadata::ErrorCode ptree_to_json(
    const boost::property_tree::ptree& pt, std::string& json) {
  ErrorCode error = ErrorCode::UNKNOWN;

  if (!pt.empty()) {
    std::stringstream ss;
    try {
      json_parser::write_json(ss, pt, false);
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

std::string ptree_to_json(const boost::property_tree::ptree& pt) {
  std::string json;

  ptree_to_json(pt, json);

  return json;
}

std::vector<boost::property_tree::ptree> array_to_vector(
    const boost::property_tree::ptree& pt) {
  std::vector<boost::property_tree::ptree> result = {};

  std::transform(
      pt.begin(), pt.end(), std::back_inserter(result),
      [](boost::property_tree::ptree::value_type vt) { return vt.second; });

  return result;
}

template <typename T>
boost::property_tree::ptree vector_to_array(const std::vector<T>& vc) {
  boost::property_tree::ptree root;

  for (const auto& value : vc) {
    boost::property_tree::ptree child;
    child.put("", value);
    root.push_back(std::make_pair("", child));
  }

  return root;
}

bool is_array(const boost::property_tree::ptree& pt) {
  for (const auto& child : pt) {
    if (!child.first.empty()) {
      return false;
    }
  }
  return true;
}

bool is_match(const boost::property_tree::ptree& pt,
              const std::map<std::string_view, std::string_view>& keys) {
  for (const auto& key : keys) {
    // Get the value of the key.
    std::string data_value(ptree_value_to_string<std::string>(pt, key.first));

    // Check if the value matches.
    if (data_value != key.second) {
      // If any one of them is a mismatch, a false is returned.
      return false;
    }
  }
  // If all matches, a true is returned.
  return true;
}

}  // namespace ptree_helper
