/*
 * Copyright 2022-2023 tsurugi project.
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
#include "manager/metadata/dao/json/index_dao_json.h"

#include <boost/foreach.hpp>
#include <boost/format.hpp>

#include "manager/metadata/common/config.h"
#include "manager/metadata/common/message.h"
#include "manager/metadata/helper/logging_helper.h"
#include "manager/metadata/helper/ptree_helper.h"
#include "manager/metadata/indexes.h"

namespace manager::metadata::db {

using boost::property_tree::ptree;

ErrorCode IndexDaoJson::insert(
    const boost::property_tree::ptree& object,
    ObjectId& object_id) const {
  ErrorCode error = ErrorCode::UNKNOWN;

  // Check if the object is already exists.
  if (this->exists(object)) {
    return ErrorCode::ALREADY_EXISTS;
  }

  ptree contents;
  // Load the metadata from the JSON file.
  error = this->session()->load_contents(this->database(), kRootNode, contents);
  if (error != ErrorCode::OK) {
    return error;
  }

  // Copy to the temporary area.
  ptree temp_obj = object;

  // Generate the object ID of the metadata object to be added.
  object_id = this->oid_generator()->generate(kOidKeyNameIndex);

  // Generate management metadata.
  temp_obj.put<int64_t>(Object::FORMAT_VERSION, Indexes::format_version());
  temp_obj.put<int64_t>(Object::GENERATION, Indexes::generation());
  temp_obj.put<ObjectId>(Object::ID, object_id);

  ptree root = contents.get_child(kRootNode);
  root.push_back(std::make_pair("", temp_obj));
  contents.put_child(kRootNode, root);

  // Set updated content.
  this->session()->set_contents(this->database(), contents);

  // The metadata object is NOT yet persisted, persisted on commit.

  return  ErrorCode::OK;
}

ErrorCode IndexDaoJson::select_all(
    std::vector<boost::property_tree::ptree>& objects) const {
  
  ErrorCode error = ErrorCode::UNKNOWN;

  ptree contents;
  // Load the metadata from the JSON file.
  error = this->session()->load_contents(this->database(), kRootNode, contents);
  if (error != ErrorCode::OK) {
    return error;
  }

  // Convert from ptree structure type to vector<ptree>.
  auto node = contents.get_child(kRootNode);
  std::transform(node.begin(), node.end(), std::back_inserter(objects),
                 [](ptree::value_type v) { return v.second; });

  return error;
}

ErrorCode IndexDaoJson::select(
    std::string_view key, const std::vector<std::string_view>& values,
    boost::property_tree::ptree& object) const {
  ErrorCode error = ErrorCode::UNKNOWN;

  if (values.size() == 0) {
    LOG_ERROR << Message::PARAMETER_FAILED << "Key value is unspecified.";
    error = ErrorCode::INVALID_PARAMETER;
    return error;
  }

  ptree contents;
  // Load the metadata from the JSON file.
  error = this->session()->load_contents(this->database(), kRootNode, contents);
  if (error != ErrorCode::OK) {
    return error;
  }

  error = find_metadata_object(contents, key, values[0], object);

  return error;
}

/**
 * @brief
 */
ErrorCode IndexDaoJson::update(
    std::string_view key, const std::vector<std::string_view>& values,
    const boost::property_tree::ptree& object) const {
  ErrorCode error = ErrorCode::UNKNOWN;

  if (values.size() == 0) {
    LOG_ERROR << Message::PARAMETER_FAILED << "Key value is unspecified.";
    error = ErrorCode::INVALID_PARAMETER;
    return error;
  }

  ptree contents;
  // Load the metadata from the JSON file.
  error = this->session()->load_contents(this->database(), kRootNode, contents);
  if (error != ErrorCode::OK) {
    return error;
  }

  ptree temp_objects;
  error = find_metadata_object(contents, key, values[0], temp_objects);
  if (error != ErrorCode::OK) {
    return error;
  }
  ptree temp_obj = temp_objects.front().second;

  ObjectId object_id;
  delete_metadata_object(contents, key, values[0], object_id);

  // copy management metadata.
  auto format_version = temp_obj.get_optional<int64_t>(Object::FORMAT_VERSION)
                            .value_or(INVALID_VALUE);
  auto generation = temp_obj.get_optional<int64_t>(Object::GENERATION)
                        .value_or(INVALID_VALUE);
  auto id =
      temp_obj.get_optional<ObjectId>(Object::ID).value_or(INVALID_OBJECT_ID);

  auto new_obj = object;
  new_obj.put<int64_t>(Object::FORMAT_VERSION, format_version);
  new_obj.put<int64_t>(Object::GENERATION, generation);
  new_obj.put<ObjectId>(Object::ID, id);

  ptree root = contents.get_child(kRootNode);
  root.push_back(std::make_pair("", new_obj));
  contents.put_child(kRootNode, root);

  // Set updated content.
  this->session()->set_contents(this->database(), contents);

  error = ErrorCode::OK;

  return error;
}

ErrorCode IndexDaoJson::remove(
    std::string_view key, const std::vector<std::string_view>& values,
    ObjectId& object_id) const {
  ErrorCode error = ErrorCode::UNKNOWN;

  if (values.size() == 0) {
    LOG_ERROR << Message::PARAMETER_FAILED << "Key value is unspecified.";
    error = ErrorCode::INVALID_PARAMETER;
    return error;
  }

  ptree contents;
  // Load the metadata from the JSON file.
  error = this->session()->load_contents(this->database(), kRootNode, contents);
  if (error != ErrorCode::OK) {
    return error;
  }

  error = delete_metadata_object(contents, key, values[0], object_id);
  if (error == ErrorCode::OK) {
    // Set updated content.
    this->session()->set_contents(this->database(), contents);
  }

  return error;
}

/* =============================================================================
 * Private method area
 */

ErrorCode IndexDaoJson::find_metadata_object(
    const boost::property_tree::ptree& objects, std::string_view key,
    std::string_view value, boost::property_tree::ptree& object) const {
  ErrorCode error = ErrorCode::UNKNOWN;

  object.clear();
  BOOST_FOREACH (const auto& node, objects.get_child(IndexDaoJson::kRootNode)) {
    const auto& temp_obj = node.second;

    // Get the value of the key.
    std::string data_value(
        ptree_helper::ptree_value_to_string<std::string>(temp_obj, key));
    // If the key value matches, the metadata is added.
    if (data_value == value) {
      // Add metadata.
      object.push_back(std::make_pair("", temp_obj));
    }
  }
  error = (!object.empty() ? ErrorCode::OK : get_not_found_error_code(key));

  return error;
}

ErrorCode IndexDaoJson::delete_metadata_object(
    boost::property_tree::ptree& objects, std::string_view key,
    std::string_view value, ObjectId& object_id) const {
  ErrorCode error = ErrorCode::UNKNOWN;

  // Initialize the error code.
  error = Dao::get_not_found_error_code(key);

  object_id   = -1;
  ptree& node = objects.get_child(IndexDaoJson::kRootNode);
  for (ptree::iterator ite = node.begin(); ite != node.end();) {
    const auto& temp_obj = ite->second;

    // Get the value of the key.
    std::string data_value(
        ptree_helper::ptree_value_to_string<std::string>(temp_obj, key));
    // If the key value matches, the metadata is removed.
    if (data_value == value) {
      auto opt_oid_value = temp_obj.get_optional<ObjectId>(Object::ID);
      auto tmp_object_id = opt_oid_value.get_value_or(-1);

      LOG_DEBUG << "Remove index metadata. " << key << "=\"" << value
                << "\" ID=" << tmp_object_id;

      // Remove index metadata.
      ite = node.erase(ite);

      object_id = (object_id == -1 ? tmp_object_id : object_id);
      error     = ErrorCode::OK;
    }
    ++ite;
  }

  return error;
}

}  // namespace manager::metadata::db
