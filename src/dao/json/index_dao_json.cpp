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
  temp_obj.put<int64_t>(Object::FORMAT_VERSION, Object::DEFAULT_FORMAT_VERSION);
  temp_obj.put<int64_t>(Object::GENERATION, Object::DEFAULT_GENERATION);
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

  ptree temp_obj;
  error = find_metadata_object(contents, key, values[0], temp_obj);
  if (error != ErrorCode::OK) {
    return error;
  }

  ObjectId object_id;
  delete_metadata_object(contents, key, values[0], object_id);

  // copy management metadata.
  auto format_version = temp_obj.get_optional<int64_t>(Object::FORMAT_VERSION)
                            .value_or(INVALID_VALUE);
  auto generation = temp_obj.get_optional<int64_t>(Object::GENERATION)
                        .value_or(INVALID_VALUE);
  auto id =
      temp_obj.get_optional<ObjectId>(Object::ID).value_or(INVALID_OBJECT_ID);

  temp_obj = object;
  temp_obj.put<int64_t>(Object::FORMAT_VERSION, format_version);
  temp_obj.put<int64_t>(Object::GENERATION, generation);
  temp_obj.put<ObjectId>(Object::ID, id);

  ptree root = contents.get_child(kRootNode);
  root.push_back(std::make_pair("", temp_obj));
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

  // Initialize the error code.
  error = Dao::get_not_found_error_code(key);

  BOOST_FOREACH (const auto& node, objects.get_child(IndexDaoJson::kRootNode)) {
    const auto& temp_obj = node.second;

    std::string key_value(
        ptree_helper::ptree_value_to_string<std::string>(temp_obj, key));
    if (key_value == value) {
      // find the object.
      object = temp_obj;
      error = ErrorCode::OK;
      break;
    }
  }

  return error;
}

ErrorCode IndexDaoJson::delete_metadata_object(
    boost::property_tree::ptree& objects, std::string_view key,
    std::string_view value, ObjectId& object_id) const {
  ErrorCode error = ErrorCode::UNKNOWN;

  // Initialize the error code.
  error = Dao::get_not_found_error_code(key);

  ptree& node = objects.get_child(IndexDaoJson::kRootNode);
  for (ptree::iterator ite = node.begin(); ite != node.end();) {
    const auto& temp_obj = ite->second;
    auto id = temp_obj.get_optional<std::string>(Object::ID);
    if (!id) {
      error = ErrorCode::INTERNAL_ERROR;
      break;
    }

    if (key == Object::ID) {
      if (id.get() == value) {
        // find the object.
        ite = node.erase(ite);
        object_id = std::stoul(id.get());
        error = ErrorCode::OK;
        break;
      }
    } else if (key == Object::NAME) {
      auto name = temp_obj.get_optional<std::string>(Object::NAME);
      if (name && (name.get() == value)) {
        // find the object.
        ite = node.erase(ite);
        object_id = std::stoul(id.get());
        error = ErrorCode::OK;
        break;
      }
    } else {
      // Unsupported keys.
      error = ErrorCode::NOT_SUPPORTED;
      break;
    }
    ++ite;
  }

  return error;
}

}  // namespace manager::metadata::db
