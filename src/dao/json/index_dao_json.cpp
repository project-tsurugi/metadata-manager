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
#include "manager/metadata/dao/json/index_dao_json.h"

#include <boost/foreach.hpp>

#include "manager/metadata/common/message.h"
#include "manager/metadata/helper/logging_helper.h"
#include "manager/metadata/indexes.h"

namespace manager::metadata::db {

using boost::property_tree::ptree;

ErrorCode IndexDaoJson::insert(const boost::property_tree::ptree& object,
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

  return ErrorCode::OK;
}

ErrorCode IndexDaoJson::select(
    const std::map<std::string_view, std::string_view>& keys,
    boost::property_tree::ptree& object) const {
  ErrorCode error = ErrorCode::UNKNOWN;

  ptree contents;
  // Load the metadata from the JSON file.
  error = this->session()->load_contents(this->database(), kRootNode, contents);
  if (error != ErrorCode::OK) {
    return error;
  }

  // Get metadata where the given key equals the given value.
  error = find_metadata_object(contents, keys, object);

  return error;
}

ErrorCode IndexDaoJson::update(
    const std::map<std::string_view, std::string_view>& keys,
    const boost::property_tree::ptree& object, uint64_t& rows) const {
  ErrorCode error = ErrorCode::UNKNOWN;

  if (keys.empty()) {
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

  ptree indexes;
  // Get metadata where the given key equals the given value.
  error = find_metadata_object(contents, keys, indexes);
  if (error != ErrorCode::OK) {
    return error;
  } else if (indexes.empty()) {
    rows  = 0;
    error = ErrorCode::OK;
    return error;
  }

  // Delete a metadata object.
  std::vector<ObjectId> removed_ids;
  error = this->delete_metadata_object(contents, keys, removed_ids);
  if (error != ErrorCode::OK) {
    return error;
  }

  BOOST_FOREACH (const auto& node, indexes) {
    const auto& index = node.second;

    // Copy management metadata.
    auto index_id =
        index.get_optional<ObjectId>(Indexes::ID).value_or(INVALID_OBJECT_ID);

    // Copy to the temporary area.
    ptree new_object = object;

    // Update format_version.
    new_object.put(Index::FORMAT_VERSION, Indexes::format_version());
    // Update generation.
    new_object.put(Index::GENERATION, Indexes::generation());
    // Update object id.
    new_object.put(Index::ID, index_id);

    // Add new element.
    ptree root_node = contents.get_child(kRootNode);
    root_node.push_back(std::make_pair("", new_object));
    contents.put_child(kRootNode, root_node);
  }

  // Set updated content.
  this->session()->set_contents(this->database(), contents);

  // Set number of updated metadata object.
  if (error == ErrorCode::OK) {
    rows = indexes.size();
  }

  return error;
}

ErrorCode IndexDaoJson::remove(
    const std::map<std::string_view, std::string_view>& keys,
    std::vector<ObjectId>& object_ids) const {
  ErrorCode error = ErrorCode::UNKNOWN;

  ptree contents;
  // Load the metadata from the JSON file.
  error = this->session()->load_contents(this->database(), kRootNode, contents);
  if (error != ErrorCode::OK) {
    return error;
  }

  // Delete a metadata object.
  error = this->delete_metadata_object(contents, keys, object_ids);

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
    const boost::property_tree::ptree& objects,
    const std::map<std::string_view, std::string_view>& keys,
    boost::property_tree::ptree& object) const {
  ErrorCode error = ErrorCode::UNKNOWN;

  if (keys.empty()) {
    // Extract all metadata.
    LOG_DEBUG << "Select the index metadata. [*]";
  } else {
    // Extract metadata with key values.
    LOG_DEBUG << "Select the index metadata. [" << keys << "]";
  }

  object.clear();
  BOOST_FOREACH (const auto& node, objects.get_child(kRootNode)) {
    const auto& index = node.second;

    if (ptree_helper::is_match(index, keys)) {
      // Add metadata.
      object.push_back(std::make_pair("", index));
    }
  }

  error = ErrorCode::OK;
  return error;
}

ErrorCode IndexDaoJson::delete_metadata_object(
    boost::property_tree::ptree& objects,
    const std::map<std::string_view, std::string_view>& keys,
    std::vector<ObjectId>& object_ids) const {
  ErrorCode error = ErrorCode::UNKNOWN;

  LOG_DEBUG << "Delete the index metadata. [" << keys << "]";

  // Getting a metadata container.
  ptree& indexes_node = objects.get_child(kRootNode);

  object_ids.clear();
  for (ptree::iterator it_indexes = indexes_node.begin();
       it_indexes != indexes_node.end();) {
    const auto& index = it_indexes->second;

    if (ptree_helper::is_match(index, keys)) {
      auto opt_object_id = index.get_optional<ObjectId>(Index::ID);
      auto object_id     = opt_object_id.get_value_or(-1);

      LOG_DEBUG << "Remove index metadata. " << keys << " ID=" << object_id;
      LOG_DEBUG << "IndexID: " << object_id;

      // Remove table metadata.
      it_indexes = indexes_node.erase(it_indexes);

      object_ids.push_back(object_id);
    }
    ++it_indexes;
  }

  error = ErrorCode::OK;
  return error;
}

}  // namespace manager::metadata::db
