/*
 * Copyright 2021-2023 tsurugi project.
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
#include "manager/metadata/dao/json/tables_dao_json.h"

#include <boost/foreach.hpp>
#include <boost/format.hpp>

#include "manager/metadata/common/config.h"
#include "manager/metadata/common/message.h"
#include "manager/metadata/helper/logging_helper.h"
#include "manager/metadata/helper/ptree_helper.h"

// =============================================================================
namespace manager::metadata::db {

using boost::property_tree::ptree;

ErrorCode TablesDaoJson::insert(const boost::property_tree::ptree& object,
                                ObjectId& object_id) const {
  ErrorCode error = ErrorCode::UNKNOWN;

  // Check if the object is already exists.
  if (this->exists(object)) {
    auto opt_name_value = object.get_optional<std::string>(Table::NAME);
    LOG_WARNING << Message::ALREADY_EXISTS << opt_name_value.value();
    return ErrorCode::ALREADY_EXISTS;
  }

  ptree contents;
  // Load the metadata from the JSON file.
  error = this->session()->load_contents(this->database(), kRootNode, contents);
  if (error != ErrorCode::OK) {
    return error;
  }

  // Generate the object ID of the metadata object to be added.
  object_id = this->oid_generator()->generate(kOidKeyNameTable);

  // Copy to the temporary area.
  ptree tmp_object = object;

  // format_version
  tmp_object.put(Table::FORMAT_VERSION, Tables::format_version());
  // generation
  tmp_object.put(Table::GENERATION, Tables::generation());
  // table ID
  tmp_object.put(Table::ID, object_id);

  // Column metadata is not stored here.
  tmp_object.erase(Table::COLUMNS_NODE);

  // Constraint metadata is not stored here.
  tmp_object.erase(Table::CONSTRAINTS_NODE);

  // Add new element.
  ptree node = contents.get_child(kRootNode);

  node.push_back(std::make_pair("", tmp_object));
  contents.put_child(kRootNode, node);

  // Set updated content.
  this->session()->set_contents(this->database(), contents);

  error = ErrorCode::OK;

  return error;
}

ErrorCode TablesDaoJson::select(
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

ErrorCode TablesDaoJson::update(
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

  ptree tables;
  // Get metadata where the given key equals the given value.
  error = find_metadata_object(contents, keys, tables);
  if (error != ErrorCode::OK) {
    return error;
  } else if (tables.empty()) {
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

  BOOST_FOREACH (const auto& node, tables) {
    const auto& table = node.second;

    // Copy management metadata.
    auto table_id =
        table.get_optional<ObjectId>(Table::ID).value_or(INVALID_OBJECT_ID);

    // Copy to the temporary area.
    ptree new_object = object;

    // Update format_version.
    new_object.put(Table::FORMAT_VERSION, Tables::format_version());
    // Update generation.
    new_object.put(Table::GENERATION, Tables::generation());
    // Update object id.
    new_object.put(Table::ID, table_id);

    // The column metadata will not be updated here,
    // so it will be overwritten with the original data.
    new_object.erase(Table::COLUMNS_NODE);
    const auto& opt_column = table.get_child_optional(Table::COLUMNS_NODE);
    if (opt_column) {
      new_object.add_child(Table::COLUMNS_NODE, opt_column.get());
    }

    // The constraint metadata will not be updated here,
    // so it will be overwritten with the original data.
    new_object.erase(Table::CONSTRAINTS_NODE);
    const auto& opt_constraint =
        table.get_child_optional(Table::CONSTRAINTS_NODE);
    if (opt_constraint) {
      new_object.add_child(Table::CONSTRAINTS_NODE, opt_constraint.get());
    }

    // Add new element.
    ptree root_node = contents.get_child(kRootNode);
    root_node.push_back(std::make_pair("", new_object));
    contents.put_child(kRootNode, root_node);
  }

  // Set updated content.
  this->session()->set_contents(this->database(), contents);

  // Set number of updated metadata object.
  if (error == ErrorCode::OK) {
    rows = tables.size();
  }

  return error;
}

ErrorCode TablesDaoJson::remove(
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

ErrorCode TablesDaoJson::find_metadata_object(
    const boost::property_tree::ptree& objects,
    const std::map<std::string_view, std::string_view>& keys,
    boost::property_tree::ptree& object) const {
  ErrorCode error = ErrorCode::UNKNOWN;

  if (keys.empty()) {
    // Extract all metadata.
    LOG_DEBUG << "Select the table metadata. [*]";
  } else {
    // Extract metadata with key values.
    LOG_DEBUG << "Select the table metadata. [" << keys << "]";
  }

  object.clear();
  BOOST_FOREACH (const auto& node, objects.get_child(kRootNode)) {
    const auto& table = node.second;

    if (ptree_helper::is_match(table, keys)) {
      // Add metadata.
      object.push_back(std::make_pair("", table));
    }
  }

  error = ErrorCode::OK;
  return error;
}

ErrorCode TablesDaoJson::delete_metadata_object(
    boost::property_tree::ptree& objects,
    const std::map<std::string_view, std::string_view>& keys,
    std::vector<ObjectId>& object_ids) const {
  ErrorCode error = ErrorCode::UNKNOWN;

  LOG_DEBUG << "Delete the table metadata. [" << keys << "]";

  // Getting a metadata container.
  ptree& tables_node = objects.get_child(kRootNode);

  object_ids.clear();
  for (ptree::iterator it_tables = tables_node.begin();
       it_tables != tables_node.end();) {
    const auto& table = it_tables->second;

    if (ptree_helper::is_match(table, keys)) {
      auto opt_object_id = table.get_optional<ObjectId>(Table::ID);
      auto object_id     = opt_object_id.get_value_or(-1);

      LOG_DEBUG << "TableID: " << object_id;
      // Remove table metadata.
      it_tables = tables_node.erase(it_tables);

      object_ids.push_back(object_id);
    }
    ++it_tables;
  }

  error = ErrorCode::OK;
  return error;
}

}  // namespace manager::metadata::db
