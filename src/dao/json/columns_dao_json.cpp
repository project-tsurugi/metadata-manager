/*
 * Copyright 2021-2023 Project Tsurugi.
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
#include "manager/metadata/dao/json/columns_dao_json.h"

#include <boost/foreach.hpp>

#include "manager/metadata/helper/logging_helper.h"
#include "manager/metadata/helper/ptree_helper.h"

namespace manager::metadata::db {

using boost::property_tree::ptree;

ErrorCode ColumnsDaoJson::insert(const boost::property_tree::ptree& object,
                                     ObjectId& object_id) const {
  ErrorCode error = ErrorCode::UNKNOWN;

  ptree contents;
  // Load the metadata from the JSON file.
  error = this->session()->load_contents(this->database(), kRootNode, contents);
  if (error != ErrorCode::OK) {
    return error;
  }

  // Getting a table id.
  std::string table_id(ptree_helper::ptree_value_to_string<ObjectId>(
      object, Column::TABLE_ID));

  ptree::iterator it_tables;
  ptree& root_node = contents.get_child(kRootNode);
  for (it_tables = root_node.begin(); it_tables != root_node.end();
       it_tables++) {
    std::string oid_value(ptree_helper::ptree_value_to_string<ObjectId>(
        it_tables->second, Table::ID));
    if (oid_value == table_id) {
      break;
    }
  }

  if (it_tables == root_node.end()) {
    LOG_INFO << "Table metadata for the specified table ID does not exist.: "
             << table_id;
    error == ErrorCode::NOT_FOUND;
    return error;
  }

  ptree empty_ptree;
  ptree& table_metadata = it_tables->second;
  if (table_metadata.find(Table::COLUMNS_NODE) == table_metadata.not_found()) {
    table_metadata.add_child(Table::COLUMNS_NODE, empty_ptree);
  }
  auto& columns_node = table_metadata.get_child(Table::COLUMNS_NODE);

  // Copy to the temporary area.
  ptree tmp_metadata = object;

  // Checks for INSERT execution with object-id specified.
  auto opt_object_id = object.get_optional<ObjectId>(Column::ID);
  if (opt_object_id.value_or(-1) > 0) {
    // Column ID is specified, the metadata for that column ID is
    // updated.
    object_id =
        this->oid_generator()->update(kOidKeyNameColumn, opt_object_id.value());
    LOG_INFO
        << "Add column metadata with specified column ID. ColumnID: "
        << opt_object_id.value();
  } else {
    // Column ID is not specified, the column ID is generated and
    // metadata is added.
    object_id = this->oid_generator()->generate(kOidKeyNameColumn);
    tmp_metadata.put(Column::ID, object_id);
  }

  // Add new element.
  columns_node.push_back(std::make_pair("", tmp_metadata));

  // Set updated content.
  this->session()->set_contents(this->database(), contents);

  error = ErrorCode::OK;

  return error;
}

ErrorCode ColumnsDaoJson::select(
    const std::map<std::string_view, std::string_view>& keys,
    boost::property_tree::ptree& object) const {
  ErrorCode error = ErrorCode::UNKNOWN;

  ptree contents;
  // Load the metadata from the JSON file.
  error = this->session()->load_contents(this->database(), kRootNode, contents);
  if (error != ErrorCode::OK) {
    return error;
  }

  // Initialize table metadata list
  object.clear();

  // Getting a metadata object.
  error = find_metadata_object(contents, keys, object);

  return error;
}

ErrorCode ColumnsDaoJson::remove(
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

ErrorCode ColumnsDaoJson::find_metadata_object(
    const boost::property_tree::ptree& objects,
    const std::map<std::string_view, std::string_view>& keys,
    boost::property_tree::ptree& object) const {
  ErrorCode error = ErrorCode::UNKNOWN;

  if (keys.empty()) {
    // Extract all metadata.
    LOG_DEBUG << "Select the column metadata. [*]";
  } else {
    // Extract metadata with key values.
    LOG_DEBUG << "Select the column metadata. [" << keys << "]";
  }

  // If the table ID is specified as the key, pre-qualify the table metadata.
  ptree tables_metadata;
  std::map<std::string_view, std::string_view> table_keys = {};
  auto it = keys.find(Column::TABLE_ID);
  if (it != keys.end()) {
    table_keys.insert({Table::ID, it->second});
  }
  if (!table_keys.empty()) {
    BOOST_FOREACH (const auto& root_node, objects.get_child(kRootNode)) {
      const auto& table = root_node.second;
      if (ptree_helper::is_match(table, table_keys)) {
        tables_metadata.push_back(std::make_pair("", table));
        LOG_DEBUG << " [FIND] TableID: " << table.get<std::string>(Table::ID);
        break;
      } else {
        LOG_DEBUG << " [SKIP] TableID: " << table.get<std::string>(Table::ID);
      }
    }
  } else {
    auto root_node = objects.get_child_optional(kRootNode);
    if (root_node) {
      tables_metadata = root_node.get();
    }
  }
  LOG_DEBUG << "tables filter.: " << objects.size() << " -> "
            << tables_metadata.size();

  object.clear();
  // Extract the relevant metadata.
  BOOST_FOREACH (const auto& table_node, tables_metadata) {
    const auto& table_metadata = table_node.second;
    auto opt_column_node =
        table_metadata.get_child_optional(Table::COLUMNS_NODE);

    if (opt_column_node) {
      BOOST_FOREACH (const auto& column_node, opt_column_node.get()) {
        const auto& column_metadata = column_node.second;

        // If the key value matches, the metadata is added.
        if (ptree_helper::is_match(column_metadata, keys)) {
          // Add metadata.
          object.push_back(std::make_pair("", column_metadata));
        }
      }
    }
  }

  error = ErrorCode::OK;
  return error;
}

ErrorCode ColumnsDaoJson::delete_metadata_object(
    boost::property_tree::ptree& objects,
    const std::map<std::string_view, std::string_view>& keys,
    std::vector<ObjectId>& object_ids) const {
  ErrorCode error = ErrorCode::UNKNOWN;

  LOG_DEBUG << "Delete the column metadata. [" << keys << "]";

  // Getting a metadata container.
  ptree& root_node = objects.get_child(kRootNode);

  std::map<std::string_view, std::string_view> table_keys = {};
  auto it = keys.find(Column::TABLE_ID);
  if (it != keys.end()) {
    table_keys.insert({Table::ID, it->second});
  }

  object_ids.clear();
  for (ptree::iterator it_tables = root_node.begin();
       it_tables != root_node.end(); it_tables++) {
    // If the table ID is specified as the key, pre-qualify the table metadata.
    if (!table_keys.empty()) {
      if (!ptree_helper::is_match(it_tables->second, table_keys)) {
        LOG_DEBUG << " [SKIP] TableID: "
                  << it_tables->second.get<std::string>(Table::ID);
        continue;
      } else {
        LOG_DEBUG << " [FIND] TableID: "
                  << it_tables->second.get<std::string>(Table::ID);
      }
    }

    auto opt_columns =
        it_tables->second.get_child_optional(Table::COLUMNS_NODE);
    if (!opt_columns) {
      continue;
    }

    ptree& columns_node = opt_columns.value();
    for (ptree::iterator it_columns = columns_node.begin();
         it_columns != columns_node.end();) {
      const auto& column_metadata = it_columns->second;

      // If the key value matches, the metadata is added.
      if (ptree_helper::is_match(column_metadata, keys)) {
        auto opt_object_id = column_metadata.get_optional<ObjectId>(Column::ID);
        auto tmp_object_id = opt_object_id.get_value_or(-1);

        LOG_DEBUG << "ColumnID: " << tmp_object_id;

        // Remove column metadata.
        it_columns = columns_node.erase(it_columns);

        object_ids.push_back(tmp_object_id);
      } else {
        ++it_columns;
      }
    }
  }

  error = ErrorCode::OK;
  return error;
}

}  // namespace manager::metadata::db
