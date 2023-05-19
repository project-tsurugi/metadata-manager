/*
 * Copyright 2021-2022 tsurugi project.
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
#include "manager/metadata/dao/json/object_id_json.h"
#include "manager/metadata/helper/logging_helper.h"

// =============================================================================
namespace {

std::unique_ptr<manager::metadata::db::ObjectIdGenerator> oid_generator =
    nullptr;

}  // namespace

// =============================================================================
namespace manager::metadata::db {

using boost::property_tree::ptree;

ErrorCode TablesDaoJson::prepare() {
  ErrorCode error = ErrorCode::UNKNOWN;

  // Filename of the table metadata.
  boost::format filename = boost::format("%s/%s.json") %
                           Config::get_storage_dir_path() % kTablesMetadataName;

  // Connect to the table metadata file.
  error = session_->connect(filename.str(), kRootNode);

  // Create the ObjectId.
  oid_generator = std::make_unique<ObjectIdGenerator>();

  return error;
}

ErrorCode TablesDaoJson::insert(const boost::property_tree::ptree& object,
                                ObjectId& object_id) const {
  ErrorCode error = ErrorCode::UNKNOWN;

  // Check if the object is already exists.
  if (this->exists(object)) {
    auto opt_name_value = object.get_optional<std::string>(Table::NAME);
    LOG_WARNING << Message::ALREADY_EXISTS << opt_name_value.value();
    return ErrorCode::ALREADY_EXISTS;
  }

  // Load the metadata from the JSON file.
  error = session_->load_contents();
  if (error != ErrorCode::OK) {
    return error;
  }

  // Get a metadata contents.
  ptree* contents = session_->get_contents();

  // Generate the object ID of the metadata object to be added.
  object_id = oid_generator->generate(kOidKeyNameTable);

  // Copy to the temporary area.
  ptree tmp_object = object;

  // format_version
  tmp_object.put(Table::FORMAT_VERSION, Tables::format_version());
  // generation
  tmp_object.put(Table::GENERATION, Tables::generation());
  // table ID
  tmp_object.put(Table::ID, object_id);

  // column metadata
  BOOST_FOREACH (ptree::value_type& node,
                 tmp_object.get_child(Table::COLUMNS_NODE)) {
    ptree& column = node.second;

    // Generate the object ID of the metadata object to be added.
    ObjectId columns_id = oid_generator->generate(kOidKeyNameColumn);

    // column ID
    column.put(Column::ID, columns_id);

    // table ID
    column.put(Column::TABLE_ID, object_id);
  }

  // Constraint metadata is not stored here.
  tmp_object.erase(Table::CONSTRAINTS_NODE);

  // Add new element.
  ptree node = contents->get_child(kRootNode);

  node.push_back(std::make_pair("", tmp_object));
  contents->put_child(kRootNode, node);

  error = ErrorCode::OK;

  return error;
}

ErrorCode TablesDaoJson::select_all(
    std::vector<boost::property_tree::ptree>& objects) const {
  ErrorCode error = ErrorCode::UNKNOWN;

  // Load the metadata from the JSON file.
  error = session_->load_contents();
  if (error != ErrorCode::OK) {
    return error;
  }

  // Getting a metadata contents.
  ptree* contents = session_->get_contents();

  // Convert from ptree structure type to vector<ptree>.
  auto node = contents->get_child(kRootNode);
  std::transform(node.begin(), node.end(), std::back_inserter(objects),
                 [](ptree::value_type v) { return v.second; });

  return error;
}

ErrorCode TablesDaoJson::select(std::string_view key,
                                const std::vector<std::string_view>& values,
                                boost::property_tree::ptree& object) const {
  ErrorCode error = ErrorCode::UNKNOWN;

  if (values.size() == 0) {
    LOG_ERROR << Message::PARAMETER_FAILED << "Key value is unspecified.";
    error = ErrorCode::INVALID_PARAMETER;
    return error;
  }

  // Load the metadata from the JSON file.
  error = session_->load_contents();
  if (error != ErrorCode::OK) {
    return error;
  }

  // Initialize table metadata list
  object = {};

  // Getting a metadata contents.
  ptree* contents = session_->get_contents();

  // Getting a metadata object.
  error = get_metadata_object(*contents, key, values[0], object);

  // Convert the error code.
  if (error == ErrorCode::NOT_FOUND) {
    // Get a NOT_FOUND error code corresponding to the key.
    error = get_not_found_error_code(key);
  }

  return error;
}

ErrorCode TablesDaoJson::update(
    std::string_view key, const std::vector<std::string_view>& values,
    const boost::property_tree::ptree& object) const {
  ErrorCode error = ErrorCode::UNKNOWN;

  if (values.size() == 0) {
    LOG_ERROR << Message::PARAMETER_FAILED << "Key value is unspecified.";
    error = ErrorCode::INVALID_PARAMETER;
    return error;
  }

  // Load the metadata from the JSON file.
  error = session_->load_contents();
  if (error != ErrorCode::OK) {
    return error;
  }

  // Getting a metadata contents.
  ptree* contents = session_->get_contents();

  ObjectId table_id;
  // Delete a metadata object.
  error = this->delete_metadata_object(*contents, key, values[0], &table_id);
  if (error != ErrorCode::OK) {
    return error;
  }

  // Copy to the temporary area.
  ptree tmp_table = object;

  // format_version
  tmp_table.put(Table::FORMAT_VERSION, Tables::format_version());

  // generation
  tmp_table.put(Table::GENERATION, Tables::generation());

  // table ID
  tmp_table.put(Table::ID, table_id);

  // column metadata
  BOOST_FOREACH (ptree::value_type& node,
                 tmp_table.get_child(Table::COLUMNS_NODE)) {
    ptree& column       = node.second;
    ObjectId columns_id = 0;

    auto opt_columns_id = column.get_optional<ObjectId>(Column::ID);
    if (opt_columns_id) {
      // Set the specified object ID to the metadata object to be added.
      columns_id = opt_columns_id.value();
      oid_generator->update(kOidKeyNameColumn, columns_id);
    } else {
      // Generate the object ID of the metadata object to be added.
      columns_id = oid_generator->generate(kOidKeyNameColumn);
    }

    // Add or update column and table IDs.
    column.put(Column::ID, columns_id);
    column.put(Column::TABLE_ID, table_id);
  }

  // Constraint metadata is not updated here.
  tmp_table.erase(Table::CONSTRAINTS_NODE);

  // Add new element.
  ptree node = contents->get_child(kRootNode);

  node.push_back(std::make_pair("", tmp_table));
  contents->put_child(kRootNode, node);

  error = ErrorCode::OK;

  return error;
}

ErrorCode TablesDaoJson::remove(std::string_view key,
                                const std::vector<std::string_view>& values,
                                ObjectId& object_id) const {
  ErrorCode error = ErrorCode::UNKNOWN;

  if (values.size() == 0) {
    LOG_ERROR << Message::PARAMETER_FAILED << "Key value is unspecified.";
    error = ErrorCode::INVALID_PARAMETER;
    return error;
  }

  // Load the metadata from the JSON file.
  error = session_->load_contents();
  if (error != ErrorCode::OK) {
    return error;
  }

  // Getting a metadata contents.
  ptree* contents = session_->get_contents();

  // Delete a metadata object.
  error = this->delete_metadata_object(*contents, key, values[0], &object_id);

  return error;
}

/* =============================================================================
 * Private method area
 */

ErrorCode TablesDaoJson::get_metadata_object(
    const boost::property_tree::ptree& objects, std::string_view key,
    std::string_view value, boost::property_tree::ptree& object) const {
  ErrorCode error = ErrorCode::UNKNOWN;

  LOG_DEBUG << "get_metadata_object \"" << key << "\"=\"" << value << "\"";

  error = ErrorCode::NOT_FOUND;
  BOOST_FOREACH (const ptree::value_type& node, objects.get_child(kRootNode)) {
    const ptree& metadata = node.second;

    boost::optional<std::string> opt_key_value =
        metadata.get_optional<std::string>(key.data());
    if (!opt_key_value) {
      LOG_DEBUG << "\"" << key << "\" not found." << value;
      error = ErrorCode::NOT_FOUND;
      break;
    }

    if (opt_key_value.value() == value) {
      object = metadata;
      error  = ErrorCode::OK;
      break;
    }
  }
  return error;
}

ErrorCode TablesDaoJson::delete_metadata_object(
    boost::property_tree::ptree& objects, std::string_view key,
    std::string_view value, ObjectId* object_id) const {
  ErrorCode error = ErrorCode::UNKNOWN;

  LOG_DEBUG << "Remove table metadata => "
            << "metadata " << (objects.empty() ? "empty" : "exists") << ", "
            << key << "=\"" << value << "\"";

  // Initialize the error code.
  error = get_not_found_error_code(key);

  // Getting a metadata container.
  ptree& tables_node = objects.get_child(kRootNode);

  for (ptree::iterator it_tables = tables_node.begin();
       it_tables != tables_node.end();) {
    const ptree& metadata = it_tables->second;

    // Get the value of the key.
    auto opt_key_value = metadata.get_optional<std::string>(key.data());
    // If the key value matches, the metadata is removed.
    if (opt_key_value && (opt_key_value.value() == value)) {
      if (object_id != nullptr) {
        auto opt_oid_value = metadata.get_optional<ObjectId>(Table::ID);
        *object_id         = opt_oid_value.get_value_or(-1);
      }
      error = ErrorCode::OK;
      // Remove table metadata.
      it_tables = tables_node.erase(it_tables);

      LOG_DEBUG << "Remove table metadata. " << key << "=\"" << value << "\"";
      break;
    } else {
      it_tables++;
    }
  }

  return error;
}

}  // namespace manager::metadata::db
