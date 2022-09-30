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
#include <boost/property_tree/ptree.hpp>

#include "manager/metadata/common/config.h"
#include "manager/metadata/common/message.h"
#include "manager/metadata/dao/json/object_id_json.h"
#include "manager/metadata/error_code.h"
#include "manager/metadata/helper/logging_helper.h"
#include "manager/metadata/metadata.h"
#include "manager/metadata/tables.h"

// =============================================================================
namespace {

std::unique_ptr<manager::metadata::db::json::ObjectId> object_id = nullptr;

}  // namespace

// =============================================================================
namespace manager::metadata::db::json {

using boost::property_tree::ptree;

/**
 * @brief Prepare to access the JSON file of table metadata.
 * @param none.
 * @return ErrorCode::OK if success, otherwise an error code.
 */
ErrorCode TablesDAO::prepare() const {
  ErrorCode error = ErrorCode::UNKNOWN;

  // Filename of the table metadata.
  boost::format filename = boost::format("%s/%s.json") % Config::get_storage_dir_path() %
                           std::string(TablesDAO::TABLES_METADATA_NAME);

  // Connect to the table metadata file.
  error = session_manager_->connect(filename.str(), TablesDAO::TABLES_NODE);

  // Create the ObjectId.
  object_id = std::make_unique<ObjectId>();

  return error;
}

/**
 * @brief Add metadata object to metadata table file.
 * @param table_metadata  [in]  one table metadata to add.
 * @param table_id        [out] table id.
 * @return ErrorCode::OK if success, otherwise an error code.
 */

ErrorCode TablesDAO::insert_table_metadata(const boost::property_tree::ptree& table_metadata,
                                           ObjectIdType& table_id) const {
  ErrorCode error = ErrorCode::UNKNOWN;

  // Load the metadata from the JSON file.
  error = session_manager_->load_object();
  if (error != ErrorCode::OK) {
    return error;
  }

  // Getting a metadata container.
  ptree* container = session_manager_->get_container();

  // Getting a metadata object.
  auto optional_name = table_metadata.get_optional<std::string>(Tables::NAME);
  ptree table_name_searched;
  error = get_metadata_object(*container, Tables::NAME, optional_name.get(), table_name_searched);
  if (error == ErrorCode::OK) {
    LOG_WARNING << Message::ALREADY_EXISTS << optional_name.get();
    error = ErrorCode::ALREADY_EXISTS;
    return error;
  }

  // Copy to the temporary area.
  ptree tmp_table = table_metadata;

  // format_version
  tmp_table.put(Tables::FORMAT_VERSION, Tables::format_version());

  // generation
  tmp_table.put(Tables::GENERATION, Tables::generation());

  // Generate the object ID of the metadata object to be added.
  table_id = object_id->generate(OID_KEY_NAME_TABLE);

  // table ID
  tmp_table.put(Tables::ID, table_id);

  // column metadata
  BOOST_FOREACH (ptree::value_type& node, tmp_table.get_child(Tables::COLUMNS_NODE)) {
    ptree& column = node.second;

    // Generate the object ID of the metadata object to be added.
    ObjectIdType columns_id = object_id->generate(OID_KEY_NAME_COLUMN);

    // column ID
    column.put(Tables::Column::ID, columns_id);

    // table ID
    column.put(Tables::Column::TABLE_ID, table_id);
  }

  // Constraint metadata is not stored here.
  tmp_table.erase(Tables::CONSTRAINTS_NODE);

  // Add new element.
  ptree node = container->get_child(TablesDAO::TABLES_NODE);

  node.push_back(std::make_pair("", tmp_table));
  container->put_child(TablesDAO::TABLES_NODE, node);

  error = ErrorCode::OK;

  return error;
}

/**
 * @brief Get metadata object from a metadata table file.
 * @param object_key      [in]  key. column name of a table metadata table.
 * @param object_value    [in]  value to be filtered.
 * @param table_metadata  [out] table metadata to get, where the given key equals the given value.
 * @retval ErrorCode::OK if success,
 * @retval ErrorCode::ID_NOT_FOUND if the table id does not exist.
 * @retval ErrorCode::NAME_NOT_FOUND if the table name does not exist.
 * @retval otherwise an error code.
 */
ErrorCode TablesDAO::select_table_metadata(std::string_view object_key,
                                           std::string_view object_value,
                                           boost::property_tree::ptree& table_metadata) const {
  ErrorCode error = ErrorCode::UNKNOWN;

  // Load the meta data from the JSON file.
  error = session_manager_->load_object();
  if (error != ErrorCode::OK) {
    return error;
  }

  // Getting a metadata container.
  ptree* container = session_manager_->get_container();

  // Getting a metadata object.
  error = get_metadata_object(*container, object_key, object_value, table_metadata);

  // Convert the error code.
  if (error == ErrorCode::NOT_FOUND) {
    if (object_key == Tables::ID) {
      error = ErrorCode::ID_NOT_FOUND;
    } else if (object_key == Tables::NAME) {
      error = ErrorCode::NAME_NOT_FOUND;
    }
  }

  return error;
}

/**
 * @brief Get all metadata objects from a metadata table file.
 *   If the table metadata does not exist, return the container as empty.
 * @param table_container  [out] all table metadata.
 * @return ErrorCode::OK if success, otherwise an error code.
 */
ErrorCode TablesDAO::select_table_metadata(
    std::vector<boost::property_tree::ptree>& table_container) const {
  ErrorCode error = ErrorCode::UNKNOWN;

  // Load the meta data from the JSON file.
  error = session_manager_->load_object();
  if (error != ErrorCode::OK) {
    return error;
  }

  // Getting a metadata container.
  ptree* container = session_manager_->get_container();

  // Convert from ptree structure type to vector<ptree>.
  auto node = container->get_child(TablesDAO::TABLES_NODE);
  std::transform(node.begin(), node.end(), std::back_inserter(table_container),
                 [](ptree::value_type v) { return v.second; });

  return error;
}

/**
 * @brief Executes an UPDATE statement to update the table metadata table with the specified
 *   table metadata.
 * @param table_id        [in]  table id.
 * @param table_metadata  [in]  table metadata object to be updated.
 * @return ErrorCode::OK if success, otherwise an error code.
 */
ErrorCode TablesDAO::update_table_metadata(
    const ObjectIdType table_id, const boost::property_tree::ptree& table_metadata) const {
  ErrorCode error = ErrorCode::UNKNOWN;

  // Load the meta data from the JSON file.
  error = session_manager_->load_object();
  if (error != ErrorCode::OK) {
    return error;
  }

  // Getting a metadata container.
  ptree* container = session_manager_->get_container();

  // Delete a metadata object.
  error = this->delete_metadata_object(*container, Tables::ID, std::to_string(table_id), nullptr);
  if (error != ErrorCode::OK) {
    return error;
  }

  // Copy to the temporary area.
  ptree tmp_table = table_metadata;

  // format_version
  tmp_table.put(Tables::FORMAT_VERSION, Tables::format_version());

  // generation
  tmp_table.put(Tables::GENERATION, Tables::generation());

  // table ID
  tmp_table.put(Tables::ID, table_id);

  // column metadata
  BOOST_FOREACH (ptree::value_type& node, tmp_table.get_child(Tables::COLUMNS_NODE)) {
    ptree& column           = node.second;
    ObjectIdType columns_id = 0;

    auto optional_columns_id = column.get_optional<ObjectIdType>(Tables::Column::ID);
    if (optional_columns_id) {
      // Set the specified object ID to the metadata object to be added.
      columns_id = optional_columns_id.value();
      object_id->update(OID_KEY_NAME_COLUMN, columns_id);
    } else {
      // Generate the object ID of the metadata object to be added.
      columns_id = object_id->generate(OID_KEY_NAME_COLUMN);
    }

    // Add or update column and table IDs.
    column.put(Tables::Column::ID, columns_id);
    column.put(Tables::Column::TABLE_ID, table_id);
  }

  // Constraint metadata is not updated here.
  tmp_table.erase(Tables::CONSTRAINTS_NODE);

  // Add new element.
  ptree node = container->get_child(TablesDAO::TABLES_NODE);

  node.push_back(std::make_pair("", tmp_table));
  container->put_child(TablesDAO::TABLES_NODE, node);

  error = ErrorCode::OK;

  return error;
}

/**
 * @brief Delete a metadata object from a metadata table file.
 * @param object_key    [in]  key. column name of a table metadata table.
 * @param object_value  [in]  value to be filtered.
 * @param table_id      [out]  table id of the row deleted.
 * @retval ErrorCode::OK if success.
 * @retval ErrorCode::ID_NOT_FOUND if the table id does not exist.
 * @retval ErrorCode::NAME_NOT_FOUND if the table name does not exist.
 * @retval otherwise an error code.
 */
ErrorCode TablesDAO::delete_table_metadata(std::string_view object_key,
                                           std::string_view object_value,
                                           ObjectIdType& table_id) const {
  ErrorCode error = ErrorCode::UNKNOWN;

  // Load the meta data from the JSON file.
  error = session_manager_->load_object();
  if (error != ErrorCode::OK) {
    return error;
  }

  // Getting a metadata container.
  ptree* container = session_manager_->get_container();

  // Delete a metadata object.
  error = this->delete_metadata_object(*container, object_key, object_value, &table_id);

  return error;
}

/* =============================================================================
 * Private method area
 */

/**
 * @brief Get metadata-object.
 * @param container       [in]  metadata container.
 * @param object_key      [in]  key. column name of a table metadata table.
 * @param object_value    [in]  value to be filtered.
 * @param table_metadata  [out] metadata-object with the specified name.
 * @return ErrorCode::OK if success, otherwise an error code.
 */
ErrorCode TablesDAO::get_metadata_object(const boost::property_tree::ptree& container,
                                         std::string_view object_key, std::string_view object_value,
                                         boost::property_tree::ptree& table_metadata) const {
  ErrorCode error = ErrorCode::UNKNOWN;

  LOG_DEBUG << "get_metadata metadata:" << (container.empty() ? "empty" : "exists") << " \""
            << object_key << "\"=\"" << object_value << "\"";

  error = ErrorCode::NOT_FOUND;
  BOOST_FOREACH (const ptree::value_type& node, container.get_child(TablesDAO::TABLES_NODE)) {
    const ptree& temp_obj = node.second;

    boost::optional<std::string> value = temp_obj.get_optional<std::string>(object_key.data());
    if (!value) {
      LOG_DEBUG << "\"" << object_key << "\" not found." << object_value;
      error = ErrorCode::NOT_FOUND;
      break;
    }

    if (value.get() == object_value) {
      table_metadata = temp_obj;
      error          = ErrorCode::OK;
      break;
    }
  }
  return error;
}

/**
 * @brief Delete a metadata object from a metadata table file.
 * @param container     [in/out] metadata container.
 * @param object_key    [in]     key. column name of a table metadata table.
 * @param object_value  [in]     value to be filtered.
 * @param table_id      [out]    table id of the row deleted.
 * @retval ErrorCode::OK if success.
 * @retval ErrorCode::ID_NOT_FOUND if the table id does not exist.
 * @retval ErrorCode::NAME_NOT_FOUND if the table name does not exist.
 * @retval otherwise an error code.
 */
ErrorCode TablesDAO::delete_metadata_object(boost::property_tree::ptree& container,
                                            std::string_view object_key,
                                            std::string_view object_value,
                                            ObjectIdType* table_id) const {
  ErrorCode error = ErrorCode::UNKNOWN;

  LOG_DEBUG << "delete_metadata_object metadata:" << (container.empty() ? "empty" : "exists")
            << " \"" << object_key << "\"=\"" << object_value << "\"";;

  // Initialize the error code.
  if (object_key == Tables::ID) {
    error = ErrorCode::ID_NOT_FOUND;
  } else if (object_key == Tables::NAME) {
    error = ErrorCode::NAME_NOT_FOUND;
  } else {
    error = ErrorCode::NOT_FOUND;
  }

  // Getting a metadata container.
  ptree& node = container.get_child(TablesDAO::TABLES_NODE);

  for (ptree::iterator it = node.begin(); it != node.end();) {
    const ptree& temp_obj                  = it->second;
    boost::optional<std::string> object_id = temp_obj.get_optional<std::string>(Tables::ID);

    if (object_key == Tables::ID) {
      // Delete metadata with table-id as a key.
      if (object_id && (object_id.get() == object_value)) {
        it = node.erase(it);
        if (table_id != nullptr) {
          *table_id = std::stoul(object_id.get());
        }
        error = ErrorCode::OK;
        break;
      } else {
        ++it;
      }
    } else if (object_key == Tables::NAME) {
      // Delete metadata with table-name as a key.
      boost::optional<std::string> name = temp_obj.get_optional<std::string>(Tables::NAME);

      if (name && (name.get() == object_value)) {
        if (object_id) {
          if (table_id != nullptr) {
            *table_id = std::stoul(object_id.get());
          }
          error = ErrorCode::OK;
          it    = node.erase(it);
        } else {
          error = ErrorCode::UNKNOWN;
        }
        break;
      } else {
        ++it;
      }
    } else {
      break;
    }
  }

  return error;
}

}  // namespace manager::metadata::db::json
