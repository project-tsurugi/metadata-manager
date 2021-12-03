/*
 * Copyright 2021 tsurugi project.
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
#include "manager/metadata/dao/json/tables_dao.h"

#include <boost/foreach.hpp>
#include <boost/format.hpp>
#include <boost/property_tree/ptree.hpp>

#include "manager/metadata/dao/common/config.h"
#include "manager/metadata/dao/json/object_id.h"
#include "manager/metadata/error_code.h"
#include "manager/metadata/metadata.h"
#include "manager/metadata/tables.h"

// =============================================================================
namespace {

std::unique_ptr<manager::metadata::db::json::ObjectId> object_id = nullptr;

}  // namespace

// =============================================================================
namespace manager::metadata::db::json {

using boost::property_tree::ptree;
using manager::metadata::ErrorCode;

/**
 * @brief Prepare to access the JSON file of table metadata.
 * @param none.
 * @return ErrorCode::OK if success, otherwise an error code.
 */
ErrorCode TablesDAO::prepare() const {
  ErrorCode error = ErrorCode::UNKNOWN;

  // Filename of the table metadata.
  boost::format filename = boost::format("%s/%s.json") %
                           Config::get_storage_dir_path() %
                           std::string(TablesDAO::TABLE_NAME);

  // Connect to the table metadata file.
  error = session_manager_->connect(filename.str(), TablesDAO::TABLES_NODE);

  // Create the ObjectId.
  object_id = std::make_unique<ObjectId>();

  return error;
}

/**
 * @brief Add metadata object to metadata table file.
 * @param (table)     [in]   one table metadata to add.
 * @param (table_id)  [out]  table id.
 * @return ErrorCode::OK if success, otherwise an error code.
 */

ErrorCode TablesDAO::insert_table_metadata(
    const boost::property_tree::ptree& table, ObjectIdType& table_id) const {
  ErrorCode error = ErrorCode::UNKNOWN;

  boost::optional<std::string> optional_name =
      table.get_optional<std::string>(Tables::NAME);

  // Load the metadata from the JSON file.
  error = session_manager_->load_object();
  if (error != ErrorCode::OK) {
    return error;
  }

  // Getting a metadata object.
  ptree table_name_searched;
  error = get_metadata_object(Tables::NAME, optional_name.get(),
                              table_name_searched);
  if (error == ErrorCode::OK) {
    error = ErrorCode::TABLE_NAME_ALREADY_EXISTS;
    return error;
  }

  // Copy to the temporary area.
  ptree tmp_table = table;

  // format_version.
  tmp_table.put(Tables::FORMAT_VERSION, Tables::format_version());

  // generation.
  tmp_table.put(Tables::GENERATION, Tables::generation());

  // generate the object ID of the added metadata-object.
  table_id = object_id->generate(TABLE_NAME);
  tmp_table.put(Tables::ID, table_id);

  // column metadata
  BOOST_FOREACH (ptree::value_type& node,
                 tmp_table.get_child(Tables::COLUMNS_NODE)) {
    ptree& column = node.second;
    // column ID
    column.put(Tables::Column::ID, object_id->generate("column"));
    // table ID
    column.put(Tables::Column::TABLE_ID, table_id);
  }

  // Getting a metadata object.
  ptree* meta_object = session_manager_->get_container();

  // add new element.
  ptree node = meta_object->get_child(TablesDAO::TABLES_NODE);

  node.push_back(std::make_pair("", tmp_table));
  meta_object->put_child(TablesDAO::TABLES_NODE, node);

  error = ErrorCode::OK;

  return error;
}

/**
 * @brief Get metadata object from a metadata table file.
 * @param (object_key)    [in]  key. column name of a table metadata table.
 * @param (object_value)  [in]  value to be filtered.
 * @param (object)        [out] table metadata to get,
 *   where the given key equals the given value.
 * @retval ErrorCode::OK if success,
 * @retval ErrorCode::ID_NOT_FOUND if the table id does not exist.
 * @retval ErrorCode::NAME_NOT_FOUND if the table name does not exist.
 * @retval otherwise an error code.
 */
ErrorCode TablesDAO::select_table_metadata(
    std::string_view object_key, std::string_view object_value,
    boost::property_tree::ptree& object) const {
  ErrorCode error = ErrorCode::UNKNOWN;

  // Load the meta data from the JSON file.
  error = session_manager_->load_object();
  if (error != ErrorCode::OK) {
    return error;
  }

  // Getting a metadata object.
  error = get_metadata_object(object_key, object_value, object);

  // Converte the error code.
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
 * @param (container)  [out] all table metadata.
 * @return ErrorCode::OK if success, otherwise an error code.
 */
ErrorCode TablesDAO::select_table_metadata(
    std::vector<boost::property_tree::ptree>& container) const {
  ErrorCode error = ErrorCode::UNKNOWN;

  // Load the meta data from the JSON file.
  error = session_manager_->load_object();
  if (error != ErrorCode::OK) {
    return error;
  }

  // Getting a metadata object.
  ptree* meta_object = session_manager_->get_container();

  // Convert from ptree structure type to vector<ptree>.
  auto node = meta_object->get_child(TablesDAO::TABLES_NODE);
  std::transform(node.begin(), node.end(), std::back_inserter(container),
                 [](ptree::value_type v) { return v.second; });

  return error;
}

/**
 * @brief Delete a metadata object from a metadata table file.
 * @param (object_key)    [in]  key. column name of a table metadata table.
 * @param (object_value)  [in]  value to be filtered.
 * @param (table_id)      [out]  table id of the row deleted.
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

  // Initialize the error code.
  if (object_key == Tables::ID) {
    error = ErrorCode::ID_NOT_FOUND;
  } else if (object_key == Tables::NAME) {
    error = ErrorCode::NAME_NOT_FOUND;
  } else {
    error = ErrorCode::NOT_FOUND;
  }

  // Getting a metadata object.
  ptree* meta_object = session_manager_->get_container();
  ptree& node = meta_object->get_child(TablesDAO::TABLES_NODE);

  for (ptree::iterator it = node.begin(); it != node.end();) {
    const ptree& temp_obj = it->second;
    boost::optional<std::string> object_id =
        temp_obj.get_optional<std::string>(Tables::ID);

    if (object_key == Tables::ID) {
      // Delete metadata with table-id as a key.
      if (object_id && (object_id.get() == object_value)) {
        it = node.erase(it);
        table_id = std::stoul(object_id.get());
        error = ErrorCode::OK;
        break;
      } else {
        ++it;
      }
    } else if (object_key == Tables::NAME) {
      // Delete metadata with table-name as a key.
      boost::optional<std::string> name =
          temp_obj.get_optional<std::string>(Tables::NAME);

      if (name && (name.get() == object_value)) {
        if (object_id) {
          table_id = std::stoul(object_id.get());
          error = ErrorCode::OK;
          it = node.erase(it);
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

/* =============================================================================
 * Private method area
 */

/**
 * @brief Get metadata-object.
 * @param (object_key)    [in]  key. column name of a table metadata table.
 * @param (object_value)  [in]  value to be filtered.
 * @param (object)        [out] metadata-object with the specified name.
 * @return ErrorCode::OK if success, otherwise an error code.
 */
ErrorCode TablesDAO::get_metadata_object(
    std::string_view object_key, std::string_view object_value,
    boost::property_tree::ptree& object) const {
  ErrorCode error = ErrorCode::UNKNOWN;

  ptree* meta_object = session_manager_->get_container();

  error = ErrorCode::NOT_FOUND;
  BOOST_FOREACH (const ptree::value_type& node,
                 meta_object->get_child(TablesDAO::TABLES_NODE)) {
    const ptree& temp_obj = node.second;

    boost::optional<std::string> value =
        temp_obj.get_optional<std::string>(object_key.data());
    if (!value) {
      error = ErrorCode::NOT_FOUND;
      break;
    }
    if (value.get() == object_value) {
      object = temp_obj;
      error = ErrorCode::OK;
      break;
    }
  }
  return error;
}

}  // namespace manager::metadata::db::json
