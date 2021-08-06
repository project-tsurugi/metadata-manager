/*
 * Copyright 2020 tsurugi project.
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
#include <boost/optional.hpp>
#include <boost/property_tree/ptree.hpp>

#include "manager/metadata/dao/common/config.h"
#include "manager/metadata/dao/json/object_id.h"
#include "manager/metadata/error_code.h"
#include "manager/metadata/metadata.h"
#include "manager/metadata/tables.h"

// =============================================================================
namespace manager::metadata::db::json {

using boost::property_tree::ptree;
using manager::metadata::ErrorCode;

/**
 *  @brief  Prepare to access the JSON file of table metadata.
 *  @param  none.
 *  @return  ErrorCode::OK if success, otherwise an error code.
 */
ErrorCode TablesDAO::prepare() const {
  // Filename of the table metadata.
  boost::format filename = boost::format("%s/%s.json") %
                           Config::get_storage_dir_path() %
                           std::string(TablesDAO::TABLE_NAME);

  // Connect to the table metadata file.
  ErrorCode error =
      session_manager_->connect(filename.str(), Tables::TABLES_NODE);

  return error;
}

/**
 *  @brief  This feature is not supported, so it will always return
 *      ErrorCode::NOT_SUPPORTED.
 *  @param  (reltuples)  [in]  the number of rows to update.
 *  @param  (table_id)   [in]  table id.
 *  @return  ErrorCode::NOT_SUPPORTED.
 */
ErrorCode TablesDAO::update_reltuples_by_table_id(float reltuples,
                                                  ObjectIdType table_id) const {
  // This feature is not supported.
  return ErrorCode::NOT_SUPPORTED;
}

/**
 *  @brief  This feature is not supported, so it will always return
 *      ErrorCode::NOT_SUPPORTED.
 *  @param  (reltuples)   [in]  the number of rows to update.
 *  @param  (table_name)  [in]  table name.
 *  @param  (table_id)    [out] table id of the row updated.
 *  @return  ErrorCode::NOT_SUPPORTED.
 */
ErrorCode TablesDAO::update_reltuples_by_table_name(
    float reltuples, std::string_view table_name,
    ObjectIdType &table_id) const {
  // This feature is not supported.
  return ErrorCode::NOT_SUPPORTED;
}

/**
 *  @brief  This feature is not supported, so it will always return
 *     ErrorCode::NOT_SUPPORTED.
 *  @param  (table_id)         [in]  table id.
 *  @param  (table_statistic)  [out] table statistic to get.
 *  @return  ErrorCode::NOT_SUPPORTED.
 */
ErrorCode TablesDAO::select_table_statistic_by_table_id(
    ObjectIdType table_id, TableStatistic &table_statistic) const {
  // This feature is not supported.
  return ErrorCode::NOT_SUPPORTED;
}

/**
 *  @brief  This feature is not supported, so it will always return
 *      ErrorCode::NOT_SUPPORTED.
 *  @param  (table_name)        [in]  table name.
 *  @param  (table_statistic)   [out] table statistic to get.
 *  @return  ErrorCode::NOT_SUPPORTED.
 */
ErrorCode TablesDAO::select_table_statistic_by_table_name(
    std::string_view table_name, TableStatistic &table_statistic) const {
  // This feature is not supported.
  return ErrorCode::NOT_SUPPORTED;
}

/**
 *  @brief  Add metadata-object to metadata-table.
 *  @param  (table)     [in]   one table metadata to add.
 *  @param  (table_id)  [out]  table id.
 *  @return  ErrorCode::OK if success, otherwise an error code.
 */

ErrorCode TablesDAO::insert_table_metadata(boost::property_tree::ptree &table,
                                           ObjectIdType &table_id) const {
  ErrorCode error = ErrorCode::UNKNOWN;

  ptree table_name_searched;
  boost::optional<std::string> name =
      table.get_optional<std::string>(Metadata::NAME);

  // Load the metadata from the JSON file.
  error = session_manager_->load_object();
  if (error != ErrorCode::OK) {
    return error;
  }

  if (get_metadata_object(name.get(), table_name_searched) == ErrorCode::OK) {
    return ErrorCode::TABLE_NAME_ALREADY_EXISTS;
  }

  // generate the object ID of the added metadata-object.
  table_id = ObjectId::generate(TABLE_NAME);
  table.put(Tables::ID, table_id);

  // column metadata
  BOOST_FOREACH (ptree::value_type &node,
                 table.get_child(Tables::COLUMNS_NODE)) {
    ptree &column = node.second;
    // column ID
    column.put(Tables::Column::ID, ObjectId::generate("column"));
    // table ID
    column.put(Tables::Column::TABLE_ID, table_id);
  }

  // Getting a metadata object.
  ptree *meta_object = session_manager_->get_container();

  // add new element.
  ptree node = meta_object->get_child(Tables::TABLES_NODE);

  node.push_back(std::make_pair("", table));
  meta_object->put_child(Tables::TABLES_NODE, node);

  error = ErrorCode::OK;

  return error;
}

/**
 *  @brief  Executes a SELECT statement to get table metadata rows from the
 * table metadata table, where the given key equals the given value.
 *  @param  (object_key)    [in]  key. column name of a table metadata table.
 *  @param  (object_value)  [in]  value to be filtered.
 *  @param  (object)        [out] table metadatas to get, where the given key
 * equals the given value.
 *  @return  ErrorCode::OK if success, otherwise an error code.
 */
ErrorCode TablesDAO::select_table_metadata(std::string_view object_key,
                                           std::string_view object_value,
                                           ptree &object) const {
  assert(!object_key.empty());
  assert(!object_value.empty());

  ErrorCode error = ErrorCode::UNKNOWN;

  // Load the meta data from the JSON file.
  error = session_manager_->load_object();
  if (error != ErrorCode::OK) {
    return error;
  }

  // Getting a metadata object.
  ptree *meta_object = session_manager_->get_container();

  // Initialization of return value.
  error = ErrorCode::NOT_FOUND;

  BOOST_FOREACH (const ptree::value_type &node,
                 meta_object->get_child(Tables::TABLES_NODE)) {
    const ptree &temp_obj = node.second;

    boost::optional<std::string> value =
        temp_obj.get_optional<std::string>(object_key.data());
    if (!value) {
      return ErrorCode::NOT_FOUND;
    }
    if (!value.get().compare(object_value)) {
      object = temp_obj;
      error = ErrorCode::OK;
      break;
    }
  }

  return error;
}

/**
 *  @brief  Executes DELETE statement to delete table metadata from the table
 * metadata table based on the given table id.
 *  @param  (table_id)  [in]  table id.
 *  @return  ErrorCode::OK if success, otherwise an error code.
 */
ErrorCode TablesDAO::delete_table_metadata_by_table_id(
    ObjectIdType table_id) const {
  ErrorCode error = ErrorCode::UNKNOWN;

  // Load the meta data from the JSON file.
  error = session_manager_->load_object();
  if (error != ErrorCode::OK) {
    return error;
  }

  // Getting a metadata object.
  ptree *meta_object = session_manager_->get_container();

  ptree &node = meta_object->get_child(Tables::TABLES_NODE);

  error = ErrorCode::ID_NOT_FOUND;
  for (ptree::iterator it = node.begin(); it != node.end();) {
    const ptree &temp_obj = it->second;
    boost::optional<ObjectIdType> id =
        temp_obj.get_optional<ObjectIdType>(Tables::ID);
    if (id && (id.get() == table_id)) {
      it = node.erase(it);
      error = ErrorCode::OK;
    } else {
      ++it;
    }
  }

  return error;
}

/**
 *  @brief  Executes DELETE statement to delete table metadata from the table
 * metadata table based on the given table name.
 *  @param  (table_name)  [id]   table name.
 *  @param  (table_id)    [out]  table id of the row deleted.
 *  @return  ErrorCode::OK if success, otherwise an error code.
 */
ErrorCode TablesDAO::delete_table_metadata_by_table_name(
    std::string_view table_name, ObjectIdType &table_id) const {
  assert(!table_name.empty());

  ErrorCode error = ErrorCode::UNKNOWN;

  // Load the meta data from the JSON file.
  error = session_manager_->load_object();
  if (error != ErrorCode::OK) {
    return error;
  }

  // Getting a metadata object.
  ptree *meta_object = session_manager_->get_container();

  ptree &node = meta_object->get_child(Tables::TABLES_NODE);

  error = ErrorCode::NAME_NOT_FOUND;
  for (ptree::iterator it = node.begin(); it != node.end();) {
    const ptree &temp_obj = it->second;
    boost::optional<std::string> name =
        temp_obj.get_optional<std::string>(Tables::NAME);
    boost::optional<ObjectIdType> id =
        temp_obj.get_optional<ObjectIdType>(Tables::ID);
    if (name && (!name.get().compare(table_name))) {
      if (!id) {
        error = ErrorCode::UNKNOWN;
        break;
      }
      table_id = id.get();
      it = node.erase(it);
      error = ErrorCode::OK;
    } else {
      ++it;
    }
  }

  return error;
}

// -----------------------------------------------------------------------------
// Private method area

/**
 *  @brief  Get metadata-object.
 *  @param  (object_name)   [in]  metadata-object name. (Value of "name" key.)
 *  @param  (object)        [out] metadata-object with the specified name.
 *  @return  ErrorCode::OK if success, otherwise an error code.
 */
ErrorCode TablesDAO::get_metadata_object(
    std::string_view object_name, boost::property_tree::ptree &object) const {
  assert(!object_name.empty());

  ErrorCode error = ErrorCode::NAME_NOT_FOUND;

  ptree *meta_object = session_manager_->get_container();

  BOOST_FOREACH (const ptree::value_type &node,
                 meta_object->get_child(Tables::TABLES_NODE)) {
    const ptree &temp_obj = node.second;

    boost::optional<std::string> name =
        temp_obj.get_optional<std::string>(Metadata::NAME);
    if (!name) {
      return ErrorCode::NOT_FOUND;
    }
    if (!name.get().compare(object_name)) {
      object = temp_obj;
      error = ErrorCode::OK;
      break;
    }
  }
  return error;
}

}  // namespace manager::metadata::db::json
