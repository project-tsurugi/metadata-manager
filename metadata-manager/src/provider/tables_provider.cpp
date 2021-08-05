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
#include "manager/metadata/provider/tables_provider.h"

#include <boost/foreach.hpp>

#include "manager/metadata/tables.h"

// =============================================================================
namespace manager::metadata::db {

using boost::property_tree::ptree;
using manager::metadata::ErrorCode;

/**
 *  @brief  Initialize and prepare to access the metadata repository.
 *  @return  ErrorCode::OK if success, otherwise an error code.
 */
ErrorCode TablesProvider::init() {
  ErrorCode error = ErrorCode::OK;
  std::shared_ptr<GenericDAO> gdao = nullptr;

  if (tables_dao_ == nullptr) {
    // Get an instance of the TablesDAO class.
    error =
        session_manager_->get_dao(GenericDAO::TableName::TABLES, gdao);
    tables_dao_ = (error == ErrorCode::OK)
                      ? std::static_pointer_cast<TablesDAO>(gdao)
                      : nullptr;
  }

  if ((columns_dao_ == nullptr) && (error == ErrorCode::OK)) {
    // Get an instance of the ColumnsDAO class.
    error =
        session_manager_->get_dao(GenericDAO::TableName::COLUMNS, gdao);
    columns_dao_ = (error == ErrorCode::OK)
                       ? std::static_pointer_cast<ColumnsDAO>(gdao)
                       : nullptr;
  }

  return error;
}

/**
 *  @brief  Add table metadata to table metadata repository.
 *  @param  (object)     [in]  table metadata to add.
 *  @param  (table_id)   [out] ID of the added table metadata.
 *  @return  ErrorCode::OK if success, otherwise an error code.
 */
ErrorCode TablesProvider::add_table_metadata(ptree &object,
                                             ObjectIdType &table_id) {
  // Initialization
  ErrorCode error = init();
  if (error != ErrorCode::OK) {
    return error;
  }

  error = session_manager_->start_transaction();
  if (error != ErrorCode::OK) {
    return error;
  }

  // Add table metadata object to table metadata table.
  error = tables_dao_->insert_table_metadata(object, table_id);
  if (error != ErrorCode::OK) {
    ErrorCode rollback_result = session_manager_->rollback();
    if (rollback_result != ErrorCode::OK) {
      return rollback_result;
    }
    return error;
  }

  // Add column metadata object to column metadata table.
  BOOST_FOREACH (const ptree::value_type &node,
                 object.get_child(Tables::COLUMNS_NODE)) {
    ptree column = node.second;
    error = columns_dao_->insert_one_column_metadata(table_id, column);
    if (error != ErrorCode::OK) {
      ErrorCode rollback_result = session_manager_->rollback();
      if (rollback_result != ErrorCode::OK) {
        return rollback_result;
      }
      return error;
    }
  }

  error = session_manager_->commit();
  return error;
}

/**
 *  @brief  Gets one table metadata object from the table metadata repository,
 *  where key = value.
 *  @param  (key)      [in]  key of table metadata object.
 *  @param  (value)    [in]  value of table metadata object.
 *  @param  (object)   [out] one table metadata object to get,
 * where key = value.
 *  @return  ErrorCode::OK if success, otherwise an error code.
 */
ErrorCode TablesProvider::get_table_metadata(std::string_view key,
                                             std::string_view value,
                                             ptree &object) {
  // Initialization
  ErrorCode error = init();
  if (error != ErrorCode::OK) {
    return error;
  }

  error = tables_dao_->select_table_metadata(key, value, object);
  if (error != ErrorCode::OK) {
    return error;
  }

  std::string object_id = "";
  if (key == Tables::ID) {
    error = get_all_column_metadatas(value, object);
  } else {
    BOOST_FOREACH (ptree::value_type &node, object) {
      ptree &table = node.second;

      if (table.empty()) {
        boost::optional<std::string> o_table_id =
            object.get_optional<std::string>(Tables::ID);
        if (!o_table_id) {
          return ErrorCode::INTERNAL_ERROR;
        }

        error = get_all_column_metadatas(o_table_id.get(), object);
        break;
      } else {
        boost::optional<std::string> o_table_id =
            table.get_optional<std::string>(Tables::ID);
        if (!o_table_id) {
          return ErrorCode::INTERNAL_ERROR;
        }

        error = get_all_column_metadatas(o_table_id.get(), table);
        if (error != ErrorCode::OK) {
          break;
        }
      }
    }
  }
  return error;
}

/**
 * @brief  Remove all metadata-object based on the given table id
 *  (table metadata, column metadata and column statistics)
 *  from metadata-repositorys
 *  (the table metadata repository, the column metadata repository and the
 *  column statistics repository).
 *  @param  (table_id) [in] ID of the table metadata.
 *  @return  ErrorCode::OK if success, otherwise an error code.
 */
ErrorCode TablesProvider::remove_table_metadata(const ObjectIdType table_id) {
  // Initialization
  ErrorCode error = init();
  if (error != ErrorCode::OK) {
    return error;
  }

  if (table_id <= 0) {
    return ErrorCode::INVALID_PARAMETER;
  }

  error = session_manager_->start_transaction();
  if (error != ErrorCode::OK) {
    return error;
  }

  error = tables_dao_->delete_table_metadata_by_table_id(table_id);
  if (error != ErrorCode::OK) {
    ErrorCode rollback_result = session_manager_->rollback();
    if (rollback_result != ErrorCode::OK) {
      return rollback_result;
    }
    return error;
  }

  error = columns_dao_->delete_column_metadata_by_table_id(table_id);
  if (error == ErrorCode::OK) {
    error = session_manager_->commit();
  } else {
    ErrorCode rollback_result = session_manager_->rollback();
    if (rollback_result != ErrorCode::OK) {
      return rollback_result;
    }
  }
  return error;
}

/**
 *  @brief  Remove all metadata-object based on the given table name
 *  (table metadata, column metadata and column statistics)
 *  from metadata-repositorys
 *  (the table metadata repository, the column metadata repository and the
 *  column statistics repository).
 *  @param  (table_name) [in]  table name.
 *  @param  (table_id)   [out] ID of the removed table metadata.
 *  @return  ErrorCode::OK if success, otherwise an error code.
 */
ErrorCode TablesProvider::remove_table_metadata(std::string_view table_name,
                                                ObjectIdType &table_id) {
  // Initialization
  ErrorCode error = init();
  if (error != ErrorCode::OK) {
    return error;
  }

  std::string s_object_name = std::string(table_name);
  if (s_object_name.empty()) {
    return ErrorCode::INVALID_PARAMETER;
  }

  error = session_manager_->start_transaction();
  if (error != ErrorCode::OK) {
    return error;
  }

  error =
      tables_dao_->delete_table_metadata_by_table_name(s_object_name, table_id);
  if (error != ErrorCode::OK) {
    ErrorCode rollback_result = session_manager_->rollback();
    if (rollback_result != ErrorCode::OK) {
      return rollback_result;
    }
    return error;
  }

  error = columns_dao_->delete_column_metadata_by_table_id(table_id);
  if (error == ErrorCode::OK) {
    error = session_manager_->commit();
  } else {
    ErrorCode rollback_result = session_manager_->rollback();
    if (rollback_result != ErrorCode::OK) {
      return rollback_result;
    }
  }
  return error;
}

//_/_/_/_/_/_/_/_/_/_/_/_/_/_/_/_/_/_/_/_/_/_/_/_/_/_/_/_/_/_/_/_/_/_/_/_/_/_/_/
// Private method area

/**
 *  @brief  Get column metadata-object based on the given table id.
 *  @param  (table_id) [in]  table id.
 *  @param  (tables)   [out] table metadata-object with the specified table
 * id.
 *  @return  ErrorCode::OK if success, otherwise an error code.
 */
ErrorCode TablesProvider::get_all_column_metadatas(std::string_view table_id,
                                                   ptree &tables) {
  assert(!table_id.empty());

  ptree columns;
  ErrorCode error = columns_dao_->select_column_metadata(
      Tables::Column::TABLE_ID, table_id, columns);

  if ((error == ErrorCode::OK) || (error == ErrorCode::INVALID_PARAMETER)) {
    tables.add_child(Tables::COLUMNS_NODE, columns);
    error = ErrorCode::OK;
  }

  return error;
}

}  // namespace manager::metadata::db
