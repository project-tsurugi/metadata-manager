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
#include "manager/metadata/provider/tables_provider.h"

#include <boost/foreach.hpp>

#include "manager/metadata/datatypes.h"
#include "manager/metadata/helper/logging_helper.h"
#include "manager/metadata/tables.h"

// =============================================================================
namespace manager::metadata::db {

using boost::property_tree::ptree;

/**
 * @brief Initialize and prepare to access the metadata repository.
 * @return ErrorCode::OK if success, otherwise an error code.
 */
ErrorCode TablesProvider::init() {
  ErrorCode error = ErrorCode::UNKNOWN;
  std::shared_ptr<GenericDAO> gdao = nullptr;

  if (tables_dao_ == nullptr) {
    // Get an instance of the TablesDAO class.
    error = session_manager_->get_dao(GenericDAO::TableName::TABLES, gdao);
    if (error != ErrorCode::OK) {
      return error;
    }
    // Set TablesDAO instance.
    tables_dao_ = std::static_pointer_cast<TablesDAO>(gdao);
  }

  if (columns_dao_ == nullptr) {
    // Get an instance of the ColumnsDAO class.
    error = session_manager_->get_dao(GenericDAO::TableName::COLUMNS, gdao);
    if (error != ErrorCode::OK) {
      return error;
    }
    // Set ColumnsDAO instance.
    columns_dao_ = std::static_pointer_cast<ColumnsDAO>(gdao);
  }

  if (constraints_dao_ == nullptr) {
    // Get an instance of the ConstraintsDAO class.
    error = session_manager_->get_dao(GenericDAO::TableName::CONSTRAINTS, gdao);
    if (error != ErrorCode::OK) {
      return error;
    }
    // Set ConstraintsDAO instance.
    constraints_dao_ = std::static_pointer_cast<ConstraintsDAO>(gdao);
  }

  if (privileges_dao_ == nullptr) {
    // Get an instance of the PrivilegesDAO class.
    error = session_manager_->get_dao(GenericDAO::TableName::PRIVILEGES, gdao);
    if (error != ErrorCode::OK) {
      return error;
    }
    // Set PrivilegesDAO instance.
    privileges_dao_ = std::static_pointer_cast<PrivilegesDAO>(gdao);
  }

  error = ErrorCode::OK;
  return error;
}

/**
 * @brief Add table metadata to table metadata repository.
 * @param (object)     [in]  table metadata to add.
 * @param (table_id)   [out] ID of the added table metadata.
 * @return ErrorCode::OK if success, otherwise an error code.
 */
ErrorCode TablesProvider::add_table_metadata(
    const boost::property_tree::ptree& object, ObjectIdType& table_id) {
  ErrorCode error = ErrorCode::UNKNOWN;

  // Initialization
  error = init();
  if (error != ErrorCode::OK) {
    return error;
  }

  // Start the transaction.
  error = session_manager_->start_transaction();
  if (error != ErrorCode::OK) {
    return error;
  }

  // Add metadata object to table metadata table.
  error = tables_dao_->insert_table_metadata(object, table_id);

  // Add column metadata object to column metadata table.
  if (error == ErrorCode::OK) {
    BOOST_FOREACH (const ptree::value_type& node,
                   object.get_child(Tables::COLUMNS_NODE)) {
      // Copy to the temporary area.
      ptree column = node.second;

      // Erase  the columns ID.
      column.erase(Tables::Column::ID);

      // Add metadata object to columns metadata table.
      error = columns_dao_->insert_column_metadata(table_id, column);
      if (error != ErrorCode::OK) {
        // When an error occurs, the process is aborted.
        break;
      }
    }
  }

  // Add constraint metadata object to constraint metadata table.
  BOOST_FOREACH (const ptree::value_type& node,
                 object.get_child(Tables::CONSTRAINTS_NODE)) {
    ptree constraint = node.second;
    error = constraints_dao_->insert_one_constraint_metadata(table_id, constraint);
    if (error != ErrorCode::OK) {
      // Roll back the transaction.
      ErrorCode rollback_result = session_manager_->rollback();
      if (rollback_result != ErrorCode::OK) {
        error = rollback_result;
      }
      return error;
    }
  }

  // Commit the transaction.
  error = session_manager_->commit();
  if (error == ErrorCode::OK) {
    // Commit the transaction.
    error = session_manager_->commit();
  } else {
    // Roll back the transaction.
    ErrorCode rollback_result = session_manager_->rollback();
    if (rollback_result != ErrorCode::OK) {
      error = rollback_result;
    }
  }

  return error;
}

/**
 * @brief Gets one table metadata object from the table metadata repository,
 *   where key = value.
 * @param (key)     [in]  key of table metadata object.
 * @param (value)   [in]  value of table metadata object.
 * @param (object)  [out] one table metadata object to get,
 *   where key = value.
 * @retval ErrorCode::OK if success,
 * @retval ErrorCode::ID_NOT_FOUND if the table id does not exist.
 * @retval ErrorCode::NAME_NOT_FOUND if the table name does not exist.
 * @retval otherwise an error code.
 */
ErrorCode TablesProvider::get_table_metadata(
    std::string_view key, std::string_view value,
    boost::property_tree::ptree& object) {
  ErrorCode error = ErrorCode::UNKNOWN;

  // Parameter value check.
  if ((key != Tables::ID) && (key != Tables::NAME)) {
    error = ErrorCode::INVALID_PARAMETER;
    return error;
  }

  // Initialization
  error = init();
  if (error != ErrorCode::OK) {
    return error;
  }

  // Get table metadata.
  error = tables_dao_->select_table_metadata(key, value, object);
  if (error != ErrorCode::OK) {
    return error;
  }

  // Set the search key for column metadata and constraint metadata.
  std::string table_id;
  if (key == Tables::ID) {
    table_id = value;
  } else {
    auto o_table_id = object.get_optional<std::string>(Tables::ID);
    if (o_table_id) {
      table_id = o_table_id.value();
    }
  }

  // Get column metadata.
  error = get_column_metadata(table_id, object);

  // Get constraint metadata.
  error = get_constraint_metadata(table_id, object);

  return error;
}

/**
 * @brief Gets all table metadata object from the table metadata repository.
 *   If the table metadata does not exist, return the container as empty.
 * @param (container)   [out] table metadata object to get.
 * @return ErrorCode::OK if success, otherwise an error code.
 */
ErrorCode TablesProvider::get_table_metadata(
    std::vector<boost::property_tree::ptree>& container) {
  ErrorCode error = ErrorCode::UNKNOWN;

  // Initialization
  error = init();
  if (error != ErrorCode::OK) {
    return error;
  }

  // Get table metadata.
  error = tables_dao_->select_table_metadata(container);
  if (error != ErrorCode::OK) {
    return error;
  }

  // Get column metadata.
  BOOST_FOREACH (auto& table_object, container) {
    std::string table_id = "";
    auto o_table_id = table_object.get_optional<std::string>(Tables::ID);
    if (!o_table_id) {
      error = ErrorCode::INTERNAL_ERROR;
      break;
    }

    // Get column metadata.
    error = get_column_metadata(o_table_id.value(), table_object);
    if (error != ErrorCode::OK) {
      break;
    }
  }

  // Get constraint metadata.
  BOOST_FOREACH (auto& table_object, container) {
    std::string table_id = "";
    auto o_table_id = table_object.get_optional<std::string>(Tables::ID);
    if (!o_table_id) {
      error = ErrorCode::INTERNAL_ERROR;
      break;
    }

    // Get constraint metadata.
    error = get_constraint_metadata(o_table_id.value(), table_object);
    if (error != ErrorCode::OK) {
      break;
    }
  }

  return error;
}

/**
 * @brief Gets one table statistic from the table metadata repository,
 *   where key = value.
 * @param (key)      [in]  key of table metadata object.
 * @param (value)    [in]  value of table metadata object.
 * @param (object)   [out] one table metadata object to get,
 *   where key = value.
 * @retval ErrorCode::OK if success,
 * @retval ErrorCode::ID_NOT_FOUND if the table id does not exist.
 * @retval ErrorCode::NAME_NOT_FOUND if the table name does not exist.
 * @retval otherwise an error code.
 */
ErrorCode TablesProvider::get_table_statistic(
    std::string_view key, std::string_view value,
    boost::property_tree::ptree& object) {
  ErrorCode error = ErrorCode::UNKNOWN;

  // Initialization
  error = init();
  if (error != ErrorCode::OK) {
    return error;
  }

  // Get table metadata.
  error = tables_dao_->select_table_metadata(key, value, object);

  return error;
}

/**
 * @brief Updates the table metadata table with the specified table statistics.
 * @param (table_id)  [in]  Table ID of the table metadata to be updated.
 * @param (object)    [in]  Table metadata object.
 * @retval ErrorCode::OK if success,
 * @retval ErrorCode::ID_NOT_FOUND if the table id does not exist.
 * @retval otherwise an error code.
 */
ErrorCode TablesProvider::update_table_metadata(
    const ObjectIdType table_id, const boost::property_tree::ptree& object) {
  ErrorCode error = ErrorCode::UNKNOWN;

  // Initialization
  error = init();
  if (error != ErrorCode::OK) {
    return error;
  }

  // Start the transaction.
  error = session_manager_->start_transaction();
  if (error != ErrorCode::OK) {
    return error;
  }

  // Update table metadata table.
  if (error == ErrorCode::OK) {
    error = tables_dao_->update_table_metadata(table_id, object);
  }

  // Remove a metadata object from the column metadata table.
  if (error == ErrorCode::OK) {
    error = columns_dao_->delete_column_metadata(Tables::Column::TABLE_ID,
                                                 std::to_string(table_id));
  }

  // Add metadata object to column metadata table.
  if (error == ErrorCode::OK) {
    BOOST_FOREACH (const ptree::value_type& node,
                   object.get_child(Tables::COLUMNS_NODE)) {
      ptree column = node.second;

      // Add metadata object to columns metadata table.
      error = columns_dao_->insert_column_metadata(table_id, column);
      if (error != ErrorCode::OK) {
        // When an error occurs, the process is aborted.
        break;
      }
    }
  }

  if (error == ErrorCode::OK) {
    // Commit the transaction.
    error = session_manager_->commit();
  } else {
    // Roll back the transaction.
    ErrorCode rollback_result = session_manager_->rollback();
    if (rollback_result != ErrorCode::OK) {
      error = rollback_result;
    }
  }

  return error;
}

/**
 * @brief Updates the table metadata table with the specified table statistics.
 * @param (object)    [in]  Table statistic object.
 * @param (table_id)  [out] ID of the added table metadata.
 * @retval ErrorCode::OK if success,
 * @retval ErrorCode::ID_NOT_FOUND if the table id does not exist.
 * @retval ErrorCode::NAME_NOT_FOUND if the table name does not exist.
 * @retval otherwise an error code.
 */
ErrorCode TablesProvider::set_table_statistic(
    const boost::property_tree::ptree& object, ObjectIdType& table_id) {
  ErrorCode error = ErrorCode::UNKNOWN;

  // Initialization
  error = init();
  if (error != ErrorCode::OK) {
    return error;
  }

  // id
  boost::optional<std::string> optional_id =
      object.get_optional<std::string>(Tables::ID);
  // name
  boost::optional<std::string> optional_name =
      object.get_optional<std::string>(Tables::NAME);
  // tuples
  boost::optional<float> optional_tuples =
      object.get_optional<float>(Tables::TUPLES);

  // Set the key items and values to be updated.
  std::string key;
  std::string value;
  if (optional_id) {
    key = Tables::ID;
    value = optional_id.get();
  } else {
    key = Tables::NAME;
    value = optional_name.get();
  }

  // Set the update value.
  float tuples = optional_tuples.get();

  // Start the transaction.
  error = session_manager_->start_transaction();
  if (error != ErrorCode::OK) {
    return error;
  }

  // Update table statistics to table metadata table.
  error = tables_dao_->update_reltuples(tuples, key, value, table_id);

  if (error == ErrorCode::OK) {
    // Commit the transaction.
    error = session_manager_->commit();
  } else {
    // Roll back the transaction.
    ErrorCode rollback_result = session_manager_->rollback();
    if (rollback_result != ErrorCode::OK) {
      error = rollback_result;
    }
  }

  return error;
}

/**
 * @brief Remove all metadata-object based on the given table name
 *   (table metadata, column metadata and column statistics)
 *   from metadata-repositories
 *   (the table metadata repository, the column metadata repository and the
 *   column statistics repository).
 * @param (key)       [in]  key of table metadata object.
 * @param (value)     [in]  value of table metadata object.
 * @param (table_id)  [out] ID of the removed table metadata.
 * @retval ErrorCode::OK if success.
 * @retval ErrorCode::ID_NOT_FOUND if the table id does not exist.
 * @retval ErrorCode::NAME_NOT_FOUND if the table name does not exist.
 * @retval otherwise an error code.
 */
ErrorCode TablesProvider::remove_table_metadata(std::string_view key,
                                                std::string_view value,
                                                ObjectIdType& table_id) {
  ErrorCode error = ErrorCode::UNKNOWN;

  // Initialization
  error = init();
  if (error != ErrorCode::OK) {
    return error;
  }

  // Start the transaction.
  error = session_manager_->start_transaction();
  if (error != ErrorCode::OK) {
    return error;
  }

  error = tables_dao_->delete_table_metadata(key, value, table_id);
  if (error != ErrorCode::OK) {
    // Roll back the transaction.
    ErrorCode rollback_result = session_manager_->rollback();
    if (rollback_result != ErrorCode::OK) {
      return rollback_result;
    }
    return error;
  }

  error = constraints_dao_->delete_constraint_metadata(Tables::Constraint::TABLE_ID,
                                               std::to_string(table_id));
  if (error != ErrorCode::OK) {
    // Roll back the transaction.
    ErrorCode rollback_result = session_manager_->rollback();
    if (rollback_result != ErrorCode::OK) {
      return rollback_result;
    }
    return error;
  }

  // Remove a metadata object from the column metadata table.
  error = columns_dao_->delete_column_metadata(Tables::Column::TABLE_ID,
                                               std::to_string(table_id));
  if (error == ErrorCode::OK) {
    // Commit the transaction.
    error = session_manager_->commit();
  } else {
    // Roll back the transaction.
    ErrorCode rollback_result = session_manager_->rollback();
    if (rollback_result != ErrorCode::OK) {
      error = rollback_result;
    }
  }

  return error;
}

/**
 * @brief Gets the presence or absence of the specified permission
 *   from the PostgreSQL system catalog.
 * @param (key)           [in]  key of role metadata object.
 * @param (value)         [in]  value of role metadata object.
 * @param (permission)    [in]  permissions.
 * @param (check_result)  [out] presence or absence of the specified
 *   permissions.
 * @retval ErrorCode::OK if success.
 * @retval ErrorCode::NOT_FOUND if the foreign table does not exist.
 * @retval ErrorCode::ID_NOT_FOUND if the role id does not exist.
 * @retval ErrorCode::NAME_NOT_FOUND if the role name does not exist.
 * @retval otherwise an error code.
 */
ErrorCode TablesProvider::confirm_permission(std::string_view key,
                                             std::string_view value,
                                             std::string_view permission,
                                             bool& check_result) {
  ErrorCode error = ErrorCode::UNKNOWN;

  // Initialization
  error = init();
  if (error != ErrorCode::OK) {
    return error;
  }

  error = privileges_dao_->confirm_tables_permission(key, value, permission,
                                                     check_result);

  return error;
}

/* =============================================================================
 * Private method area
 */

/**
 * @brief Get column metadata-object based on the given table id.
 * @param (table_id)      [in]  table id.
 * @param (table_object)  [out] table metadata-object
 *   with the specified table id.
 * @return ErrorCode::OK if success, otherwise an error code.
 */
ErrorCode TablesProvider::get_column_metadata(
    std::string_view table_id,
    boost::property_tree::ptree& table_object) const {
  ErrorCode error = ErrorCode::UNKNOWN;

  if (table_id.empty()) {
    error = ErrorCode::INTERNAL_ERROR;
    return error;
  }

  ptree columns;
  error = columns_dao_->select_column_metadata(Tables::Column::TABLE_ID,
                                               table_id, columns);
  if ((error == ErrorCode::OK) || (error == ErrorCode::INVALID_PARAMETER)) {
    if (table_object.find(Tables::COLUMNS_NODE) == table_object.not_found()) {
      table_object.add_child(Tables::COLUMNS_NODE, columns);
    }
    error = ErrorCode::OK;
  }

  return error;
}

/**
 * @brief Get constraint metadata-object based on the given table id.
 * @param (table_id)      [in]  table id.
 * @param (table_object)  [out] table metadata-object
 *   with the specified table id.
 * @return ErrorCode::OK if success, otherwise an error code.
 */
ErrorCode TablesProvider::get_constraint_metadata(
    std::string_view table_id,
    boost::property_tree::ptree& table_object) const {
  ErrorCode error = ErrorCode::UNKNOWN;

  if (table_id.empty()) {
    error = ErrorCode::INTERNAL_ERROR;
    return error;
  }

  ptree constraints;
  error = constraints_dao_->select_constraint_metadata(Tables::Constraint::TABLE_ID,
                                               table_id, constraints);
  if ((error == ErrorCode::OK) || (error == ErrorCode::INVALID_PARAMETER)) {
    table_object.add_child(Tables::CONSTRAINTS_NODE, constraints);
    error = ErrorCode::OK;
  }

  return error;
}

}  // namespace manager::metadata::db
