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
#include "manager/metadata/provider/tables_provider.h"

#include <regex>

#include <boost/foreach.hpp>

#include "manager/metadata/common/message.h"
#include "manager/metadata/common/utility.h"
#include "manager/metadata/datatype.h"
#include "manager/metadata/helper/logging_helper.h"

// =============================================================================
namespace manager::metadata::db {

using boost::property_tree::ptree;

/**
 * @brief Initialize and prepare to access the metadata repository.
 * @return ErrorCode::OK if success, otherwise an error code.
 */
ErrorCode TablesProvider::init() {
  ErrorCode error = ErrorCode::UNKNOWN;

  // Establish a connection to the metadata repository.
  error = session_manager_->connect();
  if (error != ErrorCode::OK) {
    return error;
  }

  // TablesDAO
  if (!tables_dao_) {
    // Get an instance of the TablesDAO.
    tables_dao_ = session_manager_->get_tables_dao();

    // Prepare to access table metadata.
    error = tables_dao_->prepare();
    if (error != ErrorCode::OK) {
      tables_dao_.reset();
      return error;
    }
  }

  // ColumnsDAO
  if (!columns_dao_) {
    // Get an instance of the ColumnsDAO.
    columns_dao_ = session_manager_->get_columns_dao();

    // Prepare to access table metadata.
    error = columns_dao_->prepare();
    if (error != ErrorCode::OK) {
      columns_dao_.reset();
      return error;
    }
  }

  // ConstraintsDAO
  if (!constraints_dao_) {
    // Get an instance of the ConstraintsDAO.
    constraints_dao_ = session_manager_->get_constraints_dao();

    // Prepare to access table metadata.
    error = constraints_dao_->prepare();
    if (error != ErrorCode::OK) {
      constraints_dao_.reset();
      return error;
    }
  }

  // PrivilegesDAO
  if (!privileges_dao_) {
    // Get an instance of the PrivilegesDAO.
    privileges_dao_ = session_manager_->get_privileges_dao();

    // Prepare to access table metadata.
    error = privileges_dao_->prepare();
    if (error != ErrorCode::OK) {
      privileges_dao_.reset();
      if (error != ErrorCode::NOT_SUPPORTED) {
        return error;
      }
    }
  }

  error = ErrorCode::OK;
  return error;
}

/**
 * @brief Add table metadata to table metadata repository.
 * @param object    [in]  table metadata to add.
 * @param table_id  [out] ID of the added table metadata.
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
  error = tables_dao_->insert(object, table_id);

  // Add column metadata object to column metadata table.
  if (error == ErrorCode::OK) {
    BOOST_FOREACH (const ptree::value_type& node,
                   object.get_child(Table::COLUMNS_NODE)) {
      // Copy to the temporary area.
      ptree column = node.second;

      // Erase the columns ID.
      column.erase(Column::ID);
      // Set table-id.
      column.put(Column::TABLE_ID, table_id);

      ObjectId added_id = 0;
      // Insert the column metadata.
      error = columns_dao_->insert(column, added_id);
      if (error != ErrorCode::OK) {
        // When an error occurs, the process is aborted.
        break;
      }
    }
  }

  // Add constraint metadata object to constraint metadata table.
  if (error == ErrorCode::OK) {
    auto constraints_node = object.get_child_optional(Table::CONSTRAINTS_NODE);
    if (constraints_node) {
      BOOST_FOREACH (const ptree::value_type& node, constraints_node.get()) {
        // Copy to the temporary area.
        ptree constraint = node.second;

        // Erase constraint-id.
        constraint.erase(Constraint::ID);
        // Set table-id.
        constraint.put(Constraint::TABLE_ID, table_id);

        ObjectId added_id = 0;
        // Insert the constraint metadata.
        error = constraints_dao_->insert(constraint, added_id);
        error = (error == ErrorCode::NOT_FOUND ? ErrorCode::OK : error);

        if (error != ErrorCode::OK) {
          break;
        }
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
 * @brief Gets one table metadata object from the table metadata repository,
 * where key = value.
 * @param key     [in]  key of table metadata object.
 * @param value   [in]  value of table metadata object.
 * @param object  [out] one table metadata object to get, where key = value.
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
  if ((key != Table::ID) && (key != Table::NAME)) {
    error = ErrorCode::INVALID_PARAMETER;
    return error;
  }

  // Initialization
  error = init();
  if (error != ErrorCode::OK) {
    return error;
  }

  // Get table metadata.
  error = tables_dao_->select(key, {value}, object);
  if (error != ErrorCode::OK) {
    return error;
  }

  // Set the search key for column metadata and constraint metadata.
  std::string table_id;
  if (key == Table::ID) {
    table_id = value;
  } else {
    auto o_table_id = object.get_optional<std::string>(Table::ID);
    if (o_table_id) {
      table_id = o_table_id.value();
    }
  }

  // Get column metadata.
  error = get_column_metadata(table_id, object);
  if (error != ErrorCode::OK) {
    return error;
  }

  // Get constraint metadata.
  error = get_constraint_metadata(table_id, object);

  return error;
}

/**
 * @brief Gets all table metadata object from the table metadata repository.
 *   If the table metadata does not exist, return the container as empty.
 * @param container  [out] table metadata object to get.
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
  error = tables_dao_->select_all(container);
  if (error != ErrorCode::OK) {
    return error;
  }

  // Get columns and constraints metadata.
  BOOST_FOREACH (auto& table_object, container) {
    std::string table_id = "";
    auto o_table_id      = table_object.get_optional<std::string>(Table::ID);
    if (!o_table_id) {
      error = ErrorCode::INTERNAL_ERROR;
      break;
    }

    // Get column metadata.
    error = get_column_metadata(o_table_id.value(), table_object);
    if (error != ErrorCode::OK) {
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
 * @brief Gets one table statistic from the table metadata repository, where key
 * = value.
 * @param key     [in]  key of table metadata object.
 * @param value   [in]  value of table metadata object.
 * @param object  [out] one table metadata object to get, where key = value.
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
  error = tables_dao_->select(key, {value}, object);

  return error;
}

/**
 * @brief Updates the table metadata table with the specified table statistics.
 * @param table_id  [in]  Table ID of the table metadata to be updated.
 * @param object    [in]  Table metadata object.
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
    error = tables_dao_->update(Table::ID, {std::to_string(table_id)}, object);
  }

  // Remove a metadata object from the column metadata table.
  if (error == ErrorCode::OK) {
    ObjectId removed_id = 0;
    // Delete records associated with TableID.
    error = columns_dao_->remove(Column::TABLE_ID, {std::to_string(table_id)},
                                 removed_id);
    // No record is found, it is treated as a success.
    error = (error == ErrorCode::NOT_FOUND ? ErrorCode::OK : error);
  }

  // Add metadata object to column metadata table.
  if (error == ErrorCode::OK) {
    // Add metadata object to columns metadata table.
    BOOST_FOREACH (const ptree::value_type& node,
                   object.get_child(Table::COLUMNS_NODE)) {
      ptree column = node.second;

      // Set table-id.
      column.put(Column::TABLE_ID, table_id);

      ObjectId added_id = 0;
      // Add metadata object to columns metadata table.
      error = columns_dao_->insert(column, added_id);
      if (error != ErrorCode::OK) {
        // When an error occurs, the process is aborted.
        break;
      }
    }
  }

  // Remove a metadata object from the constraint metadata table.
  if (error == ErrorCode::OK) {
    ObjectId removed_id = 0;
    // Delete records associated with TableID.
    error = constraints_dao_->remove(Constraint::TABLE_ID,
                                     {std::to_string(table_id)}, removed_id);
    // No record is found, it is treated as a success.
    error = (error == ErrorCode::NOT_FOUND ? ErrorCode::OK : error);
  }

  // Add metadata object to constraints metadata table.
  if (error == ErrorCode::OK) {
    auto constraints_node = object.get_child_optional(Table::CONSTRAINTS_NODE);
    if (constraints_node) {
      BOOST_FOREACH (const ptree::value_type& node, constraints_node.get()) {
        // Copy to the temporary area.
        ptree constraint = node.second;

        // Set table-id.
        constraint.put(Constraint::TABLE_ID, table_id);

        ObjectId added_id = 0;
        // Insert the constraint metadata.
        error = constraints_dao_->insert(constraint, added_id);
        if (error != ErrorCode::OK) {
          // When an error occurs, the process is aborted.
          break;
        }
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
 * @param object    [in]  Table statistic object.
 * @param table_id  [out] ID of the added table metadata.
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
  auto optional_id = object.get_optional<ObjectId>(Table::ID);
  // name
  auto optional_name = object.get_optional<std::string>(Table::NAME);
  // number_of_tuples
  auto optional_tuples = object.get_optional<int64_t>(Table::NUMBER_OF_TUPLES);

  // Set the key items and values to be updated.
  std::string key;
  std::string value;
  if (optional_id) {
    key   = Table::ID;
    value = std::to_string(optional_id.get());
  } else if (optional_name) {
    key   = Table::NAME;
    value = optional_name.get();
  } else {
    LOG_ERROR << Message::PARAMETER_FAILED << "\"" << Table::ID << "\" or \""
              << Table::NAME << "\" is required.";
    error = ErrorCode::INVALID_PARAMETER;
    return error;
  }

  // Start the transaction.
  error = session_manager_->start_transaction();
  if (error != ErrorCode::OK) {
    return error;
  }

  boost::property_tree::ptree table_metadata;
  // Get table metadata.
  error = get_table_metadata(key, value, table_metadata);
  if (error != ErrorCode::OK) {
    return error;
  }

  // Get the updated object id.
  auto opt_object_id = table_metadata.get_optional<ObjectId>(Table::ID);
  table_id           = opt_object_id.value_or(-1);

  // Set the update value.
  table_metadata.put(Table::NUMBER_OF_TUPLES, optional_tuples.get());

  // Update table statistics to table metadata table.
  error = tables_dao_->update(key, {value}, table_metadata);

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
 *   (table metadata, column metadata and constraint metadata and column
 * statistics) from metadata-repositories (the table metadata repository, the
 * column metadata repository and the constraint metadata repository and the
 * column statistics repository).
 * @param key       [in]  key of table metadata object.
 * @param value     [in]  value of table metadata object.
 * @param table_id  [out] ID of the removed table metadata.
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

  // Remove a metadata object from the tables metadata table.
  error = tables_dao_->remove(key, {value}, table_id);

  if (error == ErrorCode::OK) {
    ObjectId removed_id = 0;
    // Remove a metadata object from the constraints metadata table.
    error = columns_dao_->remove(Column::TABLE_ID, {std::to_string(table_id)},
                                 removed_id);
    // No record is found, it is treated as a success.
    error = (error == ErrorCode::NOT_FOUND ? ErrorCode::OK : error);
  }

  if (error == ErrorCode::OK) {
    ObjectId removed_id = 0;
    // Remove a metadata object from the constraints metadata table.
    error = constraints_dao_->remove(Constraint::TABLE_ID,
                                     {std::to_string(table_id)}, removed_id);
    // No record is found, it is treated as a success.
    error = (error == ErrorCode::NOT_FOUND ? ErrorCode::OK : error);
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
 * @brief Gets the presence or absence of the specified permission from the
 * PostgreSQL system catalog.
 * @param key           [in]  key of role metadata object.
 * @param value         [in]  value of role metadata object.
 * @param permission    [in]  permissions.
 * @param check_result  [out] presence or absence of the specified permissions.
 * @retval ErrorCode::OK if success.
 * @retval ErrorCode::NOT_FOUND if the foreign table does not exist.
 * @retval ErrorCode::ID_NOT_FOUND if the role id does not exist.
 * @retval ErrorCode::NAME_NOT_FOUND if the role name does not exist.
 * @retval ErrorCode::NOT_SUPPORTED If the function is not supported.
 * @retval otherwise an error code.
 */
ErrorCode TablesProvider::confirm_permission(std::string_view key,
                                             std::string_view value,
                                             std::string_view permission,
                                             bool& check_result) {
  ErrorCode error = ErrorCode::UNKNOWN;

  if (!privileges_dao_) {
    error = ErrorCode::NOT_SUPPORTED;
    return error;
  }

  // Extracts the valid part of the given authentication string.
  std::string match_string(permission);
  std::smatch matcher;
  std::regex_match(match_string, matcher, std::regex(R"((.*=|)(.+)(/.*|))"));
  std::string check_privilege(
      std::regex_replace(matcher[2].str(), std::regex(R"(\s)"), ""));

  if (check_privilege.empty()) {
    LOG_ERROR << Message::INCORRECT_DATA << "The permission to check is empty.";
    error = ErrorCode::INVALID_PARAMETER;
    return error;
  }

  // Checks the normality of the specified authorization string.
  for (auto privilege = check_privilege.c_str(); *privilege != 0x00;
       privilege++) {
    if (privileges_map_.find(*privilege) == privileges_map_.end()) {
      LOG_ERROR << Message::INCORRECT_DATA
                << "The privilege format is incorrect.: "
                << "\"" << permission << "\"";
      error = ErrorCode::INVALID_PARAMETER;
      return error;
    }
  }

  // Initialization
  error = init();
  if (error != ErrorCode::OK) {
    return error;
  }

  // In the case of ID specification, check for the presence of the
  // specified ID.
  if (key == Object::ID) {
    ObjectId object_id;
    error = Utility::str_to_numeric<ObjectId>(value, object_id);
    if (error != ErrorCode::OK) {
      return error;
    }

    // Check for the presence of the specified ID.
    auto exists = privileges_dao_->exists(object_id);
    if (!exists) {
      LOG_INFO << "The role with the specified ID does not exist.: "
               << object_id;

      error = ErrorCode::ID_NOT_FOUND;
      return error;
    }
  }

  ptree object;
  // Get privileges for all tables included in the table metadata.
  error = privileges_dao_->select(key, {value}, object);
  if (error != ErrorCode::OK) {
    LOG_INFO << "Target table metadata did not exist.";

    return error;
  }

  // Checks for the presence of specified privileges.
  check_result = check_of_privilege(object, check_privilege);

  error = ErrorCode::OK;
  return error;
}

/* =============================================================================
 * Private method area
 */

/**
 * @brief Get column metadata-object based on the given table id.
 * @param table_id      [in]  table id.
 * @param table_object  [out] table metadata-object with the specified table id.
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
  error = columns_dao_->select(Column::TABLE_ID, {table_id}, columns);
  if ((error == ErrorCode::OK) || (error == ErrorCode::NOT_FOUND)) {
    if (table_object.find(Table::COLUMNS_NODE) == table_object.not_found()) {
      table_object.add_child(Table::COLUMNS_NODE, columns);
    }
    error = ErrorCode::OK;
  }

  return error;
}

/**
 * @brief Get constraint metadata-object based on the given table id.
 * @param table_id      [in]  table id.
 * @param table_object  [out] table metadata-object with the specified table id.
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
  error =
      constraints_dao_->select(Constraint::TABLE_ID, {table_id}, constraints);
  if ((error == ErrorCode::OK) || (error == ErrorCode::NOT_FOUND)) {
    if (table_object.find(Table::CONSTRAINTS_NODE) ==
        table_object.not_found()) {
      table_object.add_child(Table::CONSTRAINTS_NODE, constraints);
    }
    error = ErrorCode::OK;
  }

  return error;
}

/**
 * @brief Checks for the presence of specified privileges.
 * @param object      [in]  ptree of table privileges.
 * @param privileges  [in]  privileges to check.
 * @return bool
 */
bool TablesProvider::check_of_privilege(
    const boost::property_tree::ptree& object,
    std::string_view privileges) const {
  auto check_result = true;

  for (auto iterator = object.begin(); iterator != object.end(); iterator++) {
    if (iterator->second.empty()) {
      break;
    }

    auto iterator_child = iterator->second.begin();
    if (iterator_child->second.empty()) {
      auto table_name      = iterator->first;
      auto privileges_node = iterator->second;

      LOG_DEBUG << "Check table privileges: [" << table_name << "]["
                << privileges << "]";

      for (auto privilege = privileges.data(); *privilege != 0x00;
           privilege++) {
        std::string key;
        try {
          key = privileges_map_.at(*privilege);
        } catch (...) {
          check_result = false;
          break;
        }

        auto value   = privileges_node.get_optional<std::string>(key);
        check_result = Utility::str_to_boolean(value.get_value_or(""));

        LOG_DEBUG << "=>" << key << " was " << value.get_value_or("");

        if (!check_result) {
          break;
        }
      }
      LOG_INFO << "Check table privileges: [" << table_name << "]"
               << " [" << privileges << "]"
               << " => " << Utility::boolean_to_str(check_result);
    } else {
      check_result = check_of_privilege(iterator->second, privileges);
    }
    if (!check_result) {
      break;
    }
  }

  return check_result;
}

}  // namespace manager::metadata::db
