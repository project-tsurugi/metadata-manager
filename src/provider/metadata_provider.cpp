/*
 * Copyright 2022-2023 tsurugi project.
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
#include "manager/metadata/provider/metadata_provider.h"

#include <regex>
#include <typeinfo>

#include <boost/foreach.hpp>

#include "manager/metadata/common/message.h"
#include "manager/metadata/common/utility.h"
#include "manager/metadata/constraints.h"
#include "manager/metadata/dao/db_session_manager.h"
#include "manager/metadata/datatypes.h"
#include "manager/metadata/helper/logging_helper.h"
#include "manager/metadata/index.h"
#include "manager/metadata/roles.h"
#include "manager/metadata/statistics.h"
#include "manager/metadata/tables.h"

namespace manager::metadata::db {

using boost::property_tree::ptree;

// ============================================================================
// MetadataProvider class methods.

ErrorCode MetadataProvider::init() {
  ErrorCode error = ErrorCode::UNKNOWN;

  auto& session = DbSessionManager::get_instance();

  // Establish a connection to the metadata repository.
  error = session.connect();
  if (error != ErrorCode::OK) {
    return error;
  }

  // Table metadata DAO.
  error = session.get_tables_dao(table_dao_);
  if (error != ErrorCode::OK) {
    return error;
  }

  // Column metadata DAO.
  error = session.get_columns_dao(column_dao_);
  if (error != ErrorCode::OK) {
    return error;
  }

  // Index metadata DAO.
  error = session.get_indexes_dao(index_dao_);
  if (error != ErrorCode::OK) {
    return error;
  }

  // Constraint metadata DAO.
  error = session.get_constraints_dao(constraint_dao_);
  if (error != ErrorCode::OK) {
    return error;
  }

  // Table privileges DAO.
  error = session.get_privileges_dao(privilege_dao_);
  if (error != ErrorCode::OK) {
    return error;
  }

  // Column statistics DAO.
  error = session.get_statistics_dao(statistic_dao_);
  if (error != ErrorCode::OK) {
    return error;
  }

  // DataType metadata DAO.
  error = session.get_datatypes_dao(datatype_dao_);
  if (error != ErrorCode::OK) {
    return error;
  }

  error = ErrorCode::OK;
  return error;
}

// ============================================================================
ErrorCode MetadataProvider::add_table_metadata(
    const boost::property_tree::ptree& object, ObjectId& object_id) {
  ErrorCode error = ErrorCode::UNKNOWN;

  // Initialization
  error = init();
  if (error != ErrorCode::OK) {
    return error;
  }

  // Start the transaction.
  error = this->start_transaction();
  if (error != ErrorCode::OK) {
    return error;
  }

  // Add metadata object to table metadata table.
  error = table_dao_->insert(object, object_id);

  // Add column metadata object to column metadata table.
  if (error == ErrorCode::OK) {
    BOOST_FOREACH (const ptree::value_type& node,
                   object.get_child(Table::COLUMNS_NODE)) {
      // Copy to the temporary area.
      ptree column = node.second;

      // Erase the columns ID.
      column.erase(Column::ID);
      // Set table-id.
      column.put(Column::TABLE_ID, object_id);

      ObjectId added_id = 0;
      // Insert the column metadata.
      error = column_dao_->insert(column, added_id);
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
        constraint.put(Constraint::TABLE_ID, object_id);

        ObjectId added_id = 0;
        // Insert the constraint metadata.
        error = constraint_dao_->insert(constraint, added_id);
        error = (error == ErrorCode::NOT_FOUND ? ErrorCode::OK : error);

        if (error != ErrorCode::OK) {
          break;
        }
      }
    }
  }

  // End the transaction.
  error = this->end_transaction(error);

  return error;
}

ErrorCode MetadataProvider::add_index_metadata(
    const boost::property_tree::ptree& object, ObjectId& object_id) {
  ErrorCode error = ErrorCode::UNKNOWN;

  // Initialization
  error = this->init();
  if (error != ErrorCode::OK) {
    return error;
  }

  // Start the transaction.
  error = this->start_transaction();
  if (error != ErrorCode::OK) {
    return error;
  }

  // Add metadata object to index metadata table.
  error = index_dao_->insert(object, object_id);

  // End the transaction.
  error = this->end_transaction(error);

  return error;
}

ErrorCode MetadataProvider::add_constraint_metadata(
    const boost::property_tree::ptree& object, ObjectId& object_id) {
  ErrorCode error = ErrorCode::UNKNOWN;

  // Initialization
  error = init();
  if (error != ErrorCode::OK) {
    return error;
  }

  // Start the transaction.
  error = this->start_transaction();
  if (error != ErrorCode::OK) {
    return error;
  }

  // Add metadata object to constraint metadata table.
  error = constraint_dao_->insert(object, object_id);

  // End the transaction.
  error = this->end_transaction(error);

  return error;
}

ErrorCode MetadataProvider::add_column_statistic(
    const boost::property_tree::ptree& object, ObjectId& object_id) {
  ErrorCode error = ErrorCode::UNKNOWN;

  // Initialization
  error = init();
  if (error != ErrorCode::OK) {
    return error;
  }

  // Start the transaction.
  error = this->start_transaction();
  if (error != ErrorCode::OK) {
    return error;
  }

  // Register column statistics via DAO.
  error = statistic_dao_->insert(object, object_id);

  // End the transaction.
  error = this->end_transaction(error);

  return error;
}

// ============================================================================
ErrorCode MetadataProvider::get_table_metadata(
    std::string_view key, std::string_view value,
    boost::property_tree::ptree& object) {
  ErrorCode error = ErrorCode::UNKNOWN;

  // Parameter value check.
  if ((key != Table::ID) && (key != Table::NAME)) {
    error = ErrorCode::INVALID_PARAMETER;
    return error;
  }

  // Initialization
  error = this->init();
  if (error != ErrorCode::OK) {
    return error;
  }

  // Get table metadata.
  error = this->select_single(*table_dao_, key, {value}, object);
  // Treat as normal, even if there are multiple rows.
  error = (error == ErrorCode::RESULT_MULTIPLE_ROWS ? ErrorCode::OK : error);
  if (error != ErrorCode::OK) {
    return error;
  }

  // Set the search key for column metadata and constraint metadata.
  std::string table_id;
  if (key == Table::ID) {
    table_id = value;
  } else {
    auto o_table_id = object.get_optional<ObjectId>(Table::ID);
    if (o_table_id) {
      table_id = std::to_string(o_table_id.value());
    }
  }

  // Retrieve columns metadata.
  if (error == ErrorCode::OK) {
    // If the column has not yet been retrieved, the column metadata is
    // retrieved.
    if (object.find(Table::COLUMNS_NODE) == object.not_found()) {
      LOG_INFO << "Retrieve columns metadata: [" << Column::TABLE_ID << "="
               << table_id << "]";

      ptree metadata;
      // Get column metadata.
      error = column_dao_->select(Column::TABLE_ID, {table_id}, metadata);
      object.add_child(Table::COLUMNS_NODE, metadata);

      // Convert error codes.
      error = (error == ErrorCode::NOT_FOUND ? ErrorCode::OK : error);
    }
  }

  // Retrieve constraints metadata.
  if (error == ErrorCode::OK) {
    // If the constraint has not yet been retrieved, the constraint metadata is
    // retrieved.
    if (object.find(Table::CONSTRAINTS_NODE) == object.not_found()) {
      LOG_INFO << "Retrieve constraint metadata: [" << Column::TABLE_ID << "="
               << table_id << "]";

      ptree metadata;
      // Get column metadata.
      error = constraint_dao_->select(Column::TABLE_ID, {table_id}, metadata);
      object.add_child(Table::CONSTRAINTS_NODE, metadata);

      // Convert error codes.
      error = (error == ErrorCode::NOT_FOUND ? ErrorCode::OK : error);
    }
  }

  return error;
}

ErrorCode MetadataProvider::get_table_metadata(
    std::vector<boost::property_tree::ptree>& objects) {
  ErrorCode error = ErrorCode::UNKNOWN;

  // Initialization
  error = init();
  if (error != ErrorCode::OK) {
    return error;
  }

  // Get table metadata.
  error = table_dao_->select_all(objects);
  if (error != ErrorCode::OK) {
    return error;
  }

  // Get columns and constraints metadata.
  BOOST_FOREACH (auto& object, objects) {
    auto o_table_id = object.get_optional<ObjectId>(Table::ID);
    if (!o_table_id) {
      error = ErrorCode::INTERNAL_ERROR;
      break;
    }
    std::string table_id(std::to_string(o_table_id.value()));

    // If the column has not yet been retrieved, the column metadata is
    // retrieved.
    if (object.find(Table::COLUMNS_NODE) == object.not_found()) {
      ptree columns = {};
      error = column_dao_->select(Column::TABLE_ID, {table_id}, columns);
      object.add_child(Table::COLUMNS_NODE, columns);

      // Convert error codes.
      error = (error == ErrorCode::NOT_FOUND ? ErrorCode::OK : error);

      if (error != ErrorCode::OK) {
        break;
      }
    }

    // If the constraint has not yet been retrieved, the constraint metadata is
    // retrieved.
    if (object.find(Table::CONSTRAINTS_NODE) == object.not_found()) {
      ptree constraints = {};
      error = constraint_dao_->select(Constraint::TABLE_ID, {table_id},
                                      constraints);
      object.add_child(Table::CONSTRAINTS_NODE, constraints);

      // Convert error codes.
      error = (error == ErrorCode::NOT_FOUND ? ErrorCode::OK : error);

      if (error != ErrorCode::OK) {
        break;
      }
    }
  }

  return error;
}

ErrorCode MetadataProvider::get_table_statistic(
    std::string_view key, std::string_view value,
    boost::property_tree::ptree& object) {
  ErrorCode error = ErrorCode::UNKNOWN;

  // Initialization
  error = this->init();
  if (error != ErrorCode::OK) {
    return error;
  }

  // Get table metadata.
  error = this->select_single(*table_dao_, key, {value}, object);
  // Treat as normal, even if there are multiple rows.
  error = (error == ErrorCode::RESULT_MULTIPLE_ROWS ? ErrorCode::OK : error);

  return error;
}

ErrorCode MetadataProvider::get_index_metadata(
    std::string_view key, std::string_view value,
    boost::property_tree::ptree& object) {
  ErrorCode error = ErrorCode::UNKNOWN;

  // Initialization
  error = this->init();
  if (error != ErrorCode::OK) {
    return error;
  }

  ptree tmp_object;
  // Get index metadata.
  error = index_dao_->select(key, {value}, tmp_object);

  if (error == ErrorCode::OK) {
    if (tmp_object.size() == 1) {
      object = tmp_object.front().second;
    } else {
      error = ErrorCode::RESULT_MULTIPLE_ROWS;
      LOG_WARNING << "Multiple rows retrieved.: " << key << "=" << value
                  << " exists " << tmp_object.size() << " rows";
    }
  }

  return error;
}

ErrorCode MetadataProvider::get_index_metadata(
    std::vector<boost::property_tree::ptree>& objects) {
  ErrorCode error = ErrorCode::UNKNOWN;

  // Initialization
  error = init();
  if (error != ErrorCode::OK) {
    return error;
  }

  // Get index metadata.
  error = index_dao_->select_all(objects);

  return error;
}

ErrorCode MetadataProvider::get_constraint_metadata(
    std::string_view key, std::string_view value,
    boost::property_tree::ptree& object) {
  ErrorCode error = ErrorCode::UNKNOWN;

  // Initialization
  error = this->init();
  if (error != ErrorCode::OK) {
    return error;
  }

  // Get constraint metadata.
  error = this->select_single(*constraint_dao_, key, {value}, object);
  // Treat as normal, even if there are multiple rows.
  error = (error == ErrorCode::RESULT_MULTIPLE_ROWS ? ErrorCode::OK : error);

  return error;
}

ErrorCode MetadataProvider::get_constraint_metadata(
    std::vector<boost::property_tree::ptree>& container) {
  ErrorCode error = ErrorCode::UNKNOWN;

  // Initialization
  error = init();
  if (error != ErrorCode::OK) {
    return error;
  }

  // Get constraint metadata.
  error = constraint_dao_->select_all(container);

  return error;
}

ErrorCode MetadataProvider::get_column_statistic(
    std::string_view key, std::string_view value,
    boost::property_tree::ptree& object) {
  ErrorCode error = ErrorCode::UNKNOWN;

  // Initialization
  error = this->init();
  if (error != ErrorCode::OK) {
    return error;
  }

  // Get column statistic.
  error = this->select_single(*statistic_dao_, key, {value}, object);
  // Treat as normal, even if there are multiple rows.
  error = (error == ErrorCode::RESULT_MULTIPLE_ROWS ? ErrorCode::OK : error);

  return error;
}

ErrorCode MetadataProvider::get_column_statistic(
    const ObjectId table_id, std::string_view key, std::string_view value,
    boost::property_tree::ptree& object) {
  ErrorCode error = ErrorCode::UNKNOWN;

  // Initialization
  error = this->init();
  if (error != ErrorCode::OK) {
    return error;
  }

  // Get column statistic.
  error = this->select_single(*statistic_dao_, key,
                              {std::to_string(table_id), value}, object);
  // Treat as normal, even if there are multiple rows.
  error = (error == ErrorCode::RESULT_MULTIPLE_ROWS ? ErrorCode::OK : error);

  return error;
}

ErrorCode MetadataProvider::get_column_statistics(
    std::vector<boost::property_tree::ptree>& objects) {
  ErrorCode error = ErrorCode::UNKNOWN;

  // Initialization
  error = init();
  if (error != ErrorCode::OK) {
    return error;
  }

  // Get column statistic.
  error = statistic_dao_->select_all(objects);

  return error;
}

ErrorCode MetadataProvider::get_column_statistics(
    const ObjectId table_id,
    std::vector<boost::property_tree::ptree>& objects) {
  ErrorCode error = ErrorCode::UNKNOWN;

  // Initialization
  error = init();
  if (error != ErrorCode::OK) {
    return error;
  }

  // Get column statistic.
  error = this->select_multiple(*statistic_dao_, Statistics::TABLE_ID,
                                {std::to_string(table_id)}, objects);

  return error;
}

ErrorCode MetadataProvider::get_datatype_metadata(
    std::string_view key, std::string_view value,
    boost::property_tree::ptree& object) {
  ErrorCode error = ErrorCode::UNKNOWN;

  // Initialization
  error = this->init();
  if (error != ErrorCode::OK) {
    return error;
  }

  // Get datatype metadata.
  error = this->select_single(*datatype_dao_, key, {value}, object);
  // Treat as normal, even if there are multiple rows.
  error = (error == ErrorCode::RESULT_MULTIPLE_ROWS ? ErrorCode::OK : error);

  return error;
}

// ============================================================================
ErrorCode MetadataProvider::update_table_metadata(
    const ObjectId object_id, const boost::property_tree::ptree& object) {
  ErrorCode error = ErrorCode::UNKNOWN;

  // Initialization
  error = init();
  if (error != ErrorCode::OK) {
    return error;
  }

  // Start the transaction.
  error = this->start_transaction();
  if (error != ErrorCode::OK) {
    return error;
  }

  // Update table metadata table.
  if (error == ErrorCode::OK) {
    error = table_dao_->update(Table::ID, {std::to_string(object_id)}, object);
  }

  // Remove a metadata object from the column metadata table.
  if (error == ErrorCode::OK) {
    ObjectId removed_id = 0;
    // Delete records associated with TableID.
    error = column_dao_->remove(Column::TABLE_ID, {std::to_string(object_id)},
                                removed_id);
    // No record is found, it is treated as a success.
    error = (error == ErrorCode::NOT_FOUND ? ErrorCode::OK : error);
  }

  // Add metadata object to column metadata table.
  if (error == ErrorCode::OK) {
    // Add metadata object to column metadata table.
    BOOST_FOREACH (const ptree::value_type& node,
                   object.get_child(Table::COLUMNS_NODE)) {
      ptree column = node.second;

      // Set table-id.
      column.put(Column::TABLE_ID, object_id);

      ObjectId added_id = 0;
      // Add metadata object to column metadata table.
      error = column_dao_->insert(column, added_id);
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
    error = constraint_dao_->remove(Constraint::TABLE_ID,
                                    {std::to_string(object_id)}, removed_id);
    // No record is found, it is treated as a success.
    error = (error == ErrorCode::NOT_FOUND ? ErrorCode::OK : error);
  }

  // Add metadata object to constraint metadata table.
  if (error == ErrorCode::OK) {
    auto constraints_node = object.get_child_optional(Table::CONSTRAINTS_NODE);
    if (constraints_node) {
      BOOST_FOREACH (const ptree::value_type& node, constraints_node.get()) {
        // Copy to the temporary area.
        ptree constraint = node.second;

        // Set table-id.
        constraint.put(Constraint::TABLE_ID, object_id);

        ObjectId added_id = 0;
        // Insert the constraint metadata.
        error = constraint_dao_->insert(constraint, added_id);
        if (error != ErrorCode::OK) {
          // When an error occurs, the process is aborted.
          break;
        }
      }
    }
  }

  // End the transaction.
  error = this->end_transaction(error);

  return error;
}

ErrorCode MetadataProvider::update_index_metadata(
    const ObjectId object_id, const boost::property_tree::ptree& object) {
  ErrorCode error = ErrorCode::UNKNOWN;

  // Initialization
  error = init();
  if (error != ErrorCode::OK) {
    return error;
  }

  // Start the transaction.
  error = this->start_transaction();
  if (error != ErrorCode::OK) {
    return error;
  }

  // Update metadata object to index metadata table.
  error = index_dao_->update(Index::ID, {std::to_string(object_id)}, object);

  // End the transaction.
  error = this->end_transaction(error);

  return error;
}

// ============================================================================
ErrorCode MetadataProvider::remove_table_metadata(std::string_view key,
                                                  std::string_view value,
                                                  ObjectId& object_id) {
  ErrorCode error = ErrorCode::UNKNOWN;

  // Initialization
  error = init();
  if (error != ErrorCode::OK) {
    return error;
  }

  // Start the transaction.
  error = this->start_transaction();
  if (error != ErrorCode::OK) {
    return error;
  }

  // Remove a metadata object from the table metadata table.
  error = table_dao_->remove(key, {value}, object_id);

  if (error == ErrorCode::OK) {
    ObjectId removed_id = 0;
    // Remove a metadata object from the constraint metadata table.
    error = column_dao_->remove(Column::TABLE_ID, {std::to_string(object_id)},
                                removed_id);
    // No record is found, it is treated as a success.
    error = (error == ErrorCode::NOT_FOUND ? ErrorCode::OK : error);
  }

  if (error == ErrorCode::OK) {
    ObjectId removed_id = 0;
    // Remove a metadata object from the constraint metadata table.
    error = constraint_dao_->remove(Constraint::TABLE_ID,
                                    {std::to_string(object_id)}, removed_id);
    // No record is found, it is treated as a success.
    error = (error == ErrorCode::NOT_FOUND ? ErrorCode::OK : error);
  }

  // End the transaction.
  error = this->end_transaction(error);

  return error;
}

ErrorCode MetadataProvider::remove_index_metadata(std::string_view key,
                                                  std::string_view value,
                                                  ObjectId& object_id) {
  ErrorCode error = ErrorCode::UNKNOWN;

  // Initialization
  error = init();
  if (error != ErrorCode::OK) {
    return error;
  }

  // Start the transaction.
  error = this->start_transaction();
  if (error != ErrorCode::OK) {
    return error;
  }

  // Remove a metadata object from the index metadata table.
  error = index_dao_->remove(key, {value}, object_id);

  // End the transaction.
  error = this->end_transaction(error);

  return error;
}

ErrorCode MetadataProvider::remove_constraint_metadata(
    const ObjectId object_id) {
  ErrorCode error = ErrorCode::UNKNOWN;

  // Initialization
  error = init();
  if (error != ErrorCode::OK) {
    return error;
  }

  // Start the transaction.
  error = this->start_transaction();
  if (error != ErrorCode::OK) {
    return error;
  }

  ObjectId removed_id = 0;
  // Remove a metadata object from the constraint metadata table.
  error = constraint_dao_->remove(Constraint::ID, {std::to_string(object_id)},
                                  removed_id);

  // End the transaction.
  error = this->end_transaction(error);

  return error;
}

ErrorCode MetadataProvider::remove_column_statistic(std::string_view key,
                                                    std::string_view value,
                                                    ObjectId& object_id) {
  ErrorCode error = ErrorCode::UNKNOWN;

  // Initialization
  error = init();
  if (error != ErrorCode::OK) {
    return error;
  }

  // Start the transaction.
  error = this->start_transaction();
  if (error != ErrorCode::OK) {
    return error;
  }

  // Remove a statistics from the column statistics table.
  error = statistic_dao_->remove(key, {value}, object_id);

  // End the transaction.
  error = this->end_transaction(error);

  return error;
}

ErrorCode MetadataProvider::remove_column_statistics(const ObjectId table_id) {
  ErrorCode error = ErrorCode::UNKNOWN;

  ObjectId removed_id = 0;
  // Remove a statistics from the column statistics table.
  error = this->remove_column_statistic(Statistics::TABLE_ID,
                                        std::to_string(table_id), removed_id);

  return error;
}

ErrorCode MetadataProvider::remove_column_statistic(const ObjectId table_id,
                                                    std::string_view key,
                                                    std::string_view value,
                                                    ObjectId& object_id) {
  ErrorCode error = ErrorCode::UNKNOWN;

  // Initialization
  error = init();
  if (error != ErrorCode::OK) {
    return error;
  }

  // Start the transaction.
  error = this->start_transaction();
  if (error != ErrorCode::OK) {
    return error;
  }

  // Remove a statistics from the column statistics table.
  error =
      statistic_dao_->remove(key, {std::to_string(table_id), value}, object_id);

  // End the transaction.
  error = this->end_transaction(error);

  return error;
}

// ============================================================================
ErrorCode MetadataProvider::set_table_statistic(
    const boost::property_tree::ptree& object, ObjectId& object_id) {
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
    key   = Tables::ID;
    value = std::to_string(optional_id.get());
  } else if (optional_name) {
    key   = Tables::NAME;
    value = optional_name.get();
  } else {
    LOG_ERROR << Message::PARAMETER_FAILED << "\"" << Tables::ID << "\" or \""
              << Tables::NAME << "\" is required.";
    error = ErrorCode::INVALID_PARAMETER;
    return error;
  }

  boost::property_tree::ptree table_metadata;
  // Get table metadata.
  error = get_table_metadata(key, value, table_metadata);
  if (error != ErrorCode::OK) {
    return error;
  }

  // Start the transaction.
  error = this->start_transaction();
  if (error != ErrorCode::OK) {
    return error;
  }

  // Get the updated object id.
  auto opt_object_id = table_metadata.get_optional<ObjectId>(Table::ID);
  object_id          = opt_object_id.value_or(-1);

  // Set the update value.
  table_metadata.put(Table::NUMBER_OF_TUPLES, optional_tuples.get());

  // Update table statistics to table metadata table.
  error = table_dao_->update(key, {value}, table_metadata);

  // End the transaction.
  error = this->end_transaction(error);

  return error;
}

// ============================================================================
ErrorCode MetadataProvider::confirm_permission(std::string_view key,
                                               std::string_view value,
                                               std::string_view permission,
                                               bool& check_result) {
  ErrorCode error = ErrorCode::UNKNOWN;

  // Initialization
  error = init();
  if (error != ErrorCode::OK) {
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

  // In the case of ID specification, check for the presence of the
  // specified ID.
  if (key == Roles::ID) {
    ObjectId object_id;
    error = Utility::str_to_numeric(value, object_id);
    if (error != ErrorCode::OK) {
      return error;
    }

    // Check for the presence of the specified ID.
    auto exists = privilege_dao_->exists(object_id);
    if (!exists) {
      LOG_INFO << "The role with the specified ID does not exist.: "
               << object_id;

      error = ErrorCode::ID_NOT_FOUND;
      return error;
    }
  }

  ptree object;
  // Get privileges for all tables included in the table metadata.
  error = privilege_dao_->select(key, {value}, object);
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

ErrorCode MetadataProvider::start_transaction() const {
  LOG_INFO << "Start a transaction.";

  // Start the transaction.
  return DbSessionManager::get_instance().start_transaction();
}

ErrorCode MetadataProvider::end_transaction(const ErrorCode& result) const {
  ErrorCode error = ErrorCode::UNKNOWN;

  if (result == ErrorCode::OK) {
    LOG_INFO << "Commit a transaction.";

    // Commit the transaction.
    error = DbSessionManager::get_instance().commit();
  } else {
    LOG_INFO << "Rollback a transaction.";

    error = result;
    // Roll back the transaction.
    ErrorCode rollback_result = DbSessionManager::get_instance().rollback();
    if (rollback_result != ErrorCode::OK) {
      error = rollback_result;
    }
  }

  return error;
}

bool MetadataProvider::check_of_privilege(
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

template <typename T, typename = std::enable_if_t<std::is_base_of_v<Dao, T>>>
ErrorCode MetadataProvider::select_single(
    const T& dao, std::string_view key,
    const std::vector<std::string_view> values,
    boost::property_tree::ptree& object) const {
  ErrorCode error = ErrorCode::UNKNOWN;

  ptree tmp_object;
  // Select data from the DAO.
  error = dao.select(key, values, tmp_object);

  if (error == ErrorCode::OK) {
    LOG_DEBUG << "Select the metadata. " << typeid(dao).name() << ":[" << key
              << "][" << values << "]=> " << tmp_object.size() << " rows";

    object = tmp_object.front().second;
    if (tmp_object.size() >= 2) {
      error = ErrorCode::RESULT_MULTIPLE_ROWS;

      LOG_WARNING
          << "Multiple rows retrieved. Use the metadata from the first row. "
          << typeid(dao).name() << ":[" << key << "][" << values << "]=> "
          << tmp_object.size() << " rows";
    }
  } else {
    LOG_DEBUG << "Select the metadata. " << typeid(dao).name() << ":[" << key
              << "][" << values << "]=> ErrorCode:" << error;
  }

  return error;
}

template <typename T, typename = std::enable_if_t<std::is_base_of_v<Dao, T>>>
ErrorCode MetadataProvider::select_multiple(
    const T& dao, std::string_view key,
    const std::vector<std::string_view> values,
    std::vector<boost::property_tree::ptree>& objects) const {
  ErrorCode error = ErrorCode::UNKNOWN;

  ptree tmp_object;
  // Select data from the DAO.
  error = dao.select(key, values, tmp_object);

  objects.clear();
  if (error == ErrorCode::OK) {
    std::transform(tmp_object.begin(), tmp_object.end(),
                   std::back_inserter(objects),
                   [](ptree::value_type vt) { return vt.second; });

    LOG_DEBUG << "Select the metadata. " << typeid(dao).name() << ":[" << key
              << "][" << values << "]=> " << tmp_object.size() << " rows";
  } else {
    LOG_DEBUG << "Select the metadata. " << typeid(dao).name() << ":[" << key
              << "][" << values << "]=> ErrorCode: " << error;
  }

  return error;
}

}  // namespace manager::metadata::db
