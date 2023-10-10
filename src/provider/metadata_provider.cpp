/*
 * Copyright 2022-2023 Project Tsurugi.
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

#include <boost/foreach.hpp>

#include "manager/metadata/common/utility.h"
#include "manager/metadata/constraints.h"
#include "manager/metadata/helper/logging_helper.h"
#include "manager/metadata/helper/ptree_helper.h"
#include "manager/metadata/roles.h"
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

  // Role metadata DAO.
  error = session.get_roles_dao(role_dao_);
  if (error != ErrorCode::OK) {
    return error;
  }

  error = ErrorCode::OK;
  return error;
}

// ============================================================================
ErrorCode MetadataProvider::transaction(
    std::function<ErrorCode()> trans_function) {
  ErrorCode error = ErrorCode::UNKNOWN;

  LOG_INFO << "Start a transaction.";

  // Initialization
  error = this->init();
  if (error != ErrorCode::OK) {
    return error;
  }

  auto& session = DbSessionManager::get_instance();
  // Start the transaction.
  error = session.start_transaction();
  if (error != ErrorCode::OK) {
    return error;
  }

  // Perform transaction processing.
  error = trans_function();

  // End the transaction.
  if (error == ErrorCode::OK) {
    LOG_INFO << "Commit a transaction.";

    // Commit the transaction.
    error = session.commit();
  } else {
    LOG_INFO << "Rollback a transaction.";

    // Roll back the transaction.
    ErrorCode rollback_result = session.rollback();
    if (rollback_result != ErrorCode::OK) {
      error = rollback_result;
    }
  }

  return error;
}

// ============================================================================
ErrorCode MetadataProvider::add_table_metadata(
    const boost::property_tree::ptree& object, ObjectId* object_id) {
  ErrorCode error = ErrorCode::UNKNOWN;

  // Initialization
  error = this->init();
  if (error != ErrorCode::OK) {
    return error;
  }

  ObjectId added_oid = INVALID_OBJECT_ID;
  // Add metadata object to table metadata table.
  error = table_dao_->insert(object, added_oid);

  // Set a value if object_id is not null.
  if ((error == ErrorCode::OK) && (object_id != nullptr)) {
    *object_id = added_oid;
  }

  return error;
}

ErrorCode MetadataProvider::add_column_metadata(
    const boost::property_tree::ptree& object, ObjectId* object_id) {
  ErrorCode error = ErrorCode::UNKNOWN;

  // Initialization
  error = this->init();
  if (error != ErrorCode::OK) {
    return error;
  }

  ptree tmp_object;
  if (ptree_helper::is_array(object)) {
    // Copy to the temporary area.
    tmp_object = object;
  } else {
    // Convert to array format.
    tmp_object.push_back(std::make_pair("", object));
  }

  std::vector<ObjectId> added_oids;
  added_oids.reserve(tmp_object.size());

  BOOST_FOREACH (auto& node, tmp_object) {
    auto& column = node.second;

    // Erase the columns-id.
    column.erase(Column::ID);

    ObjectId temp_oid = INVALID_OBJECT_ID;
    // Insert the column metadata.
    error = column_dao_->insert(column, temp_oid);
    if (error != ErrorCode::OK) {
      // When an error occurs, the process is aborted.
      break;
    }

    added_oids.push_back(temp_oid);
  }

  // Set a value if object_id is not null.
  if ((error == ErrorCode::OK) && (object_id != nullptr)) {
    *object_id = (!added_oids.empty() ? added_oids.front() : INVALID_OBJECT_ID);
  }

  return error;
}

ErrorCode MetadataProvider::add_index_metadata(
    const boost::property_tree::ptree& object, ObjectId* object_id) {
  ErrorCode error = ErrorCode::UNKNOWN;

  // Initialization
  error = this->init();
  if (error != ErrorCode::OK) {
    return error;
  }

  ObjectId added_oid = INVALID_OBJECT_ID;
  // Add metadata object to index metadata table.
  error = index_dao_->insert(object, added_oid);

  // Set a value if object_id is not null.
  if ((error == ErrorCode::OK) && (object_id != nullptr)) {
    *object_id = added_oid;
  }

  return error;
}

ErrorCode MetadataProvider::add_constraint_metadata(
    const boost::property_tree::ptree& object, ObjectId* object_id) {
  ErrorCode error = ErrorCode::UNKNOWN;

  // Initialization
  error = this->init();
  if (error != ErrorCode::OK) {
    return error;
  }

  ptree tmp_object;
  if (ptree_helper::is_array(object)) {
    // Copy to the temporary area.
    tmp_object = object;
  } else {
    // Convert to array format.
    tmp_object.push_back(std::make_pair("", object));
  }

  std::vector<ObjectId> added_oids;
  added_oids.reserve(tmp_object.size());

  BOOST_FOREACH (auto& node, tmp_object) {
    auto& constraint = node.second;

    // Erase constraint-id.
    constraint.erase(Constraint::ID);

    ObjectId temp_oid = INVALID_OBJECT_ID;
    // Insert the constraint metadata.
    error = constraint_dao_->insert(constraint, temp_oid);
    if (error != ErrorCode::OK) {
      // When an error occurs, the process is aborted.
      break;
    }

    added_oids.push_back(temp_oid);
  }

  // Set a value if object_id is not null.
  if ((error == ErrorCode::OK) && (object_id != nullptr)) {
    *object_id = (!added_oids.empty() ? added_oids.front() : INVALID_OBJECT_ID);
  }

  return error;
}

ErrorCode MetadataProvider::add_column_statistic(
    const boost::property_tree::ptree& object, ObjectId* object_id) {
  ErrorCode error = ErrorCode::UNKNOWN;

  // Initialization
  error = this->init();
  if (error != ErrorCode::OK) {
    return error;
  }

  ObjectId added_oid = INVALID_OBJECT_ID;
  // Register column statistics via DAO.
  error = statistic_dao_->insert(object, added_oid);

  // Set a value if object_id is not null.
  if ((error == ErrorCode::OK) && (object_id != nullptr)) {
    *object_id = added_oid;
  }

  return error;
}

// ============================================================================
ErrorCode MetadataProvider::get_table_metadata(
    const std::map<std::string_view, std::string_view>& keys,
    boost::property_tree::ptree& object) {
  ErrorCode error = ErrorCode::UNKNOWN;

  // Initialization
  error = this->init();
  if (error != ErrorCode::OK) {
    return error;
  }

  // Get table metadata.
  error = table_dao_->select(keys, object);

  // Set the error code when metadata is not found.
  if ((error == ErrorCode::OK) && (object.size() == 0)) {
    error = get_not_found_error_code(keys);
  }

  LOG_DEBUG << "Select the table metadata. [" << keys << "]=> " << object.size()
            << " rows, ErrorCode:" << error;

  return error;
}

ErrorCode MetadataProvider::get_column_metadata(
    const std::map<std::string_view, std::string_view>& keys,
    boost::property_tree::ptree& object) {
  ErrorCode error = ErrorCode::UNKNOWN;

  // Initialization
  error = init();
  if (error != ErrorCode::OK) {
    return error;
  }

  // Get column metadata.
  error = column_dao_->select(keys, object);

  // Set the error code when metadata is not found.
  if ((error == ErrorCode::OK) && (object.size() == 0)) {
    error = get_not_found_error_code(keys);
  }

  LOG_DEBUG << "Select the column metadata. [" << keys << "]=> "
            << object.size() << " rows, ErrorCode:" << error;

  return error;
}

ErrorCode MetadataProvider::get_index_metadata(
    const std::map<std::string_view, std::string_view>& keys,
    boost::property_tree::ptree& object) {
  ErrorCode error = ErrorCode::UNKNOWN;

  // Initialization
  error = init();
  if (error != ErrorCode::OK) {
    return error;
  }

  // Get index metadata.
  error = index_dao_->select(keys, object);

  // Set the error code when metadata is not found.
  if ((error == ErrorCode::OK) && (object.size() == 0)) {
    error = get_not_found_error_code(keys);
  }

  LOG_DEBUG << "Select the index metadata. [" << keys << "]=> "
            << object.size() << " rows, ErrorCode:" << error;

  return error;
}

ErrorCode MetadataProvider::get_constraint_metadata(
    const std::map<std::string_view, std::string_view>& keys,
    boost::property_tree::ptree& object) {
  ErrorCode error = ErrorCode::UNKNOWN;

  // Initialization
  error = init();
  if (error != ErrorCode::OK) {
    return error;
  }

  // Get constraint metadata.
  error = constraint_dao_->select(keys, object);

  // Set the error code when metadata is not found.
  if ((error == ErrorCode::OK) && (object.size() == 0)) {
    error = get_not_found_error_code(keys);
  }

  LOG_DEBUG << "Select the constraint metadata. [" << keys << "]=> "
            << object.size() << " rows, ErrorCode:" << error;

  return error;
}

ErrorCode MetadataProvider::get_column_statistic(
    const std::map<std::string_view, std::string_view>& keys,
    boost::property_tree::ptree& object) {
  ErrorCode error = ErrorCode::UNKNOWN;

  // Initialization
  error = init();
  if (error != ErrorCode::OK) {
    return error;
  }

  // Get column statistic.
  error = statistic_dao_->select(keys, object);

  // Set the error code when metadata is not found.
  if ((error == ErrorCode::OK) && (object.size() == 0)) {
    error = get_not_found_error_code(keys);
  }

  LOG_DEBUG << "Select the column statistic. [" << keys << "]=> "
            << object.size() << " rows, ErrorCode:" << error;

  return error;
}

ErrorCode MetadataProvider::get_datatype_metadata(
    const std::map<std::string_view, std::string_view>& keys,
    boost::property_tree::ptree& object) {
  ErrorCode error = ErrorCode::UNKNOWN;

  // Initialization
  error = this->init();
  if (error != ErrorCode::OK) {
    return error;
  }

  // Get _datatype metadata.
  error = datatype_dao_->select(keys, object);

  // Set the error code when metadata is not found.
  if ((error == ErrorCode::OK) && (object.size() == 0)) {
    error = get_not_found_error_code(keys);
  }

  LOG_DEBUG << "Select the datatype metadata. [" << keys << "]=> "
            << object.size() << " rows, ErrorCode:" << error;

  return error;
}

ErrorCode MetadataProvider::get_role_metadata(
    const std::map<std::string_view, std::string_view>& keys,
    boost::property_tree::ptree& object) {
  ErrorCode error = ErrorCode::UNKNOWN;

  // Initialization
  error = this->init();
  if (error != ErrorCode::OK) {
    return error;
  }

  // Get role info.
  error = role_dao_->select(keys, object);

  // Set the error code when metadata is not found.
  if ((error == ErrorCode::OK) && (object.size() == 0)) {
    error = get_not_found_error_code(keys);
  }

  LOG_DEBUG << "Select the roles. [" << keys << "]=> " << object.size()
            << " rows, ErrorCode:" << error;

  return error;
}

ErrorCode MetadataProvider::get_privileges(
    const std::map<std::string_view, std::string_view>& keys,
    boost::property_tree::ptree& object) {
  ErrorCode error = ErrorCode::UNKNOWN;

  // Initialization
  error = this->init();
  if (error != ErrorCode::OK) {
    return error;
  }

  // In the case of ID specification, check for the presence of the
  // specified ID.
  auto it = keys.find(Roles::ROLE_OID);
  if (it != keys.end()) {
    ObjectId object_id;
    error = Utility::str_to_numeric(it->second, object_id);
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

  ptree privileges;
  // Get privileges for all tables included in the table metadata.
  error = privilege_dao_->select(keys, privileges);

  // Set the error code when metadata is not found.
  if ((error == ErrorCode::OK) && (privileges.size() == 0)) {
    error = ErrorCode::NOT_FOUND;
  } else if (error == ErrorCode::NOT_FOUND) {
    // If NOT_FOUND is returned from the DAO, convert.
    error = get_not_found_error_code(keys);
  }

  object.clear();
  if (error == ErrorCode::OK) {
    // Convert DAO data format to Provider data format.
    convert_privilege(privileges, object);
  }
  LOG_DEBUG << "Select the privileges. [" << keys << "]=> " << object.size()
            << " rows, ErrorCode:" << error;

  return error;
}

// ============================================================================
ErrorCode MetadataProvider::update_table_metadata(
    const std::map<std::string_view, std::string_view>& keys,
    const boost::property_tree::ptree& object, uint64_t* rows) {
  ErrorCode error = ErrorCode::UNKNOWN;

  // Initialization
  error = init();
  if (error != ErrorCode::OK) {
    return error;
  }

  uint64_t updated_rows;
  // Update table metadata table.
  error = table_dao_->update(keys, object, updated_rows);

  if (error == ErrorCode::OK) {
    if (updated_rows == 0) {
      // Set the error code when metadata is not found.
      error = get_not_found_error_code(keys);
    }
    if (rows != nullptr) {
      // Set the number of rows updated.
      *rows = updated_rows;
    }
  }

  return error;
}

ErrorCode MetadataProvider::update_column_metadata(
    const std::map<std::string_view, std::string_view>& keys,
    const boost::property_tree::ptree& object, uint64_t* rows) {
  ErrorCode error = ErrorCode::UNKNOWN;

  // Initialization
  error = init();
  if (error != ErrorCode::OK) {
    return error;
  }

  // Extract the table-id.
  std::string table_id;
  auto it = keys.find(Column::TABLE_ID);
  if (it != keys.end()) {
    table_id = it->second;
  } else {
    error = ErrorCode::NOT_SUPPORTED;
    return error;
  }

  std::vector<ObjectId> removed_ids;
  // Remove a metadata object from the column metadata table.
  error = column_dao_->remove(keys, removed_ids);

  uint64_t updated_rows = 0;
  if (error == ErrorCode::OK) {
    ptree tmp_object;
    if (ptree_helper::is_array(object)) {
      // Copy to the temporary area.
      tmp_object = object;
    } else {
      // Convert to array format.
      tmp_object.push_back(std::make_pair("", object));
    }
    updated_rows = tmp_object.size();

    BOOST_FOREACH (auto& node, tmp_object) {
      auto& column = node.second;

      // Set the table-id.
      column.put(Column::TABLE_ID, table_id);

      ObjectId temp_oid = INVALID_OBJECT_ID;
      // Insert the column metadata.
      error = column_dao_->insert(column, temp_oid);
      if (error != ErrorCode::OK) {
        // When an error occurs, the process is aborted.
        break;
      }
    }
  }

  // Set the number of rows updated.
  if ((error == ErrorCode::OK) && (rows != nullptr)) {
    *rows = updated_rows;
  }

  return error;
}

ErrorCode MetadataProvider::update_index_metadata(
    const std::map<std::string_view, std::string_view>& keys,
    const boost::property_tree::ptree& object, uint64_t* rows) {
  ErrorCode error = ErrorCode::UNKNOWN;

  // Initialization
  error = init();
  if (error != ErrorCode::OK) {
    return error;
  }

  uint64_t updated_rows;
  // Update table metadata table.
  error = index_dao_->update(keys, object, updated_rows);

  if (error == ErrorCode::OK) {
    if (updated_rows == 0) {
      // Set the error code when metadata is not found.
      error = get_not_found_error_code(keys);
    }
    if (rows != nullptr) {
      // Set the number of rows updated.
      *rows = updated_rows;
    }
  }

  return error;
}

ErrorCode MetadataProvider::update_constraint_metadata(
    const std::map<std::string_view, std::string_view>& keys,
    const boost::property_tree::ptree& object, uint64_t* rows) {
  ErrorCode error = ErrorCode::UNKNOWN;

  // Initialization
  error = init();
  if (error != ErrorCode::OK) {
    return error;
  }

  // Extract the table-id.
  std::string table_id;
  auto it = keys.find(Constraint::TABLE_ID);
  if (it != keys.end()) {
    table_id = it->second;
  } else {
    error = ErrorCode::NOT_SUPPORTED;
    return error;
  }

  std::vector<ObjectId> removed_ids;
  // Remove a metadata object from the constraint metadata table.
  error = constraint_dao_->remove(keys, removed_ids);

  uint64_t updated_rows = 0;
  if (error == ErrorCode::OK) {
    ptree tmp_object;
    if (ptree_helper::is_array(object)) {
      // Copy to the temporary area.
      tmp_object = object;
    } else {
      // Convert to array format.
      tmp_object.push_back(std::make_pair("", object));
    }
    updated_rows = tmp_object.size();

    BOOST_FOREACH (auto& node, tmp_object) {
      auto& constraint = node.second;

      // Set the table-id.
      constraint.put(Constraint::TABLE_ID, table_id);

      ObjectId temp_oid = INVALID_OBJECT_ID;
      // Insert the column metadata.
      error = constraint_dao_->insert(constraint, temp_oid);
      if (error != ErrorCode::OK) {
        // When an error occurs, the process is aborted.
        break;
      }
    }
  }

  // Set the number of rows updated.
  if ((error == ErrorCode::OK) && (rows != nullptr)) {
    *rows = updated_rows;
  }

  return error;
}

// ============================================================================
ErrorCode MetadataProvider::remove_table_metadata(
    const std::map<std::string_view, std::string_view>& keys,
    std::vector<ObjectId>* object_ids) {
  ErrorCode error = ErrorCode::UNKNOWN;

  // Initialization
  error = init();
  if (error != ErrorCode::OK) {
    return error;
  }

  std::vector<ObjectId> removed_ids;
  // Remove a metadata object from the table metadata table.
  error = table_dao_->remove(keys, removed_ids);

  if (error == ErrorCode::OK) {
    if (removed_ids.size() == 0) {
      // Set the error code when metadata is not found.
      error = get_not_found_error_code(keys);
    }
    // Set the id of remove.
    if (object_ids != nullptr) {
      *object_ids = removed_ids;
    }
  }

  return error;
}

ErrorCode MetadataProvider::remove_column_metadata(
    const std::map<std::string_view, std::string_view>& keys,
    std::vector<ObjectId>* object_ids) {
  ErrorCode error = ErrorCode::UNKNOWN;

  // Initialization
  error = init();
  if (error != ErrorCode::OK) {
    return error;
  }

  std::vector<ObjectId> removed_ids;
  // Remove a metadata object from the column metadata table.
  error = column_dao_->remove(keys, removed_ids);

  if (error == ErrorCode::OK) {
    if (removed_ids.size() == 0) {
      // Set the error code when metadata is not found.
      error = get_not_found_error_code(keys);
    }
    // Set the id of remove.
    if (object_ids != nullptr) {
      *object_ids = removed_ids;
    }
  }

  return error;
}

ErrorCode MetadataProvider::remove_index_metadata(
    const std::map<std::string_view, std::string_view>& keys,
    std::vector<ObjectId>* object_ids) {
  ErrorCode error = ErrorCode::UNKNOWN;

  // Initialization
  error = init();
  if (error != ErrorCode::OK) {
    return error;
  }

  std::vector<ObjectId> removed_ids;
  // Remove a metadata object from the index metadata table.
  error = index_dao_->remove(keys, removed_ids);

  if (error == ErrorCode::OK) {
    if (removed_ids.size() == 0) {
      // Set the error code when metadata is not found.
      error = get_not_found_error_code(keys);
    }
    // Set the id of remove.
    if (object_ids != nullptr) {
      *object_ids = removed_ids;
    }
  }

  return error;
}

ErrorCode MetadataProvider::remove_constraint_metadata(
    const std::map<std::string_view, std::string_view>& keys,
    std::vector<ObjectId>* object_ids) {
  ErrorCode error = ErrorCode::UNKNOWN;

  // Initialization
  error = init();
  if (error != ErrorCode::OK) {
    return error;
  }

  std::vector<ObjectId> removed_ids;
  // Remove a metadata object from the constraint metadata table.
  error = constraint_dao_->remove(keys, removed_ids);

  if (error == ErrorCode::OK) {
    if (removed_ids.size() == 0) {
      // Set the error code when metadata is not found.
      error = get_not_found_error_code(keys);
    }
    // Set the id of remove.
    if (object_ids != nullptr) {
      *object_ids = removed_ids;
    }
  }

  return error;
}

ErrorCode MetadataProvider::remove_column_statistics(
    const std::map<std::string_view, std::string_view>& keys,
    std::vector<ObjectId>* object_ids) {
  ErrorCode error = ErrorCode::UNKNOWN;

  // Initialization
  error = init();
  if (error != ErrorCode::OK) {
    return error;
  }

  std::vector<ObjectId> removed_ids;
  // Remove a metadata object from the column statistics table.
  error = statistic_dao_->remove(keys, removed_ids);

  if (error == ErrorCode::OK) {
    if (removed_ids.size() == 0) {
      // Set the error code when metadata is not found.
      error = get_not_found_error_code(keys);
    }
    // Set the id of remove.
    if (object_ids != nullptr) {
      *object_ids = removed_ids;
    }
  }

  return error;
}

/* =============================================================================
 * Private method area
 */

void MetadataProvider::convert_privilege(
    const boost::property_tree::ptree& src,
    boost::property_tree::ptree& dst) const {
  ErrorCode error = ErrorCode::UNKNOWN;

  for (auto iterator = src.begin(); iterator != src.end(); iterator++) {
    if (iterator->second.empty()) {
      break;
    }

    auto iterator_child = iterator->second.begin();
    if (iterator_child->second.empty()) {
      auto table_name       = iterator->first;
      auto table_privileges = iterator->second;

      // Convert DAO data format to Provider data format.
      ptree child_object;
      for (const auto& map_value : privileges_map_) {
        auto value =
            table_privileges.get_optional<std::string>(map_value.first);
        if (value) {
          child_object.put(map_value.second, value.value());
        }
      }

      // Add a list of privileges to the child node of the table.
      dst.add_child(table_name, child_object);
    } else {
      convert_privilege(iterator->second, dst);
    }
  }
}

}  // namespace manager::metadata::db
