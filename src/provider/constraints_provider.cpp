/*
 * Copyright 2022 tsurugi project.
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
#include "manager/metadata/provider/constraints_provider.h"

#include "manager/metadata/helper/logging_helper.h"
#include "manager/metadata/constraints.h"

// =============================================================================
namespace manager::metadata::db {

using boost::property_tree::ptree;

/**
 * @brief Initialize and prepare to access the metadata repository.
 * @return ErrorCode::OK if success, otherwise an error code.
 */
ErrorCode ConstraintsProvider::init() {
  ErrorCode error = ErrorCode::UNKNOWN;
  std::shared_ptr<GenericDAO> gdao = nullptr;

  if (constraints_dao_ == nullptr) {
    // Get an instance of the TablesDAO class.
    error = session_manager_->get_dao(GenericDAO::TableName::CONSTRAINTS, gdao);
    if (error != ErrorCode::OK) {
      return error;
    }
    // Set ConstraintsDAO instance.
    constraints_dao_ = std::static_pointer_cast<ConstraintsDAO>(gdao);
  }

  error = ErrorCode::OK;
  return error;
}

/**
 * @brief Add constraints metadata to constraint metadata repository.
 * @param object         [in]  constraints metadata to add.
 * @param constraint_id  [out] ID of the added constraint metadata.
 * @return ErrorCode::OK if success, otherwise an error code.
 */
ErrorCode ConstraintsProvider::add_constraint_metadata(
    const boost::property_tree::ptree& object, ObjectIdType& constraint_id) {
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

  // Add metadata object to constraint metadata table.
  error = constraints_dao_->insert_constraint_metadata(object, constraint_id);

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
 * @brief Gets one constraint metadata object from the constraint metadata repository,
 *   where key = value.
 * @param key     [in]  key of constraint metadata object.
 * @param value   [in]  value of constraint metadata object.
 * @param object  [out] one constraint metadata object to get, where key = value.
 * @retval ErrorCode::OK if success,
 * @retval ErrorCode::ID_NOT_FOUND if the constraint id does not exist.
 * @retval ErrorCode::NAME_NOT_FOUND if the constraint name does not exist.
 * @retval otherwise an error code.
 */
ErrorCode ConstraintsProvider::get_constraint_metadata(
    std::string_view key, std::string_view value,
    boost::property_tree::ptree& object) {
  ErrorCode error = ErrorCode::UNKNOWN;

  // Parameter value check.
  if ((key != Constraint::ID) && (key != Constraint::NAME)) {
    error = ErrorCode::INVALID_PARAMETER;
    return error;
  }

  // Initialization
  error = init();
  if (error != ErrorCode::OK) {
    return error;
  }

  // Get constraint metadata.
  error = constraints_dao_->select_constraint_metadata(key, value, object);
  if (error != ErrorCode::OK) {
    return error;
  }

  // Set the search key for column metadata.
  std::string constraint_id;
  if (key == Constraint::ID) {
    constraint_id = value;
  } else {
    auto o_constraint_id = object.get_optional<std::string>(Constraint::ID);
    if (o_constraint_id) {
      constraint_id = o_constraint_id.value();
    }
  }

  return error;
}

/**
 * @brief Gets all constraint metadata object from the constraint metadata repository.
 *   If the constraint metadata does not exist, return the container as empty.
 * @param container  [out] constraint metadata object to get.
 * @return ErrorCode::OK if success, otherwise an error code.
 */
ErrorCode ConstraintsProvider::get_constraint_metadata(
    std::vector<boost::property_tree::ptree>& container) {
  ErrorCode error = ErrorCode::UNKNOWN;

  // Initialization
  error = init();
  if (error != ErrorCode::OK) {
    return error;
  }

  // Get constraint metadata.
  error = constraints_dao_->select_constraint_metadata(container);

  return error;
}

/**
 * @brief Remove all metadata-object based on the given constraint name from metadata-repositories.
 * @param key            [in]  key of constraint metadata object.
 * @param value          [in]  value of constraint metadata object.
 * @param constraint_id  [out] ID of the removed constraint metadata.
 * @retval ErrorCode::OK if success.
 * @retval ErrorCode::ID_NOT_FOUND if the constraint id does not exist.
 * @retval ErrorCode::NAME_NOT_FOUND if the constraint name does not exist.
 * @retval otherwise an error code.
 */
ErrorCode ConstraintsProvider::remove_constraint_metadata(std::string_view key,
                                                std::string_view value,
                                                ObjectIdType& constraint_id) {
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

  error = constraints_dao_->delete_constraint_metadata(key, value, constraint_id);
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

}  // namespace manager::metadata::db
