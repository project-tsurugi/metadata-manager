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

// =============================================================================
namespace manager::metadata::db {

using boost::property_tree::ptree;

/**
 * @brief Initialize and prepare to access the metadata repository.
 * @return ErrorCode::OK if success, otherwise an error code.
 */
ErrorCode ConstraintsProvider::init() {
  ErrorCode error = ErrorCode::UNKNOWN;

  // ConstraintsDAO
  if (!constraints_dao_) {
    // Get an instance of the ConstraintsDAO.
    constraints_dao_ = session_manager_->get_constraints_dao();
    if (!constraints_dao_) {
      error = ErrorCode::DATABASE_ACCESS_FAILURE;
      return error;
    }
    // Prepare to access table metadata.
    error = constraints_dao_->prepare();
    if (error != ErrorCode::OK) {
      constraints_dao_.reset();
      return error;
    }
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
ErrorCode ConstraintsProvider::add_constraint_metadata(const boost::property_tree::ptree& object,
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

  // Add metadata object to constraint metadata table.
  error = constraints_dao_->insert(object, constraint_id);

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
 * @param constraint_id  [in]  constraint id.
 * @param object         [out] one constraint metadata object to get, where key = value.
 * @retval ErrorCode::OK if success,
 * @retval ErrorCode::ID_NOT_FOUND if the constraint id does not exist.
 * @retval ErrorCode::NOT_FOUND if the table id does not exist.
 * @retval otherwise an error code.
 */
ErrorCode ConstraintsProvider::get_constraint_metadata(const ObjectId constraint_id,
                                                       boost::property_tree::ptree& object) {
  ErrorCode error = ErrorCode::UNKNOWN;

  // Initialization
  error = init();
  if (error != ErrorCode::OK) {
    return error;
  }

  std::string key_constraint_id = std::to_string(constraint_id);
  // Get constraint metadata.
  error = constraints_dao_->select(Constraint::ID, key_constraint_id, object);
  if (error != ErrorCode::OK) {
    return error;
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
  error = constraints_dao_->select_all(container);

  return error;
}

/**
 * @brief Remove metadata-object based on the given constraint id from metadata-repositories.
 * @param constraint_id  [in]  constraint id.
 * @retval ErrorCode::OK if success.
 * @retval ErrorCode::ID_NOT_FOUND if the constraint id does not exist.
 * @retval otherwise an error code.
 */
ErrorCode ConstraintsProvider::remove_constraint_metadata(const ObjectId constraint_id) {
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

  ObjectId removed_id = 0;
  // Remove a metadata object from the constraints metadata table.
  error = constraints_dao_->remove(Constraint::ID, std::to_string(constraint_id), removed_id);

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
