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
#include "manager/metadata/provider/indexes_provider.h"

#include <boost/foreach.hpp>

#include "manager/metadata/dao/generic_dao.h"
#include "manager/metadata/datatypes.h"
#include "manager/metadata/indexes.h"
#include "manager/metadata/dao/dao.h"
#include "manager/metadata/dao/json/index_dao_json.h"
#include "manager/metadata/dao/db_session_manager.h"

namespace manager::metadata::db {

using boost::property_tree::ptree;

// ============================================================================
// IndexesProvider class methods.

/**
 * @brief Initialize and prepare to access the metadata repository.
 * @return ErrorCode::OK if success, otherwise an error code.
 */
ErrorCode IndexesProvider::init() {

  std::shared_ptr<Dao> gdao = nullptr;

  if (index_dao_ == nullptr) {
    index_dao_ = session_->get_index_dao();
  }

  return ErrorCode::OK;
}

/**
 * @brief Add index metadata to index metadata repository.
 * @param (object)     [in]  index metadata to add.
 * @param (index_id)   [out] ID of the added index metadata.
 * @return ErrorCode::OK if success, otherwise an error code.
 */
ErrorCode IndexesProvider::add_index_metadata(
    const boost::property_tree::ptree& object, ObjectIdType& index_id) {
  
  ErrorCode error = ErrorCode::UNKNOWN;

  error = this->init();
  if (error != ErrorCode::OK) {
    return error;
  }

  error = session_->start_transaction();
  if (error != ErrorCode::OK) {
    return error;
  }

  // Add index metadata object to index metadata table.
  error = index_dao_->insert(object, index_id);
  if (error != ErrorCode::OK) {
    // Roll back the transaction.
    ErrorCode rollback_result = session_->rollback();
    if (rollback_result != ErrorCode::OK) {
      return rollback_result;
    }

    return error;
  }

  error = session_->commit();

  return error;
}

/**
 * @brief Gets one index metadata object from the index metadata table,
 *   where key = value.
 * @param (key)     [in]  key of index metadata object.
 * @param (value)   [in]  value of index metadata object.
 * @param (object)  [out] one index metadata object to get,
 *   where key = value.
 * @retval ErrorCode::OK if success,
 * @retval ErrorCode::ID_NOT_FOUND if the index id does not exist.
 * @retval ErrorCode::NAME_NOT_FOUND if the indexes name does not exist.
 * @retval otherwise an error code.
 */
ErrorCode IndexesProvider::get_index_metadata(
    std::string_view key, std::string_view value,
    boost::property_tree::ptree& object) {

  ErrorCode error = ErrorCode::UNKNOWN;

  if ((key != Indexes::ID) && (key != Indexes::NAME)) {
    error = ErrorCode::INVALID_PARAMETER;
    return error;
  }

  error = this->init();
  if (error != ErrorCode::OK) {
    return error;
  }

  // Get index metadata.
  error = index_dao_->select(key, value, object);
  if (error != ErrorCode::OK) {
    return error;
  }

  return error;
}

/**
 * @brief Get all index metadata objects from metadata table.
 * @param (objects) [out] table metadata object to get.
 * @return ErrorCode::OK if success, otherwise an error code.
 */
ErrorCode IndexesProvider::get_index_metadata(
    std::vector<boost::property_tree::ptree>& objects) {

  ErrorCode error = ErrorCode::UNKNOWN;

  error = init();
  if (error != ErrorCode::OK) {
    return error;
  }

  error = index_dao_->select_all(objects);
  if (error != ErrorCode::OK) {
    return error;
  }

  return error;
}

/**
 * @brief Remove a metadata object which has the specified name 
 * from metadata table.
 * @param (key)       [in]  key of index metadata object.
 * @param (value)     [in]  value of index metadata object.
 * @param (index_id)  [out] ID of the removed index metadata.
 * @retval ErrorCode::OK if success.
 * @retval ErrorCode::ID_NOT_FOUND if the index id does not exist.
 * @retval ErrorCode::NAME_NOT_FOUND if the index name does not exist.
 * @retval otherwise an error code.
 */
ErrorCode IndexesProvider::remove_index_metadata(std::string_view key,
                                                std::string_view value,
                                                ObjectIdType& index_id) {
  ErrorCode error = ErrorCode::UNKNOWN;

  error = init();
  if (error != ErrorCode::OK) {
    return error;
  }

  error = session_->start_transaction();
  if (error != ErrorCode::OK) {
    return error;
  }

  error = index_dao_->remove(key, value, index_id);
  if (error == ErrorCode::OK) {
    error = session_->commit();
  } else {
    // Roll back the transaction.
    ErrorCode rollback_result = session_->rollback();
    if (rollback_result != ErrorCode::OK) {
      error = rollback_result;
    }
  }

  return error;
}

}  // namespace manager::metadata::db
