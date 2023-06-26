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

#include <boost/foreach.hpp>

#include "manager/metadata/common/constants.h"
#include "manager/metadata/datatypes.h"
#include "manager/metadata/index.h"
#include "manager/metadata/dao/dao.h"
#include "manager/metadata/dao/db_session_manager.h"

namespace manager::metadata::db {

using boost::property_tree::ptree;

// ============================================================================
// MetadataProvider class methods.

/**
 * @brief Initialize and prepare to access the metadata repository.
 * @return ErrorCode::OK if success, otherwise an error code.
 */
ErrorCode MetadataProvider::init() {
  ErrorCode error = ErrorCode::UNKNOWN;

  // Establish a connection to the metadata repository.
  error = session_->connect();
  if (error != ErrorCode::OK) {
    return error;
  }

  // IndexesDAO
  if (index_dao_ == nullptr) {
    // Get an instance of the IndexesDAO.
    index_dao_ = session_->get_indexes_dao();

    // Prepare to access table metadata.
    error = index_dao_->prepare();
    if (error != ErrorCode::OK) {
      return error;
    }
  }

  return ErrorCode::OK;
}

/**
 * @brief Add a index metadata object to the metadata table.
 * @param object    [in]  index metadata to add.
 * @param object_id [out] ID of the added index metadata.
 * @return ErrorCode::OK if success, otherwise an error code.
 */
ErrorCode MetadataProvider::add_index_metadata(
    const boost::property_tree::ptree& object, ObjectIdType& object_id) {
  
  ErrorCode error = ErrorCode::UNKNOWN;

  error = this->init();
  if (error != ErrorCode::OK) {
    return error;
  }

  error = session_->start_transaction();
  if (error != ErrorCode::OK) {
    return error;
  }

  error = index_dao_->insert(object, object_id);
  if (error != ErrorCode::OK) {
    ErrorCode rollback_result = session_->rollback();
    if (rollback_result != ErrorCode::OK) {
      return rollback_result;
    }
    return error;
  }

  error = session_->commit();
  if (error != ErrorCode::OK) {
    return error;
  }

  return error;
}

/**
 * @brief Gets one index metadata object from the metadata table,
 *   where key = value.
 * @param key     [in]  key of index metadata object. e.g. id or name.
 * @param value   [in]  key value.
 * @param object  [out] index metadata object to get.
 * @retval ErrorCode::OK              if success,
 * @retval ErrorCode::ID_NOT_FOUND    if the id does not exist.
 * @retval ErrorCode::NAME_NOT_FOUND  if the name does not exist.
 * @retval otherwise an error code.
 */
ErrorCode MetadataProvider::get_index_metadata(
    std::string_view key, std::string_view value,
    boost::property_tree::ptree& object) {

  ErrorCode error = ErrorCode::UNKNOWN;

  if ((key != Object::ID) && (key != Object::NAME)) {
    return ErrorCode::INVALID_PARAMETER;
  }

  error = this->init();
  if (error != ErrorCode::OK) {
    return error;
  }

  error = index_dao_->select(key, {value}, object);
  if (error != ErrorCode::OK) {
    return error;
  }

  return error;
}

/**
 * @brief Get all index metadata objects from the metadata table.
 * @param objects [out] table metadata objects.
 * @return ErrorCode::OK if success, otherwise an error code.
 */
ErrorCode MetadataProvider::get_index_metadata(
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
 * @brief Update a index metadata in the metdata table.
 * @param object_id  [in]  object ID of the index metadata to be updated.
 * @param object     [in]  Table metadata object.
 * @retval ErrorCode::OK if success,
 * @retval ErrorCode::ID_NOT_FOUND if the table id does not exist.
 * @retval otherwise an error code.
 */
ErrorCode MetadataProvider::update_index_metadata(
    const ObjectIdType object_id,
    const boost::property_tree::ptree& object) {

  ErrorCode error = ErrorCode::UNKNOWN;

  error = init();
  if (error != ErrorCode::OK) {
    return error;
  }

  error = session_->start_transaction();
  if (error != ErrorCode::OK) {
    return error;
  }

  if (error == ErrorCode::OK) {
    error = index_dao_->update(Object::ID, {std::to_string(object_id)}, object);
  }

  if (error == ErrorCode::OK) {
    error = session_->commit();
  } else {
    ErrorCode rollback_result = session_->rollback();
    if (rollback_result != ErrorCode::OK) {
      error = rollback_result;
    }
  }  
  
  return error;
}

/**
 * @brief Remove a index metadata object which has the specified name 
 * from the metadata table.
 * @param key       [in]  key of index metadata object.
 * @param value     [in]  value of index metadata object.
 * @param object_id [out] ID of the removed index metadata.
 * @retval ErrorCode::OK if success.
 * @retval ErrorCode::ID_NOT_FOUND if the index id does not exist.
 * @retval ErrorCode::NAME_NOT_FOUND if the index name does not exist.
 * @retval otherwise an error code.
 */
ErrorCode MetadataProvider::remove_index_metadata(std::string_view key,
                                                std::string_view value,
                                                ObjectIdType& object_id) {
  ErrorCode error = ErrorCode::UNKNOWN;

  error = init();
  if (error != ErrorCode::OK) {
    return error;
  }

  error = session_->start_transaction();
  if (error != ErrorCode::OK) {
    return error;
  }

  error = index_dao_->remove(key, {value}, object_id);
  if (error == ErrorCode::OK) {
    error = session_->commit();
  } else {
    ErrorCode rollback_result = session_->rollback();
    if (rollback_result != ErrorCode::OK) {
      error = rollback_result;
    }
  }

  return error;
}

}  // namespace manager::metadata::db
