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

#include "manager/metadata/dao/db_session_manager.h"
#include "manager/metadata/helper/logging_helper.h"
#include "manager/metadata/index.h"

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

  // Index metadata DAO.
  error = session.get_indexes_dao(index_dao_);
  if (error != ErrorCode::OK) {
    return error;
  }

  error = ErrorCode::OK;
  return error;
}

// ============================================================================
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

// ============================================================================
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

// ============================================================================
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

}  // namespace manager::metadata::db
