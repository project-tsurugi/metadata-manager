/*
 * Copyright 2021 tsurugi project.
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
#include "manager/metadata/provider/roles_provider.h"

#include <boost/foreach.hpp>

// =============================================================================
namespace manager::metadata::db {

/**
 *  @brief  Initialize and prepare to access the metadata repository.
 *  @return  ErrorCode::OK if success, otherwise an error code.
 */
ErrorCode RolesProvider::init() {
  ErrorCode error = ErrorCode::UNKNOWN;

  // Establish a connection to the metadata repository.
  error = session_manager_->connect();
  if (error != ErrorCode::OK) {
    return error;
  }

  // RolesDAO
  if (!roles_dao_) {
    // Get an instance of the RolesDAO.
    roles_dao_ = session_manager_->get_roles_dao();

    // Prepare to access table metadata.
    error = roles_dao_->prepare();
    if (error != ErrorCode::OK) {
      roles_dao_.reset();
      return error;
    }
  }

  error = ErrorCode::OK;
  return error;
}

/**
 * @brief Gets one role metadata object from the PostgreSQL repository,
 *   where key = value.
 * @param (key)     [in]  key of role metadata object.
 * @param (value)   [in]  value of role metadata object.
 * @param (object)  [out] one role metadata object to get,
 *   where key = value.
 * @retval ErrorCode::OK if success.
 * @retval ErrorCode::ID_NOT_FOUND if the role id does not exist.
 * @retval ErrorCode::NAME_NOT_FOUND if the role name does not exist.
 * @retval otherwise an error code.
 */
ErrorCode RolesProvider::get_role_metadata(
    std::string_view key, std::string_view value,
    boost::property_tree::ptree& object) {
  // Initialization
  ErrorCode error = init();
  if (error != ErrorCode::OK) {
    return error;
  }

  error = roles_dao_->select(key, {value}, object);

  return error;
}

}  // namespace manager::metadata::db
