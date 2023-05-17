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
#include "manager/metadata/provider/datatypes_provider.h"

// =============================================================================
namespace manager::metadata::db {

/**
 * @brief Initialize and prepare to access the metadata repository.
 * @return ErrorCode::OK if success, otherwise an error code.
 */
ErrorCode DataTypesProvider::init() {
  ErrorCode error = ErrorCode::UNKNOWN;

  // DataTypeDAO
  if (!datatypes_dao_) {
    // Get an instance of the DataTypeDAO.
    datatypes_dao_ = session_manager_->get_datatypes_dao();
    if (!datatypes_dao_) {
      error = ErrorCode::DATABASE_ACCESS_FAILURE;
      return error;
    }
    // Prepare to access table metadata.
    error = datatypes_dao_->prepare();
    if (error != ErrorCode::OK) {
      datatypes_dao_.reset();
      return error;
    }
  }

  error = ErrorCode::OK;
  return error;
}

/**
 * @brief Gets one data type metadata object from the data types
 *   metadata repository, where key = value.
 * @param (key)     [in]  key of data type metadata object.
 * @param (value)   [in]  value of data type metadata object.
 * @param (object)  [out] one data type metadata object to get,
 *   where key = value.
 * @retval ErrorCode::OK if success.
 * @retval ErrorCode::ID_NOT_FOUND if the data types id does not exist.
 * @retval ErrorCode::NAME_NOT_FOUND if the data types name does not exist.
 * @retval ErrorCode::NOT_FOUND if the other data types key does not exist.
 * @retval otherwise an error code.
 */
ErrorCode DataTypesProvider::get_datatype_metadata(
    std::string_view key, std::string_view value,
    boost::property_tree::ptree& object) {
  ErrorCode error = ErrorCode::UNKNOWN;

  // Initialization
  error = init();
  if (error != ErrorCode::OK) {
    return error;
  }

  error = datatypes_dao_->select(key, value, object);

  return error;
}

}  // namespace manager::metadata::db
