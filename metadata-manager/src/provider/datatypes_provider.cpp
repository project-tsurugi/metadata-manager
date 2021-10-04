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

#include "manager/metadata/datatypes.h"

// =============================================================================
namespace manager::metadata::db {

using boost::property_tree::ptree;
using manager::metadata::ErrorCode;

/**
 * @brief Initialize and prepare to access the metadata repository.
 * @return ErrorCode::OK if success, otherwise an error code.
 */
ErrorCode DataTypesProvider::init() {
  ErrorCode error = ErrorCode::UNKNOWN;
  std::shared_ptr<GenericDAO> gdao = nullptr;

  if (datatypes_dao_ != nullptr) {
    // Instance of the DataTypeDAO class has already been obtained.
    error = ErrorCode::OK;
  } else {
    // Get an instance of the DataTypeDAO class.
    error = session_manager_->get_dao(GenericDAO::TableName::DATATYPES, gdao);
    if (error != ErrorCode::OK) {
      return error;
    }
    // Set DataTypesDAO instance.
    datatypes_dao_ = std::static_pointer_cast<DataTypesDAO>(gdao);
  }

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
 * @retval ErrorCode::NOT_FOUND if the data type id or data type name
 *   does not exist.
 * @retval otherwise an error code.
 */
ErrorCode DataTypesProvider::get_datatype_metadata(std::string_view key,
                                                   std::string_view value,
                                                   ptree& object) {
  ErrorCode error = ErrorCode::UNKNOWN;

  // Initialization
  error = init();
  if (error != ErrorCode::OK) {
    return error;
  }

  error = datatypes_dao_->select_one_data_type_metadata(key, value, object);

  return error;
}

}  // namespace manager::metadata::db
