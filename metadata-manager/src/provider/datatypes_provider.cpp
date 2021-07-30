/*
 * Copyright 2020 tsurugi project.
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

using boost::property_tree::ptree;
using manager::metadata::ErrorCode;

/**
 *  @brief  Initialize and prepare to access the metadata repository.
 *  @return  ErrorCode::OK if success, otherwise an error code.
 */
ErrorCode DataTypesProvider::init() {
  ErrorCode result = ErrorCode::OK;
  std::shared_ptr<GenericDAO> gdao = nullptr;

  if (datatypes_dao_ == nullptr) {
    // Get an instance of the DataTypeDAO class.
    result = session_manager_->get_dao(GenericDAO::TableName::DATATYPES, gdao);
    datatypes_dao_ = (result == ErrorCode::OK)
                         ? std::static_pointer_cast<DataTypesDAO>(gdao)
                         : nullptr;
  }

  return result;
}

/**
 *  @brief  Gets one data type metadata object from the data types metadata
 * repository, where key = value.
 *  @param  (key)      [in]  key of data type metadata object.
 *  @param  (value)    [in]  value of data type metadata object.
 *  @param  (object)   [out] one data type metadata object to get, where key =
 * value.
 *  @return  ErrorCode::OK if success, otherwise an error code.
 */
ErrorCode DataTypesProvider::get_datatype_metadata(std::string_view key,
                                                   std::string_view value,
                                                   ptree &object) {
  // Initialization
  ErrorCode result = init();
  if (result != ErrorCode::OK) {
    return result;
  }

  if (key.empty() || value.empty()) {
    return ErrorCode::INVALID_PARAMETER;
  }

  result = datatypes_dao_->select_one_data_type_metadata(key, value, object);

  return result;
}

}  // namespace manager::metadata::db
