/*
 * Copyright 2020-2021 tsurugi project.
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
#include "manager/metadata/datatypes.h"

#include <memory>

#include "manager/metadata/provider/datatypes_provider.h"

// =============================================================================
namespace {

std::unique_ptr<manager::metadata::db::DataTypesProvider> provider = nullptr;

}  // namespace

// =============================================================================
namespace manager::metadata {

using manager::metadata::ErrorCode;

/**
 * @brief Constructor
 * @param (database)   [in]  database name.
 * @param (component)  [in]  component name.
 */
DataTypes::DataTypes(std::string_view database, std::string_view component)
    : Metadata(database, component) {
  // Create the provider.
  provider = std::make_unique<db::DataTypesProvider>();
}

/**
 * @brief Initialization.
 * @param none.
 * @return ErrorCode::OK if success, otherwise an error code.
 */
ErrorCode DataTypes::init() {
  // Initialize the provider.
  ErrorCode error = provider->init();

  return error;
}

/**
 * @brief Gets one data type metadata object
 *   from the data types metadata table based on the given object_name.
 * @param (object_id)  [in]  metadata-object ID.
 * @param (object)     [out] one data type metadata object to get
 *   based on the given object_name.
 * @retval ErrorCode::OK if success.
 * @retval ErrorCode::ID_NOT_FOUND if the data type id does not exist.
 * @retval otherwise an error code.
 */
ErrorCode DataTypes::get(const ObjectIdType object_id,
                         boost::property_tree::ptree& object) {
  ErrorCode error = ErrorCode::UNKNOWN;

  // Parameter value check.
  if (object_id <= 0) {
    error = ErrorCode::ID_NOT_FOUND;
    return error;
  }

  // Get the data type metadata through the class method.
  error = get(DataTypes::ID, std::to_string(object_id), object);

  return error;
}

/**
 * @brief Gets one data type metadata object
 *   from the data types metadata table based on the given object_name.
 * @param (object_name)  [in]  data type metadata name. (Value of "name" key.)
 * @param (object)       [out] one data type metadata object to get
 *   based on the given object_name.
 * @retval ErrorCode::OK if success.
 * @retval ErrorCode::NAME_NOT_FOUND if the data type name does not exist.
 * @retval otherwise an error code.
 */
ErrorCode DataTypes::get(std::string_view object_name,
                         boost::property_tree::ptree& object) {
  ErrorCode error = ErrorCode::UNKNOWN;

  // Parameter value check.
  if (object_name.empty()) {
    error = ErrorCode::NAME_NOT_FOUND;
    return error;
  }

  // Get the data type metadata through the class method.
  error = get(DataTypes::NAME, object_name, object);

  return error;
}

/**
 * @brief Gets one data type metadata object
 *   from the data types metadata table, where key = value.
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
ErrorCode DataTypes::get(std::string_view object_key,
                         std::string_view object_value,
                         boost::property_tree::ptree& object) {
  ErrorCode error = ErrorCode::UNKNOWN;
  std::string_view s_object_key = std::string_view(object_key);

  // Parameter value check.
  if (s_object_key.empty()) {
    error = ErrorCode::INVALID_PARAMETER;
    return error;
  } else if (object_value.empty()) {
    // Convert the error code.
    if (object_key == DataTypes::ID) {
      error = ErrorCode::ID_NOT_FOUND;
    } else if (object_key == DataTypes::NAME) {
      error = ErrorCode::NAME_NOT_FOUND;
    } else {
      error = ErrorCode::NOT_FOUND;
    }
    return error;
  }

  // Get the data type metadata through the provider.
  error = provider->get_datatype_metadata(s_object_key, object_value, object);

  return error;
}

}  // namespace manager::metadata
