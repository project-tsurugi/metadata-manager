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
 *  @brief  Constructor
 *  @param  (database) [in]  database name.
 *  @param  (component) [in]  component name.
 */
DataTypes::DataTypes(std::string_view database, std::string_view component)
    : Metadata(database, component) {
  // Create the provider.
  provider = std::make_unique<db::DataTypesProvider>();
}

/**
 *  @brief  Initialization.
 *  @param  none.
 *  @return  ErrorCode::OK if success, otherwise an error code.
 */
ErrorCode DataTypes::init() {
  // Initialize the provider.
  ErrorCode error = provider->init();

  return error;
}

/**
 *  @brief  Gets one data type metadata object
 *  from the data types metadata table
 *  based on the given object_name.
 *  @param  (object_id) [in]  metadata-object ID.
 *  @param  (object)        [out] one data type metadata object to get
 *  based on the given object_name.
 *  @return  ErrorCode::OK if success, otherwise an error code.
 */
ErrorCode DataTypes::get(const ObjectIdType object_id,
                         boost::property_tree::ptree& object) {
  // Parameter value check
  if (object_id <= 0) {
    return ErrorCode::ID_NOT_FOUND;
  }

  ErrorCode error = get(DataTypes::ID, std::to_string(object_id), object);

  // Convert the return value
  error = (error == ErrorCode::NOT_FOUND ? ErrorCode::ID_NOT_FOUND : error);

  return error;
}

/**
 *  @brief  Gets one data type metadata object
 *  from the data types metadata table
 *  based on the given object_name.
 *  @param  (object_name)   [in]  data type metadata name. (Value of "name"
 * key.)
 *  @param  (object)        [out] one data type metadata object to get
 *  based on the given object_name.
 *  @return  ErrorCode::OK if success, otherwise an error code.
 */
ErrorCode DataTypes::get(std::string_view object_name,
                         boost::property_tree::ptree& object) {
  // Parameter value check
  if (object_name.empty()) {
    return ErrorCode::NAME_NOT_FOUND;
  }

  ErrorCode error = get(DataTypes::NAME, object_name, object);

  // Convert the return value
  error = (error == ErrorCode::NOT_FOUND ? ErrorCode::NAME_NOT_FOUND : error);

  return error;
}

/**
 *  @brief  Gets one data type metadata object
 *  from the data types metadata table,
 *  where key = value.
 *  @param  (key)           [in]  key of data type metadata object.
 *  @param  (value)         [in]  value of data type metadata object.
 *  @param  (object)        [out] one data type metadata object to get,
 *  where key = value.
 *  @return  ErrorCode::OK if success, otherwise an error code.
 */
ErrorCode DataTypes::get(const char* object_key, std::string_view object_value,
                         boost::property_tree::ptree& object) {
  std::string_view s_object_key = std::string_view(object_key);

  // Parameter value check
  if (s_object_key.empty()) {
    return ErrorCode::INVALID_PARAMETER;
  } else if (object_value.empty()) {
    return ErrorCode::NOT_FOUND;
  }

  // Get the data type metadata through the provider.
  ErrorCode error =
      provider->get_datatype_metadata(s_object_key, object_value, object);

  return error;
}

}  // namespace manager::metadata
