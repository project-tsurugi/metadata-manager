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
  if (object_id <= 0) {
    return ErrorCode::INVALID_PARAMETER;
  }

  return get(DataTypes::ID, std::to_string(object_id), object);
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
  if (object_name.empty()) {
    return ErrorCode::NOT_FOUND;
  }

  return get(DataTypes::NAME, object_name, object);
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
  ErrorCode error = ErrorCode::INTERNAL_ERROR;

  std::string_view s_object_key = std::string_view(object_key);
  // Get the data type metadata through the provider.
  error = provider->get_datatype_metadata(s_object_key, object_value, object);

  return error;
}

}  // namespace manager::metadata
