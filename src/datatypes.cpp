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

#include "manager/metadata/common/message.h"
#include "manager/metadata/helper/logging_helper.h"
#include "manager/metadata/provider/datatypes_provider.h"

// =============================================================================
namespace {

std::unique_ptr<manager::metadata::db::DataTypesProvider> provider = nullptr;

}  // namespace

// =============================================================================
namespace manager::metadata {

// ==========================================================================
// DataType struct methods.
/** 
 * @brief  Transform datatype metadata from structure object to ptree object.
 * @return ptree object.
 */
boost::property_tree::ptree DataType::convert_to_ptree() const
{
  auto pt = this->base_convert_to_ptree();
  pt.put<int64_t>(PG_DATA_TYPE, this->pg_data_type);
  pt.put(PG_DATA_TYPE_NAME, this->pg_data_type_name);
  pt.put(PG_DATA_TYPE_QUALIFIED_NAME, this->pg_data_type_qualified_name);

  return pt;
}

/**
 * @brief   Transform datatype metadata from ptree object to structure object.
 * @param   ptree [in] ptree object of metdata.
 * @return  structure object of metadata.
 */
void DataType::convert_from_ptree(const boost::property_tree::ptree& pt)
{
  this->base_convert_from_ptree(pt);
  auto opt_int = pt.get_optional<int64_t>(DataType::PG_DATA_TYPE);
  this->pg_data_type = opt_int ? opt_int.get() : INVALID_VALUE;

  auto opt_str = pt.get_optional<std::string>(DataType::PG_DATA_TYPE_NAME);
  this->pg_data_type_name = opt_str ? opt_str.get() : "";

  opt_str = 
      pt.get_optional<std::string>(DataType::PG_DATA_TYPE_QUALIFIED_NAME);
  this->pg_data_type_qualified_name = opt_str ? opt_str.get() : ""; 
}

// ==========================================================================
// DataTypes class methods.
/**
 * @brief Constructor
 * @param database   [in]  database name.
 * @param component  [in]  component name.
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
ErrorCode DataTypes::init() const {
  // Log of API function start.
  log::function_start("DataTypes::init()");

  // Initialize the provider.
  ErrorCode error = provider->init();

  // Log of API function finish.
  log::function_finish("DataTypes::init()", error);

  return error;
}

/**
 * @brief Gets one data type metadata object
 *   from the data types metadata table based on the given object_name.
 * @param object_id  [in]  metadata-object ID.
 * @param object     [out] one data type metadata object to get
 *   based on the given object_name.
 * @retval ErrorCode::OK if success.
 * @retval ErrorCode::ID_NOT_FOUND if the data type id does not exist.
 * @retval otherwise an error code.
 */
ErrorCode DataTypes::get(const ObjectIdType object_id,
                         boost::property_tree::ptree& object) const {
  ErrorCode error = ErrorCode::UNKNOWN;

  // Log of API function start.
  log::function_start("DataTypes::get(DataTypeId)");

  // Parameter value check.
  if (object_id > 0) {
    error = ErrorCode::OK;
  } else {
    LOG_WARNING
        << "An out-of-range value (0 or less) was specified for DataTypeId.: "
        << object_id;
    error = ErrorCode::ID_NOT_FOUND;
  }

  // Get the data type metadata through the class method.
  if (error == ErrorCode::OK) {
    error = provider->get_datatype_metadata(DataTypes::ID,
                                            std::to_string(object_id), object);
  }

  // Log of API function finish.
  log::function_finish("DataTypes::get(DataTypeId)", error);

  return error;
}

/**
 * @brief Gets one data type metadata object
 *   from the data types metadata table based on the given object_name.
 * @param object_name  [in]  data type metadata name. (Value of "name" key.)
 * @param object       [out] one data type metadata object to get
 *   based on the given object_name.
 * @retval ErrorCode::OK if success.
 * @retval ErrorCode::NAME_NOT_FOUND if the data type name does not exist.
 * @retval otherwise an error code.
 */
ErrorCode DataTypes::get(std::string_view object_name,
                         boost::property_tree::ptree& object) const {
  ErrorCode error = ErrorCode::UNKNOWN;

  // Log of API function start.
  log::function_start("DataTypes::get(DataTypeName)");

  // Parameter value check.
  if (!object_name.empty()) {
    error = ErrorCode::OK;
  } else {
    LOG_WARNING << "An empty value was specified for DataTypeName.";
    error = ErrorCode::NAME_NOT_FOUND;
  }

  // Get the data type metadata through the class method.
  if (error == ErrorCode::OK) {
    error =
        provider->get_datatype_metadata(DataTypes::NAME, object_name, object);
  }

  // Log of API function finish.
  log::function_finish("DataTypes::get(DataTypeName)", error);

  return error;
}

/**
 * @brief Gets one data type metadata object
 *   from the data types metadata table, where key = value.
 * @param key     [in]  key of data type metadata object.
 * @param value   [in]  value of data type metadata object.
 * @param object  [out] one data type metadata object to get,
 *   where key = value.
 * @retval ErrorCode::OK if success.
 * @retval ErrorCode::ID_NOT_FOUND if the data types id does not exist.
 * @retval ErrorCode::NAME_NOT_FOUND if the data types name does not exist.
 * @retval ErrorCode::NOT_FOUND if the other data types key does not exist.
 * @retval otherwise an error code.
 */
ErrorCode DataTypes::get(std::string_view object_key,
                         std::string_view object_value,
                         boost::property_tree::ptree& object) const {
  ErrorCode error = ErrorCode::UNKNOWN;

  // Log of API function start.
  log::function_start("DataTypes::get(Key/Value)");

  std::string_view s_object_key = std::string_view(object_key);

  // Parameter value check.
  if ((!s_object_key.empty()) && (!object_value.empty())) {
    error = ErrorCode::OK;
  } else if (s_object_key.empty()) {
    LOG_ERROR << Message::PARAMETER_FAILED << "Object key is empty.";
    error = ErrorCode::INVALID_PARAMETER;
  } else {
    // Convert the error code.
    if (object_key == DataTypes::ID) {
      LOG_ERROR << Message::PARAMETER_FAILED << "DataType id is empty.";
      error = ErrorCode::ID_NOT_FOUND;
    } else if (object_key == DataTypes::NAME) {
      LOG_ERROR << Message::PARAMETER_FAILED << "DataType name is empty.";
      error = ErrorCode::NAME_NOT_FOUND;
    } else {
      LOG_ERROR << Message::PARAMETER_FAILED << "Object value is empty.";
      error = ErrorCode::NOT_FOUND;
    }
  }

  // Get the data type metadata through the provider.
  if (error == ErrorCode::OK) {
    error = provider->get_datatype_metadata(s_object_key, object_value, object);
  }

  // Log of API function finish.
  log::function_finish("DataTypes::get(Key/Value)", error);

  return error;
}

/**
 *  @brief
 */
std::shared_ptr<Object> DataTypes::create_object() const {
  return std::make_shared<DataType>();
}

}  // namespace manager::metadata
