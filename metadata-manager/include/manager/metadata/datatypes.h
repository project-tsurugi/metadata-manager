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
#ifndef MANAGER_METADATA_DATATYPES_H_
#define MANAGER_METADATA_DATATYPES_H_

#include "manager/metadata/metadata.h"

namespace manager::metadata {

class DataTypes : public Metadata {
 public:
  // root object.
  static constexpr const char* const DATATYPES_NODE = "dataTypes";

  // data type metadata-object.
  // ID is defined in base class.
  // NAME is defined in base class.
  static constexpr const char* const PG_DATA_TYPE = "pg_dataType";
  static constexpr const char* const PG_DATA_TYPE_NAME = "pg_dataTypeName";
  static constexpr const char* const PG_DATA_TYPE_QUALIFIED_NAME =
      "pg_dataTypeQualifiedName";

  /**
   * @brief represents data types id.
   */
  enum class DataTypesId : ObjectIdType {
    INT32 = 4,    //!< @brief INT32.
    INT64 = 6,    //!< @brief INT64.
    FLOAT32 = 8,  //!< @brief FLOAT32.
    FLOAT64 = 9,  //!< @brief FLOAT64.
    CHAR = 13,    //!< @brief CHAR.
    VARCHAR = 14  //!< @brief VARCHAR.
  };

  DataTypes(std::string_view database, std::string_view component = "visitor");

  DataTypes(const DataTypes&) = delete;
  DataTypes& operator=(const DataTypes&) = delete;

  ErrorCode init() override;

  ErrorCode add(boost::property_tree::ptree& object
                __attribute__((unused))) override {
    return ErrorCode::UNKNOWN;
  }
  ErrorCode add(boost::property_tree::ptree& object __attribute__((unused)),
                ObjectIdType* object_id __attribute__((unused))) override {
    return ErrorCode::UNKNOWN;
  }

  ErrorCode get(const ObjectIdType object_id,
                boost::property_tree::ptree& object) override;
  ErrorCode get(std::string_view object_name,
                boost::property_tree::ptree& object) override;
  ErrorCode get(const char* object_key, std::string_view object_value,
                boost::property_tree::ptree& object);

  ErrorCode remove(const ObjectIdType object_id
                   __attribute__((unused))) override {
    return ErrorCode::UNKNOWN;
  }
  ErrorCode remove(const char* object_name __attribute__((unused)),
                   ObjectIdType* object_id __attribute__((unused))) override {
    return ErrorCode::UNKNOWN;
  }

};  // class DataTypes

}  // namespace manager::metadata

#endif  // MANAGER_METADATA_DATATYPES_H_
