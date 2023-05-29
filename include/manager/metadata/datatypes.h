/*
 * Copyright 2020-2022 tsurugi project.
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

#include <string>
#include <string_view>
#include <vector>

#include <boost/property_tree/ptree.hpp>

#include "manager/metadata/error_code.h"
#include "manager/metadata/metadata.h"

namespace manager::metadata {

struct DataType : public Object {
  int64_t pg_data_type;
  std::string pg_data_type_name;
  std::string pg_data_type_qualified_name;

  static constexpr const char* const PG_DATA_TYPE = "pg_dataType";
  static constexpr const char* const PG_DATA_TYPE_NAME = "pg_dataTypeName";
  static constexpr const char* const PG_DATA_TYPE_QUALIFIED_NAME =
      "pg_dataTypeQualifiedName";

  DataType() 
      : pg_data_type(INVALID_VALUE),
        pg_data_type_name(""),
        pg_data_type_qualified_name("") 
      {}
  boost::property_tree::ptree convert_to_ptree() const override;
  void convert_from_ptree(const boost::property_tree::ptree& ptree) override;
};

class DataTypes : public Metadata {
 public:
  // data type metadata-object.
  // FORMAT_VERSION is defined in base class.
  // GENERATION is defined in base class.
  // ID is defined in base class.
  // NAME is defined in base class.

  /**
   * @brief Field name constant indicating the data type of the metadata.
   */
  static constexpr const char* const PG_DATA_TYPE = "pg_dataType";
  /**
   * @brief Field name constant indicating the data type name of the metadata.
   */
  static constexpr const char* const PG_DATA_TYPE_NAME = "pg_dataTypeName";
  /**
   * @brief Field name constant indicating the qualified name of the metadata.
   */
  static constexpr const char* const PG_DATA_TYPE_QUALIFIED_NAME =
      "pg_dataTypeQualifiedName";

  /**
   * @brief represents data types id.
   */
  enum class DataTypesId : ObjectIdType {
    INT64       = 20,   //!< @brief INT64.
    INT32       = 23,   //!< @brief INT32.
    FLOAT32     = 700,  //!< @brief FLOAT32.
    FLOAT64     = 701,  //!< @brief FLOAT64.
    CHAR        = 1042, //!< @brief CHAR.
    VARCHAR     = 1043, //!< @brief VARCHAR.
    DATE        = 1082, //!< @brief DATE.
    TIME        = 1083, //!< @brief TIME.
    TIMESTAMP   = 1114, //!< @brief TIMESTAMP.
    TIMESTAMPTZ = 1184, //!< @brief TIMESTAMPTZ.
    INTERVAL    = 1186, //!< @brief INTERVAL.
    TIMETZ      = 1266, //!< @brief TIMETZ.
    NUMERIC     = 1700  //!< @brief NUMERIC.
  };

  /**
   * @brief Interval Fields option.
   */
  enum class IntervalFields : int64_t {
    OMITTED = 0x7FFF,           //!< @brief fields parameter omitted.
    YEAR = 0x0004,              //!< @brief YEAR.
    MONTH = 0x0002,             //!< @brief MONTH.
    DAY = 0x0008,               //!< @brief DAY.
    HOUR = 0x0400,              //!< @brief HOUR.
    MINUTE = 0x0800,            //!< @brief MIUNTE.
    SECOND = 0x1000,            //!< @brief SECOND.
    YEAR_TO_MONTH = 0x0006,     //!< @brief YEAR TO MONTH.
    DAY_TO_HOUR = 0x0408,       //!< @brief DAY TO HOUR.
    DAY_TO_MINUTE = 0x0C08,     //!< @brief DAY TO MINUTE.
    DAY_TO_SECOND = 0x1C08,     //!< @brief DAY TO SECOND.
    HOUR_TO_MINUTE = 0x0C00,    //!< @brief HOUR TO MINUTE.
    HOUR_TO_SECOND = 0x1C00,    //!< @brief HOUR TO SECOND.
    MINUTE_TO_SECOND = 0x1800,  //!< @brief MINUTE TO SECOND.
    UNKNOWN = INVALID_VALUE,    //!< @brief Unknown.
  };

  explicit DataTypes(std::string_view database)
      : DataTypes(database, kDefaultComponent) {}
  DataTypes(std::string_view database, std::string_view component);

  DataTypes(const DataTypes&) = delete;
  DataTypes& operator=(const DataTypes&) = delete;

  ErrorCode init() const override;

  ErrorCode add([[maybe_unused]] const boost::property_tree::ptree& object)
      const override {
    return ErrorCode::UNKNOWN;
  }
  ErrorCode add([[maybe_unused]] const boost::property_tree::ptree& object,
                [[maybe_unused]] ObjectIdType* object_id) const override {
    return ErrorCode::UNKNOWN;
  }

  ErrorCode get(const ObjectIdType object_id,
                boost::property_tree::ptree& object) const override;
  ErrorCode get(std::string_view object_name,
                boost::property_tree::ptree& object) const override;
  ErrorCode get(std::string_view object_key, std::string_view object_value,
                boost::property_tree::ptree& object) const;
  ErrorCode get_all([[maybe_unused]] std::vector<boost::property_tree::ptree>&
                        container) const override {
    return ErrorCode::UNKNOWN;
  }

  ErrorCode update([[maybe_unused]] const ObjectIdType object_id,
                   [[maybe_unused]] const boost::property_tree::ptree& object) const override {
    return ErrorCode::UNKNOWN;
  }

  ErrorCode remove(
      [[maybe_unused]] const ObjectIdType object_id) const override {
    return ErrorCode::UNKNOWN;
  }
  ErrorCode remove([[maybe_unused]] std::string_view object_name,
                   [[maybe_unused]] ObjectIdType* object_id) const override {
    return ErrorCode::UNKNOWN;
  }

};  // class DataTypes

}  // namespace manager::metadata

#endif  // MANAGER_METADATA_DATATYPES_H_
