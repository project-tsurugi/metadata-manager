/*
 * Copyright 2020-2023 tsurugi project.
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

#include <string_view>
#include <vector>

#include <boost/property_tree/ptree.hpp>

#include "manager/metadata/datatype.h"
#include "manager/metadata/error_code.h"
#include "manager/metadata/metadata.h"

namespace manager::metadata {

class DataTypes : public Metadata {
 public:
  /**
   * @brief Field name constant indicating the data type of the metadata.
   * @deprecated Deprecated in the future. Please use DataType::PG_DATA_TYPE.
   */
  static constexpr const char* const PG_DATA_TYPE = DataType::PG_DATA_TYPE;
  /**
   * @brief Field name constant indicating the data type name of the metadata.
   * @deprecated Deprecated in the future. Please use
   *   DataType::PG_DATA_TYPE_NAME.
   */
  static constexpr const char* const PG_DATA_TYPE_NAME =
      DataType::PG_DATA_TYPE_NAME;
  /**
   * @brief Field name constant indicating the qualified name of the metadata.
   * @deprecated Deprecated in the future. Please use
   *   DataType::PG_DATA_TYPE_QUALIFIED_NAME.
   */
  static constexpr const char* const PG_DATA_TYPE_QUALIFIED_NAME =
      DataType::PG_DATA_TYPE_QUALIFIED_NAME;

  /**
   * @brief represents data types id.
   * @deprecated Deprecated in the future. Please use DataType::DataTypesId.
   */
  enum class DataTypesId : ObjectIdType {
    /**
     * @brief INT64
     * @deprecated Deprecated in the future. Please use
     *   DataType::DataTypesId::INT64.
     */
    INT64 = static_cast<ObjectIdType>(DataType::DataTypesId::INT64),
    /**
     * @brief INT32
     * @deprecated Deprecated in the future. Please use
     *   DataType::DataTypesId::INT32.
     */
    INT32 = static_cast<ObjectIdType>(DataType::DataTypesId::INT32),
    /**
     * @brief FLOAT32
     * @deprecated Deprecated in the future. Please use
     *   DataType::DataTypesId::FLOAT32.
     */
    FLOAT32 = static_cast<ObjectIdType>(DataType::DataTypesId::FLOAT32),
    /**
     * @brief FLOAT64
     * @deprecated Deprecated in the future. Please use
     *   DataType::DataTypesId::FLOAT64.
     */
    FLOAT64 = static_cast<ObjectIdType>(DataType::DataTypesId::FLOAT64),
    /**
     * @brief CHAR
     * @deprecated Deprecated in the future. Please use
     *   DataType::DataTypesId::CHAR.
     */
    CHAR = static_cast<ObjectIdType>(DataType::DataTypesId::CHAR),
    /**
     * @brief VARCHAR
     * @deprecated Deprecated in the future. Please use
     *   DataType::DataTypesId::VARCHAR.
     */
    VARCHAR = static_cast<ObjectIdType>(DataType::DataTypesId::VARCHAR),
    /**
     * @brief DATE
     * @deprecated Deprecated in the future. Please use
     *   DataType::DataTypesId::DATE.
     */
    DATE = static_cast<ObjectIdType>(DataType::DataTypesId::DATE),
    /**
     * @brief TIME
     * @deprecated Deprecated in the future. Please use
     *   DataType::DataTypesId::TIME.
     */
    TIME = static_cast<ObjectIdType>(DataType::DataTypesId::TIME),
    /**
     * @brief TIMESTAMP
     * @deprecated Deprecated in the future. Please use
     *   DataType::DataTypesId::TIMESTAMP.
     */
    TIMESTAMP = static_cast<ObjectIdType>(DataType::DataTypesId::TIMESTAMP),
    /**
     * @brief TIMESTAMPTZ
     * @deprecated Deprecated in the future. Please use
     *   DataType::DataTypesId::TIMESTAMPTZ.
     */
    TIMESTAMPTZ = static_cast<ObjectIdType>(DataType::DataTypesId::TIMESTAMPTZ),
    /**
     * @brief INTERVAL
     * @deprecated Deprecated in the future. Please use
     *   DataType::DataTypesId::INTERVAL.
     */
    INTERVAL = static_cast<ObjectIdType>(DataType::DataTypesId::INTERVAL),
    /**
     * @brief TIMETZ
     * @deprecated Deprecated in the future. Please use
     *   DataType::DataTypesId::TIMETZ.
     */
    TIMETZ = static_cast<ObjectIdType>(DataType::DataTypesId::TIMETZ),
    /**
     * @brief NUMERIC
     * @deprecated Deprecated in the future. Please use
     *   DataType::DataTypesId::NUMERIC.
     */
    NUMERIC = static_cast<ObjectIdType>(DataType::DataTypesId::NUMERIC)
  };

  /**
   * @brief Interval Fields option.
   * @deprecated Deprecated in the future. Please use DataType::IntervalFields.
   */
  enum class IntervalFields : int64_t {
    /**
     * @brief fields parameter omitted.
     * @deprecated Deprecated in the future. Please use
     *   DataType::IntervalFields::OMITTED.
     */
    OMITTED = static_cast<int64_t>(DataType::IntervalFields::OMITTED),
    /**
     * @brief YEAR.
     * @deprecated Deprecated in the future. Please use
     *   DataType::IntervalFields::YEAR.
     */
    YEAR = static_cast<int64_t>(DataType::IntervalFields::YEAR),
    /**
     * @brief MONTH.
     * @deprecated Deprecated in the future. Please use
     *   DataType::IntervalFields::MONTH.
     */
    MONTH = static_cast<int64_t>(DataType::IntervalFields::MONTH),
    /**
     * @brief DAY.
     * @deprecated Deprecated in the future. Please use
     *   DataType::IntervalFields::DAY.
     */
    DAY = static_cast<int64_t>(DataType::IntervalFields::DAY),
    /**
     * @brief HOUR.
     * @deprecated Deprecated in the future. Please use
     *   DataType::IntervalFields::HOUR.
     */
    HOUR = static_cast<int64_t>(DataType::IntervalFields::HOUR),
    /**
     * @brief MINUTE.
     * @deprecated Deprecated in the future. Please use
     *   DataType::IntervalFields::MINUTE.
     */
    MINUTE = static_cast<int64_t>(DataType::IntervalFields::MINUTE),
    /**
     * @brief SECOND.
     * @deprecated Deprecated in the future. Please use
     *   DataType::IntervalFields::SECOND.
     */
    SECOND = static_cast<int64_t>(DataType::IntervalFields::SECOND),
    /**
     * @brief YEAR TO MONTH.
     * @deprecated Deprecated in the future. Please use
     *   DataType::IntervalFields::YEAR_TO_MONTH.
     */
    YEAR_TO_MONTH =
        static_cast<int64_t>(DataType::IntervalFields::YEAR_TO_MONTH),
    /**
     * @brief DAY TO HOUR.
     * @deprecated Deprecated in the future. Please use
     *   DataType::IntervalFields::DAY_TO_HOUR.
     */
    DAY_TO_HOUR = static_cast<int64_t>(DataType::IntervalFields::DAY_TO_HOUR),
    /**
     * @brief DAY TO MINUTE.
     * @deprecated Deprecated in the future. Please use
     *   DataType::IntervalFields::DAY_TO_MINUTE.
     */
    DAY_TO_MINUTE =
        static_cast<int64_t>(DataType::IntervalFields::DAY_TO_MINUTE),
    /**
     * @brief DAY TO SECOND.
     * @deprecated Deprecated in the future. Please use
     *   DataType::IntervalFields::DAY_TO_SECOND.
     */
    DAY_TO_SECOND =
        static_cast<int64_t>(DataType::IntervalFields::DAY_TO_SECOND),
    /**
     * @brief HOUR TO MINUTE.
     * @deprecated Deprecated in the future. Please use
     *   DataType::IntervalFields::HOUR_TO_MINUTE.
     */
    HOUR_TO_MINUTE =
        static_cast<int64_t>(DataType::IntervalFields::HOUR_TO_MINUTE),
    /**
     * @brief HOUR TO SECOND.
     * @deprecated Deprecated in the future. Please use
     *   DataType::IntervalFields::HOUR_TO_SECOND.
     */
    HOUR_TO_SECOND =
        static_cast<int64_t>(DataType::IntervalFields::HOUR_TO_SECOND),
    /**
     * @brief MINUTE TO SECOND.
     * @deprecated Deprecated in the future. Please use
     *   DataType::IntervalFields::MINUTE_TO_SECOND.
     */
    MINUTE_TO_SECOND =
        static_cast<int64_t>(DataType::IntervalFields::MINUTE_TO_SECOND),
    /**
     * @brief Unknown.
     * @deprecated Deprecated in the future. Please use
     *   DataType::IntervalFields::UNKNOWN.
     */
    UNKNOWN = static_cast<int64_t>(DataType::IntervalFields::UNKNOWN),
  };

  explicit DataTypes(std::string_view database)
      : DataTypes(database, kDefaultComponent) {}
  DataTypes(std::string_view database, std::string_view component);

  DataTypes(const DataTypes&)            = delete;
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
                   [[maybe_unused]] const boost::property_tree::ptree& object)
      const override {
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
