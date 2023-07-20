/*
 * Copyright 2023 tsurugi project.
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
#ifndef MANAGER_METADATA_COLUMN_H_
#define MANAGER_METADATA_COLUMN_H_

#include <string>
#include <vector>

#include <boost/property_tree/ptree.hpp>

#include "manager/metadata/object.h"

namespace manager::metadata {

/**
 * @brief Column metadata object.
 */
struct Column : public Object {
  static constexpr const int64_t ORDINAL_POSITION_BASE_INDEX = 1;

  /**
   * @brief Field name constant indicating the table id of the metadata.
   */
  static constexpr const char* const TABLE_ID = "tableId";
  /**
   * @brief Field name constant indicating the column number of the metadata.
   */
  static constexpr const char* const COLUMN_NUMBER = "columnNumber";
  /**
   * @brief Field name constant indicating the data type id of the metadata.
   */
  static constexpr const char* const DATA_TYPE_ID = "dataTypeId";
  /**
   * @brief Field name constant indicating the data length of the metadata.
   */
  static constexpr const char* const DATA_LENGTH = "dataLength";
  /**
   * @brief Field name constant indicating the varying of the metadata.
   */
  static constexpr const char* const VARYING = "varying";
  /**
   * @brief Field name constant indicating the not null constraints of the
   *   metadata.
   */
  static constexpr const char* const IS_NOT_NULL = "isNotNull";
  /**
   * @brief Field name constant indicating the default expression of the
   *   metadata.
   */
  static constexpr const char* const DEFAULT_EXPR = "defaultExpression";
  /**
   * @brief Field name constant indicating the function expression of the
   *   metadata.
   */
  static constexpr const char* const IS_FUNCEXPR = "isFuncExpr";

  /**
   * @brief represents sort direction of elements.
   */
  enum class Direction {
    DEFAULT = 0,  //!< @brief Default order.
    ASCENDANT,    //!< @brief Ascendant order.
    DESCENDANT,   //!< @brief Descendant order.
  };

  /**
   * @brief Table ID to which the column belongs.
   */
  ObjectId table_id;
  /**
   * @brief Column number.
   */
  int64_t column_number;
  /**
   * @brief Data type ID of the column.
   */
  ObjectId data_type_id;
  /**
   * @brief Data length (array length).
   */
  std::vector<int64_t> data_length;
  /**
   * @brief Variable string length.
   */
  bool varying;
  /**
   * @brief Not NULL constraints.
   */
  bool is_not_null;
  /**
   * @brief Default value of the default constraint.
   */
  std::string default_expression;
  /**
   * @brief Function expression constraints.
   */
  bool is_funcexpr;

  Column()
      : Object(),
        table_id(INVALID_OBJECT_ID),
        column_number(INVALID_VALUE),
        data_type_id(INVALID_OBJECT_ID),
        data_length({}),
        varying(false),
        is_not_null(false),
        is_funcexpr(false) {}

  /**
   * @brief Transform column metadata from structure object to ptree object.
   * @return ptree object.
   */
  boost::property_tree::ptree convert_to_ptree() const override;

  /**
   * @brief Transform metadata from ptree object to structure object.
   * @param pt  [in]  ptree object of metadata.
   * @return structure object of metadata.
   */
  void convert_from_ptree(const boost::property_tree::ptree& pt) override;
};  // class Column

}  // namespace manager::metadata

#endif  // MANAGER_METADATA_COLUMN_H_
