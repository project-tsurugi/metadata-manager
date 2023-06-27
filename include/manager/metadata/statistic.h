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
#ifndef MANAGER_METADATA_STATISTIC_H_
#define MANAGER_METADATA_STATISTIC_H_

#include <string>

#include <boost/property_tree/ptree.hpp>

#include "manager/metadata/object.h"

namespace manager::metadata {

/**
 * @brief Column statistic object.
 */
class Statistic : public Object {
 public:
  /**
   * @brief Field name constant indicating the table id of the metadata.
   */
  static constexpr const char* const TABLE_ID = "tableId";
  /**
   * @brief Field name constant indicating the column number of the metadata.
   */
  static constexpr const char* const COLUMN_NUMBER = "columnNumber";
  /**
   * @brief Field name constant indicating the columns id of the metadata.
   */
  static constexpr const char* const COLUMN_ID = "columnId";
  /**
   * @brief Field name constant indicating the columns name of the metadata.
   */
  static constexpr const char* const COLUMN_NAME = "columnName";
  /**
   * @brief Field name constant indicating the columns statistic of the
   *   metadata.
   */
  static constexpr const char* const COLUMN_STATISTIC = "columnStatistic";

  /**
   * @brief Table ID.
   */
  ObjectId table_id;
  /**
   * @brief Column number.
   */
  int64_t column_number;
  /**
   * @brief Column ID.
   */
  ObjectId column_id;
  /**
   * @brief Columns name.
   */
  std::string column_name;
  /**
   * @brief Columns statistic.
   */
  std::string column_statistic;

  Statistic()
      : Object(),
        table_id(INVALID_OBJECT_ID),
        column_number(INVALID_VALUE),
        column_id(INVALID_OBJECT_ID) {}

  /**
   * @brief Transform column statistic from structure object to ptree object.
   * @return ptree object.
   */
  boost::property_tree::ptree convert_to_ptree() const override;

  /**
   * @brief Transform column statistic from ptree object to structure object.
   * @param pt  [in]  ptree object of metadata.
   * @return structure object of column statistic.
   */
  void convert_from_ptree(const boost::property_tree::ptree& pt) override;
};  // class Statistic

}  // namespace manager::metadata

#endif  // MANAGER_METADATA_STATISTIC_H_
