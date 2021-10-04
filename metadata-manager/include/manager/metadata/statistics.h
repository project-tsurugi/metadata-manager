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
#ifndef MANAGER_METADATA_STATISTICS_H_
#define MANAGER_METADATA_STATISTICS_H_

#include "manager/metadata/metadata.h"

namespace manager::metadata {

class Statistics : public Metadata {
 public:
  // statistics object.
  // FORMAT_VERSION is defined in base class.
  // GENERATION is defined in base class.
  // ID is defined in base class.
  // NAME is defined in base class.
  static constexpr const char* const TABLE_ID = "tableId";
  static constexpr const char* const ORDINAL_POSITION = "ordinalPosition";
  static constexpr const char* const COLUMN_ID = "columnId";
  static constexpr const char* const COLUMN_NAME = "columnName";
  static constexpr const char* const COLUMN_STATISTIC = "columnStatistic";

  Statistics(std::string_view database, std::string_view component = "visitor");

  Statistics(const Statistics&) = delete;
  Statistics& operator=(const Statistics&) = delete;

  ErrorCode init() override;

  ErrorCode add(boost::property_tree::ptree& object) override;
  ErrorCode add(boost::property_tree::ptree& object,
                ObjectIdType* object_id) override;

  ErrorCode get(const ObjectIdType object_id,
                boost::property_tree::ptree& object) override;
  ErrorCode get(std::string_view object_name,
                boost::property_tree::ptree& object) override;
  ErrorCode get_by_column_id(const ObjectIdType column_id,
                             boost::property_tree::ptree& object);
  ErrorCode get_by_column_number(const ObjectIdType table_id,
                                 const int64_t ordinal_position,
                                 boost::property_tree::ptree& object);
  ErrorCode get_by_column_name(const ObjectIdType table_id,
                               std::string_view column_name,
                               boost::property_tree::ptree& object);
  ErrorCode get_all(
      std::vector<boost::property_tree::ptree>& container) override;
  ErrorCode get_all(const ObjectIdType table_id,
                    std::vector<boost::property_tree::ptree>& object);

  ErrorCode remove(const ObjectIdType object_id) override;
  ErrorCode remove(std::string_view object_name,
                   ObjectIdType* object_id) override;
  ErrorCode remove_by_table_id(const ObjectIdType table_id);
  ErrorCode remove_by_column_id(const ObjectIdType column_id);
  ErrorCode remove_by_column_number(const ObjectIdType table_id,
                                    const int64_t ordinal_position);
  ErrorCode remove_by_column_name(const ObjectIdType table_id,
                                  std::string_view column_name);
};  // class Statistics

}  // namespace manager::metadata

#endif  // MANAGER_METADATA_STATISTICS_H_
