/*
 * Copyright 2020-2021 tsurugi project.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
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
#ifndef MANAGER_METADATA_DAO_STATISTICS_DAO_H_
#define MANAGER_METADATA_DAO_STATISTICS_DAO_H_

#include <string>
#include <string_view>

#include "manager/metadata/dao/generic_dao.h"
#include "manager/metadata/statistics.h"

namespace manager::metadata::db {

class StatisticsDAO : public GenericDAO {
 public:
  virtual manager::metadata::ErrorCode upsert_column_statistic(
      const ObjectIdType column_id, const std::string* column_name,
      boost::property_tree::ptree* column_statistic,
      ObjectIdType& statistic_id) const = 0;
  virtual manager::metadata::ErrorCode upsert_column_statistic(
      const ObjectIdType table_id, std::string_view object_key,
      std::string_view object_value, const std::string* column_name,
      boost::property_tree::ptree* column_statistic,
      ObjectIdType& statistic_id) const = 0;

  virtual manager::metadata::ErrorCode select_column_statistic(
      std::string_view object_key, std::string_view object_value,
      boost::property_tree::ptree& object) const = 0;
  virtual manager::metadata::ErrorCode select_column_statistic(
      const ObjectIdType table_id, std::string_view object_key,
      std::string_view object_value,
      boost::property_tree::ptree& object) const = 0;
  virtual manager::metadata::ErrorCode select_column_statistic(
      std::vector<boost::property_tree::ptree>& container) const = 0;
  virtual manager::metadata::ErrorCode select_column_statistic(
      const ObjectIdType table_id,
      std::vector<boost::property_tree::ptree>& container) const = 0;

  virtual manager::metadata::ErrorCode delete_column_statistic(
      std::string_view object_key, std::string_view object_value,
      ObjectIdType& statistic_id) const = 0;
  virtual manager::metadata::ErrorCode delete_column_statistic(
      const ObjectIdType table_id) const = 0;
  virtual manager::metadata::ErrorCode delete_column_statistic(
      const ObjectIdType table_id, std::string_view object_key,
      std::string_view object_value, ObjectIdType& statistic_id) const = 0;
};  // class StatisticsDAO

}  // namespace manager::metadata::db

#endif  // MANAGER_METADATA_DAO_POSTGRESQL_STATISTICS_DAO_H_
