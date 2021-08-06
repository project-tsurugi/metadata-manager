/*
 * Copyright 2020 tsurugi project.
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
#ifndef MANAGER_METADATA_PROVIDER_STATISTICS_PROVIDER_H_
#define MANAGER_METADATA_PROVIDER_STATISTICS_PROVIDER_H_

#include <boost/property_tree/ptree.hpp>
#include <string_view>
#include <unordered_map>

#include "manager/metadata/dao/statistics_dao.h"
#include "manager/metadata/dao/tables_dao.h"
#include "manager/metadata/entity/column_statistic.h"
#include "manager/metadata/entity/table_statistic.h"
#include "manager/metadata/error_code.h"
#include "manager/metadata/metadata.h"
#include "manager/metadata/provider/provider_base.h"

namespace manager::metadata::db {

class StatisticsProvider : public ProviderBase {
 public:
  manager::metadata::ErrorCode init();
  manager::metadata::ErrorCode add_table_statistic(ObjectIdType table_id,
                                                   float reltuples);
  manager::metadata::ErrorCode add_table_statistic(
      std::string_view table_name, float reltuples,
      ObjectIdType* table_id = nullptr);
  manager::metadata::ErrorCode add_column_statistic(
      ObjectIdType table_id, ObjectIdType ordinal_position,
      boost::property_tree::ptree& column_statistic);
  manager::metadata::ErrorCode get_table_statistic(
      ObjectIdType table_id, TableStatistic& table_statistic);
  manager::metadata::ErrorCode get_table_statistic(
      std::string_view table_name, TableStatistic& table_statistic);
  manager::metadata::ErrorCode get_column_statistic(
      ObjectIdType table_id, ObjectIdType ordinal_position,
      ColumnStatistic& column_statistic);
  manager::metadata::ErrorCode get_all_column_statistics(
      ObjectIdType table_id,
      std::unordered_map<ObjectIdType, ColumnStatistic>& column_statistics);
  manager::metadata::ErrorCode remove_column_statistic(
      ObjectIdType table_id, ObjectIdType ordinal_position);
  manager::metadata::ErrorCode remove_all_column_statistics(
      ObjectIdType table_id);

 private:
  std::shared_ptr<TablesDAO> tables_dao_ = nullptr;
  std::shared_ptr<StatisticsDAO> statistics_dao_ = nullptr;

};  // class StatisticsProvider

}  // namespace manager::metadata::db

#endif  // MANAGER_METADATA_PROVIDER_STATISTICS_PROVIDER_H_
