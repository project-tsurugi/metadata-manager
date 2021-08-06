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
#ifndef MANAGER_METADATA_DAO_POSTGRESQL_STATISTICS_DAO_H_
#define MANAGER_METADATA_DAO_POSTGRESQL_STATISTICS_DAO_H_

#include <string>
#include <string_view>
#include <unordered_map>

#include "manager/metadata/dao/postgresql/db_session_manager.h"
#include "manager/metadata/dao/postgresql/dbc_utils.h"
#include "manager/metadata/dao/statistics_dao.h"
#include "manager/metadata/entity/column_statistic.h"
#include "manager/metadata/metadata.h"

namespace manager::metadata::db::postgresql {

class StatisticsDAO : public manager::metadata::db::StatisticsDAO {
 public:
  explicit StatisticsDAO(DBSessionManager* session_manager)
      : connection_(session_manager->get_connection()){};

  manager::metadata::ErrorCode prepare() const override;

  manager::metadata::ErrorCode
  upsert_one_column_statistic_by_table_id_column_ordinal_position(
      ObjectIdType table_id, ObjectIdType ordinal_position,
      std::string_view column_statistic) const override;
  manager::metadata::ErrorCode
  select_one_column_statistic_by_table_id_column_ordinal_position(
      ObjectIdType table_id, ObjectIdType ordinal_position,
      ColumnStatistic& column_statistic) const override;
  manager::metadata::ErrorCode select_all_column_statistic_by_table_id(
      ObjectIdType table_id,
      std::unordered_map<ObjectIdType, ColumnStatistic>& column_statistics)
      const override;
  manager::metadata::ErrorCode delete_all_column_statistic_by_table_id(
      ObjectIdType table_id) const override;
  manager::metadata::ErrorCode
  delete_one_column_statistic_by_table_id_column_ordinal_position(
      ObjectIdType table_id, ObjectIdType ordinal_position) const override;

 private:
  ConnectionSPtr connection_;

  manager::metadata::ErrorCode get_column_statistic_from_p_gresult(
      PGresult*& res, int ordinal_position,
      ColumnStatistic& column_statistic) const;
};  // class StatisticsDAO

}  // namespace manager::metadata::db::postgresql

#endif  // MANAGER_METADATA_DAO_POSTGRESQL_STATISTICS_DAO_H_
