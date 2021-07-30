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
#ifndef MANAGER_METADATA_DAO_POSTGRESQL_TABLES_DAO_H_
#define MANAGER_METADATA_DAO_POSTGRESQL_TABLES_DAO_H_

#include <unordered_map>

#include "manager/metadata/dao/postgresql/db_session_manager.h"
#include "manager/metadata/dao/postgresql/dbc_utils.h"
#include "manager/metadata/dao/tables_dao.h"

namespace manager::metadata::db::postgresql {

class TablesDAO : public manager::metadata::db::TablesDAO {
 public:
  explicit TablesDAO(DBSessionManager *session_manager);

  manager::metadata::ErrorCode prepare() const override;

  manager::metadata::ErrorCode update_reltuples_by_table_id(
      float reltuples, ObjectIdType table_id) const override;
  manager::metadata::ErrorCode update_reltuples_by_table_name(
      float reltuples, std::string_view table_name,
      ObjectIdType &table_id) const override;
  manager::metadata::ErrorCode select_table_statistic_by_table_id(
      ObjectIdType table_id,
      manager::metadata::TableStatistic &table_statistic) const override;
  manager::metadata::ErrorCode select_table_statistic_by_table_name(
      std::string_view table_name,
      TableStatistic &table_statistic) const override;
  manager::metadata::ErrorCode insert_table_metadata(
      boost::property_tree::ptree &table,
      ObjectIdType &table_id) const override;
  manager::metadata::ErrorCode select_table_metadata(
      std::string_view object_key, std::string_view object_value,
      boost::property_tree::ptree &object) const override;
  manager::metadata::ErrorCode delete_table_metadata_by_table_id(
      ObjectIdType table_id) const override;
  manager::metadata::ErrorCode delete_table_metadata_by_table_name(
      std::string_view table_name, ObjectIdType &table_id) const override;

 private:
  ConnectionSPtr connection_;
  std::vector<std::string> column_names;
  std::unordered_map<std::string, std::string> statement_names_select_equal_to;

  manager::metadata::ErrorCode get_table_statistic_from_p_gresult(
      PGresult *&res, int ordinal_position,
      TableStatistic &table_statistic) const;
  manager::metadata::ErrorCode get_ptree_from_p_gresult(
      PGresult *&res, int ordinal_position,
      boost::property_tree::ptree &table) const;
};  // class TablesDAO

}  // namespace manager::metadata::db::postgresql

#endif  // MANAGER_METADATA_DAO_POSTGRESQL_TABLES_DAO_H_
