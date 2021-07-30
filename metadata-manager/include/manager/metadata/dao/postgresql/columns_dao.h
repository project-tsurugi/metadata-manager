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
#ifndef MANAGER_METADATA_DAO_POSTGRESQL_COLUMNS_DAO_H_
#define MANAGER_METADATA_DAO_POSTGRESQL_COLUMNS_DAO_H_

#include "manager/metadata/dao/columns_dao.h"
#include "manager/metadata/dao/postgresql/common.h"
#include "manager/metadata/dao/postgresql/db_session_manager.h"

namespace manager::metadata::db::postgresql {

class ColumnsDAO : public manager::metadata::db::ColumnsDAO {
 public:
  explicit ColumnsDAO(DBSessionManager *session_manager)
      : connection_(session_manager->get_connection()){};

  manager::metadata::ErrorCode prepare() const override;

  manager::metadata::ErrorCode insert_one_column_metadata(
      ObjectIdType table_id,
      boost::property_tree::ptree &column) const override;
  manager::metadata::ErrorCode select_column_metadata(
      std::string_view object_key, std::string_view object_value,
      boost::property_tree::ptree &object) const override;
  manager::metadata::ErrorCode delete_column_metadata_by_table_id(
      ObjectIdType table_id) const override;

 private:
  ConnectionSPtr connection_;

  manager::metadata::ErrorCode get_ptree_from_p_gresult(
      PGresult *&res, int ordinal_position,
      boost::property_tree::ptree &column) const;
};  // class ColumnsDAO

}  // namespace manager::metadata::db::postgresql

#endif  // MANAGER_METADATA_DAO_POSTGRESQL_COLUMNS_DAO_H_
