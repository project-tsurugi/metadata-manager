/*
 * Copyright 2021 tsurugi project.
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
#ifndef MANAGER_METADATA_DAO_POSTGRESQL_ROLES_DAO_PG_H_
#define MANAGER_METADATA_DAO_POSTGRESQL_ROLES_DAO_PG_H_

#include <boost/property_tree/ptree.hpp>

#include "manager/metadata/dao/postgresql/db_session_manager_pg.h"
#include "manager/metadata/dao/postgresql/dbc_utils_pg.h"
#include "manager/metadata/dao/roles_dao.h"
#include "manager/metadata/error_code.h"

namespace manager::metadata::db::postgresql {

class RolesDAO : public manager::metadata::db::RolesDAO {
 public:
  /**
   * @brief Column ordinal position of the role metadata table
   *   in the PostgreSQL repository.
   */
  enum class OrdinalPosition {
    kOid = 0,
    kName,
    kSuper,
    kInherit,
    kCreateRole,
    kCreateDb,
    kCanLogin,
    kReplication,
    kBypassRls,
    kConnLimit,
    kPassword,
    kValidUntil,
  };  // enum class OrdinalPosition

  explicit RolesDAO(DBSessionManager* session_manager);

  manager::metadata::ErrorCode prepare() const override;

  manager::metadata::ErrorCode select_role_metadata(
      std::string_view object_key, std::string_view object_value,
      boost::property_tree::ptree& object) const override;

 private:
  ConnectionSPtr connection_;

  manager::metadata::ErrorCode convert_pgresult_to_ptree(
      const PGresult* res, const int row_number,
      boost::property_tree::ptree& role) const;
};  // class RolesDAO

}  // namespace manager::metadata::db::postgresql

#endif  // MANAGER_METADATA_DAO_POSTGRESQL_ROLES_DAO_PG_H_
