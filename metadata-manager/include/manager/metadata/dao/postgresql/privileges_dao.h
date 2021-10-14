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
#ifndef MANAGER_METADATA_DAO_POSTGRESQL_PRIVILEGES_DAO_H_
#define MANAGER_METADATA_DAO_POSTGRESQL_PRIVILEGES_DAO_H_

#include "manager/metadata/dao/postgresql/db_session_manager.h"
#include "manager/metadata/dao/postgresql/dbc_utils.h"
#include "manager/metadata/dao/privileges_dao.h"

namespace manager::metadata::db::postgresql {

class PrivilegesDAO : public manager::metadata::db::PrivilegesDAO {
 public:
  /**
   * @brief Column ordinal position.
   */
  enum class OrdinalPosition {
    kSelect = 0,
    kInsert,
    kUpdate,
    kDelete,
    kTruncate,
    kReferences,
    kTrigger
  };  // enum class OrdinalPosition

  /**
   * @brief Valid privilege code. The order of definition is
   *   based on OrdinalPosition.
   */
  static constexpr std::string_view kValidPrivileges = "rawdDxt";

  explicit PrivilegesDAO(DBSessionManager* session_manager);

  manager::metadata::ErrorCode prepare() const override;

  virtual manager::metadata::ErrorCode confirm_tables_permission(
      std::string_view object_key, std::string_view object_value,
      std::string_view permission, bool& check_result) const override;

 private:
  ConnectionSPtr connection_;

  static manager::metadata::ErrorCode check_of_privilege(PGresult*& res,
                                                  const int ordinal_position,
                                                  const char* permission,
                                                  bool& check_result);

  manager::metadata::ErrorCode check_exists_authid(std::string_view auth_id,
                                                   bool& exists_result) const;
};  // class PrivilegesDAO

}  // namespace manager::metadata::db::postgresql

#endif  // MANAGER_METADATA_DAO_POSTGRESQL_PRIVILEGES_DAO_H_
