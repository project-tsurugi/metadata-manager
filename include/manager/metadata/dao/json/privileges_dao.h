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
#ifndef MANAGER_METADATA_DAO_JSON_PRIVILEGES_DAO_H_
#define MANAGER_METADATA_DAO_JSON_PRIVILEGES_DAO_H_

#include "manager/metadata/dao/json/db_session_manager.h"
#include "manager/metadata/dao/privileges_dao.h"
#include "manager/metadata/error_code.h"

namespace manager::metadata::db::json {

class PrivilegesDAO : public manager::metadata::db::PrivilegesDAO {
 public:
  explicit PrivilegesDAO(DBSessionManager* session_manager) {}

  manager::metadata::ErrorCode prepare() const override {
    // Do nothing and return of ErrorCode::NOT_SUPPORTED.
    return ErrorCode::OK;
  };

  manager::metadata::ErrorCode confirm_tables_permission(
      std::string_view object_key, std::string_view object_value,
      std::string_view permission, bool& check_result) const override {
    // Do nothing and return of ErrorCode::NOT_SUPPORTED.
    return ErrorCode::NOT_SUPPORTED;
  }
};  // class PrivilegesDAO

}  // namespace manager::metadata::db::json

#endif  // MANAGER_METADATA_DAO_JSON_PRIVILEGES_DAO_H_
