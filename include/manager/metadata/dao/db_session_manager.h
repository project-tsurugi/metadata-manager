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
#ifndef MANAGER_METADATA_DAO_DB_SESSION_MANAGER_H_
#define MANAGER_METADATA_DAO_DB_SESSION_MANAGER_H_

#include <memory>
#include <string>

#include "manager/metadata/dao/generic_dao.h"
#include "manager/metadata/error_code.h"

namespace manager::metadata::db {

class DBSessionManager {
 public:
  virtual ~DBSessionManager() {}

  virtual manager::metadata::ErrorCode get_dao(
      const GenericDAO::TableName table_name,
      std::shared_ptr<GenericDAO>& gdao) = 0;

  virtual manager::metadata::ErrorCode start_transaction() = 0;
  virtual manager::metadata::ErrorCode commit() = 0;
  virtual manager::metadata::ErrorCode rollback() = 0;

 protected:
  manager::metadata::ErrorCode create_dao(
      const GenericDAO::TableName table_name,
      const manager::metadata::db::DBSessionManager* session_manager,
      std::shared_ptr<GenericDAO>& gdao) const;
};  // class DBSessionManager

}  // namespace manager::metadata::db

#endif  // MANAGER_METADATA_DAO_DB_SESSION_MANAGER_H_
