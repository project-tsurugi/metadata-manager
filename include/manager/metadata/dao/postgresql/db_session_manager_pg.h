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
#pragma once

#include <memory>

#include "manager/metadata/dao/db_session_manager.h"
#include "manager/metadata/dao/postgresql/common_pg.h"
#include "manager/metadata/dao/postgresql/pg_common.h"
#include "manager/metadata/error_code.h"

namespace manager::metadata::db {
/**
 * @brief
 */
struct Connection {
  PgConnectionPtr pg_conn;
};

/**
 * @brief
 */
class DbSessionManagerPg : public DBSessionManager {
 public:
  manager::metadata::ErrorCode get_dao(
      const GenericDAO::TableName,
      std::shared_ptr<GenericDAO>&) override { return ErrorCode::UNKNOWN; }

  std::shared_ptr<Dao> get_index_dao() override;

  Connection connection() const { return conn_; }
  manager::metadata::ErrorCode start_transaction() override;
  manager::metadata::ErrorCode commit() override;
  manager::metadata::ErrorCode rollback() override;

 private:
   Connection conn_;

  manager::metadata::ErrorCode connect();
  manager::metadata::ErrorCode set_always_secure_search_path() const;
};  // class DBSessionManager

}  // namespace manager::metadata::db



namespace manager::metadata::db::postgresql {
/**
 * @brief
 */
class DBSessionManager : public manager::metadata::db::DBSessionManager {
 public:
  manager::metadata::ErrorCode get_dao(
      const GenericDAO::TableName table_name,
      std::shared_ptr<GenericDAO>& gdao) override;

  std::shared_ptr<Dao> get_index_dao() override { return nullptr; } // dummy

  Connection connection() const { return conn_; }
  manager::metadata::ErrorCode start_transaction() override;
  manager::metadata::ErrorCode commit() override;
  manager::metadata::ErrorCode rollback() override;

  ConnectionSPtr get_connection() const { return connection_; }

 private:
  manager::metadata::db::Connection conn_;  // dummy for build.
  ConnectionSPtr connection_;

  manager::metadata::ErrorCode connect();
  manager::metadata::ErrorCode set_always_secure_search_path() const;
};  // class DBSessionManager

}  // namespace manager::metadata::db::postgresql
