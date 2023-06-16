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
#include "manager/metadata/dao/postgresql/pg_common.h"
#include "manager/metadata/error_code.h"

namespace manager::metadata::db {

/**
 * @brief Class that manages connection information.
 */
struct Connection {
  PgConnectionPtr pg_conn;
};

/**
 * @brief Class for managing sessions with PostgreSQL.
 */
class DbSessionManagerPg : public DbSessionManager {
 public:
  /**
   * @brief Establish a connection to the metadata repository
   *   using a connection string.
   * @return ErrorCode::OK if success, otherwise an error code.
   */
  ErrorCode connect() override;

  /**
   * @brief Get an instance of a DAO for table metadata.
   * @return DAO instance.
   */
  std::shared_ptr<Dao> get_tables_dao() override;

  /**
   * @brief Get an instance of a DAO for column metadata.
   * @return DAO instance.
   */
  std::shared_ptr<Dao> get_columns_dao() override;

  /**
   * @brief Get an instance of a DAO for index metadata.
   * @return DAO instance.
   */
  std::shared_ptr<Dao> get_indexes_dao() override;

  /**
   * @brief Get an instance of a DAO for constraint metadata.
   *   Returns nullptr if the database connection fails.
   * @return DAO instance or nullptr.
   */
  std::shared_ptr<Dao> get_constraints_dao() override;

  /**
   * @brief Get an instance of a DAO for data-type metadata.
   * @return DAO instance.
   */
  std::shared_ptr<Dao> get_datatypes_dao() override;

  /**
   * @brief Get an instance of a DAO for role metadata.
   * @return DAO instance.
   */
  std::shared_ptr<Dao> get_roles_dao() override;

  /**
   * @brief Get an instance of a DAO for privilege metadata.
   * @return DAO instance.
   */
  std::shared_ptr<Dao> get_privileges_dao() override;

  /**
   * @brief Get an instance of a DAO for statistic metadata.
   * @return DAO instance.
   */
  std::shared_ptr<Dao> get_statistics_dao() override;

  /**
   * @brief Starts a transaction scope managed by this DBSessionManager.
   * @param none.
   * @return ErrorCode::OK if success, otherwise an error code.
   */
  ErrorCode start_transaction() override;

  /**
   * @brief Commits all transactions currently started for all DAO contexts
   *   managed by this DBSessionManager.
   * @param none.
   * @return ErrorCode::OK if success, otherwise an error code.
   */
  ErrorCode commit() override;

  /**
   * @brief Rollbacks all transactions currently started for all DAO contexts
   *   managed by this DBSessionManager.
   * @param none.
   * @return ErrorCode::OK if success, otherwise an error code.
   */
  ErrorCode rollback() override;

  /**
   * @brief Get connection information.
   * @return connection.
   */
  Connection connection() const { return conn_; }

 private:
  Connection conn_;

  /**
   * @brief Sends a query to set always-secure search path
   *   to the metadata repository.
   * @param none.
   * @return ErrorCode::OK if success, otherwise an error code.
   */
  ErrorCode set_always_secure_search_path() const;
};  // class DBSessionManager

}  // namespace manager::metadata::db
