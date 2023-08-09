/*
 * Copyright 2021-2023 tsurugi project.
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
   * @param dao  [out] DAO instance.
   * @return ErrorCode::OK if success, otherwise an error code.
   */
  ErrorCode get_tables_dao(std::shared_ptr<Dao>& dao) override;

  /**
   * @brief Get an instance of a DAO for column metadata.
   * @param dao  [out] DAO instance.
   * @return ErrorCode::OK if success, otherwise an error code.
   */
  ErrorCode get_columns_dao(std::shared_ptr<Dao>& dao) override;

  /**
   * @brief Get an instance of a DAO for index metadata.
   * @param dao  [out] DAO instance.
   * @return ErrorCode::OK if success, otherwise an error code.
   */
  ErrorCode get_indexes_dao(std::shared_ptr<Dao>& dao) override;

  /**
   * @brief Get an instance of a DAO for constraint metadata.
   * @param dao  [out] DAO instance or nullptr.
   * @return DAO instance ErrorCode::OK if success, otherwise an error code.
   */
  ErrorCode get_constraints_dao(std::shared_ptr<Dao>& dao) override;

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
   * @param dao  [out] DAO instance.
   * @return ErrorCode::OK if success, otherwise an error code.
   */
  ErrorCode get_privileges_dao(std::shared_ptr<Dao>& dao) override;

  /**
   * @brief Get an instance of a DAO for statistic metadata.
   * @return DAO instance.
   */
  std::shared_ptr<Dao> get_statistics_dao() override;

  /**
   * @brief Starts a transaction scope managed by this DBSessionManager.
   * @return ErrorCode::OK if success, otherwise an error code.
   */
  ErrorCode start_transaction() override;

  /**
   * @brief Commits all transactions currently started for all DAO contexts
   *   managed by this DBSessionManager.
   * @return ErrorCode::OK if success, otherwise an error code.
   */
  ErrorCode commit() override;

  /**
   * @brief Rollbacks all transactions currently started for all DAO contexts
   *   managed by this DBSessionManager.
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
   * @return ErrorCode::OK if success, otherwise an error code.
   */
  ErrorCode set_always_secure_search_path() const;

  /**
   * @brief Create and initialize an instance of the DAO.
   * @param dao  [in/out] DAO of the metadata.
   * @return ErrorCode::OK if success, otherwise an error code.
   */
  template <typename T, typename = std::enable_if_t<std::is_base_of_v<Dao, T>>>
  ErrorCode create_dao_instance(std::shared_ptr<Dao>& dao) {
    return DbSessionManager::create_dao_instance<T>(dao, this);
  }
};  // class DBSessionManager

}  // namespace manager::metadata::db
