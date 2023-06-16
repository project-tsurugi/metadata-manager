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
#pragma once

#include <memory>
#include <string>
#include <string_view>

#include "manager/metadata/error_code.h"

namespace manager::metadata::db {

class Dao;

class DbSessionManager {
 public:
  /**
   * @brief Returns an instance of the DB session manager.
   * @return DB session manager instance.
   */
  static DbSessionManager& get_instance();

  DbSessionManager() {}
  explicit DbSessionManager(std::string_view database) : database_(database) {}

  virtual ~DbSessionManager() {}

  DbSessionManager(const DbSessionManager&)            = delete;
  DbSessionManager& operator=(const DbSessionManager&) = delete;

  /**
   * @brief Establish a connection to the metadata repository
   *   using a connection string.
   * @return ErrorCode::OK if success, otherwise an error code.
   */
  virtual ErrorCode connect() = 0;

  /**
   * @brief Get an instance of a DAO for table metadata.
   * @return DAO instance.
   */
  virtual std::shared_ptr<Dao> get_tables_dao() = 0;

  /**
   * @brief Get an instance of a DAO for column metadata.
   * @return DAO instance.
   */
  virtual std::shared_ptr<Dao> get_columns_dao() = 0;

  /**
   * @brief Get an instance of a DAO for index metadata.
   * @return DAO instance.
   */
  virtual std::shared_ptr<Dao> get_indexes_dao() = 0;

  /**
   * @brief Get an instance of a DAO for constraint metadata.
   *   Returns nullptr if the database connection fails.
   * @return DAO instance or nullptr.
   */
  virtual std::shared_ptr<Dao> get_constraints_dao() = 0;

  /**
   * @brief Get an instance of a DAO for data-type metadata.
   * @return DAO instance.
   */
  virtual std::shared_ptr<Dao> get_datatypes_dao() = 0;

  /**
   * @brief Get an instance of a DAO for role metadata.
   * @return DAO instance.
   */
  virtual std::shared_ptr<Dao> get_roles_dao() = 0;

  /**
   * @brief Get an instance of a DAO for privilege metadata.
   * @return DAO instance.
   */
  virtual std::shared_ptr<Dao> get_privileges_dao() = 0;

  /**
   * @brief Get an instance of a DAO for statistic metadata.
   * @return DAO instance.
   */
  virtual std::shared_ptr<Dao> get_statistics_dao() = 0;

  /**
   * @brief Starts a transaction scope managed by this DBSessionManager.
   * @param none.
   * @return ErrorCode::OK if success, otherwise an error code.
   */
  virtual ErrorCode start_transaction() = 0;

  /**
   * @brief Commits all transactions currently started for all DAO contexts
   *   managed by this DBSessionManager.
   * @param none.
   * @return ErrorCode::OK if success, otherwise an error code.
   */
  virtual ErrorCode commit() = 0;

  /**
   * @brief Rollbacks all transactions currently started for all DAO contexts
   *   managed by this DBSessionManager.
   * @param none.
   * @return ErrorCode::OK if success, otherwise an error code.
   */
  virtual ErrorCode rollback() = 0;

 protected:
  std::string database_;
};  // class DbSessionManager

}  // namespace manager::metadata::db
