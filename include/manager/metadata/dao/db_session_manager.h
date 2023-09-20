/*
 * Copyright 2020-2023 tsurugi project.
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
   * @param dao  [out] DAO instance.
   * @return ErrorCode::OK if success, otherwise an error code.
   */
  virtual ErrorCode get_tables_dao(std::shared_ptr<Dao>& dao) = 0;

  /**
   * @brief Get an instance of a DAO for column metadata.
   * @param dao  [out] DAO instance.
   * @return ErrorCode::OK if success, otherwise an error code.
   */
  virtual ErrorCode get_columns_dao(std::shared_ptr<Dao>& dao) = 0;

  /**
   * @brief Get an instance of a DAO for index metadata.
   * @param dao  [out] DAO instance.
   * @return ErrorCode::OK if success, otherwise an error code.
   */
  virtual ErrorCode get_indexes_dao(std::shared_ptr<Dao>& dao) = 0;

  /**
   * @brief Get an instance of a DAO for constraint metadata.
   * @param dao  [out] DAO instance or nullptr.
   * @return DAO instance ErrorCode::OK if success, otherwise an error code.
   */
  virtual ErrorCode get_constraints_dao(std::shared_ptr<Dao>& dao) = 0;

  /**
   * @brief Get an instance of a DAO for data-type metadata.
   * @param dao  [out] DAO instance.
   * @return ErrorCode::OK if success, otherwise an error code.
   */
  virtual ErrorCode get_datatypes_dao(std::shared_ptr<Dao>& dao) = 0;

  /**
   * @brief Get an instance of a DAO for role metadata.
   * @param dao  [out] DAO instance.
   * @return ErrorCode::OK if success, otherwise an error code.
   */
  virtual ErrorCode get_roles_dao(std::shared_ptr<Dao>& dao) = 0;

  /**
   * @brief Get an instance of a DAO for privilege metadata.
   * @param dao  [out] DAO instance.
   * @return ErrorCode::OK if success, otherwise an error code.
   */
  virtual ErrorCode get_privileges_dao(std::shared_ptr<Dao>& dao) = 0;

  /**
   * @brief Get an instance of a DAO for statistic metadata.
   * @param dao  [out] DAO instance.
   * @return ErrorCode::OK if success, otherwise an error code.
   */
  virtual ErrorCode get_statistics_dao(std::shared_ptr<Dao>& dao) = 0;

  /**
   * @brief Starts a transaction scope managed by this DBSessionManager.
   * @return ErrorCode::OK if success, otherwise an error code.
   */
  virtual ErrorCode start_transaction() = 0;

  /**
   * @brief Commits all transactions currently started for all DAO contexts
   *   managed by this DBSessionManager.
   * @return ErrorCode::OK if success, otherwise an error code.
   */
  virtual ErrorCode commit() = 0;

  /**
   * @brief Rollbacks all transactions currently started for all DAO contexts
   *   managed by this DBSessionManager.
   * @return ErrorCode::OK if success, otherwise an error code.
   */
  virtual ErrorCode rollback() = 0;

 protected:
  std::string database_;

  /**
   * @brief Create and initialize an instance of the DAO.
   * @param dao  [in/out] DAO of the metadata.
   * @return ErrorCode::OK if success, otherwise an error code.
   */
  template <typename T1, typename T2,
            typename = std::enable_if_t<std::is_base_of_v<Dao, T1>>>
  ErrorCode create_dao_instance(std::shared_ptr<Dao>& dao, T2* session) {
    ErrorCode error = ErrorCode::UNKNOWN;

    if (dao) {
      error = ErrorCode::OK;
    } else {
      // Create an instance of the DAO for the metadata.
      auto temp_dao = std::make_shared<T1>(session);

      // Prepare to access metadata.
      error = temp_dao->prepare();
      // Convert error codes.
      error = (error == ErrorCode::NOT_SUPPORTED ? ErrorCode::OK : error);

      if (error == ErrorCode::OK) {
        dao = temp_dao;
      } else {
        dao.reset();
      }
    }

    return error;
  }
};  // class DbSessionManager

}  // namespace manager::metadata::db
