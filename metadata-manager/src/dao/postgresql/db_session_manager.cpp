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
#include "manager/metadata/dao/postgresql/db_session_manager.h"

#include <iostream>

#include <libpq-fe.h>
#include "manager/metadata/dao/common/config.h"
#include "manager/metadata/dao/common/message.h"
#include "manager/metadata/dao/postgresql/columns_dao.h"
#include "manager/metadata/dao/postgresql/datatypes_dao.h"
#include "manager/metadata/dao/postgresql/dbc_utils.h"
#include "manager/metadata/dao/postgresql/statistics_dao.h"
#include "manager/metadata/dao/postgresql/tables_dao.h"

// =============================================================================
namespace manager::metadata::db::postgresql {

using manager::metadata::ErrorCode;

/**
 * @brief Gets Dao instance for the requested table name
 *   if all the following steps are successfully completed.
 *   1. Establishes a connection_ to the metadata repository.
 *   2. Sends a query to set always-secure search path
 *      to the metadata repository.
 *   3. Defines prepared statements for returned Dao
 *      in the metadata repository.
 * @param (table_name)   [in]  unique id for the Dao.
 * @param (gdao)         [out] Dao instance if success.
 *   for the requested table name.
 * @return ErrorCode::OK if success, otherwise an error code.
 */
ErrorCode DBSessionManager::get_dao(const GenericDAO::TableName table_name,
                                    std::shared_ptr<GenericDAO>& gdao) {
  ErrorCode error = ErrorCode::UNKNOWN;

  if (!DbcUtils::is_open(connection_)) {
    error = connect();
    if (error != ErrorCode::OK) {
      return error;
    }

    error = set_always_secure_search_path();
    if (error != ErrorCode::OK) {
      return error;
    }
  }

  error = create_dao(table_name, (manager::metadata::db::DBSessionManager*)this,
                     gdao);

  return error;
}

/**
 * @brief Starts a transaction scope managed by this DBSessionManager.
 * @param none.
 * @return ErrorCode::OK if success, otherwise an error code.
 */
ErrorCode DBSessionManager::start_transaction() {
  ErrorCode error = ErrorCode::UNKNOWN;

  if (!DbcUtils::is_open(connection_)) {
    std::cerr << Message::START_TRANSACTION_FAILURE << Message::NOT_INITIALIZED
              << std::endl;
    error = ErrorCode::NOT_INITIALIZED;
    return error;
  }

  ResultUPtr res =
      DbcUtils::make_result_uptr(PQexec(connection_.get(), "BEGIN"));
  if (PQresultStatus(res.get()) == PGRES_COMMAND_OK) {
    error = ErrorCode::OK;
  } else {
    std::cerr << Message::START_TRANSACTION_FAILURE
              << PQerrorMessage(connection_.get()) << std::endl;
    error = ErrorCode::DATABASE_ACCESS_FAILURE;
  }

  return error;
}

/**
 * @brief Commits all transactions currently started for all DAO contexts
 *   managed by this DBSessionManager.
 * @param none.
 * @return ErrorCode::OK if success, otherwise an error code.
 */
ErrorCode DBSessionManager::commit() {
  ErrorCode error = ErrorCode::UNKNOWN;

  if (!DbcUtils::is_open(connection_)) {
    std::cerr << Message::COMMIT_FAILURE << Message::NOT_INITIALIZED
              << std::endl;
    error = ErrorCode::NOT_INITIALIZED;
    return error;
  }

  ResultUPtr res =
      DbcUtils::make_result_uptr(PQexec(connection_.get(), "COMMIT"));
  if (PQresultStatus(res.get()) == PGRES_COMMAND_OK) {
    error = ErrorCode::OK;
  } else {
    std::cerr << Message::COMMIT_FAILURE << PQerrorMessage(connection_.get())
              << std::endl;
    error = ErrorCode::DATABASE_ACCESS_FAILURE;
  }

  return error;
}

/**
 * @brief Rollbacks all transactions currently started for all DAO contexts
 *   managed by this DBSessionManager.
 * @param none.
 * @return ErrorCode::OK if success, otherwise an error code.
 */
ErrorCode DBSessionManager::rollback() {
  ErrorCode error = ErrorCode::UNKNOWN;

  if (!DbcUtils::is_open(connection_)) {
    std::cerr << Message::ROLLBACK_FAILURE << Message::NOT_INITIALIZED
              << std::endl;
    error = ErrorCode::NOT_INITIALIZED;
    return error;
  }
  ResultUPtr res =
      DbcUtils::make_result_uptr(PQexec(connection_.get(), "ROLLBACK"));
  if (PQresultStatus(res.get()) == PGRES_COMMAND_OK) {
    error = ErrorCode::OK;
  } else {
    std::cerr << Message::ROLLBACK_FAILURE << PQerrorMessage(connection_.get())
              << std::endl;
    error = ErrorCode::DATABASE_ACCESS_FAILURE;
  }

  return error;
}

// -----------------------------------------------------------------------------
// Private method area

/**
 * @brief Establishes a connection_ to the metadata repository
 *   using connection_ information in a string.
 * @param none.
 * @return ErrorCode::OK if success, otherwise an error code.
 */
ErrorCode DBSessionManager::connect() {
  ErrorCode error = ErrorCode::UNKNOWN;

  connection_ = DbcUtils::make_connection_sptr(
      PQconnectdb(Config::get_connection_string().c_str()));

  if (DbcUtils::is_open(connection_)) {
    error = ErrorCode::OK;
  } else {
    std::cerr << Message::CONNECT_FAILURE << std::endl;
    error = ErrorCode::DATABASE_ACCESS_FAILURE;
  }

  return error;
}

/**
 * @brief Sends a query to set always-secure search path
 *   to the metadata repository.
 * @param none.
 * @return ErrorCode::OK if success, otherwise an error code.
 */
ErrorCode DBSessionManager::set_always_secure_search_path() const {
  ErrorCode error = ErrorCode::UNKNOWN;

  if (!DbcUtils::is_open(connection_)) {
    std::cerr << Message::SET_ALWAYS_SECURE_SEARCH_PATH
              << Message::NOT_INITIALIZED << std::endl;
    error = ErrorCode::NOT_INITIALIZED;
    return error;
  }

  ResultUPtr res = DbcUtils::make_result_uptr(
      PQexec(connection_.get(),
             "SELECT pg_catalog.set_config('search_path', '', false)"));
  if (PQresultStatus(res.get()) == PGRES_TUPLES_OK) {
    error = ErrorCode::OK;
  } else {
    std::cerr << Message::SET_ALWAYS_SECURE_SEARCH_PATH
              << PQerrorMessage(connection_.get()) << std::endl;
    error = ErrorCode::DATABASE_ACCESS_FAILURE;
  }

  return error;
}

}  // namespace manager::metadata::db::postgresql
