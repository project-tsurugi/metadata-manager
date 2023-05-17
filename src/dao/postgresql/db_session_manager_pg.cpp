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
#include "manager/metadata/dao/postgresql/db_session_manager_pg.h"

#include "manager/metadata/common/config.h"
#include "manager/metadata/common/message.h"
#include "manager/metadata/dao/postgresql/columns_dao_pg.h"
#include "manager/metadata/dao/postgresql/constraints_dao_pg.h"
#include "manager/metadata/dao/postgresql/datatypes_dao_pg.h"
#include "manager/metadata/dao/postgresql/dbc_utils_pg.h"
#include "manager/metadata/dao/postgresql/index_dao_pg.h"
#include "manager/metadata/dao/postgresql/privileges_dao_pg.h"
#include "manager/metadata/dao/postgresql/roles_dao_pg.h"
#include "manager/metadata/dao/postgresql/statistics_dao_pg.h"
#include "manager/metadata/dao/postgresql/tables_dao_pg.h"
#include "manager/metadata/helper/logging_helper.h"

// =============================================================================
namespace manager::metadata::db {

/**
 * @brief Get an instance of a DAO for table metadata.
 *   Returns nullptr if the database connection fails.
 * @return DAO instance or nullptr.
 */
std::shared_ptr<Dao> DbSessionManagerPg::get_tables_dao() {
  // Connection to DB.
  if (this->connect() != ErrorCode::OK) {
    return nullptr;
  }

  // Create an instance of DAO.
  return std::make_shared<TablesDaoPg>(this);
}

/**
 * @brief Get an instance of a DAO for column metadata.
 *   Returns nullptr if the database connection fails.
 * @return DAO instance or nullptr.
 */
std::shared_ptr<Dao> DbSessionManagerPg::get_columns_dao() {
  // Connection to DB.
  if (this->connect() != ErrorCode::OK) {
    return nullptr;
  }

  // Create an instance of DAO.
  return std::make_shared<ColumnsDaoPg>(this);
}

/**
 * @brief Get an instance of a DAO for index metadata.
 *   Returns nullptr if the database connection fails.
 * @return DAO instance or nullptr.
 */
std::shared_ptr<Dao> DbSessionManagerPg::get_indexes_dao() {
  // Connection to DB.
  if (this->connect() != ErrorCode::OK) {
    return nullptr;
  }

  // Create an instance of DAO.
  return std::make_shared<IndexDaoPg>(this);
}

/**
 * @brief Get an instance of a DAO for constraint metadata.
 *   Returns nullptr if the database connection fails.
 * @return DAO instance or nullptr.
 */
std::shared_ptr<Dao> DbSessionManagerPg::get_constraints_dao() {
  // Connection to DB.
  if (this->connect() != ErrorCode::OK) {
    return nullptr;
  }

  // Create an instance of DAO.
  return std::make_shared<ConstraintsDaoPg>(this);
}

/**
 * @brief Get an instance of a DAO for data-type metadata.
 *   Returns nullptr if the database connection fails.
 * @return DAO instance or nullptr.
 */
std::shared_ptr<Dao> DbSessionManagerPg::get_datatypes_dao() {
  // Connection to DB.
  if (this->connect() != ErrorCode::OK) {
    return nullptr;
  }

  // Create an instance of DAO.
  return std::make_shared<DataTypesDaoPg>(this);
}

/**
 * @brief Get an instance of a DAO for role metadata.
 *   Returns nullptr if the database connection fails.
 * @return DAO instance or nullptr.
 */
std::shared_ptr<Dao> DbSessionManagerPg::get_roles_dao() {
  // Connection to DB.
  if (this->connect() != ErrorCode::OK) {
    return nullptr;
  }

  // Create an instance of DAO.
  return std::make_shared<RolesDaoPg>(this);
}

/**
 * @brief Get an instance of a DAO for privilege metadata.
 *   Returns nullptr if the database connection fails.
 * @return DAO instance or nullptr.
 */
std::shared_ptr<Dao> DbSessionManagerPg::get_privileges_dao() {
  // Connection to DB.
  if (this->connect() != ErrorCode::OK) {
    return nullptr;
  }

  // Create an instance of DAO.
  return std::make_shared<PrivilegesDaoPg>(this);
}

/**
 * @brief Get an instance of a DAO for statistic metadata.
 *   Returns nullptr if the database connection fails.
 * @return DAO instance or nullptr.
 */
std::shared_ptr<Dao> DbSessionManagerPg::get_statistics_dao() {
  // Connection to DB.
  if (this->connect() != ErrorCode::OK) {
    return nullptr;
  }

  // Create an instance of DAO.
  return std::make_shared<StatisticsDaoPg>(this);
}

/**
 * @brief Starts a transaction scope managed by this DBSessionManager.
 * @param none.
 * @return ErrorCode::OK if success, otherwise an error code.
 */
ErrorCode DbSessionManagerPg::start_transaction() {
  ErrorCode error = ErrorCode::UNKNOWN;

  if (!DbcUtils::is_open(conn_.pg_conn)) {
    LOG_ERROR << Message::START_TRANSACTION_FAILURE << Message::NOT_INITIALIZED;
    error = ErrorCode::NOT_INITIALIZED;
    return error;
  }

  ResultPtr res =
      DbcUtils::make_result_uptr(PQexec(conn_.pg_conn.get(), "BEGIN"));
  if (PQresultStatus(res.get()) == PGRES_COMMAND_OK) {
    error = ErrorCode::OK;
  } else {
    LOG_ERROR << Message::START_TRANSACTION_FAILURE
              << PQerrorMessage(conn_.pg_conn.get());
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
ErrorCode DbSessionManagerPg::commit() {
  ErrorCode error = ErrorCode::UNKNOWN;

  if (!DbcUtils::is_open(conn_.pg_conn)) {
    LOG_ERROR << Message::COMMIT_FAILURE << Message::NOT_INITIALIZED;
    error = ErrorCode::NOT_INITIALIZED;
    return error;
  }

  ResultPtr res =
      DbcUtils::make_result_uptr(PQexec(conn_.pg_conn.get(), "COMMIT"));
  if (PQresultStatus(res.get()) == PGRES_COMMAND_OK) {
    error = ErrorCode::OK;
  } else {
    LOG_ERROR << Message::COMMIT_FAILURE << PQerrorMessage(conn_.pg_conn.get());
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
ErrorCode DbSessionManagerPg::rollback() {
  ErrorCode error = ErrorCode::UNKNOWN;

  if (!DbcUtils::is_open(conn_.pg_conn)) {
    LOG_ERROR << Message::ROLLBACK_FAILURE << Message::NOT_INITIALIZED;
    error = ErrorCode::NOT_INITIALIZED;
    return error;
  }
  ResultPtr res =
      DbcUtils::make_result_uptr(PQexec(conn_.pg_conn.get(), "ROLLBACK"));
  if (PQresultStatus(res.get()) == PGRES_COMMAND_OK) {
    error = ErrorCode::OK;
  } else {
    LOG_ERROR << Message::ROLLBACK_FAILURE
              << PQerrorMessage(conn_.pg_conn.get());
    error = ErrorCode::DATABASE_ACCESS_FAILURE;
  }

  return error;
}

/* =============================================================================
 * Private method area
 */

/**
 * @brief Establish a connection to the metadata repository
 *   using a connection string.
 * @param none.
 * @return ErrorCode::OK if success, otherwise an error code.
 */
ErrorCode DbSessionManagerPg::connect() {
  ErrorCode error = ErrorCode::UNKNOWN;

  if (DbcUtils::is_open(conn_.pg_conn)) {
    error = ErrorCode::OK;
  } else {
    // Connection to DB.
    conn_.pg_conn = DbcUtils::make_connection_sptr(
        PQconnectdb(Config::get_connection_string().c_str()));
    if (DbcUtils::is_open(conn_.pg_conn)) {
      // Set up a secure search path.
      this->set_always_secure_search_path();
      error = ErrorCode::OK;
    } else {
      LOG_ERROR << Message::CONNECT_FAILURE << "\n  "
                << PQerrorMessage(conn_.pg_conn.get());
      error = ErrorCode::DATABASE_ACCESS_FAILURE;
    }
  }

  return error;
}

/**
 * @brief Sends a query to set always-secure search path
 *   to the metadata repository.
 * @param none.
 * @return ErrorCode::OK if success, otherwise an error code.
 */
ErrorCode DbSessionManagerPg::set_always_secure_search_path() const {
  ErrorCode error = ErrorCode::UNKNOWN;

  if (!DbcUtils::is_open(conn_.pg_conn)) {
    LOG_ERROR << Message::SET_ALWAYS_SECURE_SEARCH_PATH
              << Message::NOT_INITIALIZED;
    error = ErrorCode::NOT_INITIALIZED;
    return error;
  }

  std::string statement =
      "SELECT pg_catalog.set_config('search_path', '', false)";
  ResultPtr res =
      DbcUtils::make_result_uptr(PQexec(conn_.pg_conn.get(), statement.data()));
  if (PQresultStatus(res.get()) == PGRES_TUPLES_OK) {
    error = ErrorCode::OK;
  } else {
    LOG_ERROR << Message::SET_ALWAYS_SECURE_SEARCH_PATH
              << PQerrorMessage(conn_.pg_conn.get());
    error = ErrorCode::DATABASE_ACCESS_FAILURE;
  }

  return error;
}

}  // namespace manager::metadata::db
