/*
 * Copyright 2020-2023 Project Tsurugi.
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

ErrorCode DbSessionManagerPg::get_tables_dao(std::shared_ptr<Dao>& dao) {
  // Generate an instance of tables DAO.
  return this->create_dao_instance<TablesDaoPg>(dao);
}

ErrorCode DbSessionManagerPg::get_columns_dao(std::shared_ptr<Dao>& dao) {
  // Generate an instance of columns DAO.
  return this->create_dao_instance<ColumnsDaoPg>(dao);
}

ErrorCode DbSessionManagerPg::get_indexes_dao(std::shared_ptr<Dao>& dao) {
  // Generate an instance of indexes DAO.
  return this->create_dao_instance<IndexDaoPg>(dao);
}

ErrorCode DbSessionManagerPg::get_constraints_dao(std::shared_ptr<Dao>& dao) {
  // Generate an instance of constraints DAO.
  return this->create_dao_instance<ConstraintsDaoPg>(dao);
}

ErrorCode DbSessionManagerPg::get_datatypes_dao(std::shared_ptr<Dao>& dao) {
  // Generate an instance of datatypes DAO.
  return this->create_dao_instance<DataTypesDaoPg>(dao);
}

ErrorCode DbSessionManagerPg::get_roles_dao(std::shared_ptr<Dao>& dao) {
  // Generate an instance of roles DAO.
  return this->create_dao_instance<RolesDaoPg>(dao);
}

ErrorCode DbSessionManagerPg::get_privileges_dao(std::shared_ptr<Dao>& dao) {
  // Generate an instance of privileges DAO.
  return this->create_dao_instance<PrivilegesDaoPg>(dao);
}

ErrorCode DbSessionManagerPg::get_statistics_dao(std::shared_ptr<Dao>& dao) {
  // Generate an instance of statistics DAO.
  return this->create_dao_instance<StatisticsDaoPg>(dao);
}

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
