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
#include "manager/metadata/dao/postgresql/dbc_utils_pg.h"

#include <regex>

#include "manager/metadata/common/message.h"
#include "manager/metadata/common/utility.h"
#include "manager/metadata/helper/logging_helper.h"

// =============================================================================
namespace manager::metadata::db {

/**
 * @brief Is this connection open?
 * @param connection  [in]  a connection.
 * @retval true - if this connection is open.
 * @retval false - if this connection is close.
 */
bool DbcUtils::is_open(const PgConnectionPtr& connection) {
  return PQstatus(connection.get()) == CONNECTION_OK;
}

/**
 * @brief Converts boolean expression in metadata repository to "true" or
 * "false" in application.
 * @param string  [in]  boolean expression.
 * @return "true" or "false", otherwise an empty string.
 */
std::string DbcUtils::convert_boolean_expression(const char* string) {
  std::string bool_alpha;
  if (string) {
    std::regex regex_true  = std::regex(R"(^([tTyY].*|1)$)");
    std::regex regex_false = std::regex(R"(^([fFnN].*|0)$)");

    if (std::regex_match(string, regex_true) ||
        std::regex_match(string, regex_false)) {
      bool_alpha =
          Utility::boolean_to_str(std::regex_match(string, regex_true));
    }
  }
  return bool_alpha;
}

/**
 * @brief Gets the number of affected rows if command was INSERT, UPDATE, or
 * DELETE.
 * @param res           [in]  the result of a query.
 * @param return_value  [out] the number of affected rows.
 * @return the number of affected rows if last command was INSERT, UPDATE, or
 * DELETE. zero for all other commands.
 */
ErrorCode DbcUtils::get_number_of_rows_affected(PGresult*& pgres,
                                                uint64_t& return_value) {
  return Utility::str_to_numeric(PQcmdTuples(pgres), return_value);
}

/**
 * @brief Makes shared_ptr of PGconn with deleter.
 * @param pgconn  [in]  a connection.
 * @return shared_ptr of PGconn with deleter.
 */
PgConnectionPtr DbcUtils::make_connection_sptr(PGconn* pgconn) {
  PgConnectionPtr conn(pgconn, [](PGconn* c) { ::PQfinish(c); });
  return conn;
}

/**
 * @brief Makes unique_ptr of PGresult with deleter.
 * @param pgres  [in]  the result of a query.
 * @return unique_ptr of PGresult with deleter.
 */
ResultPtr DbcUtils::make_result_uptr(PGresult* pgres) {
  ResultPtr res(pgres, [](PGresult* r) { ::PQclear(r); });
  return res;
}

/**
 * @brief Defines a prepared statement.
 * @param connection      [in]  a connection.
 * @param statement_name  [in]  unique name for the prepared statement.
 * @param statement       [in]  SQL statement to prepare.
 * @param param_types     [in]  Data types assigned to parameter symbols.
 *   (default: nullptr)
 * @return ErrorCode::OK if success, otherwise an error code.
 */
ErrorCode DbcUtils::prepare(const PgConnectionPtr& connection,
                            std::string_view statement_name,
                            std::string_view statement,
                            std::vector<Oid>* param_types) {
  if (!DbcUtils::is_open(connection)) {
    LOG_ERROR << Message::PREPARE_FAILURE << Message::NOT_CONNECT;
    return ErrorCode::NOT_INITIALIZED;
  }

  // Existence check of prepared statements.
  auto res_describe = DbcUtils::make_result_uptr(
      PQdescribePrepared(connection.get(), statement_name.data()));
  if (PQresultStatus(res_describe.get()) == PGRES_COMMAND_OK) {
    LOG_DEBUG << "Prepared statement already exists. [" << statement_name.data()
              << "]";

    return ErrorCode::OK;
  }

  int types_size  = 0;
  Oid* types_oids = nullptr;
  if (param_types) {
    types_size = param_types->size();
    types_oids = param_types->data();
  }

  // Create a prepared statement.
  ResultPtr res = DbcUtils::make_result_uptr(
      PQprepare(connection.get(), statement_name.data(), statement.data(),
                types_size, types_oids));

  if (PQresultStatus(res.get()) != PGRES_COMMAND_OK) {
    LOG_ERROR << Message::PREPARE_FAILURE << "[" << statement_name.data()
              << "] " << PQresultErrorMessage(res.get());
    return ErrorCode::DATABASE_ACCESS_FAILURE;
  }

  return ErrorCode::OK;
}

/**
 * @brief Executes a prepared statement, with given parameters.
 * @param connection      [in]  a connection.
 * @param statement_name  [in]  unique name for the prepared statement.
 * @param param_values    [in]  the actual values of the parameters.
 *   A null pointer in this array means the corresponding parameter is null.
 * @param res             [out] the result of a query.
 * @return ErrorCode::OK if success, otherwise an error code.
 */
ErrorCode DbcUtils::execute_statement(
    const PgConnectionPtr& connection, std::string_view statement_name,
    const std::vector<const char*>& param_values, PGresult*& res) {
  ErrorCode error = ErrorCode::INVALID_PARAMETER;

  if (!DbcUtils::is_open(connection)) {
    LOG_ERROR << Message::PREPARED_STATEMENT_EXECUTION_FAILURE
              << Message::NOT_INITIALIZED;
    return ErrorCode::NOT_INITIALIZED;
  }

  res = PQexecPrepared(connection.get(), statement_name.data(),
                       param_values.size(), param_values.data(), nullptr,
                       nullptr, 0);

  if ((PQresultStatus(res) == PGRES_COMMAND_OK) ||
      (PQresultStatus(res) == PGRES_TUPLES_OK)) {
    error = ErrorCode::OK;
  } else {
    LOG_ERROR << Message::PREPARED_STATEMENT_EXECUTION_FAILURE
              << PQresultErrorMessage(res);

    std::string error_code(PQresultErrorField(res, PG_DIAG_SQLSTATE));
    if (error_code == PgErrorCode::kUniqueViolation) {
      error = ErrorCode::ALREADY_EXISTS;
    } else {
      error = ErrorCode::INVALID_PARAMETER;
    }
  }

  return error;
}

}  // namespace manager::metadata::db
