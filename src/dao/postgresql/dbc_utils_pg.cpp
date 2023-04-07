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

#include <libpq-fe.h>

#include <cerrno>
#include <cstdlib>
#include <cstring>
#include <iostream>
#include <regex>
#include <string>

#include "manager/metadata/common/message.h"
#include "manager/metadata/dao/postgresql/common_pg.h"
#include "manager/metadata/helper/logging_helper.h"

// =============================================================================
namespace manager::metadata::db::postgresql {

/**
 * @brief Is this connection open?
 * @param connection  [in]  a connection.
 * @retval true - if this connection is open.
 * @retval false - if this connection is close.
 */
bool DbcUtils::is_open(const ConnectionSPtr& connection) {
  return PQstatus(connection.get()) == CONNECTION_OK;
}

/**
 * @brief Converts boolean expression in metadata repository to "true" or "false" in application.
 * @param string  [in]  boolean expression.
 * @return "true" or "false", otherwise an empty string.
 */
std::string DbcUtils::convert_boolean_expression(const char* string) {
  std::stringstream ss;
  if (string) {
    std::regex regex_true  = std::regex(R"(^([tTyY].*|1)$)");
    std::regex regex_false = std::regex(R"(^([fFnN].*|0)$)");
    if (std::regex_match(string, regex_true)) {
      ss << std::boolalpha << true;
    } else if (std::regex_match(string, regex_false)) {
      ss << std::boolalpha << false;
    }
  }
  return ss.str();
}

/**
 * @brief Converts boolean expression in metadata repository to boolean value in application.
 * @param string  [in]  boolean expression.
 * @return Converted boolean value of the string.
 */
bool DbcUtils::str_to_boolean(const char* string) {
  bool result = false;

  if (string) {
    std::regex regex_boolean = std::regex(R"(^([tTyY].*|1)$)");
    result                   = std::regex_match(string, regex_boolean);
  }
  return result;
}

/**
 * @brief Converts boolean value in application to boolean expression in metadata repository.
 * @param value  [in]  boolean expression.
 * @return Converted string of the boolean value.
 */
std::string DbcUtils::boolean_to_str(const bool value) { return value ? "true" : "false"; }

/**
 * @brief call standard library function to convert string to float.
 * @param nptr    [in]  C-string beginning with the representation of a floating-point number.
 * @param endptr  [in]  Reference to an already allocated object of type char*, whose value is set
 *   by the function to the next character in nptr after the numerical value.
 * @return the converted floating point number as a value of type float.
 */
template <>
[[nodiscard]] float DbcUtils::call_floating_point<float>(const char* nptr, char** endptr) {
  return std::strtof(nptr, endptr);
}

/**
 * @brief Explicit Template Instantiation for str_to_floating_point(float type).
 */
template ErrorCode DbcUtils::str_to_floating_point(const char* input, float& return_value);

/**
 * @brief Convert string to floating point.
 * @param input         [in]  C-string beginning with the representation of a floating-point number.
 * @param return_value  [out] the converted floating point number.
 * @return ErrorCode::OK if success, otherwise an error code.
 */
template <typename T>
[[nodiscard]] ErrorCode DbcUtils::str_to_floating_point(const char* input, T& return_value) {
  if (!input || *input == '\0' || std::isspace(*input)) {
    return ErrorCode::INTERNAL_ERROR;
  }
  char* end;
  errno = 0;

  const T result = call_floating_point<T>(input, &end);
  if (errno == 0 && *end == '\0') {
    return_value = result;
    return ErrorCode::OK;
  }
  LOG_ERROR << Message::CONVERT_STRING_TO_FLOAT_FAILURE;
  return ErrorCode::INTERNAL_ERROR;
}

/**
 * @brief call standard library function to convert string to unsigned long integer.
 * @param nptr    [in]  C-string containing the representation of an integral number.
 * @param endptr  [in]  Reference to an object of type char*, whose value is set by the function
 *   to the next character in nptr after the numerical value.
 * @param base    [in]  Numerical base (radix) that determines the valid characters and their
 *   interpretation.
 * @return converted integral number as an unsigned long int value.
 */
template <>
[[nodiscard]] std::uint64_t DbcUtils::call_integral<std::uint64_t>(const char* nptr, char** endptr,
                                                                   int base) {
  return std::strtoul(nptr, endptr, base);
}

/**
 * @brief call standard library function to convert string to long integer.
 * @param nptr    [in]  C-string containing the representation of an integral number.
 * @param endptr  [in]  Reference to an object of type char*, whose value is set by the function to
 *   the next character in nptr after the numerical value.
 * @param base    [in]  Numerical base (radix) that determines the valid characters and their
 *   interpretation.
 * @return converted integral number as long int value.
 */
template <>
[[nodiscard]] std::int64_t DbcUtils::call_integral<std::int64_t>(const char* nptr, char** endptr,
                                                                 int base) {
  return std::strtol(nptr, endptr, base);
}

/**
 * @brief Explicit Template Instantiation for str_to_integral(unsigned long).
 */
template ErrorCode DbcUtils::str_to_integral(const char* str, std::uint64_t& return_value);
/**
 * @brief Explicit Template Instantiation for str_to_integral(long).
 */
template ErrorCode DbcUtils::str_to_integral(const char* str, std::int64_t& return_value);

/**
 * @brief Convert string to integer.
 * @param input   [in]  C-string containing the representation of 
 * decimal integer literal (base 10).
 * @param output  [out] the converted integral number.
 * @return ErrorCode::OK if success, otherwise an error code.
 */
template <typename T>
[[nodiscard]] ErrorCode DbcUtils::str_to_integral(const char* input,
                                                  T& output) {

  if (!input || *input == '\0' || std::isspace(*input)) {
    return ErrorCode::INTERNAL_ERROR;
  }

  char* end;
  errno = 0;

  const T result = call_integral<T>(input, &end, BASE_10);
  if (errno == 0 && *end == '\0') {
    output = result;
    return ErrorCode::OK;
  }
  LOG_ERROR << Message::CONVERT_STRING_TO_INT_FAILURE;
  return ErrorCode::INTERNAL_ERROR;
}

/**
 * @brief Gets the number of affected rows if command was INSERT, UPDATE, or DELETE.
 * @param res           [in]  the result of a query.
 * @param return_value  [out] the number of affected rows.
 * @return the number of affected rows if last command was INSERT, UPDATE, or DELETE. zero for
 *   all other commands.
 */
ErrorCode DbcUtils::get_number_of_rows_affected(PGresult*& pgres, uint64_t& return_value) {
  return str_to_integral(PQcmdTuples(pgres), return_value);
}

/**
 * @brief Makes shared_ptr of PGconn with deleter.
 * @param pgconn  [in]  a connection.
 * @return shared_ptr of PGconn with deleter.
 */
ConnectionSPtr DbcUtils::make_connection_sptr(PGconn* pgconn) {
  ConnectionSPtr conn(pgconn, [](PGconn* c) { ::PQfinish(c); });
  return conn;
}

/**
 * @brief Makes unique_ptr of PGresult with deleter.
 * @param pgres  [in]  the result of a query.
 * @return unique_ptr of PGresult with deleter.
 */
ResultUPtr DbcUtils::make_result_uptr(PGresult* pgres) {
  ResultUPtr res(pgres, [](PGresult* r) { ::PQclear(r); });
  return res;
}

/**
 * @brief Defines a prepared statement.
 * @param connection      [in]  a connection.
 * @param statement_name  [in]  unique name for the new prepared statement.
 * @param statement       [in]  SQL statement to prepare.
 * @param param_types     [in]  Data types assigned to parameter symbols. (default: nullptr)
 * @return ErrorCode::OK if success, otherwise an error code.
 */
ErrorCode DbcUtils::prepare(const ConnectionSPtr& connection, const StatementName& statement_name,
                            std::string_view statement, std::vector<Oid>* param_types) {
  return DbcUtils::prepare(connection, std::to_string(static_cast<int>(statement_name)), statement,
                           param_types);
}

/**
 * @brief Defines a prepared statement.
 * @param connection      [in]  a connection.
 * @param statement_name  [in]  unique name for the prepared statement.
 * @param statement       [in]  SQL statement to prepare.
 * @param param_types     [in]  Data types assigned to parameter symbols. (default: nullptr)
 * @return ErrorCode::OK if success, otherwise an error code.
 */
ErrorCode DbcUtils::prepare(const ConnectionSPtr& connection, std::string_view statement_name,
                            std::string_view statement, std::vector<Oid>* param_types) {
  if (!DbcUtils::is_open(connection)) {
    LOG_ERROR << Message::PREPARE_FAILURE << Message::NOT_INITIALIZED;
    return ErrorCode::NOT_INITIALIZED;
  }

  int types_size  = 0;
  Oid* types_oids = nullptr;
  if (param_types) {
    types_size = param_types->size();
    types_oids = param_types->data();
  }

  ResultUPtr res = DbcUtils::make_result_uptr(
      PQprepare(connection.get(), statement_name.data(), statement.data(), types_size, types_oids));
  if (PQresultStatus(res.get()) != PGRES_COMMAND_OK) {
    LOG_ERROR << Message::PREPARE_FAILURE << PQresultErrorMessage(res.get());
    return ErrorCode::DATABASE_ACCESS_FAILURE;
  }

  return ErrorCode::OK;
}

ErrorCode DbcUtils::execute_statement(const ConnectionSPtr& connection,
                                      std::string_view statement_name,
                                      const std::vector<const char*>& param_values,
                                      PGresult*& res) {
  return exec_prepared(connection,
                       statement_name,
                       param_values, res);
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
ErrorCode DbcUtils::exec_prepared(const ConnectionSPtr& connection,
                                  const StatementName& statement_name,
                                  const std::vector<const char*>& param_values, PGresult*& res) {
  return exec_prepared(connection, std::to_string(static_cast<int>(statement_name)), param_values,
                       res);
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
ErrorCode DbcUtils::exec_prepared(const ConnectionSPtr& connection, std::string_view statement_name,
                                  const std::vector<const char*>& param_values, PGresult*& res) {
  ErrorCode error = ErrorCode::INVALID_PARAMETER;

  if (!DbcUtils::is_open(connection)) {
    LOG_ERROR << Message::PREPARED_STATEMENT_EXECUTION_FAILURE << Message::NOT_INITIALIZED;
    return ErrorCode::NOT_INITIALIZED;
  }

  res = PQexecPrepared(connection.get(), statement_name.data(), param_values.size(),
                       param_values.data(), nullptr, nullptr, 0);

  if ((PQresultStatus(res) == PGRES_COMMAND_OK) || (PQresultStatus(res) == PGRES_TUPLES_OK)) {
    error = ErrorCode::OK;
  } else {
    LOG_ERROR << Message::PREPARED_STATEMENT_EXECUTION_FAILURE << PQresultErrorMessage(res);

    std::string error_code(PQresultErrorField(res, PG_DIAG_SQLSTATE));
    if (error_code == PgErrorCode::kUniqueViolation) {
      error = ErrorCode::ALREADY_EXISTS;
    } else {
      error = ErrorCode::INVALID_PARAMETER;
    }
  }

  return error;
}

/**
 * @brief get the value of the specified key_value from the statement-names map.
 * @param statement_names_map  [in]  statement names map.
 * @param key_value            [in]  key.
 * @param statement_name       [out] statement name.
 * @return ErrorCode::OK if success, otherwise an error code.
 */
ErrorCode DbcUtils::find_statement_name(
    const std::unordered_map<std::string, std::string>& statement_names_map,
    std::string_view key_value, std::string& statement_name) {
  ErrorCode error = ErrorCode::UNKNOWN;

  try {
    if (!statement_names_map.empty()) {
      statement_name = statement_names_map.at(key_value.data());
      error          = ErrorCode::OK;
    } else {
      LOG_ERROR << Message::PARAMETER_FAILED << "Statement names map is empty.: "
                << "[" << key_value << "]";
      error = ErrorCode::INVALID_PARAMETER;
    }
  } catch (std::out_of_range& e) {
    LOG_ERROR << Message::METADATA_KEY_NOT_FOUND << "[" << key_value << "]: " << e.what();
    error = ErrorCode::INVALID_PARAMETER;
  } catch (...) {
    LOG_ERROR << Message::METADATA_KEY_NOT_FOUND << "[" << key_value << "]";
    error = ErrorCode::INVALID_PARAMETER;
  }

  return error;
}

}  // namespace manager::metadata::db::postgresql
