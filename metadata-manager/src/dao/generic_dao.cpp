/*
 * Copyright 2020 tsurugi project.
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

#include "manager/metadata/dao/generic_dao.h"

#include <iostream>
#include <string>
#include <vector>

#include <libpq-fe.h>

#include "manager/metadata/dao/common/message.h"

namespace manager::metadata::db {

/**
 *  @brief Defines a prepared statement.
 *  @param  (statement_name) [in] unique name for the new prepared statement.
 *  @param  (statement)      [in] SQL statement to prepare.
 *  @return ErrorCode::OK if success, otherwise an error code.
 */
ErrorCode GenericDAO::prepare(const StatementName &statement_name,
                              const std::string &statement) const {
    return prepare(std::to_string(statement_name), statement);
}

/**
 *  @brief Defines a prepared statement.
 *  @param  (statement_name) [in] unique name for the new prepared statement.
 *  @param  (statement)      [in] SQL statement to prepare.
 *  @return ErrorCode::OK if success, otherwise an error code.
 */
ErrorCode GenericDAO::prepare(const std::string &statement_name,
                              const std::string &statement) const {
    if (!DbcUtils::is_open(connection)) {
        std::cerr << Message::PREPARE_FAILURE << Message::NOT_INITIALIZED
                  << std::endl;
        return ErrorCode::NOT_INITIALIZED;
    }
    ResultUPtr res = DbcUtils::make_result_uptr(
        PQprepare(connection.get(), statement_name.c_str(), statement.c_str(),
                  0, nullptr));
    if (PQresultStatus(res.get()) != PGRES_COMMAND_OK) {
        std::cerr << Message::PREPARE_FAILURE << PQresultErrorMessage(res.get())
                  << std::endl;
        return ErrorCode::DATABASE_ACCESS_FAILURE;
    }

    return ErrorCode::OK;
}

/**
 *  @brief Executes a prepared statement, with given parameters.
 *  @param  (statement_name)    [in]  unique name for the prepared statement.
 *  @param  (param_values)      [in]  the actual values of the parameters.
 *  A null pointer in this array means the corresponding parameter is null.
 *  @param  (res)               [out] the result of a query.
 *  @return ErrorCode::OK if success, otherwise an error code.
 */
ErrorCode GenericDAO::exec_prepared(
    const StatementName &statement_name,
    const std::vector<char const *> &param_values, PGresult *&res) const {
    return exec_prepared(std::to_string(statement_name), param_values, res);
}

/**
 *  @brief Executes a prepared statement, with given parameters.
 *  @param  (statement_name)    [in]  unique name for the prepared statement.
 *  @param  (param_values)      [in]  the actual values of the parameters.
 *  A null pointer in this array means the corresponding parameter is null.
 *  @param  (res)               [out] the result of a query.
 *  @return ErrorCode::OK if success, otherwise an error code.
 */
ErrorCode GenericDAO::exec_prepared(
    const std::string &statement_name,
    const std::vector<char const *> &param_values, PGresult *&res) const {
    if (!DbcUtils::is_open(connection)) {
        std::cerr << Message::PREPARED_STATEMENT_EXECUTION_FAILURE
                  << Message::NOT_INITIALIZED << std::endl;
        return ErrorCode::NOT_INITIALIZED;
    }

    res = PQexecPrepared(connection.get(), statement_name.c_str(),
                         param_values.size(), param_values.data(), nullptr,
                         nullptr, 0);

    if (PQresultStatus(res) != PGRES_COMMAND_OK &&
        PQresultStatus(res) != PGRES_TUPLES_OK) {
        std::cerr << Message::PREPARED_STATEMENT_EXECUTION_FAILURE
                  << PQresultErrorMessage(res) << std::endl;
        return ErrorCode::INVALID_PARAMETER;
    }

    return ErrorCode::OK;
}

}  // namespace manager::metadata::db
