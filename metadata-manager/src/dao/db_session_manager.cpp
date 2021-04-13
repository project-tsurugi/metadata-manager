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

#include "manager/metadata/dao/db_session_manager.h"

#include <iostream>
#include <memory>
#include <string>

#include <libpq-fe.h>

#include "manager/metadata/dao/columns_dao.h"
#include "manager/metadata/dao/common/config.h"
#include "manager/metadata/dao/common/message.h"
#include "manager/metadata/dao/datatypes_dao.h"
#include "manager/metadata/dao/statistics_dao.h"
#include "manager/metadata/dao/tables_dao.h"

namespace manager::metadata::db {

/**
 *  @brief  Establishes a connection to the metadata repository
 *  using connection information in a string.
 *  @param  none.
 *  @return ErrorCode::OK if success, otherwise an error code.
 */
manager::metadata::ErrorCode DBSessionManager::connect() {
    connection = DbcUtils::make_connection_sptr(
        PQconnectdb(Config::get_connection_string().c_str()));

    if (!DbcUtils::is_open(connection)) {
        std::cerr << Message::CONNECT_FAILURE << std::endl;
        return ErrorCode::DATABASE_ACCESS_FAILURE;
    }

    return ErrorCode::OK;
}

/**
 *  @brief  Gets Dao instance for the requested table name
 *  if all the following steps are successfully completed.
 *  1. Establishes a connection to the metadata repository.
 *  2. Sends a query to set always-secure search path
 *     to the metadata repository.
 *  3. Defines prepared statements for returned Dao
 *     in the metadata repository.
 *  @param  (table_name)   [in]  unique id for the Dao.
 *  @param  (gdao)         [out] Dao instance if success.
 *  for the requested table name.
 *  @return ErrorCode::OK if success, otherwise an error code.
 */
manager::metadata::ErrorCode DBSessionManager::get_dao(
    GenericDAO::TableName table_name, std::shared_ptr<GenericDAO> &gdao) {
    if (!DbcUtils::is_open(connection)) {
        ErrorCode error = connect();

        if (error != ErrorCode::OK) {
            return error;
        }

        error = set_always_secure_search_path();

        if (error != ErrorCode::OK) {
            return error;
        }
    }

    switch (table_name) {
        case GenericDAO::TableName::TABLES: {
            auto tdao = std::make_shared<TablesDAO>(connection);
            gdao = tdao;
            break;
        }
        case GenericDAO::TableName::STATISTICS: {
            auto sdao = std::make_shared<StatisticsDAO>(connection);
            gdao = sdao;
            break;
        }
        case GenericDAO::TableName::DATATYPES: {
            auto ddao = std::make_shared<DataTypesDAO>(connection);
            gdao = ddao;
            break;
        }
        case GenericDAO::TableName::COLUMNS: {
            auto cdao = std::make_shared<ColumnsDAO>(connection);
            gdao = cdao;
            break;
        }
        default: {
            return ErrorCode::INTERNAL_ERROR;
            break;
        }
    }

    if (gdao == nullptr) {
        return ErrorCode::INTERNAL_ERROR;
    }

    return gdao->prepare();
}

/**
 *  @brief  Starts a transaction scope managed by this DBSessionManager.
 *  @param  none.
 *  @return ErrorCode::OK if success, otherwise an error code.
 */
manager::metadata::ErrorCode DBSessionManager::start_transaction() {
    if (!DbcUtils::is_open(connection)) {
        std::cerr << Message::START_TRANSACTION_FAILURE
                  << Message::NOT_INITIALIZED << std::endl;
        return ErrorCode::NOT_INITIALIZED;
    }
    ResultUPtr res =
        DbcUtils::make_result_uptr(PQexec(connection.get(), "BEGIN"));
    if (PQresultStatus(res.get()) != PGRES_COMMAND_OK) {
        std::cerr << Message::START_TRANSACTION_FAILURE
                  << PQerrorMessage(connection.get()) << std::endl;
        return ErrorCode::DATABASE_ACCESS_FAILURE;
    }

    return ErrorCode::OK;
}

/**
 *  @brief  Commits all transactions currently started for all DAO contexts
 *  managed by this DBSessionManager.
 *  @param  none.
 *  @return ErrorCode::OK if success, otherwise an error code.
 */
manager::metadata::ErrorCode DBSessionManager::commit() {
    if (!DbcUtils::is_open(connection)) {
        std::cerr << Message::COMMIT_FAILURE << Message::NOT_INITIALIZED
                  << std::endl;
        return ErrorCode::NOT_INITIALIZED;
    }
    ResultUPtr res =
        DbcUtils::make_result_uptr(PQexec(connection.get(), "COMMIT"));
    if (PQresultStatus(res.get()) != PGRES_COMMAND_OK) {
        std::cerr << Message::COMMIT_FAILURE << PQerrorMessage(connection.get())
                  << std::endl;
        return ErrorCode::DATABASE_ACCESS_FAILURE;
    }

    return ErrorCode::OK;
}

/**
 *  @brief  Rollbacks all transactions currently started for all DAO contexts
 *  managed by this DBSessionManager.
 *  @param  none.
 *  @return ErrorCode::OK if success, otherwise an error code.
 */
manager::metadata::ErrorCode DBSessionManager::rollback() {
    if (!DbcUtils::is_open(connection)) {
        std::cerr << Message::ROLLBACK_FAILURE << Message::NOT_INITIALIZED
                  << std::endl;
        return ErrorCode::NOT_INITIALIZED;
    }
    ResultUPtr res =
        DbcUtils::make_result_uptr(PQexec(connection.get(), "ROLLBACK"));
    if (PQresultStatus(res.get()) != PGRES_COMMAND_OK) {
        std::cerr << Message::ROLLBACK_FAILURE
                  << PQerrorMessage(connection.get()) << std::endl;
        return ErrorCode::DATABASE_ACCESS_FAILURE;
    }

    return ErrorCode::OK;
}

/**
 *  @brief  Sends a query to set always-secure search path
 *  to the metadata repository.
 *  @param  none.
 *  @return ErrorCode::OK if success, otherwise an error code.
 */
manager::metadata::ErrorCode DBSessionManager::set_always_secure_search_path() {
    if (!DbcUtils::is_open(connection)) {
        std::cerr << Message::SET_ALWAYS_SECURE_SEARCH_PATH
                  << Message::NOT_INITIALIZED << std::endl;
        return ErrorCode::NOT_INITIALIZED;
    }
    ResultUPtr res = DbcUtils::make_result_uptr(
        PQexec(connection.get(),
               "SELECT pg_catalog.set_config('search_path', '', false)"));
    if (PQresultStatus(res.get()) != PGRES_TUPLES_OK) {
        std::cerr << Message::SET_ALWAYS_SECURE_SEARCH_PATH
                  << PQerrorMessage(connection.get()) << std::endl;
        return ErrorCode::DATABASE_ACCESS_FAILURE;
    }

    return ErrorCode::OK;
}

}  // namespace manager::metadata::db
