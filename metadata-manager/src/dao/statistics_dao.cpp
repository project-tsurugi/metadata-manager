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

#include "manager/metadata/dao/statistics_dao.h"

#include <boost/property_tree/json_parser.hpp>
#include <iostream>
#include <sstream>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include <libpq-fe.h>

#include "manager/metadata/dao/common/message.h"
#include "manager/metadata/dao/common/statement_name.h"
#include "manager/metadata/dao/dialect/dialect_strategy.h"

using namespace boost::property_tree;
using namespace manager::metadata;

using pair_const_oit_cstats = std::pair<const ObjectIdType, ColumnStatistic>;

namespace manager::metadata::db {

/**
 * @enum ColumnOrdinalPosition
 * @brief Column ordinal position of the column statistics table
 * in the metadata repository.
 */
enum ColumnOrdinalPosition { TABLE_ID = 0, ORDINAL_POSITION, COLUMN_STATISTIC };

/**
 *  @brief  Defines all prepared statements.
 *  @param  none.
 *  @return ErrorCode::OK if success, otherwise an error code.
 */
ErrorCode StatisticsDAO::prepare() const {
    ErrorCode error = GenericDAO::prepare(
        StatementName::
            STATISTICS_DAO_UPSERT_ONE_COLUMN_STATISTIC_BY_TABLE_ID_COLUMN_ORDINAL_POSITION,
        DialectStrategy::get_instance()
            ->statistics_dao_upsert_one_column_statistic_by_table_id_column_ordinal_position());
    if (error != ErrorCode::OK) {
        return error;
    }

    error = GenericDAO::prepare(
        StatementName::
            STATISTICS_DAO_SELECT_ONE_COLUMN_STATISTIC_BY_TABLE_ID_COLUMN_ORDINAL_POSITION,
        DialectStrategy::get_instance()
            ->statistics_dao_select_one_column_statistic_by_table_id_column_ordinal_position());
    if (error != ErrorCode::OK) {
        return error;
    }

    error = GenericDAO::prepare(
        StatementName::STATISTICS_DAO_SELECT_ALL_COLUMN_STATISTIC_BY_TABLE_ID,
        DialectStrategy::get_instance()
            ->statistics_dao_select_all_column_statistic_by_table_id());
    if (error != ErrorCode::OK) {
        return error;
    }

    error = GenericDAO::prepare(
        StatementName::STATISTICS_DAO_DELETE_ALL_COLUMN_STATISTIC_BY_TABLE_ID,
        DialectStrategy::get_instance()
            ->statistics_dao_delete_all_column_statistic_by_table_id());
    if (error != ErrorCode::OK) {
        return error;
    }

    error = GenericDAO::prepare(
        StatementName::
            STATISTICS_DAO_DELETE_ONE_COLUMN_STATISTIC_BY_TABLE_ID_COLUMN_ORDINAL_POSITION,
        DialectStrategy::get_instance()
            ->statistics_dao_delete_one_column_statistic_by_table_id_column_ordinal_position());
    if (error != ErrorCode::OK) {
        return error;
    }

    return error;
}

/**
 *  @brief  Executes UPSERT statement to upsert one column statistic
 *  into the column statistics table
 *  based on the given table id and the given column ordinal position.
 *  Executes a INSERT statement it if it not exists in the metadata repository,
 *  Executes a UPDATE statement it if it already exists.
 *  @param  (table_id)          [in]  table id.
 *  @param  (ordinal_position)  [in]  column ordinal position.
 *  @param  (column_statistic)  [in]  one column statistic to add or update.
 *  @return ErrorCode::OK if success, otherwise an error code.
 */
ErrorCode
StatisticsDAO::upsert_one_column_statistic_by_table_id_column_ordinal_position(
    ObjectIdType table_id, ObjectIdType ordinal_position,
    const std::string &column_statistic) const {
    std::vector<char const *> param_values;

    std::string s_table_id = std::to_string(table_id);
    std::string s_ordinal_position = std::to_string(ordinal_position);

    param_values.emplace_back(s_table_id.c_str());
    param_values.emplace_back(s_ordinal_position.c_str());

    if (column_statistic.empty()) {
        param_values.emplace_back(nullptr);
    } else {
        param_values.emplace_back(column_statistic.c_str());
    }

    PGresult *res;
    ErrorCode error = exec_prepared(
        StatementName::
            STATISTICS_DAO_UPSERT_ONE_COLUMN_STATISTIC_BY_TABLE_ID_COLUMN_ORDINAL_POSITION,
        param_values, res);

    if (error == ErrorCode::OK) {
        uint64_t number_of_rows_affected = 0;
        ErrorCode error_get =
            DbcUtils::get_number_of_rows_affected(res, number_of_rows_affected);

        if (error_get != ErrorCode::OK) {
            PQclear(res);
            return error_get;
        }

        if (number_of_rows_affected != 1) {
            PQclear(res);
            return ErrorCode::INVALID_PARAMETER;
        }
    }

    PQclear(res);
    return error;
}

/**
 *  @brief  Gets the ColumnStatistic type column statistic
 *  converted from the given PGresult type value.
 *  @param  (res)               [in]  the result of a query.
 *  @param  (ordinal_position)  [in]  column ordinal position of PGresult.
 *  @param  (column_statistic)  [out] one column
 * statistic (ColumnStatistic type).
 *  @return ErrorCode::OK if success, otherwise an error code.
 */
ErrorCode StatisticsDAO::get_column_statistic_from_p_gresult(
    PGresult *&res, int ordinal_position,
    ColumnStatistic &column_statistic) const {
    // table id
    ErrorCode error_str_to_int = DbcUtils::str_to_integral<ObjectIdType>(
        PQgetvalue(res, ordinal_position, ColumnOrdinalPosition::TABLE_ID),
        column_statistic.table_id);

    if (error_str_to_int != ErrorCode::OK) {
        return error_str_to_int;
    }

    // ordinal position
    error_str_to_int = DbcUtils::str_to_integral<ObjectIdType>(
        PQgetvalue(res, ordinal_position,
                   ColumnOrdinalPosition::ORDINAL_POSITION),
        column_statistic.ordinal_position);

    if (error_str_to_int != ErrorCode::OK) {
        return error_str_to_int;
    }

    // column statistic
    std::string s_column_statistic = PQgetvalue(
        res, ordinal_position, ColumnOrdinalPosition::COLUMN_STATISTIC);

    if (!s_column_statistic.empty()) {
        std::stringstream ss;
        ss << s_column_statistic;
        try {
            json_parser::read_json(ss, column_statistic.column_statistic);
        } catch (boost::property_tree::json_parser_error &e) {
            std::cerr << Message::READ_JSON_FAILURE << e.what() << std::endl;
            return ErrorCode::INTERNAL_ERROR;
        } catch (...) {
            std::cerr << Message::READ_JSON_FAILURE << std::endl;
            return ErrorCode::INTERNAL_ERROR;
        }
    }

    return ErrorCode::OK;
}

/**
 *  @brief  Executes a SELECT statement to get one column statistic
 *  from the column statistics table
 *  based on the given table id and the given column ordinal position.
 *  @param  (table_id)          [in]  table id.
 *  @param  (ordinal_position)  [in]  column ordinal position.
 *  @param  (column_statistic)  [out] one column statistic
 *  with the specified table id and column ordinal position.
 *  @return ErrorCode::OK if success, otherwise an error code.
 */
ErrorCode
StatisticsDAO::select_one_column_statistic_by_table_id_column_ordinal_position(
    ObjectIdType table_id, ObjectIdType ordinal_position,
    ColumnStatistic &column_statistic) const {
    std::vector<const char *> param_values;

    std::string s_table_id = std::to_string(table_id);
    std::string s_ordinal_position = std::to_string(ordinal_position);

    param_values.emplace_back(s_table_id.c_str());
    param_values.emplace_back(s_ordinal_position.c_str());

    PGresult *res;
    ErrorCode error = exec_prepared(
        StatementName::
            STATISTICS_DAO_SELECT_ONE_COLUMN_STATISTIC_BY_TABLE_ID_COLUMN_ORDINAL_POSITION,
        param_values, res);

    if (error == ErrorCode::OK) {
        int nrows = PQntuples(res);

        if (nrows == 1) {
            int ordinal_position = 0;
            error = get_column_statistic_from_p_gresult(res, ordinal_position,
                                                        column_statistic);
        } else {
            PQclear(res);
            return ErrorCode::INVALID_PARAMETER;
        }
    }

    PQclear(res);
    return error;
}

/**
 *  @brief  Executes a SELECT statement to get all column statistics
 *  from the column statistics table based on the given table id.
 *  @param  (table_id)           [in]  table id.
 *  @param  (column_statistics)  [out] all column statistics
 *  with the specified table id.
 *  key : column ordinal position
 *  value : one column statistic
 *  @return ErrorCode::OK if success, otherwise an error code.
 */
ErrorCode StatisticsDAO::select_all_column_statistic_by_table_id(
    ObjectIdType table_id,
    std::unordered_map<ObjectIdType, ColumnStatistic> &column_statistics)
    const {
    std::vector<const char *> param_values;

    std::string s_table_id = std::to_string(table_id);

    param_values.emplace_back(s_table_id.c_str());

    PGresult *res;
    ErrorCode error = exec_prepared(
        StatementName::STATISTICS_DAO_SELECT_ALL_COLUMN_STATISTIC_BY_TABLE_ID,
        param_values, res);

    if (error == ErrorCode::OK) {
        int nrows = PQntuples(res);
        if (nrows <= 0) {
            PQclear(res);
            return ErrorCode::INVALID_PARAMETER;
        }
        for (int ordinal_position = 0; ordinal_position < nrows;
             ordinal_position++) {
            ColumnStatistic column_statistic;

            ErrorCode error_internal = get_column_statistic_from_p_gresult(
                res, ordinal_position, column_statistic);

            if (error_internal != ErrorCode::OK) {
                PQclear(res);
                return error_internal;
            }

            column_statistics.insert(pair_const_oit_cstats{
                column_statistic.ordinal_position, column_statistic});
        }
    }

    PQclear(res);
    return error;
}

/**
 *  @brief  Executes DELETE statement to delete all column statistics
 *  from the column statistics table based on the given table id.
 *  @param  (table_id)          [in]  table id.
 *  @return ErrorCode::OK if success, otherwise an error code.
 */
ErrorCode StatisticsDAO::delete_all_column_statistic_by_table_id(
    ObjectIdType table_id) const {
    std::vector<const char *> param_values;

    std::string s_table_id = std::to_string(table_id);

    param_values.emplace_back(s_table_id.c_str());

    PGresult *res;
    ErrorCode error = exec_prepared(
        StatementName::STATISTICS_DAO_DELETE_ALL_COLUMN_STATISTIC_BY_TABLE_ID,
        param_values, res);

    if (error == ErrorCode::OK) {
        uint64_t number_of_rows_affected = 0;
        ErrorCode error_get =
            DbcUtils::get_number_of_rows_affected(res, number_of_rows_affected);

        if (error_get != ErrorCode::OK) {
            PQclear(res);
            return error_get;
        }

        if (number_of_rows_affected <= 0) {
            PQclear(res);
            return ErrorCode::INVALID_PARAMETER;
        }
    }

    PQclear(res);
    return error;
}

/**
 *  @brief  Executes DELETE statement to delete one column statistic
 *  from the column statistics table
 *  based on the given table id and the given column ordinal position.
 *  @param  (table_id)          [in]  table id.
 *  @param  (ordinal_position)  [in]  column ordinal position.
 *  @return ErrorCode::OK if success, otherwise an error code.
 */
ErrorCode
StatisticsDAO::delete_one_column_statistic_by_table_id_column_ordinal_position(
    ObjectIdType table_id, ObjectIdType ordinal_position) const {
    std::vector<const char *> param_values;

    std::string s_table_id = std::to_string(table_id);
    std::string s_ordinal_position = std::to_string(ordinal_position);

    param_values.emplace_back(s_table_id.c_str());
    param_values.emplace_back(s_ordinal_position.c_str());

    PGresult *res;
    ErrorCode error = exec_prepared(
        StatementName::
            STATISTICS_DAO_DELETE_ONE_COLUMN_STATISTIC_BY_TABLE_ID_COLUMN_ORDINAL_POSITION,
        param_values, res);
    if (error == ErrorCode::OK) {
        uint64_t number_of_rows_affected = 0;
        ErrorCode error_get =
            DbcUtils::get_number_of_rows_affected(res, number_of_rows_affected);

        if (error_get != ErrorCode::OK) {
            PQclear(res);
            return error_get;
        }

        if (number_of_rows_affected != 1) {
            PQclear(res);
            return ErrorCode::INVALID_PARAMETER;
        }
    }

    PQclear(res);
    return error;
}

}  // namespace manager::metadata::db
