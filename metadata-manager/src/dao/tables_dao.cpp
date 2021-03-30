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

#include "manager/metadata/dao/tables_dao.h"

#include <boost/property_tree/json_parser.hpp>
#include <iostream>
#include <sstream>
#include <string>
#include <utility>
#include <vector>

#include "manager/metadata/dao/common/message.h"
#include "manager/metadata/dao/common/statement_name.h"
#include "manager/metadata/dao/dialect/dialect_strategy.h"
#include "manager/metadata/tables.h"

using namespace boost::property_tree;
using namespace manager::metadata;

namespace manager::metadata::db {

/**
 * @enum TableMetadataTable
 * @brief Column ordinal position of the table metadata table
 * in the metadata repository.
 */
struct TableMetadataTable {
    enum ColumnOrdinalPosition {
        ID = 0,
        NAME,
        NAMESPACE_NAME,
        PRIMARY_KEY,
        RELTUPLES
    };
};

/**
 *  @brief  Constructor
 *  @param  (connection)  [in]  a connection to the metadata repository.
 *  @return none.
 */
TablesDAO::TablesDAO(ConnectionSPtr connection)
    : GenericDAO(connection, TableName::TABLES) {
    // Creates a list of column names
    // in order to get values based on
    // one column included in this list
    // from metadata repository.
    //
    // For example,
    // If column name "id" is added to this list,
    // later defines a prepared statement
    // "select * from where id = ?".
    column_names.emplace_back(Tables::ID);
    column_names.emplace_back(Tables::NAME);

    // Creates a list of unique name
    // for the new prepared statement for each column names.
    for (auto column : column_names) {
        // Creates unique name
        // for the new prepared statement.
        std::string statement_name;
        statement_name.append(
            std::to_string(StatementName::DAO_SELECT_EQUAL_TO));
        statement_name.append("-");
        statement_name.append(std::to_string(TableName::TABLES));
        statement_name.append("-");
        statement_name.append(column);

        // Addes this list to unique name
        // for the new prepared statement
        //
        // key : column name
        // value : unique name for the new prepared statement.
        statement_names_select_equal_to.emplace(column, statement_name);
    }
}

/**
 *  @brief  Defines all prepared statements.
 *  @param  none.
 *  @return ErrorCode::OK if success, otherwise an error code.
 */
ErrorCode TablesDAO::prepare() const {
    ErrorCode error = GenericDAO::prepare(
        StatementName::TABLES_DAO_UPDATE_RELTUPLES_BY_TABLE_ID,
        DialectStrategy::get_instance()
            ->tables_dao_update_reltuples_by_table_id());
    if (error != ErrorCode::OK) {
        return error;
    }

    error = GenericDAO::prepare(
        StatementName::TABLES_DAO_UPDATE_RELTUPLES_BY_TABLE_NAME,
        DialectStrategy::get_instance()
            ->tables_dao_update_reltuples_by_table_name());
    if (error != ErrorCode::OK) {
        return error;
    }

    error = GenericDAO::prepare(
        StatementName::TABLES_DAO_SELECT_TABLE_STATISTIC_BY_TABLE_ID,
        DialectStrategy::get_instance()
            ->tables_dao_select_table_statistic_by_table_id());
    if (error != ErrorCode::OK) {
        return error;
    }

    error = GenericDAO::prepare(
        StatementName::TABLES_DAO_SELECT_TABLE_STATISTIC_BY_TABLE_NAME,
        DialectStrategy::get_instance()
            ->tables_dao_select_table_statistic_by_table_name());
    if (error != ErrorCode::OK) {
        return error;
    }

    error = GenericDAO::prepare(
        StatementName::TABLES_DAO_INSERT_TABLE_METADATA,
        DialectStrategy::get_instance()->tables_dao_insert_table_metadata());
    if (error != ErrorCode::OK) {
        return error;
    }

    for (const std::string &column : column_names) {
        error = GenericDAO::prepare(
            statement_names_select_equal_to.at(column),
            DialectStrategy::get_instance()->dao_select_equal_to(
                Dialect::TableName::TABLE_METADATA_TABLE, column));
        if (error != ErrorCode::OK) {
            return error;
        }
    }

    error = GenericDAO::prepare(
        StatementName::TABLES_DAO_DELETE_TABLE_METADATA_BY_TABLE_ID,
        DialectStrategy::get_instance()
            ->tables_dao_delete_table_metadata_by_table_id());
    if (error != ErrorCode::OK) {
        return error;
    }

    error = GenericDAO::prepare(
        StatementName::TABLES_DAO_DELETE_TABLE_METADATA_BY_TABLE_NAME,
        DialectStrategy::get_instance()
            ->tables_dao_delete_table_metadata_by_table_name());
    if (error != ErrorCode::OK) {
        return error;
    }

    return error;
}

/**
 *  @brief  Executes UPDATE statement to update the given number of rows
 *  into the table metadata table based on the given table id.
 *  @param  (reltuples)         [in]  the number of rows to update.
 *  @param  (table_id)          [in]  table id.
 *  @return ErrorCode::OK if success, otherwise an error code.
 */
ErrorCode TablesDAO::update_reltuples_by_table_id(float reltuples,
                                                  ObjectIdType table_id) const {
    std::vector<char const *> param_values;

    std::string s_reltuples = std::to_string(reltuples);
    std::string s_table_id = std::to_string(table_id);

    param_values.emplace_back(s_reltuples.c_str());
    param_values.emplace_back(s_table_id.c_str());

    PGresult *res;
    ErrorCode error =
        exec_prepared(StatementName::TABLES_DAO_UPDATE_RELTUPLES_BY_TABLE_ID,
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
 *  @brief  Executes UPDATE statement to update the given number of rows
 *  into the table metadata table based on the given table name.
 *  @param  (reltuples)         [in]  the number of rows to update.
 *  @param  (table_name)        [in]  table name.
 *  @param  (table_id)          [out] table id of the row updated.
 *  @return ErrorCode::OK if success, otherwise an error code.
 */
ErrorCode TablesDAO::update_reltuples_by_table_name(
    float reltuples, const std::string &table_name,
    ObjectIdType &table_id) const {
    std::vector<char const *> param_values;

    std::string s_reltuples = std::to_string(reltuples);

    param_values.emplace_back(s_reltuples.c_str());
    param_values.emplace_back(table_name.c_str());

    PGresult *res;
    ErrorCode error =
        exec_prepared(StatementName::TABLES_DAO_UPDATE_RELTUPLES_BY_TABLE_NAME,
                      param_values, res);

    if (error == ErrorCode::OK) {
        int nrows = PQntuples(res);
        if (nrows == 1) {
            int ordinal_position = 0;
            error = DbcUtils::str_to_integral<ObjectIdType>(
                PQgetvalue(res, ordinal_position,
                           TableMetadataTable::ColumnOrdinalPosition::ID),
                table_id);

        } else {
            PQclear(res);
            return ErrorCode::INVALID_PARAMETER;
        }
    }

    PQclear(res);
    return error;
}

/**
 *  @brief  Gets the TableStatistic type value
 *  converted from the given PGresult type value.
 *  @param  (res)               [in]  the result of a query.
 *  @param  (ordinal_position)  [in]  column ordinal position of PGresult.
 *  @param  (table_statistic)   [out] the TableStatistic type value.
 *  @return ErrorCode::OK if success, otherwise an error code.
 */
ErrorCode TablesDAO::get_table_statistic_from_p_gresult(
    PGresult *&res, int ordinal_position,
    TableStatistic &table_statistic) const {
    // id
    ErrorCode error_str_to_int = DbcUtils::str_to_integral<ObjectIdType>(
        PQgetvalue(res, ordinal_position,
                   TableMetadataTable::ColumnOrdinalPosition::ID),
        table_statistic.id);

    if (error_str_to_int != ErrorCode::OK) {
        return error_str_to_int;
    }

    // name
    table_statistic.name = std::string(
        PQgetvalue(res, ordinal_position,
                   TableMetadataTable::ColumnOrdinalPosition::NAME));

    // namespace
    table_statistic.namespace_name = std::string(
        PQgetvalue(res, ordinal_position,
                   TableMetadataTable::ColumnOrdinalPosition::NAMESPACE_NAME));

    // reltuples
    ErrorCode error_str_to_float = DbcUtils::str_to_floating_point<float>(
        PQgetvalue(res, ordinal_position,
                   TableMetadataTable::ColumnOrdinalPosition::RELTUPLES),
        table_statistic.reltuples);

    if (error_str_to_float != ErrorCode::OK) {
        return error_str_to_float;
    }

    return ErrorCode::OK;
}

/**
 *  @brief  Executes SELECT statement to get one table statistic
 *  from the table metadata table based on the given table id.
 *  @param  (table_id)        [in]  table id.
 *  @param  (table_statistic) [out] table statistic to get.
 *  @return ErrorCode::OK if success, otherwise an error code.
 */
ErrorCode TablesDAO::select_table_statistic_by_table_id(
    ObjectIdType table_id, TableStatistic &table_statistic) const {
    std::vector<const char *> param_values;

    std::string s_table_id = std::to_string(table_id);
    param_values.emplace_back(s_table_id.c_str());

    PGresult *res;
    ErrorCode error = exec_prepared(
        StatementName::TABLES_DAO_SELECT_TABLE_STATISTIC_BY_TABLE_ID,
        param_values, res);
    if (error == ErrorCode::OK) {
        int nrows = PQntuples(res);

        if (nrows == 1) {
            int ordinal_position = 0;

            error = get_table_statistic_from_p_gresult(res, ordinal_position,
                                                       table_statistic);
        } else {
            PQclear(res);
            return ErrorCode::INVALID_PARAMETER;
        }
    }

    PQclear(res);
    return error;
}

/**
 *  @brief  Executes SELECT statement to get one table statistic
 *  from the table metadata table based on the given table name.
 *  @param  (table_name)        [in]  table name.
 *  @param  (table_statistic)   [out] table statistic to get.
 *  @return ErrorCode::OK if success, otherwise an error code.
 */
ErrorCode TablesDAO::select_table_statistic_by_table_name(
    const std::string &table_name, TableStatistic &table_statistic) const {
    std::vector<const char *> param_values;

    param_values.emplace_back(table_name.c_str());

    PGresult *res;
    ErrorCode error = exec_prepared(
        StatementName::TABLES_DAO_SELECT_TABLE_STATISTIC_BY_TABLE_NAME,
        param_values, res);

    if (error == ErrorCode::OK) {
        int nrows = PQntuples(res);

        if (nrows == 1) {
            int ordinal_position = 0;
            error = get_table_statistic_from_p_gresult(res, ordinal_position,
                                                       table_statistic);
        } else {
            PQclear(res);
            return ErrorCode::INVALID_PARAMETER;
        }
    }

    PQclear(res);
    return error;
}

/**
 *  @brief  Gets the ptree type table metadata
 *  converted from the given PGresult type value.
 *  @param  (res)               [in]  the result of a query.
 *  @param  (ordinal_position)  [in]  column ordinal position of PGresult.
 *  @param  (table)             [out] one table metadata.
 *  @return ErrorCode::OK if success, otherwise an error code.
 */
ErrorCode TablesDAO::get_ptree_from_p_gresult(
    PGresult *&res, int ordinal_position,
    boost::property_tree::ptree &table) const {
    table.put(Tables::ID,
              PQgetvalue(res, ordinal_position,
                         TableMetadataTable::ColumnOrdinalPosition::ID));

    table.put(Tables::NAME,
              PQgetvalue(res, ordinal_position,
                         TableMetadataTable::ColumnOrdinalPosition::NAME));

    std::string namespace_name =
        PQgetvalue(res, ordinal_position,
                   TableMetadataTable::ColumnOrdinalPosition::NAMESPACE_NAME);

    if (!namespace_name.empty()) {
        table.put(Tables::NAMESPACE, namespace_name);
    }

    std::string reltuples =
        PQgetvalue(res, ordinal_position,
                   TableMetadataTable::ColumnOrdinalPosition::RELTUPLES);

    if (!reltuples.empty()) {
        table.put(Tables::RELTUPLES, reltuples);
    }

    std::string s_primary_keys =
        PQgetvalue(res, ordinal_position,
                   TableMetadataTable::ColumnOrdinalPosition::PRIMARY_KEY);

    ptree primary_keys;
    if (s_primary_keys.empty()) {
        // NOTICE:
        // MUST add empty ptree.
        // ogawayama-server read key Tables::PRIMARY_KEY_NODE.
        table.add_child(Tables::PRIMARY_KEY_NODE, primary_keys);
    } else {
        std::stringstream ss;
        ss << s_primary_keys;
        try {
            json_parser::read_json(ss, primary_keys);
        } catch (boost::property_tree::json_parser_error &e) {
            std::cerr << Message::READ_JSON_FAILURE << e.what() << std::endl;
            return ErrorCode::INTERNAL_ERROR;
        } catch (...) {
            std::cerr << Message::READ_JSON_FAILURE << std::endl;
            return ErrorCode::INTERNAL_ERROR;
        }
        table.add_child(Tables::PRIMARY_KEY_NODE, primary_keys);
    }

    return ErrorCode::OK;
}

/**
 *  @brief  Executes INSERT statement to insert the given one table metadata
 *  into the table metadata table.
 *  @param  (table)             [in]   one table metadata to add.
 *  @param  (table_id)          [out]  table id.
 *  @return ErrorCode::OK if success, otherwise an error code.
 */
ErrorCode TablesDAO::insert_table_metadata(boost::property_tree::ptree &table,
                                           ObjectIdType &table_id) const {
    std::vector<char const *> param_values;

    boost::optional<std::string> name =
        table.get_optional<std::string>(Tables::NAME);
    if (!name) {
        return ErrorCode::INVALID_PARAMETER;
    }
    param_values.emplace_back(name.value().c_str());

    boost::optional<std::string> namespace_name =
        table.get_optional<std::string>(Tables::NAMESPACE);

    if (!namespace_name) {
        param_values.emplace_back(nullptr);
    } else {
        param_values.emplace_back(namespace_name.value().c_str());
    }

    boost::optional<ptree &> o_primary_keys =
        table.get_child_optional(Tables::PRIMARY_KEY_NODE);

    std::string s_primary_keys;
    if (o_primary_keys) {
        ptree &p_primary_keys = o_primary_keys.value();

        if (!p_primary_keys.empty()) {
            std::stringstream ss;
            try {
                json_parser::write_json(ss, p_primary_keys, false);
            } catch (boost::property_tree::json_parser_error &e) {
                std::cerr << Message::WRITE_JSON_FAILURE << e.what()
                          << std::endl;
                return ErrorCode::INTERNAL_ERROR;
            } catch (...) {
                std::cerr << Message::WRITE_JSON_FAILURE << std::endl;
                return ErrorCode::INTERNAL_ERROR;
            }
            s_primary_keys = ss.str();
        }
    }

    if (s_primary_keys.empty()) {
        param_values.emplace_back(nullptr);
    } else {
        param_values.emplace_back(s_primary_keys.c_str());
    }

    boost::optional<std::string> reltuples =
        table.get_optional<std::string>(Tables::RELTUPLES);

    if (!reltuples) {
        param_values.emplace_back(nullptr);
    } else {
        param_values.emplace_back(reltuples.value().c_str());
    }

    PGresult *res;
    ErrorCode error = exec_prepared(
        StatementName::TABLES_DAO_INSERT_TABLE_METADATA, param_values, res);
    if (error == ErrorCode::OK) {
        int nrows = PQntuples(res);
        if (nrows == 1) {
            int ordinal_position = 0;
            error = DbcUtils::str_to_integral<ObjectIdType>(
                PQgetvalue(res, ordinal_position,
                           TableMetadataTable::ColumnOrdinalPosition::ID),
                table_id);
        } else {
            PQclear(res);
            return ErrorCode::INVALID_PARAMETER;
        }
    }

    PQclear(res);
    return error;
}

/**
 *  @brief  Executes a SELECT statement to get table metadata rows
 *  from the table metadata table,
 *  where the given key equals the given value.
 *  @param  (object_key)          [in]  key. column name of a table metadata
 * table.
 *  @param  (object_value)        [in]  value to be filtered.
 *  @param  (object)              [out] table metadatas to get,
 *  where the given key equals the given value.
 *  @return ErrorCode::OK if success, otherwise an error code.
 */
ErrorCode TablesDAO::select_table_metadata(const std::string &object_key,
                                           const std::string &object_value,
                                           ptree &object) const {
    std::vector<const char *> param_values;

    param_values.emplace_back(object_value.c_str());

    std::string statement_name_found;
    try {
        statement_name_found = statement_names_select_equal_to.at(object_key);
    } catch (std::out_of_range &e) {
        std::cerr << Message::METADATA_KEY_NOT_FOUND << e.what() << std::endl;
        return ErrorCode::INVALID_PARAMETER;
    } catch (...) {
        std::cerr << Message::METADATA_KEY_NOT_FOUND << std::endl;
        return ErrorCode::INVALID_PARAMETER;
    }

    PGresult *res;
    ErrorCode error = exec_prepared(statement_name_found, param_values, res);

    if (error == ErrorCode::OK) {
        int nrows = PQntuples(res);

        if (nrows <= 0) {
            PQclear(res);
            return ErrorCode::INVALID_PARAMETER;
        } else if (nrows == 1) {
            int ordinal_position = 0;

            error = get_ptree_from_p_gresult(res, ordinal_position, object);
        } else {
            for (int ordinal_position = 0; ordinal_position < nrows;
                 ordinal_position++) {
                ptree table;

                error = get_ptree_from_p_gresult(res, ordinal_position, table);

                if (error != ErrorCode::OK) {
                    PQclear(res);
                    return error;
                }

                object.push_back(std::make_pair("", table));
            }
        }
    }

    PQclear(res);
    return error;
}

/**
 *  @brief  Executes DELETE statement to delete table metadata
 *  from the table metadata table based on the given table id.
 *  @param  (table_id)          [in]  table id.
 *  @return ErrorCode::OK if success, otherwise an error code.
 */
ErrorCode TablesDAO::delete_table_metadata_by_table_id(
    ObjectIdType table_id) const {
    std::vector<const char *> param_values;

    std::string s_table_id = std::to_string(table_id);

    param_values.emplace_back(s_table_id.c_str());

    PGresult *res;
    ErrorCode error = exec_prepared(
        StatementName::TABLES_DAO_DELETE_TABLE_METADATA_BY_TABLE_ID,
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
 *  @brief  Executes DELETE statement to delete table metadata
 *  from the table metadata table based on the given table name.
 *  @param  (table_name)        [id]   table name.
 *  @param  (table_id)          [out]  table id of the row deleted.
 *  @return ErrorCode::OK if success, otherwise an error code.
 */
ErrorCode TablesDAO::delete_table_metadata_by_table_name(
    const std::string &table_name, ObjectIdType &table_id) const {
    std::vector<const char *> param_values;

    param_values.emplace_back(table_name.c_str());

    PGresult *res;
    ErrorCode error = exec_prepared(
        StatementName::TABLES_DAO_DELETE_TABLE_METADATA_BY_TABLE_NAME,
        param_values, res);

    if (error == ErrorCode::OK) {
        uint64_t number_of_rows_affected = 0;
        ErrorCode error_get =
            DbcUtils::get_number_of_rows_affected(res, number_of_rows_affected);

        if (error_get != ErrorCode::OK) {
            PQclear(res);
            return error_get;
        }

        if (number_of_rows_affected == 1) {
            int ordinal_position = 0;
            error = DbcUtils::str_to_integral<ObjectIdType>(
                PQgetvalue(res, ordinal_position,
                           TableMetadataTable::ColumnOrdinalPosition::ID),
                table_id);
        } else {
            PQclear(res);
            return ErrorCode::INVALID_PARAMETER;
        }
    }

    PQclear(res);
    return error;
}

}  // namespace manager::metadata::db
