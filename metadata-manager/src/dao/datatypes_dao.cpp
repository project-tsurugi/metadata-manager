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

#include "manager/metadata/dao/datatypes_dao.h"

#include <string>
#include <utility>
#include <vector>

#include "manager/metadata/dao/common/message.h"
#include "manager/metadata/dao/common/statement_name.h"
#include "manager/metadata/dao/dialect/dialect.h"
#include "manager/metadata/dao/dialect/dialect_strategy.h"
#include "manager/metadata/datatypes.h"

using namespace boost::property_tree;
using namespace manager::metadata;

namespace manager::metadata::db {

/**
 * @enum DataTypesMetadataTable
 * @brief Column ordinal position of the data types metadata table
 * in the metadata repository.
 */
struct DataTypesMetadataTable {
    enum ColumnOrdinalPosition {
        ID = 0,
        NAME,
        PG_DATA_TYPE,
        PG_DATA_TYPE_NAME,
        PG_DATA_TYPE_QUALIFIED_NAME
    };
};

/**
 *  @brief  Constructor
 *  @param  (connection)  [in]  a connection to the metadata repository.
 *  @return none.
 */
DataTypesDAO::DataTypesDAO(ConnectionSPtr connection)
    : GenericDAO(connection, TableName::DATATYPES) {
    // Creates a list of column names
    // in order to get values based on
    // one column included in this list
    // from metadata repository.
    //
    // For example,
    // If column name "id" is added to this list,
    // later defines a prepared statement
    // "select * from where id = ?".
    column_names.emplace_back(manager::metadata::DataTypes::ID);
    column_names.emplace_back(manager::metadata::DataTypes::NAME);
    column_names.emplace_back(manager::metadata::DataTypes::PG_DATA_TYPE);
    column_names.emplace_back(manager::metadata::DataTypes::PG_DATA_TYPE_NAME);
    column_names.emplace_back(
        manager::metadata::DataTypes::PG_DATA_TYPE_QUALIFIED_NAME);

    // Creates a list of unique name
    // for the new prepared statement for each column names.
    for (auto column : column_names) {
        // Creates unique name
        // for the new prepared statement.
        std::string statement_name;
        statement_name.append(
            std::to_string(StatementName::DAO_SELECT_EQUAL_TO));
        statement_name.append("-");
        statement_name.append(std::to_string(TableName::DATATYPES));
        statement_name.append("-");
        statement_name.append(column);

        // Addes this list to unique name
        // for the new prepared statement
        //
        // key : column name
        // value : unique name for the new prepared statement.
        statement_names_select_equal_to.emplace(column, statement_name);
    }
};

/**
 *  @brief  Defines all prepared statements.
 *  @param  none.
 *  @return ErrorCode::OK if success, otherwise an error code.
 */
ErrorCode DataTypesDAO::prepare() const {
    ErrorCode error = ErrorCode::INTERNAL_ERROR;

    for (const std::string &column : column_names) {
        error = GenericDAO::prepare(
            statement_names_select_equal_to.at(column),
            DialectStrategy::get_instance()->dao_select_equal_to(
                Dialect::TableName::DATA_TYPES_TABLE, column));
        if (error != ErrorCode::OK) {
            return error;
        }
    }

    return error;
}

/**
 *  @brief  Gets the ptree type data types metadata
 *  converted from the given PGresult type value.
 *  @param  (res)               [in]  the result of a query.
 *  @param  (ordinal_position)  [in]  column ordinal position of PGresult.
 *  @param  (object)            [out] one data type metadata.
 *  @return ErrorCode::OK if success, otherwise an error code.
 */
ErrorCode DataTypesDAO::get_ptree_from_p_gresult(
    PGresult *&res, int ordinal_position,
    boost::property_tree::ptree &object) const {
    // ID
    ObjectIdType id = -1;
    ErrorCode error_str_to_int = DbcUtils::str_to_integral<ObjectIdType>(
        PQgetvalue(res, ordinal_position,
                   DataTypesMetadataTable::ColumnOrdinalPosition::ID),
        id);
    if (error_str_to_int == ErrorCode::OK) {
        object.put(DataTypes::ID, id);
    } else {
        return error_str_to_int;
    }

    // NAME
    object.put(DataTypes::NAME,
               std::string(PQgetvalue(
                   res, ordinal_position,
                   DataTypesMetadataTable::ColumnOrdinalPosition::NAME)));

    // PG_DATA_TYPE
    ObjectIdType pg_data_type = -1;
    error_str_to_int = DbcUtils::str_to_integral<ObjectIdType>(
        PQgetvalue(res, ordinal_position,
                   DataTypesMetadataTable::ColumnOrdinalPosition::PG_DATA_TYPE),
        pg_data_type);
    if (error_str_to_int == ErrorCode::OK) {
        object.put(DataTypes::PG_DATA_TYPE, pg_data_type);
    } else {
        return error_str_to_int;
    }

    // PG_DATA_TYPE_NAME
    object.put(
        DataTypes::PG_DATA_TYPE_NAME,
        std::string(PQgetvalue(
            res, ordinal_position,
            DataTypesMetadataTable::ColumnOrdinalPosition::PG_DATA_TYPE_NAME)));

    // PG_DATA_TYPE_QUALIFIED_NAME
    object.put(
        DataTypes::PG_DATA_TYPE_QUALIFIED_NAME,
        std::string(PQgetvalue(res, ordinal_position,
                               DataTypesMetadataTable::ColumnOrdinalPosition::
                                   PG_DATA_TYPE_QUALIFIED_NAME)));
    return ErrorCode::OK;
}

/**
 *  @brief  Executes a SELECT statement to get one data type metadata
 *  from the data types metadata table,,
 *  where the given key equals to the given value.
 *  @param  (object_key)          [in]  key.
 *  column name of the data types metadata table.
 *  @param  (object_value)        [in]  value to be filtered.
 *  @param  (object)              [out] one data type metadata to get,
 *  where the given key equals to the given value.
 *  @return ErrorCode::OK if success, otherwise an error code.
 */
ErrorCode DataTypesDAO::select_one_data_type_metadata(
    const std::string &object_key, const std::string &object_value,
    ptree &object) const {
    std::vector<const char *> param_values;

    param_values.emplace_back(object_value.c_str());

    std::string statement_name_found =
        statement_names_select_equal_to.at(object_key);
    if (statement_name_found.empty()) {
        return ErrorCode::INTERNAL_ERROR;
    }

    PGresult *res;
    ErrorCode error = exec_prepared(statement_name_found, param_values, res);

    if (error == ErrorCode::OK) {
        int nrows = PQntuples(res);

        if (nrows == 1) {
            int ordinal_position = 0;
            error = get_ptree_from_p_gresult(res, ordinal_position, object);
        } else {
            PQclear(res);
            return ErrorCode::INVALID_PARAMETER;
        }
    }

    PQclear(res);
    return error;
}

}  // namespace manager::metadata::db
