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
#include "manager/metadata/dao/postgresql/datatypes_dao.h"

#include <boost/format.hpp>
#include <iostream>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include <libpq-fe.h>
#include "manager/metadata/dao/common/message.h"
#include "manager/metadata/dao/common/statement_name.h"
#include "manager/metadata/dao/postgresql/common.h"
#include "manager/metadata/dao/postgresql/dbc_utils.h"
#include "manager/metadata/datatypes.h"

// =============================================================================
namespace {
namespace statement {

using manager::metadata::db::postgresql::SCHEMA_NAME;
using manager::metadata::db::postgresql::TableName;

/**
 * @brief  Returnes a SELECT stetement to get metadata:
 *   select * from table_name where column_name = $1.
 * @param  (column_name)  [in]  column name of metadata-table.
 * @return a SELECT stetement:
 *    select * from table_name where column_name = $1.
 */
std::string select_equal_to(std::string_view column_name) {
  // SQL statement
  boost::format query = boost::format("SELECT * FROM %s.%s WHERE %s = $1") %
                        SCHEMA_NAME % TableName::DATA_TYPES_TABLE % column_name;

  return query.str();
}

}  // namespace statement
}  // namespace

// =============================================================================
namespace manager::metadata::db::postgresql {

using boost::property_tree::ptree;
using manager::metadata::DataTypes;
using manager::metadata::ErrorCode;
using manager::metadata::db::StatementName;

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
DataTypesDAO::DataTypesDAO(DBSessionManager* session_manager)
    : connection_(session_manager->get_connection()) {
  // Creates a list of column names
  // in order to get values based on
  // one column included in this list
  // from metadata repository.
  //
  // For example,
  // If column name "id" is added to this list,
  // later defines a prepared statement
  // "select * from where id = ?".
  column_names.emplace_back(DataTypes::ID);
  column_names.emplace_back(DataTypes::NAME);
  column_names.emplace_back(DataTypes::PG_DATA_TYPE);
  column_names.emplace_back(DataTypes::PG_DATA_TYPE_NAME);
  column_names.emplace_back(DataTypes::PG_DATA_TYPE_QUALIFIED_NAME);

  // Creates a list of unique name
  // for the new prepared statement for each column names.
  for (auto column : column_names) {
    // Creates unique name
    // for the new prepared statement.
    std::string statement_name;
    statement_name.append(std::to_string(StatementName::DAO_SELECT_EQUAL_TO));
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

  for (const std::string& column : column_names) {
    error = DbcUtils::prepare(connection_,
                              statement_names_select_equal_to.at(column),
                              statement::select_equal_to(column));
    if (error != ErrorCode::OK) {
      break;
    }
  }

  return error;
}

/**
 *  @brief  Executes a SELECT statement to get one data type metadata
 *  from the data types metadata table,
 *  where the given key equals the given value.
 *  @param  (object_key)          [in]  key.
 *  column name of the data types metadata table.
 *  @param  (object_value)        [in]  value to be filtered.
 *  @param  (object)              [out] one data type metadata to get,
 *  where the given key equals the given value.
 *  @return ErrorCode::OK if success, otherwise an error code.
 */
ErrorCode DataTypesDAO::select_one_data_type_metadata(
    std::string_view object_key, std::string_view object_value,
    ptree& object) const {
  std::vector<const char*> param_values;

  param_values.emplace_back(object_value.data());

  std::string statement_name_found;
  try {
    statement_name_found =
        statement_names_select_equal_to.at(std::string(object_key));
  } catch (std::out_of_range& e) {
    std::cerr << Message::METADATA_KEY_NOT_FOUND << e.what() << std::endl;
    return ErrorCode::INVALID_PARAMETER;
  } catch (...) {
    std::cerr << Message::METADATA_KEY_NOT_FOUND << std::endl;
    return ErrorCode::INVALID_PARAMETER;
  }

  PGresult* res;
  ErrorCode error = DbcUtils::exec_prepared(connection_, statement_name_found,
                                            param_values, res);

  if (error == ErrorCode::OK) {
    int nrows = PQntuples(res);

    if (nrows == 1) {
      int ordinal_position = 0;
      error = get_ptree_from_p_gresult(res, ordinal_position, object);
    } else {
      error = ErrorCode::NOT_FOUND;
    }
  }

  PQclear(res);
  return error;
}

// -----------------------------------------------------------------------------
// Private method area

/**
 *  @brief  Gets the ptree type data types metadata
 *  converted from the given PGresult type value.
 *  @param  (res)               [in]  the result of a query.
 *  @param  (ordinal_position)  [in]  column ordinal position of PGresult.
 *  @param  (object)            [out] one data type metadata.
 *  @return ErrorCode::OK if success, otherwise an error code.
 */
ErrorCode DataTypesDAO::get_ptree_from_p_gresult(PGresult*& res,
                                                 int ordinal_position,
                                                 ptree& object) const {
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

}  // namespace manager::metadata::db::postgresql
