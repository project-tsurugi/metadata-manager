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
#include "manager/metadata/dao/postgresql/datatypes_dao.h"

#include <boost/format.hpp>
#include <iostream>
#include <string>
#include <string_view>
#include <unordered_map>
#include <utility>
#include <vector>

#include <libpq-fe.h>
#include "manager/metadata/dao/common/message.h"
#include "manager/metadata/dao/common/statement_name.h"
#include "manager/metadata/dao/postgresql/common.h"
#include "manager/metadata/dao/postgresql/dbc_utils.h"

// =============================================================================
namespace {

std::unordered_map<std::string, std::string> column_names;
std::unordered_map<std::string, std::string> statement_names_select_equal_to;

namespace statement {

using manager::metadata::db::postgresql::DataTypesDAO;
using manager::metadata::db::postgresql::SCHEMA_NAME;

/**
 * @brief Returns a SELECT statement to get metadata:
 *   select * from table_name where column_name = $1.
 * @param (column_name)  [in]  column name of metadata-table.
 * @return a SELECT statement:
 *   select * from table_name where column_name = $1.
 */
std::string select_equal_to(std::string_view column_name) {
  // SQL statement
  boost::format query =
      boost::format(
          "SELECT %3%, %4%, %5%, %6%, %7%, %8%, %9%"
          " FROM %1%.%2%"
          " WHERE %10% = $1") %
      SCHEMA_NAME % DataTypesDAO::kTableName %
      DataTypesDAO::ColumnName::kFormatVersion %
      DataTypesDAO::ColumnName::kGeneration % DataTypesDAO::ColumnName::kId %
      DataTypesDAO::ColumnName::kName % DataTypesDAO::ColumnName::kPgDataType %
      DataTypesDAO::ColumnName::kPgDataTypeName %
      DataTypesDAO::ColumnName::kPgDataTypeQualifiedName % column_name;

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
 * @brief Constructor
 * @param (connection)  [in]  a connection to the metadata repository.
 * @return none.
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
  column_names.emplace(DataTypes::ID, ColumnName::kId);
  column_names.emplace(DataTypes::NAME, ColumnName::kName);
  column_names.emplace(DataTypes::PG_DATA_TYPE, ColumnName::kPgDataType);
  column_names.emplace(DataTypes::PG_DATA_TYPE_NAME,
                       ColumnName::kPgDataTypeName);
  column_names.emplace(DataTypes::PG_DATA_TYPE_QUALIFIED_NAME,
                       ColumnName::kPgDataTypeQualifiedName);

  // Creates a list of unique name
  // for the new prepared statement for each column names.
  for (auto column : column_names) {
    // Creates unique name
    // for the new prepared statement.
    boost::format statement_name =
        boost::format("%1%-%2%-%3%") %
        static_cast<int>(StatementName::DAO_SELECT_EQUAL_TO) % kTableName %
        column.first;

    // Addes this list to unique name
    // for the new prepared statement
    //
    // key : column name
    // value : unique name for the new prepared statement.
    statement_names_select_equal_to.emplace(column.first, statement_name.str());
  }
};

/**
 * @brief Defines all prepared statements.
 * @param none.
 * @return ErrorCode::OK if success, otherwise an error code.
 */
ErrorCode DataTypesDAO::prepare() const {
  ErrorCode error = ErrorCode::UNKNOWN;

  for (auto column : column_names) {
    error = DbcUtils::prepare(connection_,
                              statement_names_select_equal_to.at(column.first),
                              statement::select_equal_to(column.second));
    if (error != ErrorCode::OK) {
      break;
    }
  }

  return error;
}

/**
 * @brief Executes a SELECT statement to get one data type metadata
 *   from the data types metadata table,
 *   where the given key equals the given value.
 * @param (object_key)          [in]  key.
 *   column name of the data types metadata table.
 * @param (object_value)        [in]  value to be filtered.
 * @param (object)              [out] one data type metadata to get,
 *   where the given key equals the given value.
 * @retval ErrorCode::OK if success.
 * @retval ErrorCode::ID_NOT_FOUND if the data types id does not exist.
 * @retval ErrorCode::NAME_NOT_FOUND if the data types name does not exist.
 * @retval ErrorCode::NOT_FOUND if the other data types key does not exist.
 * @retval otherwise an error code.
 */
ErrorCode DataTypesDAO::select_one_data_type_metadata(
    std::string_view object_key, std::string_view object_value,
    ptree& object) const {
  ErrorCode error = ErrorCode::UNKNOWN;
  std::vector<const char*> param_values;

  param_values.emplace_back(object_value.data());

  // Get the name of the SQL statement to be executed.
  std::string statement_name;
  error = DbcUtils::find_statement_name(statement_names_select_equal_to,
                                        object_key, statement_name);
  if (error != ErrorCode::OK) {
    return error;
  }

  PGresult* res = nullptr;
  error =
      DbcUtils::exec_prepared(connection_, statement_name, param_values, res);

  if (error == ErrorCode::OK) {
    int nrows = PQntuples(res);
    if (nrows == 1) {
      int ordinal_position = 0;
      error = convert_pgresult_to_ptree(res, ordinal_position, object);
    } else if (nrows == 0) {
      // Convert the error code.
      if (object_key == DataTypes::ID) {
        error = ErrorCode::ID_NOT_FOUND;
      } else if (object_key == DataTypes::NAME) {
        error = ErrorCode::NAME_NOT_FOUND;
      } else {
        error = ErrorCode::NOT_FOUND;
      }
    } else {
      error = ErrorCode::INVALID_PARAMETER;
    }
  }

  PQclear(res);
  return error;
}

// -----------------------------------------------------------------------------
// Private method area

/**
 * @brief Gets the ptree type data types metadata
 *  converted from the given PGresult type value.
 * @param (res)               [in]  the result of a query.
 * @param (ordinal_position)  [in]  column ordinal position of PGresult.
 * @param (object)            [out] one data type metadata.
 * @return ErrorCode::OK if success, otherwise an error code.
 */
ErrorCode DataTypesDAO::convert_pgresult_to_ptree(PGresult*& res,
                                                  const int ordinal_position,
                                                  ptree& object) const {
  ErrorCode error = ErrorCode::UNKNOWN;

  // Set the value of the format_version column to ptree.
  object.put(DataTypes::FORMAT_VERSION,
             PQgetvalue(res, ordinal_position,
                        static_cast<int>(OrdinalPosition::kFormatVersion)));

  // Set the value of the generation column to ptree.
  object.put(DataTypes::GENERATION,
             PQgetvalue(res, ordinal_position,
                        static_cast<int>(OrdinalPosition::kGeneration)));

  // Set the value of the id to ptree.
  ObjectIdType id = -1;
  error = DbcUtils::str_to_integral<ObjectIdType>(
      PQgetvalue(res, ordinal_position, static_cast<int>(OrdinalPosition::kId)),
      id);
  if (error != ErrorCode::OK) {
    return error;
  }
  object.put(DataTypes::ID, id);

  // Set the value of the name column to ptree.
  object.put(DataTypes::NAME,
             std::string(PQgetvalue(res, ordinal_position,
                                    static_cast<int>(OrdinalPosition::kName))));

  // Set the value of the pg_data_type column to ptree.
  int64_t pg_data_type = -1;
  error = DbcUtils::str_to_integral<int64_t>(
      PQgetvalue(res, ordinal_position,
                 static_cast<int>(OrdinalPosition::kPgDataType)),
      pg_data_type);
  if (error != ErrorCode::OK) {
    return error;
  }
  object.put(DataTypes::PG_DATA_TYPE, pg_data_type);

  // Set the value of the pg_data_type_name column to ptree.
  object.put(DataTypes::PG_DATA_TYPE_NAME,
             std::string(PQgetvalue(
                 res, ordinal_position,
                 static_cast<int>(OrdinalPosition::kPgDataTypeName))));

  // Set the value of the pg_data_type_qualified_name column to ptree.
  object.put(DataTypes::PG_DATA_TYPE_QUALIFIED_NAME,
             std::string(PQgetvalue(
                 res, ordinal_position,
                 static_cast<int>(OrdinalPosition::kPgDataTypeQualifiedName))));
  error = ErrorCode::OK;

  return error;
}

}  // namespace manager::metadata::db::postgresql
