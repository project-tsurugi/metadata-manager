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
#include "manager/metadata/dao/postgresql/columns_dao.h"

#include <libpq-fe.h>

#include <boost/format.hpp>
#include <iostream>
#include <sstream>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include "manager/metadata/common/message.h"
#include "manager/metadata/common/utilitys.h"
#include "manager/metadata/dao/common/statement_name.h"
#include "manager/metadata/dao/postgresql/common.h"
#include "manager/metadata/dao/postgresql/dbc_utils.h"

// =============================================================================
namespace {

std::unordered_map<std::string, std::string> column_names;
std::unordered_map<std::string, std::string> statement_names_select;
std::unordered_map<std::string, std::string> statement_names_delete;

namespace statement {

using manager::metadata::db::postgresql::ColumnsDAO;
using manager::metadata::db::postgresql::SCHEMA_NAME;

/**
 * @brief Returns an INSERT statement for one column metadata.
 * @param none.
 * @return an INSERT statement to insert one column metadata.
 */
std::string insert_one_column_metadata() {
  // SQL statement
  boost::format query =
      boost::format(
          "INSERT INTO %1%.%2%"
          " (%3%, %4%, %5%, %6%, %7%, %8%, %9%, %10%, %11%, %12%, %13%) "
          " VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)") %
      SCHEMA_NAME % ColumnsDAO::kTableName %
      ColumnsDAO::ColumnName::kFormatVersion %
      ColumnsDAO::ColumnName::kGeneration % ColumnsDAO::ColumnName::kTableId %
      ColumnsDAO::ColumnName::kName % ColumnsDAO::ColumnName::kOrdinalPosition %
      ColumnsDAO::ColumnName::kDataTypeId %
      ColumnsDAO::ColumnName::kDataLength % ColumnsDAO::ColumnName::kVarying %
      ColumnsDAO::ColumnName::kNullable % ColumnsDAO::ColumnName::kDefaultExpr %
      ColumnsDAO::ColumnName::kDirection;

  return query.str();
}

/**
 * @brief Returns a SELECT statement to get all column metadata
 *  from column metadata table, based on table id.
 * @param (column_name)  [in]  column name of metadata-columns.
 * @return a SELECT statement to get all column metadata:
 *    select * from table_name where column_name = $1.
 */
std::string select_all_column_metadata(std::string_view column_name) {
  // SQL statement
  boost::format query =
      boost::format(
          "SELECT %3%, %4%, %5%, %6%, %7%, %8%, %9%"
          " , %10%, %11%, %12%, %13%, %14%"
          " FROM %1%.%2% WHERE %15% = $1 ORDER BY %8%") %
      SCHEMA_NAME % ColumnsDAO::kTableName %
      ColumnsDAO::ColumnName::kFormatVersion %
      ColumnsDAO::ColumnName::kGeneration % ColumnsDAO::ColumnName::kId %
      ColumnsDAO::ColumnName::kName % ColumnsDAO::ColumnName::kTableId %
      ColumnsDAO::ColumnName::kOrdinalPosition %
      ColumnsDAO::ColumnName::kDataTypeId %
      ColumnsDAO::ColumnName::kDataLength % ColumnsDAO::ColumnName::kVarying %
      ColumnsDAO::ColumnName::kNullable % ColumnsDAO::ColumnName::kDefaultExpr %
      ColumnsDAO::ColumnName::kDirection % column_name.data();

  return query.str();
}

/**
 * @brief Returns a DELETE statement for one column metadata
 *  from column metadata table, based on table id.
 * @param (column_name)  [in]  column name of metadata-columns.
 * @return a DELETE statement for one column metadata:
 *    delete from table_name where column_name = $1.
 */
std::string delete_all_column_metadata(std::string_view column_name) {
  // SQL statement
  boost::format query = boost::format("DELETE FROM %1%.%2% WHERE %3% = $1") %
                        SCHEMA_NAME % ColumnsDAO::kTableName %
                        column_name.data();

  return query.str();
}

}  // namespace statement
}  // namespace

// =============================================================================
namespace manager::metadata::db::postgresql {

using boost::property_tree::ptree;
using manager::metadata::ErrorCode;
using manager::metadata::db::StatementName;

/**
 * @brief Constructor
 * @param (connection)  [in]  a connection to the metadata repository.
 * @return none.
 */
ColumnsDAO::ColumnsDAO(DBSessionManager* session_manager)
    : connection_(session_manager->get_connection()) {
  // Creates a list of column names
  // in order to get values based on
  // one column included in this list
  // from metadata repository.
  //
  // For example,
  // If column name "tableId" is added to this list,
  // later defines a prepared statement
  // "select * from where tableId = ?".
  column_names.emplace(Tables::Column::TABLE_ID, ColumnName::kTableId);

  // Creates a list of unique name
  // for the new prepared statement for each column names.
  for (auto column : column_names) {
    // Creates unique name for the new prepared statement.
    boost::format statement_name_select =
        boost::format("%1%-%2%-%3%") %
        static_cast<int>(
            StatementName::COLUMNS_DAO_SELECT_ALL_COLUMN_METADATA) %
        kTableName % column.first;
    boost::format statement_name_delete =
        boost::format("%1%-%2%-%3%") %
        static_cast<int>(
            StatementName::COLUMNS_DAO_DELETE_ALL_COLUMN_METADATA) %
        kTableName % column.first;

    // Addes this list to unique name for the new prepared statement.
    // key : column name
    // value : unique name for the new prepared statement.
    statement_names_select.emplace(column.first, statement_name_select.str());
    statement_names_delete.emplace(column.first, statement_name_delete.str());
  }
}

/**
 * @brief Defines all prepared statements.
 * @param none.
 * @return ErrorCode::OK if success, otherwise an error code.
 */
ErrorCode ColumnsDAO::prepare() const {
  ErrorCode error = ErrorCode::UNKNOWN;

  error = DbcUtils::prepare(
      connection_, StatementName::COLUMNS_DAO_INSERT_ONE_COLUMN_METADATA,
      statement::insert_one_column_metadata());
  if (error != ErrorCode::OK) {
    return error;
  }

  for (auto column : column_names) {
    // select statement.
    error =
        DbcUtils::prepare(connection_, statement_names_select.at(column.first),
                          statement::select_all_column_metadata(column.second));
    if (error != ErrorCode::OK) {
      break;
    }

    // delete statement.
    error =
        DbcUtils::prepare(connection_, statement_names_delete.at(column.first),
                          statement::delete_all_column_metadata(column.second));
    if (error != ErrorCode::OK) {
      break;
    }
  }

  return error;
}

/**
 * @brief Executes INSERT statement to insert the given one column statistic
 *   into the column metadata table based on the given table id.
 * @param (table_id)  [in]  table id.
 * @param (column)    [in]  one column metadata to add.
 * @return ErrorCode::OK if success, otherwise an error code.
 */
ErrorCode ColumnsDAO::insert_one_column_metadata(
    const ObjectIdType table_id,
    const boost::property_tree::ptree& column) const {
  ErrorCode error = ErrorCode::UNKNOWN;
  std::vector<char const*> param_values;

  // format_version
  std::string s_format_version = std::to_string(Tables::format_version());
  param_values.emplace_back(s_format_version.c_str());

  // generation
  std::string s_generation = std::to_string(Tables::generation());
  param_values.emplace_back(s_generation.c_str());

  // table_id
  std::string s_table_id = std::to_string(table_id);
  param_values.emplace_back(s_table_id.c_str());

  // name
  boost::optional<std::string> name =
      column.get_optional<std::string>(Tables::Column::NAME);
  if (!name) {
    error = ErrorCode::INVALID_PARAMETER;
    return error;
  }
  param_values.emplace_back((name ? name.value().c_str() : nullptr));

  // ordinal_position
  boost::optional<std::string> ordinal_position =
      column.get_optional<std::string>(Tables::Column::ORDINAL_POSITION);
  param_values.emplace_back(
      (ordinal_position ? ordinal_position.value().c_str() : nullptr));

  // data_type_id
  boost::optional<std::string> data_type_id =
      column.get_optional<std::string>(Tables::Column::DATA_TYPE_ID);
  param_values.emplace_back(
      (data_type_id ? data_type_id.value().c_str() : nullptr));

  // data_length
  boost::optional<const ptree&> o_data_length =
      column.get_child_optional(Tables::Column::DATA_LENGTH);

  std::string s_data_length;
  if (!o_data_length) {
    param_values.emplace_back(nullptr);
  } else {
    const ptree& p_data_length = o_data_length.value();

    if (p_data_length.empty()) {
      param_values.emplace_back(p_data_length.data().c_str());
    } else {
      // Converts a property_tree to a JSON string.
      error = Utilitys::ptree_to_json(p_data_length, s_data_length);
      if (error != ErrorCode::OK) {
        return error;
      }
      param_values.emplace_back(s_data_length.c_str());
    }
  }

  // varying
  boost::optional<std::string> varying =
      column.get_optional<std::string>(Tables::Column::VARYING);
  param_values.emplace_back((varying ? varying.value().c_str() : nullptr));

  // nullable
  boost::optional<std::string> nullable =
      column.get_optional<std::string>(Tables::Column::NULLABLE);
  if (!nullable) {
    error = ErrorCode::INVALID_PARAMETER;
    return error;
  }
  param_values.emplace_back((nullable ? nullable.value().c_str() : nullptr));

  // default_expr
  boost::optional<std::string> default_expr =
      column.get_optional<std::string>(Tables::Column::DEFAULT);
  param_values.emplace_back(
      (default_expr ? default_expr.value().c_str() : nullptr));

  // directio
  boost::optional<std::string> direction =
      column.get_optional<std::string>(Tables::Column::DIRECTION);
  param_values.emplace_back((direction ? direction.value().c_str() : nullptr));

  PGresult* res = nullptr;
  error = DbcUtils::exec_prepared(
      connection_, StatementName::COLUMNS_DAO_INSERT_ONE_COLUMN_METADATA,
      param_values, res);

  if (error == ErrorCode::OK) {
    uint64_t number_of_rows_affected = 0;
    ErrorCode error_get =
        DbcUtils::get_number_of_rows_affected(res, number_of_rows_affected);

    if (error_get != ErrorCode::OK) {
      error = error_get;
    } else if (number_of_rows_affected != 1) {
      error = ErrorCode::INVALID_PARAMETER;
    }
  }

  PQclear(res);
  return error;
}

/**
 * @brief  Executes a SELECT statement to get column metadata rows
 *   from the column metadata table,
 *   where the given key equals the given value.
 * @param  (object_key)    [in]  key. column name of a column metadata table.
 * @param  (object_value)  [in]  value to be filtered.
 * @param  (object)        [out] column metadata to get,
 *   where the given key equals the given value.
 * @return  ErrorCode::OK if success, otherwise an error code.
 */
ErrorCode ColumnsDAO::select_column_metadata(
    std::string_view object_key, std::string_view object_value,
    boost::property_tree::ptree& object) const {
  ErrorCode error = ErrorCode::UNKNOWN;
  std::vector<const char*> param_values;

  param_values.emplace_back(object_value.data());

  // Get the name of the SQL statement to be executed.
  std::string statement_name;
  error = DbcUtils::find_statement_name(statement_names_select, object_key,
                                        statement_name);
  if (error != ErrorCode::OK) {
    return error;
  }

  PGresult* res = nullptr;
  error =
      DbcUtils::exec_prepared(connection_, statement_name, param_values, res);

  if (error == ErrorCode::OK) {
    int nrows = PQntuples(res);
    if (nrows <= 0) {
      PQclear(res);
      error = ErrorCode::INVALID_PARAMETER;
      return error;
    }
    for (int ordinal_position = 0; ordinal_position < nrows;
         ordinal_position++) {
      ptree column;
      error = convert_pgresult_to_ptree(res, ordinal_position, column);
      if (error != ErrorCode::OK) {
        break;
      }

      object.push_back(std::make_pair("", column));
    }
  }

  PQclear(res);
  return error;
}

/**
 * @brief Execute DELETE statement to delete column metadata
 *  from the column metadata table
 *  where the given key equals the given value.
 * @param (object_key)    [in]  key. column name of a column metadata table.
 * @param (object_value)  [in]  value to be filtered.
 * @return ErrorCode::OK if success, otherwise an error code.
 */
ErrorCode ColumnsDAO::delete_column_metadata(
    std::string_view object_key, std::string_view object_value) const {
  ErrorCode error = ErrorCode::UNKNOWN;
  std::vector<const char*> param_values;

  param_values.emplace_back(object_value.data());

  // Get the name of the SQL statement to be executed.
  std::string statement_name;
  error = DbcUtils::find_statement_name(statement_names_delete, object_key,
                                        statement_name);
  if (error != ErrorCode::OK) {
    return error;
  }

  PGresult* res = nullptr;
  error =
      DbcUtils::exec_prepared(connection_, statement_name, param_values, res);

  if (error == ErrorCode::OK) {
    uint64_t number_of_rows_affected = 0;
    ErrorCode error_get =
        DbcUtils::get_number_of_rows_affected(res, number_of_rows_affected);

    if (error_get != ErrorCode::OK) {
      error = error_get;
    }
  }

  PQclear(res);
  return error;
}

/* =============================================================================
 * Private method area
 */

/**
 * @brief Gets the ptree type column metadata
 *  converted from the given PGresult type value.
 * @param (res)               [in]  the result of a query.
 * @param (ordinal_position)  [in]  column ordinal position of PGresult.
 * @param (column)            [out] one column metadata.
 * @return ErrorCode::OK if success, otherwise an error code.
 */
ErrorCode ColumnsDAO::convert_pgresult_to_ptree(
    const PGresult* res, const int ordinal_position,
    boost::property_tree::ptree& column) const {
  ErrorCode error = ErrorCode::UNKNOWN;

  // Initialization.
  column.clear();

  // Set the value of the format_version column to ptree.
  column.put(Tables::Column::FORMAT_VERSION,
             PQgetvalue(res, ordinal_position,
                        static_cast<int>(OrdinalPosition::kFormatVersion)));

  // Set the value of the generation column to ptree.
  column.put(Tables::Column::GENERATION,
             PQgetvalue(res, ordinal_position,
                        static_cast<int>(OrdinalPosition::kGeneration)));

  // Set the value of the id to ptree.
  column.put(Tables::Column::ID,
             PQgetvalue(res, ordinal_position,
                        static_cast<int>(OrdinalPosition::kId)));

  // Set the value of the name to ptree.
  column.put(Tables::Column::NAME,
             PQgetvalue(res, ordinal_position,
                        static_cast<int>(OrdinalPosition::kName)));

  // Set the value of the table_id to ptree.
  column.put(Tables::Column::TABLE_ID,
             PQgetvalue(res, ordinal_position,
                        static_cast<int>(OrdinalPosition::kTableId)));

  // Set the value of the ordinal_position to ptree.
  column.put(Tables::Column::ORDINAL_POSITION,
             PQgetvalue(res, ordinal_position,
                        static_cast<int>(OrdinalPosition::kOrdinalPosition)));

  // Set the value of the data_type_id to ptree.
  column.put(Tables::Column::DATA_TYPE_ID,
             PQgetvalue(res, ordinal_position,
                        static_cast<int>(OrdinalPosition::kDataTypeId)));

  // Set the value of the data_length to ptree.
  std::string data_length = std::string(PQgetvalue(
      res, ordinal_position, static_cast<int>(OrdinalPosition::kDataLength)));
  if (!data_length.empty()) {
    ptree p_data_length;
    // Converts a JSON string to a property_tree.
    error = Utilitys::json_to_ptree(data_length, p_data_length);
    if (error != ErrorCode::OK) {
      return error;
    }
    column.add_child(Tables::Column::DATA_LENGTH, p_data_length);
  }

  // Set the value of the varying to ptree.
  std::string varying = DbcUtils::convert_boolean_expression(PQgetvalue(
      res, ordinal_position, static_cast<int>(OrdinalPosition::kVarying)));
  if (!varying.empty()) {
    column.put(Tables::Column::VARYING, varying);
  }

  // Set the value of the nullable to ptree.
  std::string nullable = DbcUtils::convert_boolean_expression(PQgetvalue(
      res, ordinal_position, static_cast<int>(OrdinalPosition::kNullable)));
  if (!nullable.empty()) {
    column.put(Tables::Column::NULLABLE, nullable);
  }

  // Set the value of the default_expr to ptree.
  std::string default_expr = PQgetvalue(
      res, ordinal_position, static_cast<int>(OrdinalPosition::kDefaultExpr));
  if (!default_expr.empty()) {
    column.put(Tables::Column::DEFAULT, default_expr);
  }

  // Set the value of the direction to ptree.
  std::string direction = PQgetvalue(
      res, ordinal_position, static_cast<int>(OrdinalPosition::kDirection));
  if (!direction.empty()) {
    column.put(Tables::Column::DIRECTION, direction);
  }

  error = ErrorCode::OK;
  return error;
}

}  // namespace manager::metadata::db::postgresql
