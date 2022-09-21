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
#include "manager/metadata/dao/postgresql/statistics_dao_pg.h"

#include <libpq-fe.h>

#include <iostream>
#include <sstream>
#include <string>
#include <utility>
#include <vector>

#include <boost/format.hpp>
#include <boost/property_tree/json_parser.hpp>

#include "manager/metadata/common/message.h"
#include "manager/metadata/dao/common/statement_name.h"
#include "manager/metadata/dao/postgresql/columns_dao_pg.h"
#include "manager/metadata/dao/postgresql/common_pg.h"
#include "manager/metadata/dao/postgresql/dbc_utils_pg.h"
#include "manager/metadata/helper/logging_helper.h"
#include "manager/metadata/statistics.h"

// =============================================================================
namespace {

using manager::metadata::db::postgresql::ColumnsDAO;
using manager::metadata::db::postgresql::StatisticsDAO;

std::unordered_map<std::string, std::string> column_names_statistics;
std::unordered_map<std::string, std::string> column_names_columns;
std::unordered_map<std::string, std::string> statement_names_insert;
std::unordered_map<std::string, std::string> statement_names_statistics_select;
std::unordered_map<std::string, std::string> statement_names_statistics_delete;
std::unordered_map<std::string, std::string> statement_names_columns_select;
std::unordered_map<std::string, std::string> statement_names_columns_delete;

namespace statement {

using manager::metadata::db::postgresql::SCHEMA_NAME;

/**
 * @brief Returns an UPSERT statement for one column statistic
 *   based on column id.
 * @param none.
 * @return an UPSERT statement to upsert one column statistic
 *   based on column id.
 */
std::string upsert_column_statistic_by_column_id() {
  // SQL statement
  boost::format query =
      boost::format(
          "INSERT INTO %1%.%2% (%3%, %4%, %5%, %6%, %7%)"
          " VALUES ($1, $2, $3, $4, $5)"
          " ON CONFLICT (%6%)"
          " DO UPDATE SET %3% = $1, %4% = $2, %5% = $3, %7% = $5"
          " RETURNING %8%") %
      SCHEMA_NAME % StatisticsDAO::kTableName %
      StatisticsDAO::ColumnName::kFormatVersion %
      StatisticsDAO::ColumnName::kGeneration %
      StatisticsDAO::ColumnName::kName % StatisticsDAO::ColumnName::kColumnId %
      StatisticsDAO::ColumnName::kColumnStatistic %
      StatisticsDAO::ColumnName::kId;

  return query.str();
}

/**
 * @brief Returns an UPSERT statement for one column statistic
 *   based on table id and column ordinal position.
 * @param (column_name)  [in]  column name of column statistic.
 * @return an UPSERT statement to upsert one column statistic
 *   based on table id and column ordinal position.
 */
std::string upsert_column_statistic_by_column_info(
    std::string_view column_name) {
  // SQL statement
  boost::format query =
      boost::format(
          "INSERT INTO %1%.%2% (%3%, %4%, %5%, %6%, %7%)"
          " VALUES ($3, $4, $5"
          " , (SELECT %9% FROM %1%.%8% WHERE %10%=$1 AND %11%=$2), $6)"
          " ON CONFLICT (%6%)"
          " DO UPDATE SET %3% = $3, %4% = $4, %5% = $5, %7% = $6"
          " RETURNING %12%") %
      SCHEMA_NAME % StatisticsDAO::kTableName %
      StatisticsDAO::ColumnName::kFormatVersion %
      StatisticsDAO::ColumnName::kGeneration %
      StatisticsDAO::ColumnName::kName % StatisticsDAO::ColumnName::kColumnId %
      StatisticsDAO::ColumnName::kColumnStatistic % ColumnsDAO::kTableName %
      ColumnsDAO::ColumnName::kId % ColumnsDAO::ColumnName::kTableId %
      column_name.data() % StatisticsDAO::ColumnName::kId;

  return query.str();
}

/**
 * @brief Returns a SELECT statement for one column statistic
 *   based on id or name or column id .
 * @param (column_name)  [in]  column name of column statistics.
 * @return a SELECT statement to get one column statistic
 *   based on id or name or column id.
 */
std::string select_column_statistic(std::string_view column_name) {
  // SQL statement
  boost::format query =
      boost::format(
          "SELECT sts.%3%, sts.%4%, sts.%5%, sts.%6%, sts.%7%, sts.%8%"
          " , col.%11%, col.%12%, col.%13% column_name"
          " FROM %1%.%2% sts JOIN %1%.%9% col ON (sts.%7% = col.%10%)"
          " WHERE (sts.%14% = $1)") %
      SCHEMA_NAME % StatisticsDAO::kTableName %
      StatisticsDAO::ColumnName::kFormatVersion %
      StatisticsDAO::ColumnName::kGeneration % StatisticsDAO::ColumnName::kId %
      StatisticsDAO::ColumnName::kName % StatisticsDAO::ColumnName::kColumnId %
      StatisticsDAO::ColumnName::kColumnStatistic % ColumnsDAO::kTableName %
      ColumnsDAO::ColumnName::kId % ColumnsDAO::ColumnName::kTableId %
      ColumnsDAO::ColumnName::kOrdinalPosition % ColumnsDAO::ColumnName::kName %
      column_name.data();

  return query.str();
}

/**
 * @brief Returns a SELECT statement for one column statistic
 *   based on table id and column ordinal position or name.
 * @param (column_name)  [in]  column name of metadata-columns.
 * @return a SELECT statement to get one column statistic
 *   based on table id and column ordinal position or name.
 */
std::string select_column_statistic_by_table_id_column_info(
    std::string_view column_name) {
  // SQL statement
  boost::format query =
      boost::format(
          "SELECT sts.%3%, sts.%4%, sts.%5%, sts.%6%, sts.%7%, sts.%8%"
          " , col.%11%, col.%12%, col.%13% column_name"
          " FROM %1%.%2% sts JOIN %1%.%9% col ON (sts.%7% = col.%10%)"
          " WHERE (col.%11% = $1) AND (col.%14% = $2)") %
      SCHEMA_NAME % StatisticsDAO::kTableName %
      StatisticsDAO::ColumnName::kFormatVersion %
      StatisticsDAO::ColumnName::kGeneration % StatisticsDAO::ColumnName::kId %
      StatisticsDAO::ColumnName::kName % StatisticsDAO::ColumnName::kColumnId %
      StatisticsDAO::ColumnName::kColumnStatistic % ColumnsDAO::kTableName %
      ColumnsDAO::ColumnName::kId % ColumnsDAO::ColumnName::kTableId %
      ColumnsDAO::ColumnName::kOrdinalPosition % ColumnsDAO::ColumnName::kName %
      column_name.data();

  return query.str();
}

/**
 * @brief Returns a SELECT statement for all column statistics.
 * @param none.
 * @return a SELECT statement to get all column statistics.
 */
std::string select_all_column_statistics() {
  // SQL statement
  boost::format query =
      boost::format(
          "SELECT sts.%3%, sts.%4%, sts.%5%, sts.%6%, sts.%7%, sts.%8%"
          " , col.%11%, col.%12%, col.%13% column_name"
          " FROM %1%.%2% sts JOIN %1%.%9% col ON (sts.%7% = col.%10%)"
          " ORDER BY %11%, %12%") %
      SCHEMA_NAME % StatisticsDAO::kTableName %
      StatisticsDAO::ColumnName::kFormatVersion %
      StatisticsDAO::ColumnName::kGeneration % StatisticsDAO::ColumnName::kId %
      StatisticsDAO::ColumnName::kName % StatisticsDAO::ColumnName::kColumnId %
      StatisticsDAO::ColumnName::kColumnStatistic % ColumnsDAO::kTableName %
      ColumnsDAO::ColumnName::kId % ColumnsDAO::ColumnName::kTableId %
      ColumnsDAO::ColumnName::kOrdinalPosition % ColumnsDAO::ColumnName::kName;

  return query.str();
}

/**
 * @brief Returns a SELECT statement for all column statistics
 *   based on table id.
 * @param none.
 * @return a SELECT statement to get all column statistics
 *   based on table id.
 */
std::string select_all_column_statistics_by_table_id() {
  // SQL statement
  boost::format query =
      boost::format(
          "SELECT sts.%3%, sts.%4%, sts.%5%, sts.%6%, sts.%7%, sts.%8%"
          " , col.%11%, col.%12%, col.%13% column_name"
          " FROM %1%.%2% sts JOIN %1%.%9% col ON (sts.%7% = col.%10%)"
          " WHERE col.%11% = $1"
          " ORDER BY %12%") %
      SCHEMA_NAME % StatisticsDAO::kTableName %
      StatisticsDAO::ColumnName::kFormatVersion %
      StatisticsDAO::ColumnName::kGeneration % StatisticsDAO::ColumnName::kId %
      StatisticsDAO::ColumnName::kName % StatisticsDAO::ColumnName::kColumnId %
      StatisticsDAO::ColumnName::kColumnStatistic % ColumnsDAO::kTableName %
      ColumnsDAO::ColumnName::kId % ColumnsDAO::ColumnName::kTableId %
      ColumnsDAO::ColumnName::kOrdinalPosition % ColumnsDAO::ColumnName::kName;

  return query.str();
}

/**
 * @brief Returns a DELETE statement for all column statistics
 *   based on id or name or column id.
 * @param (column_name)  [in]  column name of column statistics.
 * @return a DELETE statement to delete all column statistics
 *   based on id or name or column id.
 */
std::string delete_column_statistic(std::string_view column_name) {
  // SQL statement
  boost::format query =
      boost::format("DELETE FROM %1%.%2% WHERE %3% = $1 RETURNING %4%") %
      SCHEMA_NAME % StatisticsDAO::kTableName % column_name %
      StatisticsDAO::ColumnName::kId;

  return query.str();
}

/**
 * @brief Returns a DELETE statement for all column statistics
 *   based on table id.
 * @param none.
 * @return a DELETE statement to delete all column statistics
 *   based on table id.
 */
std::string delete_column_statistic_by_table_id() {
  // SQL statement
  boost::format query =
      boost::format(
          "DELETE FROM %1%.%2% sts USING %1%.%3% col"
          " WHERE (sts.%4% = col.%5%) AND (col.%6% = $1)") %
      SCHEMA_NAME % StatisticsDAO::kTableName % ColumnsDAO::kTableName %
      StatisticsDAO::ColumnName::kColumnId % ColumnsDAO::ColumnName::kId %
      ColumnsDAO::ColumnName::kTableId;

  return query.str();
}

/**
 * @brief Returns a DELETE statement for one column statistic
 *   based on table id and column ordinal position or name.
 * @param (column_name)  [in]  column name of metadata-columns.
 * @return a DELETE statement to delete all column statistics
 *   based on table id and column ordinal position or name.
 */
std::string delete_column_statistic_by_table_id_column_info(
    std::string_view column_name) {
  // SQL statement
  boost::format query =
      boost::format(
          "DELETE FROM %1%.%2% sts USING %1%.%3% col"
          " WHERE (sts.%4% = col.%5%) AND (col.%6% = $1) AND (col.%7% = $2)"
          " RETURNING sts.%8%") %
      SCHEMA_NAME % StatisticsDAO::kTableName % ColumnsDAO::kTableName %
      StatisticsDAO::ColumnName::kColumnId % ColumnsDAO::ColumnName::kId %
      ColumnsDAO::ColumnName::kTableId % column_name.data() %
      StatisticsDAO::ColumnName::kId;

  return query.str();
}

}  // namespace statement
}  // namespace

// =============================================================================
namespace manager::metadata::db::postgresql {

namespace json_parser = boost::property_tree::json_parser;
using boost::property_tree::json_parser_error;
using boost::property_tree::ptree;
using manager::metadata::ErrorCode;
using manager::metadata::db::StatementName;

/**
 * @brief Constructor
 * @param (connection)  [in]  a connection to the metadata repository.
 * @return none.
 */
StatisticsDAO::StatisticsDAO(DBSessionManager* session_manager)
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
  column_names_statistics.emplace(Statistics::ID, ColumnName::kId);
  column_names_statistics.emplace(Statistics::NAME, ColumnName::kName);
  column_names_statistics.emplace(Statistics::COLUMN_ID, ColumnName::kColumnId);

  // Creates a list of unique name
  // for the new prepared statement for each column names.
  for (auto column : column_names_statistics) {
    // Creates unique name for the new prepared statement.
    boost::format statement_name_select =
        boost::format("%1%-%2%-%3%") %
        static_cast<int>(
            StatementName::STATISTICS_DAO_SELECT_COLUMN_STATISTIC) %
        kTableName % column.first;
    boost::format statement_name_delete =
        boost::format("%1%-%2%-%3%") %
        static_cast<int>(
            StatementName::STATISTICS_DAO_DELETE_COLUMN_STATISTIC) %
        kTableName % column.first;

    // Added this list to unique name for the new prepared statement.
    // key : column name
    // value : unique name for the new prepared statement.
    statement_names_statistics_select.emplace(column.first,
                                              statement_name_select.str());
    statement_names_statistics_delete.emplace(column.first,
                                              statement_name_delete.str());
  }

  column_names_columns.emplace(Statistics::COLUMN_NAME,
                               ColumnsDAO::ColumnName::kName);
  column_names_columns.emplace(Statistics::ORDINAL_POSITION,
                               ColumnsDAO::ColumnName::kOrdinalPosition);
  for (auto column : column_names_columns) {
    // Creates unique name for the new prepared statement.
    boost::format statement_name_insert =
        boost::format("%1%-%2%-%3%") %
        static_cast<int>(
            StatementName::
                STATISTICS_DAO_UPSERT_COLUMN_STATISTIC_BY_COLUMN_INFO) %
        ColumnsDAO::kTableName % column.first;
    boost::format statement_name_select =
        boost::format("%1%-%2%-%3%") %
        static_cast<int>(
            StatementName::STATISTICS_DAO_SELECT_COLUMN_STATISTIC) %
        ColumnsDAO::kTableName % column.first;
    boost::format statement_name_delete =
        boost::format("%1%-%2%-%3%") %
        static_cast<int>(
            StatementName::STATISTICS_DAO_DELETE_COLUMN_STATISTIC) %
        ColumnsDAO::kTableName % column.first;

    // Added this list to unique name for the new prepared statement.
    // key : column name
    // value : unique name for the new prepared statement.
    statement_names_insert.emplace(column.first, statement_name_insert.str());
    statement_names_columns_select.emplace(column.first,
                                           statement_name_select.str());
    statement_names_columns_delete.emplace(column.first,
                                           statement_name_delete.str());
  }
}

/**
 * @brief Defines all prepared statements.
 * @param none.
 * @return ErrorCode::OK if success, otherwise an error code.
 */
ErrorCode StatisticsDAO::prepare() const {
  ErrorCode error = ErrorCode::UNKNOWN;

  error = DbcUtils::prepare(
      connection_,
      StatementName::STATISTICS_DAO_UPSERT_COLUMN_STATISTIC_BY_COLUMN_ID,
      statement::upsert_column_statistic_by_column_id());
  if (error != ErrorCode::OK) {
    return error;
  }

  error = DbcUtils::prepare(
      connection_, StatementName::STATISTICS_DAO_SELECT_COLUMN_STATISTIC_ALL,
      statement::select_all_column_statistics());
  if (error != ErrorCode::OK) {
    return error;
  }

  error = DbcUtils::prepare(
      connection_,
      StatementName::STATISTICS_DAO_SELECT_COLUMN_STATISTIC_ALL_BY_TABLE_ID,
      statement::select_all_column_statistics_by_table_id());
  if (error != ErrorCode::OK) {
    return error;
  }

  error = DbcUtils::prepare(
      connection_,
      StatementName::STATISTICS_DAO_DELETE_COLUMN_STATISTIC_BY_TABLE_ID,
      statement::delete_column_statistic_by_table_id());
  if (error != ErrorCode::OK) {
    return error;
  }

  // Setting column names for column-statistics table.
  for (auto column : column_names_statistics) {
    // select statement.
    error = DbcUtils::prepare(
        connection_, statement_names_statistics_select.at(column.first),
        statement::select_column_statistic(column.second));
    if (error != ErrorCode::OK) {
      return error;
    }

    // delete statement.
    error = DbcUtils::prepare(
        connection_, statement_names_statistics_delete.at(column.first),
        statement::delete_column_statistic(column.second));
    if (error != ErrorCode::OK) {
      return error;
    }
  }

  // Setting column names for columns table.
  for (auto column : column_names_columns) {
    // insert statement.
    error = DbcUtils::prepare(
        connection_, statement_names_insert.at(column.first),
        statement::upsert_column_statistic_by_column_info(column.second));
    if (error != ErrorCode::OK) {
      return error;
    }

    // select statement.
    error = DbcUtils::prepare(
        connection_, statement_names_columns_select.at(column.first),
        statement::select_column_statistic_by_table_id_column_info(
            column.second));
    if (error != ErrorCode::OK) {
      return error;
    }

    // delete statement.
    error = DbcUtils::prepare(
        connection_, statement_names_columns_delete.at(column.first),
        statement::delete_column_statistic_by_table_id_column_info(
            column.second));
    if (error != ErrorCode::OK) {
      return error;
    }
  }

  return error;
}

/**
 * @brief Executes UPSERT statement to upsert one column statistic
 *   into the column statistics table based on the given column id.
 *   Executes a INSERT statement it if it not exists in the metadata
 *   repository, Executes a UPDATE statement it if it already exists.
 * @param (column_id)         [in]  column id.
 * @param (column_name)       [in]  column name to add or update.
 * @param (column_statistic)  [in]  one column statistic to add or update.
 * @param (statistic_id)      [out] ID of the added column statistic.
 * @return ErrorCode::OK if success, otherwise an error code.
 */
ErrorCode StatisticsDAO::upsert_column_statistic(
    const ObjectIdType column_id, const std::string* column_name,
    const boost::property_tree::ptree& column_statistic,
    ObjectIdType& statistic_id) const {
  ErrorCode error = ErrorCode::UNKNOWN;
  std::vector<char const*> param_values;

  // format_version
  std::string s_format_version = std::to_string(Statistics::format_version());
  param_values.emplace_back(s_format_version.c_str());

  // generation
  std::string s_generation = std::to_string(Statistics::generation());
  param_values.emplace_back(s_generation.c_str());

  // name
  param_values.emplace_back((column_name ? (*column_name).c_str() : nullptr));

  // column_id
  std::string s_column_id = std::to_string(column_id);
  param_values.emplace_back(s_column_id.c_str());

  // column_statistic
  std::string s_column_statistic;
  if (!column_statistic.empty()) {
    std::stringstream ss;
    try {
      json_parser::write_json(ss, column_statistic, false);
    } catch (json_parser_error& e) {
      LOG_ERROR << Message::WRITE_JSON_FAILURE << e.what();
      error = ErrorCode::INVALID_PARAMETER;
      return error;
    } catch (...) {
      LOG_ERROR << Message::WRITE_JSON_FAILURE;
      error = ErrorCode::INVALID_PARAMETER;
      return error;
    }
    s_column_statistic = ss.str();
  }
  param_values.emplace_back(
      (!s_column_statistic.empty() ? s_column_statistic.c_str() : nullptr));

  PGresult* res = nullptr;
  error = DbcUtils::exec_prepared(
      connection_,
      StatementName::STATISTICS_DAO_UPSERT_COLUMN_STATISTIC_BY_COLUMN_ID,
      param_values, res);

  if (error == ErrorCode::OK) {
    int nrows = PQntuples(res);
    if (nrows == 1) {
      int ordinal_position = 0;
      error = DbcUtils::str_to_integral<ObjectIdType>(
          PQgetvalue(res, ordinal_position, 0), statistic_id);
    } else {
      error = ErrorCode::INVALID_PARAMETER;
    }
  }

  PQclear(res);
  return error;
}

/**
 * @brief Executes UPSERT statement to upsert one column statistic
 *   into the column statistics table
 *   based on the given table id and the given column name or ordinal position.
 *   Executes a INSERT statement it if it not exists in the metadata
 *   repository, Executes a UPDATE statement it if it already exists.
 * @param (table_id)          [in]  table id.
 * @param (object_key)        [in]  key. column name of a
 *   column statistics table.
 * @param (object_value)      [in]  value to be filtered.
 * @param (column_name)       [in]  column name to add or update.
 * @param (column_statistic)  [in]  one column statistic to add or update.
 * @param (statistic_id)      [out] ID of the added column statistic.
 * @return ErrorCode::OK if success, otherwise an error code.
 */
ErrorCode StatisticsDAO::upsert_column_statistic(
    const ObjectIdType table_id, std::string_view object_key,
    std::string_view object_value, const std::string* column_name,
    const boost::property_tree::ptree& column_statistic,
    ObjectIdType& statistic_id) const {
  ErrorCode error = ErrorCode::UNKNOWN;
  std::vector<char const*> param_values;

  std::string s_table_id = std::to_string(table_id);

  // table_id
  param_values.emplace_back(s_table_id.c_str());

  // name or ordinal position
  param_values.emplace_back(object_value.data());

  // format_version
  std::string s_format_version = std::to_string(Statistics::format_version());
  param_values.emplace_back(s_format_version.c_str());

  // generation
  std::string s_generation = std::to_string(Statistics::generation());
  param_values.emplace_back(s_generation.c_str());

  // name
  param_values.emplace_back((column_name ? (*column_name).c_str() : nullptr));

  // column_statistic
  std::string s_column_statistic;
  if (!column_statistic.empty()) {
    std::stringstream ss;
    try {
      json_parser::write_json(ss, column_statistic, false);
    } catch (json_parser_error& e) {
      LOG_ERROR << Message::WRITE_JSON_FAILURE << e.what();
      error = ErrorCode::INVALID_PARAMETER;
      return error;
    } catch (...) {
      LOG_ERROR << Message::WRITE_JSON_FAILURE;
      error = ErrorCode::INVALID_PARAMETER;
      return error;
    }
    s_column_statistic = ss.str();
  }
  param_values.emplace_back(
      (!s_column_statistic.empty() ? s_column_statistic.c_str() : nullptr));

  // Get the name of the SQL statement to be executed.
  std::string statement_name;
  error = DbcUtils::find_statement_name(statement_names_insert, object_key,
                                        statement_name);
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
      error = DbcUtils::str_to_integral<ObjectIdType>(
          PQgetvalue(res, ordinal_position, 0), statistic_id);
    } else {
      error = ErrorCode::INVALID_PARAMETER;
    }
  }

  PQclear(res);
  return error;
}

/**
 * @brief Executes a SELECT statement to get column statistic rows
 *   from the statistic table, where the given key equals the given value.
 * @param (object_key)    [in]  key. column name of a column statistic table.
 * @param (object_value)  [in]  value to be filtered.
 * @param (object)        [out] table metadata to get,
 *   where the given key equals the given value.
 * @retval ErrorCode::OK if success.
 * @retval ErrorCode::ID_NOT_FOUND if the statistic id or column id
 *   does not exist.
 * @retval ErrorCode::NAME_NOT_FOUND if the statistic name does not exist.
 * @retval otherwise an error code.
 */
ErrorCode StatisticsDAO::select_column_statistic(
    std::string_view object_key, std::string_view object_value,
    boost::property_tree::ptree& object) const {
  ErrorCode error = ErrorCode::UNKNOWN;
  std::vector<const char*> param_values;

  param_values.emplace_back(object_value.data());

  // Get the name of the SQL statement to be executed.
  std::string statement_name;
  error = DbcUtils::find_statement_name(statement_names_statistics_select,
                                        object_key, statement_name);
  if (error != ErrorCode::OK) {
    return error;
  }

  std::vector<ptree> container;
  error = get_column_statistics_rows(statement_name, param_values, container);

  if (error == ErrorCode::OK) {
    if (container.size() == 1) {
      object = container[0];
    } else if (container.size() == 0) {
      // Convert the error code.
      if (object_key == Statistics::ID) {
        error = ErrorCode::ID_NOT_FOUND;
      } else if (object_key == Statistics::NAME) {
        error = ErrorCode::NAME_NOT_FOUND;
      } else if (object_key == Statistics::COLUMN_ID) {
        error = ErrorCode::ID_NOT_FOUND;
      } else {
        error = ErrorCode::NOT_FOUND;
      }
    } else {
      error = ErrorCode::INVALID_PARAMETER;
    }
  }

  return error;
}

/**
 * @brief Execute a SELECT statement to get all column statistics rows
 *   from the column statistics table.
 *   If the column statistic does not exist, return the container as empty.
 * @param (container)  [out] all column statistics.
 * @return ErrorCode::OK if success, otherwise an error code.
 */
ErrorCode StatisticsDAO::select_column_statistic(
    std::vector<boost::property_tree::ptree>& container) const {
  ErrorCode error = ErrorCode::UNKNOWN;
  std::vector<const char*> param_values;

  std::string statement_name = std::to_string(static_cast<int>(
      StatementName::STATISTICS_DAO_SELECT_COLUMN_STATISTIC_ALL));

  error = get_column_statistics_rows(statement_name, param_values, container);

  return error;
}

/**
 * @brief Execute a SELECT statement to get all column statistics rows
 *   from the column statistics table based on the given table ID.
 * @param (table_id)   [in]  table id.
 * @param (container)  [out] all column statistics.
 * @retval ErrorCode::OK if success.
 * @retval ErrorCode::ID_NOT_FOUND if the table id does not exist.
 * @retval otherwise an error code.
 */
ErrorCode StatisticsDAO::select_column_statistic(
    const ObjectIdType table_id,
    std::vector<boost::property_tree::ptree>& container) const {
  ErrorCode error = ErrorCode::UNKNOWN;
  std::vector<const char*> param_values;
  std::string s_table_id;

  std::string statement_name = std::to_string(static_cast<int>(
      StatementName::STATISTICS_DAO_SELECT_COLUMN_STATISTIC_ALL_BY_TABLE_ID));

  s_table_id = std::to_string(table_id);
  param_values.emplace_back(s_table_id.c_str());

  error = get_column_statistics_rows(statement_name, param_values, container);

  if (error == ErrorCode::OK) {
    if (container.size() == 0) {
      // Convert the error code.
      error = ErrorCode::ID_NOT_FOUND;
    }
  }

  return error;
}

/**
 * @brief Executes a SELECT statement to get one column statistic row
 *   from the column statistics table
 *   based on the given table id and the given column ordinal position.
 * @param (table_id)          [in]  table id.
 * @param (object_key)        [in]  key. column name of a
 *   column statistic table.
 * @param (object_value)      [in]  value to be filtered.
 * @param (column_statistic)  [out] one column statistic
 *   with the specified table id and column ordinal position.
 * @retval ErrorCode::OK if success.
 * @retval ErrorCode::ID_NOT_FOUND if the ordinal position does not exist.
 * @retval ErrorCode::NAME_NOT_FOUND if the statistic name does not exist.
 * @retval otherwise an error code.
 */
ErrorCode StatisticsDAO::select_column_statistic(
    const ObjectIdType table_id, std::string_view object_key,
    std::string_view object_value, boost::property_tree::ptree& object) const {
  ErrorCode error = ErrorCode::UNKNOWN;
  std::vector<const char*> param_values;

  std::string s_table_id = std::to_string(table_id);

  param_values.emplace_back(s_table_id.c_str());
  param_values.emplace_back(object_value.data());

  // Get the name of the SQL statement to be executed.
  std::string statement_name;
  error = DbcUtils::find_statement_name(statement_names_columns_select,
                                        object_key, statement_name);
  if (error != ErrorCode::OK) {
    return error;
  }

  std::vector<ptree> container;
  error = get_column_statistics_rows(statement_name, param_values, container);

  if (error == ErrorCode::OK) {
    if (container.size() == 1) {
      object = container[0];
    } else if (container.size() == 0) {
      // Convert the error code.
      if (object_key == Statistics::ORDINAL_POSITION) {
        error = ErrorCode::ID_NOT_FOUND;
      } else if (object_key == Statistics::COLUMN_NAME) {
        error = ErrorCode::NAME_NOT_FOUND;
      } else {
        error = ErrorCode::NOT_FOUND;
      }
    } else {
      error = ErrorCode::INVALID_PARAMETER;
    }
  }
  return error;
}

/**
 * @brief Executes DELETE statement to delete one column statistics
 *   from the column statistics table based on the given statistic id or name.
 * @param (object_key)    [in]  key. column name of a column statistic table.
 * @param (object_value)  [in]  value to be filtered.
 * @param (statistic_id)  [out] statistic id of the row deleted.
 * @retval ErrorCode::OK if success.
 * @retval ErrorCode::ID_NOT_FOUND if the statistic id does not exist.
 * @retval ErrorCode::NAME_NOT_FOUND if the statistic name does not exist.
 * @retval otherwise an error code.
 */
ErrorCode StatisticsDAO::delete_column_statistic(
    std::string_view object_key, std::string_view object_value,
    ObjectIdType& statistic_id) const {
  ErrorCode error = ErrorCode::UNKNOWN;
  std::vector<const char*> param_values;

  param_values.emplace_back(object_value.data());

  // Get the name of the SQL statement to be executed.
  std::string statement_name;
  error = DbcUtils::find_statement_name(statement_names_statistics_delete,
                                        object_key, statement_name);
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
    } else if (number_of_rows_affected == 1) {
      int ordinal_position = 0;
      error = DbcUtils::str_to_integral<ObjectIdType>(
          PQgetvalue(res, ordinal_position, 0), statistic_id);
    } else if (number_of_rows_affected == 0) {
      // Convert the error code.
      if (object_key == Statistics::ID) {
        error = ErrorCode::ID_NOT_FOUND;
      } else if (object_key == Statistics::NAME) {
        error = ErrorCode::NAME_NOT_FOUND;
      } else if (object_key == Statistics::COLUMN_ID) {
        error = ErrorCode::ID_NOT_FOUND;
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

/**
 * @brief Executes DELETE statement to delete all column statistics
 *   from the column statistics table based on the given table id.
 * @param (table_id)  [in]  table id.
 * @retval ErrorCode::OK if success.
 * @retval ErrorCode::ID_NOT_FOUND if the table id does not exist.
 * @retval otherwise an error code.
 */
ErrorCode StatisticsDAO::delete_column_statistic(
    const ObjectIdType table_id) const {
  ErrorCode error = ErrorCode::UNKNOWN;
  std::vector<const char*> param_values;

  std::string s_table_id = std::to_string(table_id);

  param_values.emplace_back(s_table_id.c_str());

  PGresult* res = nullptr;
  error = DbcUtils::exec_prepared(
      connection_,
      StatementName::STATISTICS_DAO_DELETE_COLUMN_STATISTIC_BY_TABLE_ID,
      param_values, res);

  if (error == ErrorCode::OK) {
    uint64_t number_of_rows_affected = 0;
    ErrorCode error_get =
        DbcUtils::get_number_of_rows_affected(res, number_of_rows_affected);

    if (error_get != ErrorCode::OK) {
      error = error_get;
    } else if (number_of_rows_affected == 0) {
      // Convert the error code.
      error = ErrorCode::ID_NOT_FOUND;
    }
  }

  PQclear(res);
  return error;
}

/**
 * @brief Executes DELETE statement to delete all column statistics
 *   from the column statistics table
 *   based on the given table id and column data.
 * @param (table_id)      [in]  table id.
 * @param (object_key)    [in]  key. column name of a column statistic table.
 * @param (object_value)  [in]  value to be filtered.
 * @param (statistic_id)  [out] statistic id of the row deleted.
 * @retval ErrorCode::OK if success.
 * @retval ErrorCode::ID_NOT_FOUND if the ordinal position does not exist.
 * @retval ErrorCode::NAME_NOT_FOUND if the statistic name does not exist.
 * @retval otherwise an error code.
 */
ErrorCode StatisticsDAO::delete_column_statistic(
    const ObjectIdType table_id, std::string_view object_key,
    std::string_view object_value, ObjectIdType& statistic_id) const {
  ErrorCode error = ErrorCode::UNKNOWN;
  std::vector<const char*> param_values;

  std::string s_table_id = std::to_string(table_id);

  param_values.emplace_back(s_table_id.c_str());
  param_values.emplace_back(object_value.data());

  // Get the name of the SQL statement to be executed.
  std::string statement_name;
  error = DbcUtils::find_statement_name(statement_names_columns_delete,
                                        object_key, statement_name);
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
    } else if (number_of_rows_affected == 1) {
      int ordinal_position = 0;
      error = DbcUtils::str_to_integral<ObjectIdType>(
          PQgetvalue(res, ordinal_position, 0), statistic_id);
    } else if (number_of_rows_affected == 0) {
      // Convert the error code.
      if (object_key == Statistics::ORDINAL_POSITION) {
        error = ErrorCode::ID_NOT_FOUND;
      } else if (object_key == Statistics::COLUMN_NAME) {
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

/* =============================================================================
 * Private method area
 */

/**
 * @brief Execute a SELECT statement to get column statistics rows
 *   from the column statistics table.
 * @param (statement_name)  [in]  statement name.
 * @param (param_values)    [in]  Parameters of the statement.
 * @param (container)       [out] all column statistics.
 * @return ErrorCode::OK if success, otherwise an error code.
 */
ErrorCode StatisticsDAO::get_column_statistics_rows(
    std::string_view statement_name,
    const std::vector<const char*>& param_values,
    std::vector<boost::property_tree::ptree>& container) const {
  ErrorCode error = ErrorCode::UNKNOWN;

  PGresult* res = nullptr;
  error =
      DbcUtils::exec_prepared(connection_, statement_name, param_values, res);
  if (error == ErrorCode::OK) {
    int nrows = PQntuples(res);

    if (nrows >= 0) {
      for (int ordinal_position = 0; ordinal_position < nrows;
           ordinal_position++) {
        ptree table;
        ErrorCode error_internal =
            convert_pgresult_to_ptree(res, ordinal_position, table);
        if (error_internal != ErrorCode::OK) {
          error = error_internal;
          break;
        }
        container.emplace_back(table);
      }
    } else {
      error = ErrorCode::INVALID_PARAMETER;
    }
  }

  PQclear(res);
  return error;
}

/**
 * @brief Gets the ptree type column statistics
 *   converted from the given PGresult type value.
 * @param (res)               [in]  the result of a query.
 * @param (ordinal_position)  [in]  column ordinal position of PGresult.
 * @param (statistic)         [out] one column statistic.
 * @return ErrorCode::OK if success, otherwise an error code.
 */
ErrorCode StatisticsDAO::convert_pgresult_to_ptree(
    const PGresult* res, const int ordinal_position,
    boost::property_tree::ptree& statistic) const {
  ErrorCode error = ErrorCode::UNKNOWN;

  // Initialization.
  statistic.clear();

  // Set the value of the format_version column to ptree.
  statistic.put(Statistics::FORMAT_VERSION,
                PQgetvalue(res, ordinal_position,
                           static_cast<int>(OrdinalPosition::kFormatVersion)));

  // Set the value of the generation column to ptree.
  statistic.put(Statistics::GENERATION,
                PQgetvalue(res, ordinal_position,
                           static_cast<int>(OrdinalPosition::kGeneration)));

  // Set the value of the id column to ptree.
  statistic.put(Statistics::ID,
                PQgetvalue(res, ordinal_position,
                           static_cast<int>(OrdinalPosition::kId)));

  // Set the value of the name column to ptree.
  statistic.put(Statistics::NAME,
                PQgetvalue(res, ordinal_position,
                           static_cast<int>(OrdinalPosition::kName)));

  // Set the value of the table id column to ptree.
  statistic.put(Statistics::TABLE_ID,
                PQgetvalue(res, ordinal_position,
                           static_cast<int>(OrdinalPosition::kTableId)));

  // Set the value of the ordinal position column to ptree.
  statistic.put(
      Statistics::ORDINAL_POSITION,
      PQgetvalue(res, ordinal_position,
                 static_cast<int>(OrdinalPosition::kOrdinalPosition)));

  // Set the value of the column id column to ptree.
  statistic.put(Statistics::COLUMN_ID,
                PQgetvalue(res, ordinal_position,
                           static_cast<int>(OrdinalPosition::kColumnId)));

  // Set the value of the column name column to ptree.
  statistic.put(Statistics::COLUMN_NAME,
                PQgetvalue(res, ordinal_position,
                           static_cast<int>(OrdinalPosition::kColumnName)));

  // Set the value of the column statistic column column to ptree.
  ptree column_statistic;
  std::string s_column_statistic =
      PQgetvalue(res, ordinal_position,
                 static_cast<int>(OrdinalPosition::kColumnStatistic));
  if (!s_column_statistic.empty()) {
    std::stringstream ss;
    ss << s_column_statistic;
    try {
      json_parser::read_json(ss, column_statistic);
    } catch (json_parser_error& e) {
      LOG_ERROR << Message::READ_JSON_FAILURE << e.what();
      error = ErrorCode::INTERNAL_ERROR;
      return error;
    } catch (...) {
      LOG_ERROR << Message::READ_JSON_FAILURE;
      error = ErrorCode::INTERNAL_ERROR;
      return error;
    }
  }
  // NOTICE:
  //   If it is not set, MUST add an empty ptree.
  //   ogawayama-server read key Statistics::COLUMN_STATISTIC.
  statistic.add_child(Statistics::COLUMN_STATISTIC, column_statistic);

  error = ErrorCode::OK;
  return error;
}

}  // namespace manager::metadata::db::postgresql
