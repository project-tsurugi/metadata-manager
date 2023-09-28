/*
 * Copyright 2020-2023 tsurugi project.
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

#include <boost/format.hpp>

#include "manager/metadata/common/message.h"
#include "manager/metadata/common/utility.h"
#include "manager/metadata/dao/postgresql/columns_dao_pg.h"
#include "manager/metadata/helper/logging_helper.h"
#include "manager/metadata/helper/ptree_helper.h"

// =============================================================================
namespace manager::metadata::db {

using boost::property_tree::ptree;

ErrorCode StatisticsDaoPg::insert(const boost::property_tree::ptree& object,
                                  ObjectId& object_id) const {
  ErrorCode error = ErrorCode::UNKNOWN;
  std::vector<const char*> params;

  // format_version
  std::string s_format_version(std::to_string(Statistics::format_version()));
  params.emplace_back(s_format_version.c_str());

  // generation
  std::string s_generation(std::to_string(Statistics::generation()));
  params.emplace_back(s_generation.c_str());

  // name
  auto statistic_name = ptree_helper::ptree_value_to_string<std::string>(
      object, Statistics::NAME);
  params.emplace_back(statistic_name.c_str());

  // table_id
  auto table_id = ptree_helper::ptree_value_to_string<std::string>(
      object, Statistics::TABLE_ID);
  // column_id
  auto column_id = ptree_helper::ptree_value_to_string<std::string>(
      object, Statistics::COLUMN_ID);
  // column_number
  auto column_number = ptree_helper::ptree_value_to_string<std::int64_t>(
      object, Statistics::COLUMN_NUMBER);
  // column_name
  auto column_name = ptree_helper::ptree_value_to_string<std::string>(
      object, Statistics::COLUMN_NAME);

  if (!column_id.empty()) {
    // Insert using column id.
    params.emplace_back(column_id.c_str());
  } else {
    // table_id
    params.emplace_back(table_id.c_str());

    if (!column_number.empty()) {
      // Insert using ordinal position.
      params.emplace_back(column_number.c_str());
    } else {
      // Insert using column name.
      params.emplace_back(column_name.c_str());
    }
  }

  ptree p_statistic;
  // column_statistic
  auto opt_statistic = object.get_child_optional(Statistics::COLUMN_STATISTIC);
  if (opt_statistic) {
    p_statistic = opt_statistic.value();
  }

  std::string s_statistic;
  ptree_helper::ptree_to_json(p_statistic, s_statistic);
  params.emplace_back((!s_statistic.empty() ? s_statistic.c_str() : nullptr));

  std::string statement_key;
  if (!column_id.empty()) {
    // Use the default INSERT statement.
    statement_key = Statement::kDefaultKey;
  } else if (!column_number.empty()) {
    // Use the INSERT statement with column number specification.
    statement_key = Statistics::COLUMN_NUMBER;
  } else {
    // Use the INSERT statement with column name specification.
    statement_key = Statistics::COLUMN_NAME;
  }

  // Set INSERT statement.
  InsertStatement statement;
  try {
    statement = insert_statements_.at(statement_key);
  } catch (...) {
    LOG_ERROR << Message::INVALID_STATEMENT_KEY << statement_key;
    return ErrorCode::INVALID_PARAMETER;
  }

  PGresult* res = nullptr;
  // Execute a prepared statement.
  error = DbcUtils::execute_statement(pg_conn_, statement.name(), params, res);

  if (error == ErrorCode::OK) {
    int nrows = PQntuples(res);
    if (nrows == 1) {
      // Obtain the object ID of the added metadata object.
      std::string result_value = PQgetvalue(res, kFirstRow, kFirstColumn);
      error = Utility::str_to_numeric(result_value, object_id);
    } else {
      error = ErrorCode::INVALID_PARAMETER;
    }
  }
  PQclear(res);

  return error;
}

ErrorCode StatisticsDaoPg::select(
    const std::map<std::string_view, std::string_view>& keys,
    boost::property_tree::ptree& object) const {
  ErrorCode error = ErrorCode::UNKNOWN;
  std::string statement_key;
  std::vector<const char*> params;

  if (keys.empty()) {
    statement_key = Statement::kDefaultKey;
    // If no search key is specified, all are returned.
    params.clear();
    error = ErrorCode::OK;
  } else {
    // Sets the specified key and key value.
    error = this->set_key_params(keys, statement_key, params);
  }
  if (error != ErrorCode::OK) {
    return error;
  }

  // Set SELECT statement.
  SelectStatement statement;
  try {
    statement = select_statements_.at(statement_key);
  } catch (...) {
    LOG_ERROR << Message::INVALID_STATEMENT_KEY << statement_key;
    return ErrorCode::INVALID_PARAMETER;
  }

  PGresult* res = nullptr;
  // Execute a prepared statement.
  error = DbcUtils::execute_statement(pg_conn_, statement.name(), params, res);

  if (error == ErrorCode::OK) {
    int nrows = PQntuples(res);
    if (nrows >= 0) {
      object.clear();

      for (int row_number = 0; row_number < nrows; row_number++) {
        // Convert acquired data to ptree type.
        object.push_back(
            std::make_pair("", convert_pgresult_to_ptree(res, row_number)));
      }
    } else {
      error = ErrorCode::INVALID_PARAMETER;
    }
  }
  PQclear(res);

  return error;
}

ErrorCode StatisticsDaoPg::remove(
    const std::map<std::string_view, std::string_view>& keys,
    std::vector<ObjectId>& object_ids) const {
  ErrorCode error = ErrorCode::UNKNOWN;
  std::string statement_key;
  std::vector<const char*> params;

  if (keys.empty()) {
    error = ErrorCode::NOT_SUPPORTED;
    return error;
  }

  // Sets the specified key and key value.
  error = this->set_key_params(keys, statement_key, params);
  if (error != ErrorCode::OK) {
    return error;
  }

  // Set DELETE statement.
  DeleteStatement statement;
  try {
    statement = delete_statements_.at(statement_key.data());
  } catch (...) {
    LOG_ERROR << Message::INVALID_STATEMENT_KEY << statement_key;
    return ErrorCode::INVALID_PARAMETER;
  }

  PGresult* res = nullptr;
  // Execute a prepared statement.
  error = DbcUtils::execute_statement(pg_conn_, statement.name(), params, res);

  if (error == ErrorCode::OK) {
    uint64_t number_of_rows_affected = 0;

    ErrorCode error_get =
        DbcUtils::get_number_of_rows_affected(res, number_of_rows_affected);
    if (error_get != ErrorCode::OK) {
      error = error_get;
    } else if (number_of_rows_affected >= 0) {
      object_ids.clear();

      // Obtain the object ID of the deleted metadata object.
      for (int row_number = 0; row_number < number_of_rows_affected;
           row_number++) {
        // Obtain the object ID of the deleted metadata object.
        ObjectId object_id;
        error = Utility::str_to_numeric(
            PQgetvalue(res, row_number, kFirstColumn), object_id);
        object_ids.push_back(object_id);
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

void StatisticsDaoPg::create_prepared_statements() {
  DaoPg::create_prepared_statements();

  {
    // INSERT statement with name specified.
    InsertStatement statement_name{
        this->get_source_name(),
        this->get_insert_statement_columns(ColumnsDaoPg::ColumnName::kName),
        Statistics::COLUMN_NAME};
    insert_statements_.emplace(Statistics::COLUMN_NAME, statement_name);

    // INSERT statement with column number specified.
    InsertStatement statement_number{
        this->get_source_name(),
        this->get_insert_statement_columns(
            ColumnsDaoPg::ColumnName::kColumnNumber),
        Statistics::COLUMN_NUMBER};
    insert_statements_.emplace(Statistics::COLUMN_NUMBER, statement_number);
  }

  {
    // SELECT statement with table id specified.
    SelectStatement statement_tid{this->get_source_name(),
                                  this->get_select_statement_tid(),
                                  ColumnsDaoPg::ColumnName::kTableId};
    select_statements_.emplace(Statistics::TABLE_ID, statement_tid);

    // SELECT statement with column id specified.
    SelectStatement statement_id{
        this->get_source_name(),
        this->get_select_statement(ColumnName::kColumnId),
        Statistics::COLUMN_ID};
    select_statements_.emplace(Statistics::COLUMN_ID, statement_id);

    // SELECT statement with column name specified.
    SelectStatement statement_name{
        this->get_source_name(),
        this->get_select_statement_columns(ColumnsDaoPg::ColumnName::kName),
        Statistics::COLUMN_NAME};
    select_statements_.emplace(Statistics::COLUMN_NAME, statement_name);

    // SELECT statement with column number specified.
    SelectStatement statement_number{
        this->get_source_name(),
        this->get_select_statement_columns(
            ColumnsDaoPg::ColumnName::kColumnNumber),
        Statistics::COLUMN_NUMBER};
    select_statements_.emplace(Statistics::COLUMN_NUMBER, statement_number);
  }

  {
    // DELETE statement with table id specified.
    DeleteStatement statement_tid{this->get_source_name(),
                                  this->get_delete_statement_tid(),
                                  ColumnsDaoPg::ColumnName::kTableId};
    delete_statements_.emplace(Statistics::TABLE_ID, statement_tid);

    // DELETE statement with column id specified.
    DeleteStatement statement_cid{
        this->get_source_name(),
        this->get_delete_statement(ColumnName::kColumnId),
        Statistics::COLUMN_ID};
    delete_statements_.emplace(Statistics::COLUMN_ID, statement_cid);

    // DELETE statement with column name specified.
    DeleteStatement statement_column_name{
        this->get_source_name(),
        this->get_delete_statement_columns(ColumnsDaoPg::ColumnName::kName),
        Statistics::COLUMN_NAME};
    delete_statements_.emplace(Statistics::COLUMN_NAME, statement_column_name);

    // DELETE statement with column number specified.
    DeleteStatement statement_column_number{
        this->get_source_name(),
        this->get_delete_statement_columns(
            ColumnsDaoPg::ColumnName::kColumnNumber),
        Statistics::COLUMN_NUMBER};
    delete_statements_.emplace(Statistics::COLUMN_NUMBER,
                               statement_column_number);
  }
}

std::string StatisticsDaoPg::get_insert_statement() const {
  // SQL statement
  boost::format query =
      boost::format(
          "INSERT INTO %1%.%2% (%3%, %4%, %5%, %6%, %7%)"
          " VALUES ($1, $2, $3, $4, $5)"
          " ON CONFLICT (%6%)"
          " DO UPDATE SET %3% = $1, %4% = $2, %5% = $3, %7% = $5"
          " RETURNING %8%") %
      kSchemaTsurugiCatalog % kTableName % ColumnName::kFormatVersion %
      ColumnName::kGeneration % ColumnName::kName % ColumnName::kColumnId %
      ColumnName::kColumnStatistic % ColumnName::kId;

  return query.str();
}

std::string StatisticsDaoPg::get_insert_statement_columns(
    std::string_view key) const {
  // SQL statement
  boost::format query =
      boost::format(
          "INSERT INTO %1%.%2% (%3%, %4%, %5%, %6%, %7%)"
          " VALUES ($1, $2, $3"
          " , (SELECT %9% FROM %1%.%8% WHERE %10%=$4 AND %11%=$5), $6)"
          " ON CONFLICT (%6%)"
          " DO UPDATE SET %3% = $1, %4% = $2, %5% = $3, %7% = $6"
          " RETURNING %12%") %
      kSchemaTsurugiCatalog % kTableName % ColumnName::kFormatVersion %
      ColumnName::kGeneration % ColumnName::kName % ColumnName::kColumnId %
      ColumnName::kColumnStatistic % ColumnsDaoPg::kTableName %
      ColumnsDaoPg::ColumnName::kId % ColumnsDaoPg::ColumnName::kTableId % key %
      ColumnName::kId;

  return query.str();
}

std::string StatisticsDaoPg::get_select_all_statement() const {
  // SQL statement
  boost::format query =
      boost::format(
          "SELECT sts.%3%, sts.%4%, sts.%5%, sts.%6%, sts.%7%, sts.%8%"
          " , col.%11%, col.%12%, col.%13% column_name"
          " FROM %1%.%2% sts JOIN %1%.%9% col ON (sts.%7% = col.%10%)"
          " ORDER BY %11%, %12%") %
      kSchemaTsurugiCatalog % kTableName % ColumnName::kFormatVersion %
      ColumnName::kGeneration % ColumnName::kId % ColumnName::kName %
      ColumnName::kColumnId % ColumnName::kColumnStatistic %
      ColumnsDaoPg::kTableName % ColumnsDaoPg::ColumnName::kId %
      ColumnsDaoPg::ColumnName::kTableId %
      ColumnsDaoPg::ColumnName::kColumnNumber % ColumnsDaoPg::ColumnName::kName;

  return query.str();
}

std::string StatisticsDaoPg::get_select_statement(std::string_view key) const {
  // SQL statement
  boost::format query =
      boost::format(
          "SELECT sts.%3%, sts.%4%, sts.%5%, sts.%6%, sts.%7%, sts.%8%"
          " , col.%11%, col.%12%, col.%13% column_name"
          " FROM %1%.%2% sts JOIN %1%.%9% col ON (sts.%7% = col.%10%)"
          " WHERE (sts.%14% = $1)") %
      kSchemaTsurugiCatalog % kTableName % ColumnName::kFormatVersion %
      ColumnName::kGeneration % ColumnName::kId % ColumnName::kName %
      ColumnName::kColumnId % ColumnName::kColumnStatistic %
      ColumnsDaoPg::kTableName % ColumnsDaoPg::ColumnName::kId %
      ColumnsDaoPg::ColumnName::kTableId %
      ColumnsDaoPg::ColumnName::kColumnNumber %
      ColumnsDaoPg::ColumnName::kName % key;

  return query.str();
}

std::string StatisticsDaoPg::get_select_statement_tid() const {
  // SQL statement
  boost::format query =
      boost::format(
          "SELECT sts.%3%, sts.%4%, sts.%5%, sts.%6%, sts.%7%, sts.%8%"
          " , col.%11%, col.%12%, col.%13% column_name"
          " FROM %1%.%2% sts JOIN %1%.%9% col ON (sts.%7% = col.%10%)"
          " WHERE col.%11% = $1"
          " ORDER BY %12%") %
      kSchemaTsurugiCatalog % kTableName % ColumnName::kFormatVersion %
      ColumnName::kGeneration % ColumnName::kId % ColumnName::kName %
      ColumnName::kColumnId % ColumnName::kColumnStatistic %
      ColumnsDaoPg::kTableName % ColumnsDaoPg::ColumnName::kId %
      ColumnsDaoPg::ColumnName::kTableId %
      ColumnsDaoPg::ColumnName::kColumnNumber % ColumnsDaoPg::ColumnName::kName;

  return query.str();
}

std::string StatisticsDaoPg::get_select_statement_columns(
    std::string_view key) const {
  // SQL statement
  boost::format query =
      boost::format(
          "SELECT sts.%3%, sts.%4%, sts.%5%, sts.%6%, sts.%7%, sts.%8%"
          " , col.%11%, col.%12%, col.%13% column_name"
          " FROM %1%.%2% sts JOIN %1%.%9% col ON (sts.%7% = col.%10%)"
          " WHERE (col.%11% = $1) AND (col.%14% = $2)") %
      kSchemaTsurugiCatalog % kTableName % ColumnName::kFormatVersion %
      ColumnName::kGeneration % ColumnName::kId % ColumnName::kName %
      ColumnName::kColumnId % ColumnName::kColumnStatistic %
      ColumnsDaoPg::kTableName % ColumnsDaoPg::ColumnName::kId %
      ColumnsDaoPg::ColumnName::kTableId %
      ColumnsDaoPg::ColumnName::kColumnNumber %
      ColumnsDaoPg::ColumnName::kName % key;

  return query.str();
}

std::string StatisticsDaoPg::get_delete_statement(std::string_view key) const {
  // SQL statement
  boost::format query =
      boost::format("DELETE FROM %1%.%2% WHERE %3% = $1 RETURNING %4%") %
      kSchemaTsurugiCatalog % kTableName % key % ColumnName::kId;

  return query.str();
}

std::string StatisticsDaoPg::get_delete_statement_tid() const {
  // SQL statement
  boost::format query = boost::format(
                            "DELETE FROM %1%.%2% sts USING %1%.%3% col"
                            " WHERE (sts.%4% = col.%5%) AND (col.%6% = $1)"
                            " RETURNING sts.%7%") %
                        kSchemaTsurugiCatalog % kTableName %
                        ColumnsDaoPg::kTableName % ColumnName::kColumnId %
                        ColumnsDaoPg::ColumnName::kId %
                        ColumnsDaoPg::ColumnName::kTableId % ColumnName::kId;

  return query.str();
}

std::string StatisticsDaoPg::get_delete_statement_columns(
    std::string_view key) const {
  // SQL statement
  boost::format query =
      boost::format(
          "DELETE FROM %1%.%2% sts USING %1%.%3% col"
          " WHERE (sts.%4% = col.%5%) AND (col.%6% = $1) AND (col.%7% = $2)"
          " RETURNING sts.%8%") %
      kSchemaTsurugiCatalog % kTableName % ColumnsDaoPg::kTableName %
      ColumnName::kColumnId % ColumnsDaoPg::ColumnName::kId %
      ColumnsDaoPg::ColumnName::kTableId % key % ColumnName::kId;

  return query.str();
}

ErrorCode StatisticsDaoPg::get_column_statistics_rows(
    std::string_view statement, const std::vector<const char*>& params,
    boost::property_tree::ptree& objects) const {
  ErrorCode error = ErrorCode::UNKNOWN;

  objects.clear();

  PGresult* res = nullptr;
  // Execute a prepared statement.
  error = DbcUtils::execute_statement(pg_conn_, statement, params, res);

  if (error == ErrorCode::OK) {
    int nrows = PQntuples(res);

    if (nrows >= 0) {
      for (int row_number = 0; row_number < nrows; row_number++) {
        ptree object = convert_pgresult_to_ptree(res, row_number);
        objects.push_back(std::make_pair("", object));
      }
    } else {
      error = ErrorCode::INVALID_PARAMETER;
    }
  }
  PQclear(res);

  return error;
}

boost::property_tree::ptree StatisticsDaoPg::convert_pgresult_to_ptree(
    const PGresult* pg_result, const int row_number) const {
  boost::property_tree::ptree object;

  // Set the value of the format_version column to ptree.
  object.put(
      Statistics::FORMAT_VERSION,
      get_result_value(pg_result, row_number, OrdinalPosition::kFormatVersion));

  // Set the value of the generation column to ptree.
  object.put(
      Statistics::GENERATION,
      get_result_value(pg_result, row_number, OrdinalPosition::kGeneration));

  // Set the value of the id column to ptree.
  object.put(Statistics::ID,
             get_result_value(pg_result, row_number, OrdinalPosition::kId));

  // Set the value of the name column to ptree.
  object.put(Statistics::NAME,
             get_result_value(pg_result, row_number, OrdinalPosition::kName));

  // Set the value of the table id column to ptree.
  object.put(Statistics::TABLE_ID, get_result_value(pg_result, row_number,
                                                    OrdinalPosition::kTableId));

  // Set the value of the ordinal position column to ptree.
  object.put(
      Statistics::COLUMN_NUMBER,
      get_result_value(pg_result, row_number, OrdinalPosition::kColumnNumber));

  // Set the value of the column id column to ptree.
  object.put(
      Statistics::COLUMN_ID,
      get_result_value(pg_result, row_number, OrdinalPosition::kColumnId));

  // Set the value of the column name column to ptree.
  object.put(
      Statistics::COLUMN_NAME,
      get_result_value(pg_result, row_number, OrdinalPosition::kColumnName));

  // Set the value of the column statistic column column to ptree.
  ptree column_statistic;
  ptree_helper::json_to_ptree(
      get_result_value(pg_result, row_number,
                       OrdinalPosition::kColumnStatistic),
      column_statistic);
  object.add_child(Statistics::COLUMN_STATISTIC, column_statistic);

  return object;
}

ErrorCode StatisticsDaoPg::set_key_params(
    const std::map<std::string_view, std::string_view>& keys,
    std::string& key_name, std::vector<const char*>& params) const {
  key_name = "";

  // Extracts the specified 1st key.
  for (const auto& key : {Statistics::ID, Statistics::NAME,
                          Statistics::TABLE_ID, Statistics::COLUMN_ID}) {
    auto it = keys.find(key);
    if (it != keys.end()) {
      // Set to the 1st key name.
      key_name = it->first.data();
      // Add the 1st key value to the parameter.
      params.emplace_back(it->second.data());

      LOG_DEBUG << "StatisticsDaoPg::set_key_params(): 1st key: \"" << key_name
                << "\": \"" << it->second.data() << "\"";
      break;
    }
  }

  // Extracts the specified 2nd key.
  if (key_name == Statistics::TABLE_ID) {
    // If the primary key is table ID.
    for (const auto& key :
         {Statistics::COLUMN_NAME, Statistics::COLUMN_NUMBER}) {
      auto it = keys.find(key);
      if (it != keys.end()) {
        // Overwrite key name with 2nd key name.
        key_name = it->first.data();
        // Add the 2nd key value to the parameter.
        params.emplace_back(it->second.data());

        LOG_DEBUG << "StatisticsDaoPg::set_key_params(): 2nd key: \""
                  << key_name << "\": \"" << it->second.data() << "\"";
        break;
      }
    }
  }

  return (!key_name.empty() ? ErrorCode::OK : ErrorCode::INVALID_PARAMETER);
}

}  // namespace manager::metadata::db
