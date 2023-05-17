/*
 * Copyright 2020-2022 tsurugi project.
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
#include "manager/metadata/dao/postgresql/columns_dao_pg.h"

#include <boost/format.hpp>

#include "manager/metadata/common/message.h"
#include "manager/metadata/common/utility.h"
#include "manager/metadata/dao/postgresql/dbc_utils_pg.h"
#include "manager/metadata/helper/logging_helper.h"
#include "manager/metadata/helper/ptree_helper.h"

// =============================================================================
namespace manager::metadata::db {

using boost::property_tree::ptree;

ErrorCode ColumnsDaoPg::insert(const boost::property_tree::ptree& object,
                               ObjectId& object_id) const {
  ErrorCode error = ErrorCode::UNKNOWN;
  std::vector<const char*> params;

  // Checks for INSERT execution with object-id specified.
  auto column_id =
      ptree_helper::ptree_value_to_string<ObjectId>(object, Column::ID);
  if (!column_id.empty()) {
    LOG_INFO << "Add column metadata with specified column ID. ColumnID: "
             << column_id;
  }

  // format_version
  auto s_format_version = std::to_string(Tables::format_version());
  params.emplace_back(s_format_version.c_str());

  // generation
  auto s_generation = std::to_string(Tables::generation());
  params.emplace_back(s_generation.c_str());

  // Use an ID-specified INSERT statement.
  if (!column_id.empty()) {
    params.emplace_back(column_id.c_str());
  }

  // table_id
  auto table_id =
      ptree_helper::ptree_value_to_string<ObjectId>(object, Column::TABLE_ID);
  params.emplace_back(table_id.c_str());

  // name
  auto name =
      ptree_helper::ptree_value_to_string<std::string>(object, Column::NAME);
  if (name.empty()) {
    std::string column_name = "Column." + std::string(Column::NAME);
    LOG_ERROR << Message::PARAMETER_FAILED << "\"" << column_name << "\""
              << " => undefined or empty";
    error = ErrorCode::INVALID_PARAMETER;
    return error;
  }
  params.emplace_back(name.c_str());

  // column_number
  auto column_number = ptree_helper::ptree_value_to_string<std::string>(
      object, Column::COLUMN_NUMBER);
  params.emplace_back(
      (column_number.empty() ? nullptr : column_number.c_str()));

  // data_type_id
  auto data_type_id = ptree_helper::ptree_value_to_string<std::string>(
      object, Column::DATA_TYPE_ID);
  params.emplace_back(
      (data_type_id.empty() ? nullptr : data_type_id.c_str()));

  // data_length
  auto opt_data_length = object.get_child_optional(Column::DATA_LENGTH);
  std::string data_length_json;
  if (opt_data_length) {
    ptree pt_data_length;

    if (opt_data_length.value().empty()) {
      // Attempt to obtain by numeric.
      auto optional_number = object.get_optional<int64_t>(Column::DATA_LENGTH);
      if (optional_number) {
        pt_data_length =
            ptree_helper::make_array_ptree({optional_number.value()});
      }
    } else {
      pt_data_length = opt_data_length.value();
    }
    // Converts a property_tree to a JSON string.
    error = ptree_helper::ptree_to_json(pt_data_length, data_length_json);
    if (error != ErrorCode::OK) {
      return error;
    }
  }
  params.emplace_back((
      !data_length_json.empty() ? data_length_json.c_str() : kEmptyStringJson));

  // varying
  auto varying =
      ptree_helper::ptree_value_to_string<std::string>(object, Column::VARYING);
  params.emplace_back(varying.empty() ? nullptr : varying.c_str());

  // is_not_null
  auto is_not_null = ptree_helper::ptree_value_to_string<std::string>(
      object, Column::IS_NOT_NULL);
  if (is_not_null.empty()) {
    std::string column_name = "Column." + std::string(Column::IS_NOT_NULL);
    LOG_ERROR << Message::PARAMETER_FAILED << "\"" << column_name << "\""
              << " => undefined or empty";
    error = ErrorCode::INVALID_PARAMETER;
    return error;
  }
  params.emplace_back(is_not_null.c_str());

  // default_expr
  auto default_expr = ptree_helper::ptree_value_to_string<std::string>(
      object, Column::DEFAULT_EXPR);
  params.emplace_back(default_expr.empty() ? nullptr
                                                 : default_expr.c_str());

  // Set INSERT statement.
  InsertStatement statement;
  try {
    if (column_id.empty()) {
      // Use INSERT statement without ID specification.
      statement = insert_statements_.at(Statement::kDefaultKey);
    } else {
      // Use INSERT statement with ID specification.
      statement = insert_statements_.at(kStatementKeyInsertById);
    }
  } catch (...) {
    LOG_ERROR << Message::INVALID_STATEMENT_KEY << Statement::kDefaultKey;
    return ErrorCode::INVALID_PARAMETER;
  }

  PGresult* res = nullptr;
  // Execute a prepared statement.
  error =
      DbcUtils::exec_prepared(pg_conn_, statement.name(), params, res);

  if (error == ErrorCode::OK) {
    uint64_t number_of_rows_affected = 0;
    ErrorCode error_get =
        DbcUtils::get_number_of_rows_affected(res, number_of_rows_affected);

    if (error_get != ErrorCode::OK) {
      LOG_ERROR << Message::RECORD_INSERT_FAILURE;
      error = error_get;
    } else if (number_of_rows_affected == 1) {
      // Obtain the object ID of the deleted metadata object.
      std::string result_value = PQgetvalue(res, kFirstRow, kFirstColumn);
      error = Utility::str_to_numeric(result_value, object_id);
    } else {
      LOG_ERROR << Message::RECORD_INSERT_FAILURE;
      error = ErrorCode::INVALID_PARAMETER;
    }
  }
  PQclear(res);

  return error;
}

ErrorCode ColumnsDaoPg::select(std::string_view key, std::string_view value,
                               boost::property_tree::ptree& object) const {
  ErrorCode error = ErrorCode::UNKNOWN;

  std::vector<const char*> params;
  // Set key value.
  params.emplace_back(value.data());

  // Set SELECT statement.
  SelectStatement statement;
  try {
    statement = select_statements_.at(key.data());
  } catch (...) {
    LOG_ERROR << Message::INVALID_STATEMENT_KEY << key;
    return ErrorCode::INVALID_PARAMETER;
  }

  PGresult* res = nullptr;
  // Execute a prepared statement.
  error =
      DbcUtils::exec_prepared(pg_conn_, statement.name(), params, res);

  if (error == ErrorCode::OK) {
    int nrows = PQntuples(res);
    if (nrows >= 1) {
      for (int row_number = 0; row_number < nrows; row_number++) {
        // Convert acquired data to ptree type.
        object.push_back(
            std::make_pair("", convert_pgresult_to_ptree(res, row_number)));
      }
    } else {
      // Get a NOT_FOUND error code corresponding to the key.
      error = get_not_found_error_code(key);
    }
  }
  PQclear(res);

  return error;
}

ErrorCode ColumnsDaoPg::remove(std::string_view key, std::string_view value,
                               ObjectId& object_id) const {
  ErrorCode error = ErrorCode::UNKNOWN;

  std::vector<const char*> params;
  // Set key value.
  params.emplace_back(value.data());

  // Set DELETE statement.
  DeleteStatement statement;
  try {
    statement = delete_statements_.at(key.data());
  } catch (...) {
    LOG_ERROR << Message::INVALID_STATEMENT_KEY << key;
    return ErrorCode::INVALID_PARAMETER;
  }

  PGresult* res = nullptr;
  // Execute a prepared statement.
  error = DbcUtils::exec_prepared(pg_conn_, statement.name(),
                                  params, res);

  if (error == ErrorCode::OK) {
    uint64_t number_of_rows_affected = 0;
    ErrorCode error_get =
        DbcUtils::get_number_of_rows_affected(res, number_of_rows_affected);

    if (error_get != ErrorCode::OK) {
      error = error_get;
    } else if (number_of_rows_affected >= 1) {
      // Obtain the object ID of the deleted metadata object.
      std::string result_value = PQgetvalue(res, kFirstRow, kFirstColumn);
      error = Utility::str_to_numeric(result_value, object_id);
    } else {
      // Get a NOT_FOUND error code corresponding to the key.
      error = get_not_found_error_code(key);
    }
  }
  PQclear(res);

  return error;
}

/* =============================================================================
 * Private method area
 */

void ColumnsDaoPg::create_prepared_statements() {
  DaoPg::create_prepared_statements();

  {
    // INSERT statement with ID specified.
    InsertStatement insert_statement{this->get_source_name(),
                                    this->get_insert_statement_id(),
                                    kStatementKeyInsertById};
    insert_statements_.emplace(kStatementKeyInsertById, insert_statement);
  }

  {
    // SELECT statement with table id specified.
    SelectStatement statement_name{
        this->get_source_name(),
        this->get_select_statement(ColumnName::kTableId),
        Column::TABLE_ID};
    select_statements_.emplace(Column::TABLE_ID, statement_name);
  }

  {
    // DELETE statement with table id specified.
    DeleteStatement statement_name{
        this->get_source_name(),
        this->get_delete_statement(ColumnName::kTableId),
        Column::TABLE_ID};
    delete_statements_.emplace(Column::TABLE_ID, statement_name);
  }
}

std::string ColumnsDaoPg::get_insert_statement() const {
  // SQL statement
  boost::format query =
      boost::format(
          "INSERT INTO %1%.%2%"
          " (%3%, %4%, %5%, %6%, %7%, %8%, %9%, %10%, %11%, %12%)"
          " VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)"
          " RETURNING %13%") %
      kSchemaTsurugiCatalog % kTableName % ColumnName::kFormatVersion %
      ColumnName::kGeneration % ColumnName::kTableId % ColumnName::kName %
      ColumnName::kColumnNumber % ColumnName::kDataTypeId %
      ColumnName::kDataLength % ColumnName::kVarying % ColumnName::kIsNotNull %
      ColumnName::kDefaultExpr % ColumnName::kId;

  return query.str();
}

std::string ColumnsDaoPg::get_insert_statement_id() const {
  // SQL statement
  boost::format query =
      boost::format(
          "INSERT INTO %1%.%2%"
          " (%3%, %4%, %5%, %6%, %7%, %8%, %9%, %10%, %11%, %12%, %13%)"
          " VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)"
          " RETURNING %14%") %
      kSchemaTsurugiCatalog % kTableName % ColumnName::kFormatVersion %
      ColumnName::kGeneration % ColumnName::kId % ColumnName::kTableId %
      ColumnName::kName % ColumnName::kColumnNumber % ColumnName::kDataTypeId %
      ColumnName::kDataLength % ColumnName::kVarying % ColumnName::kIsNotNull %
      ColumnName::kDefaultExpr % ColumnName::kId;

  return query.str();
}

std::string ColumnsDaoPg::get_select_statement(std::string_view key) const {
  // SQL statement
  boost::format query =
      boost::format(
          "SELECT %3%, %4%, %5%, %6%, %7%, %8%, %9%, %10%, %11%, %12%, %13%"
          " FROM %1%.%2% WHERE %14% = $1 ORDER BY %8%") %
      kSchemaTsurugiCatalog % kTableName % ColumnName::kFormatVersion %
      ColumnName::kGeneration % ColumnName::kId % ColumnName::kName %
      ColumnName::kTableId % ColumnName::kColumnNumber %
      ColumnName::kDataTypeId % ColumnName::kDataLength % ColumnName::kVarying %
      ColumnName::kIsNotNull % ColumnName::kDefaultExpr % key;

  return query.str();
}

std::string ColumnsDaoPg::get_delete_statement(std::string_view key) const {
  // SQL statement
  boost::format query =
      boost::format("DELETE FROM %1%.%2% WHERE %3% = $1 RETURNING %4%") %
      kSchemaTsurugiCatalog % kTableName % key % ColumnName::kId;

  return query.str();
}

boost::property_tree::ptree ColumnsDaoPg::convert_pgresult_to_ptree(
    const PGresult* pg_result, const int row_number) const {
  boost::property_tree::ptree object;

  // Set the value of the format_version column to ptree.
  object.put(
      Column::FORMAT_VERSION,
      get_result_value(pg_result, row_number, OrdinalPosition::kFormatVersion));

  // Set the value of the generation column to ptree.
  object.put(
      Column::GENERATION,
      get_result_value(pg_result, row_number, OrdinalPosition::kGeneration));

  // Set the value of the id to ptree.
  object.put(Column::ID,
             get_result_value(pg_result, row_number, OrdinalPosition::kId));

  // Set the value of the name to ptree.
  object.put(Column::NAME,
             get_result_value(pg_result, row_number, OrdinalPosition::kName));

  // Set the value of the table_id to ptree.
  object.put(Column::TABLE_ID, get_result_value(pg_result, row_number,
                                                OrdinalPosition::kTableId));

  // Set the value of the ordinal_position to ptree.
  object.put(
      Column::COLUMN_NUMBER,
      get_result_value(pg_result, row_number, OrdinalPosition::kColumnNumber));

  // Set the value of the data_type_id to ptree.
  object.put(
      Column::DATA_TYPE_ID,
      get_result_value(pg_result, row_number, OrdinalPosition::kDataTypeId));

  // Set the value of the data_length to ptree.
  ptree p_data_length;
  ptree_helper::json_to_ptree(
      get_result_value(pg_result, row_number, OrdinalPosition::kDataLength),
      p_data_length);
  object.add_child(Column::DATA_LENGTH, p_data_length);

  // Set the value of the varying to ptree.
  object.put(
      Column::VARYING,
      get_result_value<bool>(pg_result, row_number, OrdinalPosition::kVarying));

  // Set the value of the nullable to ptree.
  object.put(Column::IS_NOT_NULL,
             get_result_value<bool>(pg_result, row_number,
                                    OrdinalPosition::kIsNotNull));

  // Set the value of the default_expr to ptree.
  object.put(
      Column::DEFAULT_EXPR,
      get_result_value(pg_result, row_number, OrdinalPosition::kDefaultExpr));

  return object;
}

}  // namespace manager::metadata::db
