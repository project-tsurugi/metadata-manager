/*
 * Copyright 2022-2023 tsurugi project.
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
#include "manager/metadata/dao/postgresql/constraints_dao_pg.h"

#include <boost/format.hpp>

#include "manager/metadata/common/message.h"
#include "manager/metadata/common/utility.h"
#include "manager/metadata/helper/logging_helper.h"
#include "manager/metadata/helper/ptree_helper.h"

// =============================================================================
namespace manager::metadata::db {

using boost::property_tree::ptree;

ErrorCode ConstraintsDaoPg::insert(const boost::property_tree::ptree& object,
                                   ObjectId& object_id) const {
  ErrorCode error = ErrorCode::UNKNOWN;
  std::vector<const char*> params;

  // Checks for INSERT execution with object-id specified.
  auto constraint_id =
      ptree_helper::ptree_value_to_string<ObjectId>(object, Constraint::ID);
  if (!constraint_id.empty()) {
    LOG_INFO << "Add constraint metadata with specified constraint ID. "
                "constraintID: "
             << constraint_id;
  }

  // format_version
  std::string s_format_version = std::to_string(Constraints::format_version());
  params.emplace_back(s_format_version.c_str());

  // generation
  std::string s_generation = std::to_string(Constraints::generation());
  params.emplace_back(s_generation.c_str());

  // Use an ID-specified INSERT statement.
  if (!constraint_id.empty()) {
    params.emplace_back(constraint_id.c_str());
  }

  // name
  auto name = ptree_helper::ptree_value_to_string<std::string>(
      object, Constraint::NAME);
  params.emplace_back(name.c_str());

  // tableId
  auto table_id = ptree_helper::ptree_value_to_string<ObjectId>(
      object, Constraint::TABLE_ID);
  params.emplace_back(table_id.c_str());

  // type
  auto type =
      ptree_helper::ptree_value_to_string<int64_t>(object, Constraint::TYPE);
  params.emplace_back(type.c_str());

  // columns
  auto opt_columns = object.get_child_optional(Constraint::COLUMNS);
  std::string columns_json;
  if (opt_columns) {
    ptree pt_columns;

    if (opt_columns.value().empty()) {
      // Attempt to obtain by numeric.
      auto opt_number = object.get_optional<ObjectId>(Constraint::COLUMNS);
      if (opt_number) {
        pt_columns = ptree_helper::make_array_ptree({opt_number.value()});
      }
    } else {
      pt_columns = opt_columns.value();
    }

    // Converts a property_tree to a JSON string.
    error = ptree_helper::ptree_to_json(pt_columns, columns_json);
    if (error != ErrorCode::OK) {
      return error;
    }
  }
  params.emplace_back(
      (!columns_json.empty() ? columns_json.c_str() : kEmptyStringJson));

  // columnsId
  auto opt_columns_id = object.get_child_optional(Constraint::COLUMNS_ID);
  std::string columns_id_json;
  if (opt_columns_id) {
    ptree pt_columns_id;

    if (opt_columns_id.value().empty()) {
      // Attempt to obtain by numeric.
      auto opt_number = object.get_optional<ObjectId>(Constraint::COLUMNS_ID);
      if (opt_number) {
        pt_columns_id = ptree_helper::make_array_ptree({opt_number.value()});
      }
    } else {
      pt_columns_id = opt_columns_id.value();
    }

    // Converts a property_tree to a JSON string.
    error = ptree_helper::ptree_to_json(pt_columns_id, columns_id_json);
    if (error != ErrorCode::OK) {
      return error;
    }
  }
  params.emplace_back(
      (!columns_id_json.empty() ? columns_id_json.c_str() : kEmptyStringJson));

  // indexId
  auto index_id   = object.get_optional<int64_t>(Constraint::INDEX_ID);
  auto s_index_id = (index_id ? std::to_string(index_id.value()) : "");
  params.emplace_back(!s_index_id.empty() ? s_index_id.c_str() : nullptr);

  // expression
  auto expression = object.get_optional<std::string>(Constraint::EXPRESSION);
  params.emplace_back((expression ? expression.value().c_str() : nullptr));

  // pk_table (for future expansion)
  params.emplace_back(nullptr);
  // pk_columns (for future expansion)
  params.emplace_back(nullptr);
  // pk_columns_id (for future expansion)
  params.emplace_back(nullptr);
  // fk_match_type (for future expansion)
  params.emplace_back(nullptr);
  // fk_delete_action (for future expansion)
  params.emplace_back(nullptr);
  // fk_update_action (for future expansion)
  params.emplace_back(nullptr);

  // Set INSERT statement.
  InsertStatement statement;
  try {
    if (constraint_id.empty()) {
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
  error = DbcUtils::execute_statement(pg_conn_, statement.name(), params, res);

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

ErrorCode ConstraintsDaoPg::select(
    const std::map<std::string_view, std::string_view>& keys,
    boost::property_tree::ptree& object) const {
  ErrorCode error = ErrorCode::UNKNOWN;
  std::string statement_key;
  std::vector<const char*> params;

  if (keys.empty()) {
    statement_key = Statement::kDefaultKey;
    // If no search key is specified, all are returned.
    params.clear();
  } else {
    const auto& it = keys.begin();
    // Only one search key combination is allowed.
    statement_key = it->first;
    params.push_back(it->second.data());
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
    object.clear();

    int nrows = PQntuples(res);
    if (nrows >= 0) {
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

ErrorCode ConstraintsDaoPg::remove(
    const std::map<std::string_view, std::string_view>& keys,
    std::vector<ObjectId>& object_ids) const {
  ErrorCode error = ErrorCode::UNKNOWN;

  std::string statement_key;
  ErrorCode not_found_code = ErrorCode::UNKNOWN;

  if (keys.empty()) {
    LOG_ERROR << Message::INVALID_STATEMENT_KEY << "Keys is empty.";
    error = ErrorCode::INVALID_PARAMETER;
    return error;
  }

  const auto& it = keys.begin();
  // Set DELETE statement.
  DeleteStatement statement;
  try {
    statement = delete_statements_.at(it->first.data());
  } catch (...) {
    LOG_ERROR << Message::INVALID_STATEMENT_KEY << it->first;
    return ErrorCode::INVALID_PARAMETER;
  }

  // Set SQL paramater.
  std::vector<const char*> params;
  // Only one search key combination is allowed.
  params.push_back(it->second.data());

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
      error = ErrorCode::OK;
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

void ConstraintsDaoPg::create_prepared_statements() {
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
        this->get_select_statement(ColumnName::kTableId), Constraint::TABLE_ID};
    select_statements_.emplace(Constraint::TABLE_ID, statement_name);
  }

  {
    // DELETE statement with table id specified.
    DeleteStatement statement_name{
        this->get_source_name(),
        this->get_delete_statement(ColumnName::kTableId), Constraint::TABLE_ID};
    delete_statements_.emplace(Constraint::TABLE_ID, statement_name);
  }
}

std::string ConstraintsDaoPg::get_insert_statement() const {
  // SQL statement
  boost::format query =
      boost::format(
          "INSERT INTO %1%.%2% (%4%, %5%, %6%, %7%, %8%, %9%, %10%, %11%,"
          " %12%, %13%, %14%, %15%, %16%, %17%, %18%, %19%)"
          " VALUES ($1, $2, nextval('%3%'), $3, $4, $5, $6, $7, $8, $9, $10,"
          " $11, $12, $13, $14, $15)"
          " RETURNING %20%") %
      kSchemaTsurugiCatalog % kTableName % kSequenceId %
      ColumnName::kFormatVersion % ColumnName::kGeneration % ColumnName::kId %
      ColumnName::kName % ColumnName::kTableId % ColumnName::kType %
      ColumnName::kColumns % ColumnName::kColumnsId % ColumnName::kIndexId %
      ColumnName::kExpression % ColumnName::kPkTable % ColumnName::kPkColumns %
      ColumnName::kPkColumnsId % ColumnName::kFkMatchType %
      ColumnName::kFkDeleteAction % ColumnName::kFkUpdateAction %
      ColumnName::kId;

  return query.str();
}

std::string ConstraintsDaoPg::get_insert_statement_id() const {
  // SQL statement
  boost::format query =
      boost::format(
          "INSERT INTO %1%.%2% (%3%, %4%, %5%, %6%, %7%, %8%, %9%, %10%, %11%,"
          " %12%, %13%, %14%, %15%, %16%, %17%, %18%)"
          " VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13,"
          " $14, $15, $16)"
          " RETURNING %19%") %
      kSchemaTsurugiCatalog % kTableName % ColumnName::kFormatVersion %
      ColumnName::kGeneration % ColumnName::kId % ColumnName::kName %
      ColumnName::kTableId % ColumnName::kType % ColumnName::kColumns %
      ColumnName::kColumnsId % ColumnName::kIndexId % ColumnName::kExpression %
      ColumnName::kPkTable % ColumnName::kPkColumns % ColumnName::kPkColumnsId %
      ColumnName::kFkMatchType % ColumnName::kFkDeleteAction %
      ColumnName::kFkUpdateAction % ColumnName::kId;

  return query.str();
}

std::string ConstraintsDaoPg::get_select_all_statement() const {
  // SQL statement
  boost::format query =
      boost::format(
          "SELECT"
          " %3%, %4%, %5%, %6%, %7%, %8%, %9%, %10%, %11%, %12%, %13%, %14%,"
          " %15%, %16%, %17%, %18%"
          " FROM %1%.%2%"
          " ORDER BY %7%, %5%") %
      kSchemaTsurugiCatalog % kTableName % ColumnName::kFormatVersion %
      ColumnName::kGeneration % ColumnName::kId % ColumnName::kName %
      ColumnName::kTableId % ColumnName::kType % ColumnName::kColumns %
      ColumnName::kColumnsId % ColumnName::kIndexId % ColumnName::kExpression %
      ColumnName::kPkTable % ColumnName::kPkColumns % ColumnName::kPkColumnsId %
      ColumnName::kFkMatchType % ColumnName::kFkDeleteAction %
      ColumnName::kFkUpdateAction;

  return query.str();
}

std::string ConstraintsDaoPg::get_select_statement(std::string_view key) const {
  // SQL statement
  boost::format query =
      boost::format(
          "SELECT"
          " %3%, %4%, %5%, %6%, %7%, %8%, %9%, %10%, %11%, %12%, %13%, %14%,"
          " %15%, %16%, %17%, %18%"
          " FROM %1%.%2%"
          " WHERE %19% = $1"
          " ORDER BY %7%, %5%") %
      kSchemaTsurugiCatalog % kTableName % ColumnName::kFormatVersion %
      ColumnName::kGeneration % ColumnName::kId % ColumnName::kName %
      ColumnName::kTableId % ColumnName::kType % ColumnName::kColumns %
      ColumnName::kColumnsId % ColumnName::kIndexId % ColumnName::kExpression %
      ColumnName::kPkTable % ColumnName::kPkColumns % ColumnName::kPkColumnsId %
      ColumnName::kFkMatchType % ColumnName::kFkDeleteAction %
      ColumnName::kFkUpdateAction % key;

  return query.str();
}

std::string ConstraintsDaoPg::get_delete_statement(std::string_view key) const {
  // SQL statement
  boost::format query =
      boost::format("DELETE FROM %1%.%2% WHERE %3% = $1 RETURNING %4%") %
      kSchemaTsurugiCatalog % this->get_source_name() % key.data() %
      ColumnName::kId;

  return query.str();
}

boost::property_tree::ptree ConstraintsDaoPg::convert_pgresult_to_ptree(
    const PGresult* pg_result, const int row_number) const {
  boost::property_tree::ptree object;

  // Set the value of the format_version column to ptree.
  object.put(
      Constraint::FORMAT_VERSION,
      get_result_value(pg_result, row_number, OrdinalPosition::kFormatVersion));

  // Set the value of the generation column to ptree.
  object.put(
      Constraint::GENERATION,
      get_result_value(pg_result, row_number, OrdinalPosition::kGeneration));

  // Set the value of the id column to ptree.
  object.put(Constraint::ID,
             get_result_value(pg_result, row_number, OrdinalPosition::kId));

  // Set the value of the name column to ptree.
  object.put(Constraint::NAME,
             get_result_value(pg_result, row_number, OrdinalPosition::kName));

  // Set the value of the table-id column to ptree.
  object.put(Constraint::TABLE_ID, get_result_value(pg_result, row_number,
                                                    OrdinalPosition::kTableId));

  // Set the value of the name column to ptree.
  object.put(Constraint::NAME,
             get_result_value(pg_result, row_number, OrdinalPosition::kName));

  // Set the value of the type column to ptree.
  object.put(Constraint::TYPE,
             get_result_value(pg_result, row_number, OrdinalPosition::kType));

  // Set the value of the columns column to ptree.
  ptree columns;
  // Converts a JSON string to a property_tree.
  ptree_helper::json_to_ptree(
      get_result_value(pg_result, row_number, OrdinalPosition::kColumns),
      columns);
  object.add_child(Constraint::COLUMNS, columns);

  // Set the value of the columnsId column to ptree.
  ptree columns_id;
  // Converts a JSON string to a property_tree.
  ptree_helper::json_to_ptree(
      get_result_value(pg_result, row_number, OrdinalPosition::kColumnsId),
      columns_id);
  object.add_child(Constraint::COLUMNS_ID, columns_id);

  // Set the value of the index-id column to ptree.
  object.put(Constraint::INDEX_ID, get_result_value(pg_result, row_number,
                                                    OrdinalPosition::kIndexId));

  // Set the value of the expression column to ptree.
  object.put(
      Constraint::EXPRESSION,
      get_result_value(pg_result, row_number, OrdinalPosition::kExpression));

  return object;
}

}  // namespace manager::metadata::db
