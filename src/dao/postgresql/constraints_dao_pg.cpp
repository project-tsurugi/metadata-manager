/*
 * Copyright 2022 tsurugi project.
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

#include <libpq-fe.h>

#include <iostream>
#include <regex>
#include <string>
#include <string_view>

#include <boost/format.hpp>

#include "manager/metadata/common/utility.h"
#include "manager/metadata/constraints.h"
#include "manager/metadata/dao/common/statement_name.h"
#include "manager/metadata/dao/postgresql/dbc_utils_pg.h"
#include "manager/metadata/helper/logging_helper.h"

// =============================================================================
namespace {

std::unordered_map<std::string, std::string> constraints_column_names;
std::unordered_map<std::string, std::string> statement_names_select_equal_to;
std::unordered_map<std::string, std::string> statement_names_delete_equal_to;

namespace statement {

using manager::metadata::db::postgresql::ConstraintsDAO;
using manager::metadata::db::postgresql::SCHEMA_NAME;

/**
 * @brief Returns an INSERT statement for constraint metadata.
 * @param none.
 * @return an INSERT statement to insert constraint metadata.
 */
std::string insert_constraint_metadata() {
  // SQL statement
  boost::format query =
      boost::format(
          "INSERT INTO %1%.%2% (%3%, %4%, %5%, %6%, %7%, %8%, %9%, %10%, %11%, %12%, %13%, %14%, "
          "%15%, %16%, %17%)"
          " VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15)"
          " RETURNING %18%") %
      SCHEMA_NAME % ConstraintsDAO::kTableName % ConstraintsDAO::ColumnName::kFormatVersion %
      ConstraintsDAO::ColumnName::kGeneration % ConstraintsDAO::ColumnName::kName %
      ConstraintsDAO::ColumnName::kTableId % ConstraintsDAO::ColumnName::kType %
      ConstraintsDAO::ColumnName::kColumns % ConstraintsDAO::ColumnName::kColumnsId %
      ConstraintsDAO::ColumnName::kIndexId % ConstraintsDAO::ColumnName::kExpression %
      ConstraintsDAO::ColumnName::kPkTable % ConstraintsDAO::ColumnName::kPkColumns %
      ConstraintsDAO::ColumnName::kPkColumnsId % ConstraintsDAO::ColumnName::kFkMatchType %
      ConstraintsDAO::ColumnName::kFkDeleteAction % ConstraintsDAO::ColumnName::kFkUpdateAction %
      ConstraintsDAO::ColumnName::kId;

  return query.str();
}

/**
 * @brief Returns an INSERT statement for one constraint metadata with a specified ID.
 * @param none.
 * @return an INSERT statement to insert constraint metadata.
 */
std::string insert_constraint_metadata_id() {
  // SQL statement
  boost::format query =
      boost::format(
          "INSERT INTO %1%.%2% (%3%, %4%, %5%, %6%, %7%, %8%, %9%, %10%, %11%, %12%, %13%, %14%, "
          "%15%, %16%, %17%, %18%)"
          " VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16)"
          " RETURNING %19%") %
      SCHEMA_NAME % ConstraintsDAO::kTableName % ConstraintsDAO::ColumnName::kFormatVersion %
      ConstraintsDAO::ColumnName::kGeneration % ConstraintsDAO::ColumnName::kId %
      ConstraintsDAO::ColumnName::kName % ConstraintsDAO::ColumnName::kTableId %
      ConstraintsDAO::ColumnName::kType % ConstraintsDAO::ColumnName::kColumns %
      ConstraintsDAO::ColumnName::kColumnsId % ConstraintsDAO::ColumnName::kIndexId %
      ConstraintsDAO::ColumnName::kExpression % ConstraintsDAO::ColumnName::kPkTable %
      ConstraintsDAO::ColumnName::kPkColumns % ConstraintsDAO::ColumnName::kPkColumnsId %
      ConstraintsDAO::ColumnName::kFkMatchType % ConstraintsDAO::ColumnName::kFkDeleteAction %
      ConstraintsDAO::ColumnName::kFkUpdateAction % ConstraintsDAO::ColumnName::kId;

  return query.str();
}

/**
 * @brief Returns a SELECT statement to get metadata:
 *   select * from table_name.
 * @return a SELECT statement:
 *   select * from table_name.
 */
std::string select_all() {
  // SQL statement
  boost::format query =
      boost::format(
          "SELECT"
          " %3%, %4%, %5%, %6%, %7%, %8%, %9%, %10%, %11%, %12%, %13%, %14%, %15%, %16%, %17%, %18%"
          " FROM %1%.%2%"
          " ORDER BY %7%, %5%") %
      SCHEMA_NAME % ConstraintsDAO::kTableName % ConstraintsDAO::ColumnName::kFormatVersion %
      ConstraintsDAO::ColumnName::kGeneration % ConstraintsDAO::ColumnName::kId %
      ConstraintsDAO::ColumnName::kName % ConstraintsDAO::ColumnName::kTableId %
      ConstraintsDAO::ColumnName::kType % ConstraintsDAO::ColumnName::kColumns %
      ConstraintsDAO::ColumnName::kColumnsId % ConstraintsDAO::ColumnName::kIndexId %
      ConstraintsDAO::ColumnName::kExpression % ConstraintsDAO::ColumnName::kPkTable %
      ConstraintsDAO::ColumnName::kPkColumns % ConstraintsDAO::ColumnName::kPkColumnsId %
      ConstraintsDAO::ColumnName::kFkMatchType % ConstraintsDAO::ColumnName::kFkDeleteAction %
      ConstraintsDAO::ColumnName::kFkUpdateAction;

  return query.str();
}

/**
 * @brief Returns a SELECT statement to get metadata:
 *   select * from table_name where column_name = $1.
 * @param column_name  [in]  column name of metadata-table.
 * @return a SELECT statement:
 *   select * from table_name where column_name = $1.
 */
std::string select_equal_to(std::string_view column_name) {
  // SQL statement
  boost::format query =
      boost::format(
          "SELECT"
          " %3%, %4%, %5%, %6%, %7%, %8%, %9%, %10%, %11%, %12%, %13%, %14%, %15%, %16%, %17%, %18%"
          " FROM %1%.%2%"
          " WHERE %19% = $1"
          " ORDER BY %7%, %5%") %
      SCHEMA_NAME % ConstraintsDAO::kTableName % ConstraintsDAO::ColumnName::kFormatVersion %
      ConstraintsDAO::ColumnName::kGeneration % ConstraintsDAO::ColumnName::kId %
      ConstraintsDAO::ColumnName::kName % ConstraintsDAO::ColumnName::kTableId %
      ConstraintsDAO::ColumnName::kType % ConstraintsDAO::ColumnName::kColumns %
      ConstraintsDAO::ColumnName::kColumnsId % ConstraintsDAO::ColumnName::kIndexId %
      ConstraintsDAO::ColumnName::kExpression % ConstraintsDAO::ColumnName::kPkTable %
      ConstraintsDAO::ColumnName::kPkColumns % ConstraintsDAO::ColumnName::kPkColumnsId %
      ConstraintsDAO::ColumnName::kFkMatchType % ConstraintsDAO::ColumnName::kFkDeleteAction %
      ConstraintsDAO::ColumnName::kFkUpdateAction % column_name.data();

  return query.str();
}

/**
 * @brief Returns a DELETE statement to get metadata:
 *   delete from table_name where column_name = $1.
 * @param column_name  [in]  column name of metadata-table.
 * @return a DELETE statement:
 *   delete from table_name where column_name = $1.
 */
std::string delete_equal_to(std::string_view column_name) {
  // SQL statement
  boost::format query = boost::format("DELETE FROM %1%.%2% WHERE %3% = $1") % SCHEMA_NAME %
                        ConstraintsDAO::kTableName % column_name.data();

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
 * @param session_manager  [in]  database session manager.
 * @return none.
 */
ConstraintsDAO::ConstraintsDAO(DBSessionManager* session_manager)
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
  constraints_column_names.emplace(Constraint::ID, ColumnName::kId);
  constraints_column_names.emplace(Constraint::TABLE_ID, ColumnName::kTableId);

  // Creates a list of unique name
  // for the new prepared statement for each column names.
  for (auto column : constraints_column_names) {
    // Creates unique name for the new prepared statement.
    boost::format statement_name_select = boost::format("%1%-%2%-%3%") %
                                          static_cast<int>(StatementName::DAO_SELECT_EQUAL_TO) %
                                          kTableName % column.first;
    boost::format statement_name_delete = boost::format("%1%-%2%-%3%") %
                                          static_cast<int>(StatementName::DAO_DELETE_EQUAL_TO) %
                                          kTableName % column.first;

    // Added this list to unique name for the new prepared statement.
    // key : column name
    // value : unique name for the new prepared statement.
    statement_names_select_equal_to.emplace(column.first, statement_name_select.str());
    statement_names_delete_equal_to.emplace(column.first, statement_name_delete.str());
  }
}

/**
 * @brief Defines all prepared statements.
 * @param none.
 * @return ErrorCode::OK if success, otherwise an error code.
 */
ErrorCode ConstraintsDAO::prepare() const {
  ErrorCode error = ErrorCode::UNKNOWN;

  // Set the INSERT statement.
  error = DbcUtils::prepare(connection_, StatementName::CONSTRAINTS_DAO_INSERT_CONSTRAINTS_METADATA,
                            statement::insert_constraint_metadata());
  if (error != ErrorCode::OK) {
    return error;
  }

  // Set the INSERT statement with ID specified.
  error =
      DbcUtils::prepare(connection_, StatementName::CONSTRAINTS_DAO_INSERT_CONSTRAINTS_METADATA_ID,
                        statement::insert_constraint_metadata_id());
  if (error != ErrorCode::OK) {
    return error;
  }

  // Set all constraint metadata SELECT statement.
  error =
      DbcUtils::prepare(connection_, StatementName::CONSTRAINTS_DAO_SELECT_ALL_CONSTRAINTS_METADATA,
                        statement::select_all());
  if (error != ErrorCode::OK) {
    return error;
  }

  // Set conditional SQL statement.
  for (auto column : constraints_column_names) {
    // Set SELECT statement.
    error = DbcUtils::prepare(connection_, statement_names_select_equal_to.at(column.first),
                              statement::select_equal_to(column.second));
    if (error != ErrorCode::OK) {
      return error;
    }

    // Set DELETE statement.
    error = DbcUtils::prepare(connection_, statement_names_delete_equal_to.at(column.first),
                              statement::delete_equal_to(column.second));
    if (error != ErrorCode::OK) {
      return error;
    }
  }

  return error;
}

/**
 * @brief Executes INSERT statement to insert the given one constraint metadata
 *   into the constraint metadata table.
 * @param constraint_metadata [in]  one constraint metadata to add.
 * @param constraint_id       [out] constraint id.
 * @return ErrorCode::OK if success, otherwise an error code.
 */
ErrorCode ConstraintsDAO::insert_constraint_metadata(
    const boost::property_tree::ptree& constraint_metadata, ObjectId& constraint_id) const {
  ErrorCode error = ErrorCode::UNKNOWN;
  std::vector<char const*> param_values;

  // Checks for INSERT execution with object-id specified.
  auto object_id = constraint_metadata.get_optional<ObjectIdType>(Constraint::ID);
  if (object_id) {
    LOG_INFO << "Add constraint metadata with specified constraint ID. constraintID: "
             << object_id.value();
  }

  // format_version
  std::string s_format_version = std::to_string(Constraints::format_version());
  param_values.emplace_back(s_format_version.c_str());

  // generation
  std::string s_generation = std::to_string(Constraints::generation());
  param_values.emplace_back(s_generation.c_str());

  // Use an ID-specified INSERT statement.
  std::string work_constraint_id;
  if (object_id) {
    work_constraint_id = std::to_string(object_id.value());
    param_values.emplace_back(work_constraint_id.c_str());
  }

  // name
  auto name = constraint_metadata.get_optional<std::string>(Constraint::NAME);
  param_values.emplace_back((name ? name.value().c_str() : nullptr));

  // tableId
  auto table_id   = constraint_metadata.get_optional<ObjectId>(Constraint::TABLE_ID);
  auto s_table_id = (table_id ? std::to_string(table_id.value()) : "");
  param_values.emplace_back(!s_table_id.empty() ? s_table_id.c_str() : nullptr);

  // type
  auto type   = constraint_metadata.get_optional<int64_t>(Constraint::TYPE);
  auto s_type = (type ? std::to_string(type.value()) : "");
  param_values.emplace_back(!s_type.empty() ? s_type.c_str() : nullptr);

  // columns
  auto columns = constraint_metadata.get_child_optional(Constraint::COLUMNS);
  std::string columns_json;
  if (columns) {
    // Converts a property_tree to a JSON string.
    error = Utility::ptree_to_json(columns.value(), columns_json);
    if (error != ErrorCode::OK) {
      return error;
    }
  }
  param_values.emplace_back((!columns_json.empty() ? columns_json.c_str() : "{}"));

  // columnsId
  auto columns_id = constraint_metadata.get_child_optional(Constraint::COLUMNS_ID);
  std::string columns_id_json;
  if (columns_id) {
    // Converts a property_tree to a JSON string.
    error = Utility::ptree_to_json(columns_id.value(), columns_id_json);
    if (error != ErrorCode::OK) {
      return error;
    }
  }
  param_values.emplace_back((!columns_id_json.empty() ? columns_id_json.c_str() : "{}"));

  // indexId
  auto index_id   = constraint_metadata.get_optional<int64_t>(Constraint::INDEX_ID);
  auto s_index_id = (index_id ? std::to_string(index_id.value()) : "");
  param_values.emplace_back(!s_index_id.empty() ? s_index_id.c_str() : nullptr);

  // expression
  auto expression = constraint_metadata.get_optional<std::string>(Constraint::EXPRESSION);
  param_values.emplace_back((expression ? expression.value().c_str() : nullptr));

  // pk_table (for future expansion)
  param_values.emplace_back(nullptr);
  // pk_columns (for future expansion)
  param_values.emplace_back(nullptr);
  // pk_columns_id (for future expansion)
  param_values.emplace_back(nullptr);
  // fk_match_type (for future expansion)
  param_values.emplace_back(nullptr);
  // fk_delete_action (for future expansion)
  param_values.emplace_back(nullptr);
  // fk_update_action (for future expansion)
  param_values.emplace_back(nullptr);

  // Set INSERT statement.
  StatementName statementName;
  if (object_id) {
    // Use an ID-specified INSERT statement.
    statementName = StatementName::CONSTRAINTS_DAO_INSERT_CONSTRAINTS_METADATA_ID;
  } else {
    // Use INSERT statement without ID specification.
    statementName = StatementName::CONSTRAINTS_DAO_INSERT_CONSTRAINTS_METADATA;
  }

  PGresult* res = nullptr;
  // Executes a prepared statement
  error = DbcUtils::exec_prepared(connection_, statementName, param_values, res);
  if (error == ErrorCode::OK) {
    int nrows = PQntuples(res);
    if (nrows == 1) {
      int ordinal_position = 0;
      // Get the generated ID.
      error =
          DbcUtils::str_to_integral<ObjectId>(PQgetvalue(res, ordinal_position, 0), constraint_id);
    } else {
      error = ErrorCode::INVALID_PARAMETER;
    }
  }

  PQclear(res);
  return error;
}

/**
 * @brief Executes a SELECT statement to get constraint metadata rows from the
 *   constraint metadata table, where the given key equals the given value.
 * @param object_key           [in]  key. column name of a constraint metadata table.
 * @param object_value         [in]  value to be filtered.
 * @param constraint_metadata  [out] constraint metadata to get, where the given key equals the
 *   given value.
 * @retval ErrorCode::OK if success.
 * @retval ErrorCode::ID_NOT_FOUND if the constraint id does not exist.
 * @retval ErrorCode::NOT_FOUND if the table id does not exist.
 * @retval otherwise an error code.
 */
ErrorCode ConstraintsDAO::select_constraint_metadata(
    std::string_view object_key, std::string_view object_value,
    boost::property_tree::ptree& constraint_metadata) const {
  ErrorCode error = ErrorCode::UNKNOWN;
  std::vector<const char*> param_values;

  param_values.emplace_back(object_value.data());

  std::string statement_name;
  // Get the name of the SQL statement to be executed.
  error =
      DbcUtils::find_statement_name(statement_names_select_equal_to, object_key, statement_name);
  if (error != ErrorCode::OK) {
    return error;
  }

  PGresult* res = nullptr;
  // Executes a prepared statement
  error = DbcUtils::exec_prepared(connection_, statement_name, param_values, res);

  if (error == ErrorCode::OK) {
    int nrows = PQntuples(res);
    if (nrows >= 1) {
      if (object_key == Constraint::ID) {
        int ordinal_position = 0;
        // Convert acquired data to ptree type.
        error = convert_pgresult_to_ptree(res, ordinal_position, constraint_metadata);
      } else {
        for (int ordinal_position = 0; ordinal_position < nrows; ordinal_position++) {
          ptree constraint;

          // Convert acquired data to ptree type.
          error = convert_pgresult_to_ptree(res, ordinal_position, constraint);
          if (error != ErrorCode::OK) {
            constraint_metadata.clear();
            break;
          }
          constraint_metadata.push_back(std::make_pair("", constraint));
        }
      }
    } else {
      // Convert the error code.
      if (object_key == Constraint::ID) {
        error = ErrorCode::ID_NOT_FOUND;
      } else {
        error = ErrorCode::NOT_FOUND;
      }
    }
  }

  PQclear(res);
  return error;
}

/**
 * @brief Executes a SELECT statement to get constraint metadata rows from the
 *   constraint metadata table.
 *   If the constraint metadata does not exist, return the container as empty.
 * @param container  [out] all constraint metadata.
 * @return ErrorCode::OK if success, otherwise an error code.
 */
ErrorCode ConstraintsDAO::select_constraint_metadata(
    std::vector<boost::property_tree::ptree>& container) const {
  ErrorCode error = ErrorCode::UNKNOWN;
  std::vector<const char*> param_values;

  PGresult* res = nullptr;
  // Executes a prepared statement
  error = DbcUtils::exec_prepared(connection_,
                                  StatementName::CONSTRAINTS_DAO_SELECT_ALL_CONSTRAINTS_METADATA,
                                  param_values, res);

  if (error == ErrorCode::OK) {
    int nrows = PQntuples(res);
    if (nrows >= 0) {
      for (int ordinal_position = 0; ordinal_position < nrows; ordinal_position++) {
        ptree constraint;
        // Convert acquired data to ptree type.
        error = convert_pgresult_to_ptree(res, ordinal_position, constraint);
        if (error != ErrorCode::OK) {
          break;
        }
        container.emplace_back(constraint);
      }
    } else {
      error = ErrorCode::INVALID_PARAMETER;
    }
  }

  PQclear(res);
  return error;
}

/**
 * @brief Executes DELETE statement to delete constraint metadata
 *   from the constraint metadata table based on the given constraint name.
 * @param object_key     [in]  key. column name of a constraint metadata table.
 * @param object_value   [in]  value to be filtered.
 * @retval ErrorCode::OK if success.
 * @retval ErrorCode::ID_NOT_FOUND if the constraint id does not exist.
 * @retval ErrorCode::NOT_FOUND if the table id does not exist.
 * @retval otherwise an error code.
 */
ErrorCode ConstraintsDAO::delete_constraint_metadata(std::string_view object_key,
                                                     std::string_view object_value) const {
  ErrorCode error = ErrorCode::UNKNOWN;
  std::vector<const char*> param_values;

  param_values.emplace_back(object_value.data());

  std::string statement_name;
  // Get the name of the SQL statement to be executed.
  error =
      DbcUtils::find_statement_name(statement_names_delete_equal_to, object_key, statement_name);
  if (error != ErrorCode::OK) {
    return error;
  }

  PGresult* res = nullptr;
  // Executes a prepared statement
  error = DbcUtils::exec_prepared(connection_, statement_name, param_values, res);

  if (error == ErrorCode::OK) {
    uint64_t number_of_rows_affected = 0;
    // Gets the number of affected rows.
    ErrorCode error_get = DbcUtils::get_number_of_rows_affected(res, number_of_rows_affected);

    if (error_get != ErrorCode::OK) {
      error = error_get;
    } else if (number_of_rows_affected == 0) {
      // Convert the error code.
      if (object_key == Constraint::ID) {
        error = ErrorCode::ID_NOT_FOUND;
      } else {
        error = ErrorCode::NOT_FOUND;
      }
    }
  }

  PQclear(res);
  return error;
}

/* =============================================================================
 * Private method area
 */

/**
 * @brief Gets the ptree type constraint metadata converted from the given PGresult type value.
 * @param res                  [in]  the result of a query.
 * @param ordinal_position     [in]  column ordinal position of PGresult.
 * @param constraint_metadata  [out] one constraint metadata.
 * @return ErrorCode::OK if success, otherwise an error code.
 */
ErrorCode ConstraintsDAO::convert_pgresult_to_ptree(
    const PGresult* res, const int ordinal_position,
    boost::property_tree::ptree& constraint_metadata) const {
  ErrorCode error = ErrorCode::UNKNOWN;

  // Initialization.
  constraint_metadata.clear();

  // Set the value of the format_version column to ptree.
  constraint_metadata.put(
      Constraint::FORMAT_VERSION,
      PQgetvalue(res, ordinal_position, static_cast<int>(OrdinalPosition::kFormatVersion)));

  // Set the value of the generation column to ptree.
  constraint_metadata.put(
      Constraint::GENERATION,
      PQgetvalue(res, ordinal_position, static_cast<int>(OrdinalPosition::kGeneration)));

  // Set the value of the id column to ptree.
  constraint_metadata.put(
      Constraint::ID, PQgetvalue(res, ordinal_position, static_cast<int>(OrdinalPosition::kId)));

  // Set the value of the name column to ptree.
  constraint_metadata.put(Constraint::NAME, PQgetvalue(res, ordinal_position,
                                                       static_cast<int>(OrdinalPosition::kName)));

  // Set the value of the table-id column to ptree.
  constraint_metadata.put(
      Constraint::TABLE_ID,
      PQgetvalue(res, ordinal_position, static_cast<int>(OrdinalPosition::kTableId)));

  // Set the value of the name column to ptree.
  constraint_metadata.put(Constraint::NAME, PQgetvalue(res, ordinal_position,
                                                       static_cast<int>(OrdinalPosition::kName)));

  // Set the value of the type column to ptree.
  constraint_metadata.put(Constraint::TYPE, PQgetvalue(res, ordinal_position,
                                                       static_cast<int>(OrdinalPosition::kType)));

  // Set the value of the columns column to ptree.
  ptree columns;
  // Converts a JSON string to a property_tree.
  error = Utility::json_to_ptree(
      PQgetvalue(res, ordinal_position, static_cast<int>(OrdinalPosition::kColumns)), columns);
  if (error != ErrorCode::OK) {
    return error;
  }
  // NOTICE:
  //   If it is not set, MUST add an empty ptree.
  //   ogawayama-server read key Constraint::COLUMNS.
  constraint_metadata.add_child(Constraint::COLUMNS, columns);

  // Set the value of the columnsId column to ptree.
  ptree columns_id;
  // Converts a JSON string to a property_tree.
  error = Utility::json_to_ptree(
      PQgetvalue(res, ordinal_position, static_cast<int>(OrdinalPosition::kColumnsId)), columns_id);
  if (error != ErrorCode::OK) {
    return error;
  }
  // NOTICE:
  //   If it is not set, MUST add an empty ptree.
  //   ogawayama-server read key Constraint::COLUMNS_ID.
  constraint_metadata.add_child(Constraint::COLUMNS_ID, columns_id);

  // Set the value of the index-id column to ptree.
  constraint_metadata.put(
      Constraint::INDEX_ID,
      PQgetvalue(res, ordinal_position, static_cast<int>(OrdinalPosition::kIndexId)));

  // Set the value of the expression column to ptree.
  constraint_metadata.put(
      Constraint::EXPRESSION,
      PQgetvalue(res, ordinal_position, static_cast<int>(OrdinalPosition::kExpression)));

  error = ErrorCode::OK;
  return error;
}

}  // namespace manager::metadata::db::postgresql
