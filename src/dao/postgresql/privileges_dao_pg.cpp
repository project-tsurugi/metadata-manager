/*
 * Copyright 2021-2023 tsurugi project.
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
#include "manager/metadata/dao/postgresql/privileges_dao_pg.h"

#include <boost/format.hpp>

#include "manager/metadata/common/message.h"
#include "manager/metadata/common/utility.h"
#include "manager/metadata/dao/common/pg_type.h"
#include "manager/metadata/dao/postgresql/dbc_utils_pg.h"
#include "manager/metadata/dao/postgresql/tables_dao_pg.h"
#include "manager/metadata/helper/logging_helper.h"

// =============================================================================
namespace manager::metadata::db {

using boost::property_tree::ptree;

ErrorCode PrivilegesDaoPg::prepare() {
  ErrorCode error = ErrorCode::UNKNOWN;

  // Create prepared statements.
  this->create_prepared_statements();

  error = ErrorCode::OK;

  // Set the prepared SELECT statements.
  if (!select_statements_.empty()) {
    // Set the prepared statements.
    for (const auto element : select_statements_) {
      const auto& statement = element.second;

      if (statement.key() == Object::ID) {
        std::vector<Oid> data_types = {PgType::TypeOid::kInt8};
        error = DbcUtils::prepare(pg_conn_, statement.name(),
                                  statement.statement(), &data_types);
      } else {
        error = DbcUtils::prepare(pg_conn_, statement.name(),
                                  statement.statement());
      }

      if (error != ErrorCode::OK) {
        break;
      }
    }
  } else {
    error = ErrorCode::OK;
  }

  return error;
}

bool PrivilegesDaoPg::exists(ObjectId object_id) const {
  ErrorCode error = ErrorCode::UNKNOWN;

  std::string s_object_id(std::to_string(object_id));
  std::vector<const char*> params;
  // Set key value.
  params.emplace_back(s_object_id.c_str());

  // Set SELECT statement.
  SelectStatement statement;
  try {
    statement = select_statements_.at(kStatementKeyExists);
  } catch (...) {
    LOG_ERROR << Message::INVALID_STATEMENT_KEY << kStatementKeyExists;
    return false;
  }
  PGresult* res = nullptr;
  // Execute a prepared statement.
  error = DbcUtils::exec_prepared(pg_conn_, statement.name(), params, res);

  bool exists = false;
  if (error == ErrorCode::OK) {
    int nrows = PQntuples(res);
    if (nrows == 1) {
      auto bool_alpha = DbcUtils::convert_boolean_expression(
          PQgetvalue(res, kFirstRow, kFirstColumn));
      exists = Utility::str_to_boolean(bool_alpha.c_str());
    }
  }
  PQclear(res);

  return exists;
}

ErrorCode PrivilegesDaoPg::select(
    const std::map<std::string_view, std::string_view>& keys,
    boost::property_tree::ptree& object) const {
  ErrorCode error = ErrorCode::UNKNOWN;
  std::string statement_key;
  std::vector<const char*> params;

  if (keys.empty()) {
    LOG_ERROR << Message::INVALID_STATEMENT_KEY << "empty string";
    error = ErrorCode::INVALID_PARAMETER;
    return error;
  }

  // Only one search key combination is allowed.
  const auto& it = keys.begin();
  statement_key  = it->first;
  params.emplace_back(it->second.data());

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
  error = DbcUtils::exec_prepared(pg_conn_, statement.name(), params, res);

  if (error == ErrorCode::OK) {
    int nrows = PQntuples(res);
    if (nrows >= 0) {
      object.clear();

      for (int row_number = 0; row_number < nrows; row_number++) {
        // Get the table name.
        std::string table_name(
            get_result_value(res, row_number, OrdinalPosition::kTableName));

        // Add a list of privileges to the child node of the table.
        object.add_child(table_name,
                         convert_pgresult_to_ptree(res, row_number));
      }
    } else {
      error = ErrorCode::INVALID_PARAMETER;
    }
  } else {
    // If the error code is "undefined_object", it is converted to an
    // undefined error.
    std::string result_error(PQresultErrorField(res, PG_DIAG_SQLSTATE));
    if (result_error == PgErrorCode::kUndefinedObject) {
      object.clear();
      error = ErrorCode::NOT_FOUND;
    }
  }
  PQclear(res);

  return error;
}

/* =============================================================================
 * Private method area
 */

void PrivilegesDaoPg::create_prepared_statements() {
  // SELECT statements by has_table_privilege (role id).
  SelectStatement select_statement_oid{this->get_source_name(),
                                       this->get_select_statement(Object::ID),
                                       Object::ID};
  select_statements_.emplace(Object::ID, select_statement_oid);

  // SELECT statements by has_table_privilege (role name).
  SelectStatement select_statement_name{
      this->get_source_name(), this->get_select_statement(Object::NAME),
      Object::NAME};
  select_statements_.emplace(Object::NAME, select_statement_name);

  // SELECT statements by exist.
  SelectStatement select_statement_exists{this->get_source_name(),
                                          this->get_exists_statement(),
                                          kStatementKeyExists};
  select_statements_.emplace(kStatementKeyExists, select_statement_exists);
}

std::string PrivilegesDaoPg::get_select_statement(std::string_view) const {
  // Subquery that gets the all foreign table names.
  boost::format sub_query_foreign =
      boost::format(
          "SELECT SUBSTRING(UNNEST(%2%) FROM 'table_name=(.+)') AS table_name"
          " FROM %1%") %
      PgCatalog::PgForeignTable::kTableName %
      PgCatalog::PgForeignTable::ColumnName::kOptions;
  // Subquery that gets the table name registered in the table metadata.
  boost::format sub_query_tables =
      boost::format("SELECT %3% FROM %1%.%2%") % kSchemaTsurugiCatalog %
      TablesDaoPg::kTableName % TablesDaoPg::ColumnName::kName;
  // Subquery that gets the foreign table name registered in the table metadata.
  boost::format sub_query = boost::format(
                                "SELECT '%1%.' || fgt.table_name AS table_name"
                                " FROM (%2%) fgt"
                                " WHERE fgt.table_name IN (%3%)") %
                            kSchemaPublic % sub_query_foreign.str() %
                            sub_query_tables.str();

  // SQL statement
  boost::format query =
      boost::format(
          "WITH foreign_table AS (%1%)"
          " SELECT fgt.table_name"
          " , has_table_privilege($1, fgt.table_name, 'SELECT')"
          " , has_table_privilege($1, fgt.table_name, 'INSERT')"
          " , has_table_privilege($1, fgt.table_name, 'UPDATE')"
          " , has_table_privilege($1, fgt.table_name, 'DELETE')"
          " , has_table_privilege($1, fgt.table_name, 'TRUNCATE')"
          " , has_table_privilege($1, fgt.table_name, 'REFERENCES')"
          " , has_table_privilege($1, fgt.table_name, 'TRIGGER')"
          " FROM (SELECT * FROM foreign_table) fgt") %
      sub_query.str();

  return query.str();
}

std::string PrivilegesDaoPg::get_exists_statement() const {
  // SQL statement
  boost::format query =
      boost::format("SELECT EXISTS (SELECT * FROM %1% WHERE %2% = $1)") %
      PgCatalog::PgAuth::kTableName % PgCatalog::PgAuth::ColumnName::kOid;

  return query.str();
}

boost::property_tree::ptree PrivilegesDaoPg::convert_pgresult_to_ptree(
    const PGresult* pg_result, const int row_number) const {
  boost::property_tree::ptree object;

  // Set the value of the column to ptree.
  auto obtained_info = {
      std::make_pair(OrdinalPosition::kSelect, PrivilegeColumn::kSelect),
      std::make_pair(OrdinalPosition::kInsert, PrivilegeColumn::kInsert),
      std::make_pair(OrdinalPosition::kUpdate, PrivilegeColumn::kUpdate),
      std::make_pair(OrdinalPosition::kDelete, PrivilegeColumn::kDelete),
      std::make_pair(OrdinalPosition::kTruncate, PrivilegeColumn::kTruncate),
      std::make_pair(OrdinalPosition::kReferences,
                     PrivilegeColumn::kReferences)};
  for (const auto& elem : obtained_info) {
    object.put(elem.second,
               get_result_value<bool>(pg_result, row_number, elem.first));
  }

  return object;
}

}  // namespace manager::metadata::db
