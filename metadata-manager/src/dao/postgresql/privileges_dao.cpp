/*
 * Copyright 2021 tsurugi project.
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
#include "manager/metadata/dao/postgresql/privileges_dao.h"

#include <libpq-fe.h>

#include <boost/format.hpp>
#include <iostream>
#include <regex>
#include <string>
#include <string_view>

#include "manager/metadata/common/message.h"
#include "manager/metadata/dao/common/pg_type.h"
#include "manager/metadata/dao/common/statement_name.h"
#include "manager/metadata/dao/postgresql/common.h"
#include "manager/metadata/dao/postgresql/dbc_utils.h"
#include "manager/metadata/dao/postgresql/tables_dao.h"

// =============================================================================
namespace {

using manager::metadata::db::postgresql::SCHEMA_NAME;
using manager::metadata::db::postgresql::SCHEMA_NAME_PUBLIC;
std::unordered_map<std::string, std::string> statement_names_select;

namespace statement {

using manager::metadata::db::postgresql::PgCatalog;
using manager::metadata::db::postgresql::TablesDAO;

/**
 * @brief Returns a SELECT statement to get privilege.
 * @return a SELECT statement.
 */
std::string select_table_privilege() {
  // Subquery that gets the all foreign table names.
  boost::format sub_query_foreign =
      boost::format(
          "SELECT SUBSTRING(UNNEST(%2%) FROM 'table_name=(.+)') AS table_name"
          " FROM %1%") %
      PgCatalog::PgForeignTable::kTableName %
      PgCatalog::PgForeignTable::ColumnName::kOptions;
  // Subquery that gets the table name registered in the table metadata.
  boost::format sub_query_tables = boost::format("SELECT %3% FROM %1%.%2%") %
                                   SCHEMA_NAME % TablesDAO::kTableName %
                                   TablesDAO::ColumnName::kName;
  // Subquery that gets the foreign table name registered in the table metadata.
  boost::format sub_query = boost::format(
                                "SELECT '%1%.' || fgt.table_name AS table_name"
                                " FROM (%2%) fgt"
                                " WHERE fgt.table_name IN (%3%)") %
                            SCHEMA_NAME_PUBLIC % sub_query_foreign.str() %
                            sub_query_tables.str();

  // SQL statement
  boost::format query =
      boost::format(
          "WITH foreign_table AS (%1%)"
          " SELECT"
          "   has_table_privilege($1, fgt.table_name, 'SELECT')"
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

/**
 * @brief Returns a SELECT statement to check for the existence of an auth-id.
 * @return a SELECT statement.
 */
std::string exists_authid() {
  // SQL statement
  boost::format query =
      boost::format("SELECT EXISTS (SELECT * FROM %1% WHERE %2% = $1)") %
      PgCatalog::PgAuth::kTableName % PgCatalog::PgAuth::ColumnName::kOid;

  return query.str();
}

}  // namespace statement
}  // namespace

// =============================================================================
namespace manager::metadata::db::postgresql {

using manager::metadata::ErrorCode;
using manager::metadata::db::StatementName;

/**
 * @brief Constructor
 * @param (connection)  [in]  a connection to the metadata repository.
 * @return none.
 */
PrivilegesDAO::PrivilegesDAO(DBSessionManager* session_manager)
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
  boost::format name_by_id =
      boost::format("%1%-%2%") %
      static_cast<int>(StatementName::PRIVILEGES_DAO_SELECT) % Metadata::ID;
  statement_names_select.emplace(Metadata::ID, name_by_id.str());

  boost::format name_by_name =
      boost::format("%1%-%2%") %
      static_cast<int>(StatementName::PRIVILEGES_DAO_SELECT) % Metadata::NAME;
  statement_names_select.emplace(Metadata::NAME, name_by_name.str());
}

/**
 * @brief Defines all prepared statements.
 * @param none.
 * @return ErrorCode::OK if success, otherwise an error code.
 */
ErrorCode PrivilegesDAO::prepare() const {
  ErrorCode error = ErrorCode::UNKNOWN;

  // Set table privilege SELECT statement with role id.
  std::vector<Oid> data_types = {PgType::TypeOid::kInt8};
  error =
      DbcUtils::prepare(connection_, statement_names_select.at(Metadata::ID),
                        statement::select_table_privilege(), &data_types);
  if (error != ErrorCode::OK) {
    return error;
  }

  // Set table privilege SELECT statement with role name.
  error =
      DbcUtils::prepare(connection_, statement_names_select.at(Metadata::NAME),
                        statement::select_table_privilege());
  if (error != ErrorCode::OK) {
    return error;
  }

  // Set of the EXISTS statement to check for the existence of auth-id.
  error =
      DbcUtils::prepare(connection_, StatementName::PRIVILEGES_DAO_CHECK_EXISTS,
                        statement::exists_authid());
  if (error != ErrorCode::OK) {
    return error;
  }

  return error;
}

/**
 * @brief Get table permissions from the PostgreSQL system catalog
 *   and whether the specified permissions or not.
 * @param (object_value)  [in]  role object id or name.
 * @param (permission)    [in]  permissions.
 * @param (check_result)  [out] presence or absence
 *   of the specified permissions.
 * @retval ErrorCode::OK if success.
 * @retval ErrorCode::NOT_FOUND if the foreign table does not exist.
 * @retval ErrorCode::ID_NOT_FOUND if the role id does not exist.
 * @retval ErrorCode::NAME_NOT_FOUND if the role name does not exist.
 * @retval otherwise an error code.
 */
ErrorCode PrivilegesDAO::confirm_tables_permission(
    std::string_view object_key, std::string_view object_value,
    std::string_view permission, bool& check_result) const {
  ErrorCode error = ErrorCode::UNKNOWN;
  std::vector<const char*> param_values;

  // In the case of ID specification, check for the presence of the
  // specified ID.
  if (object_key == Metadata::ID) {
    bool exists_result = false;
    error = check_exists_authid(object_value, exists_result);
    if (error != ErrorCode::OK) {
      return error;
    }
    if (!exists_result) {
      error = ErrorCode::ID_NOT_FOUND;
      return error;
    }
  }

  // Set the role ID or role name as a parameter.
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
      error = ErrorCode::NOT_FOUND;
    } else {
      boost::format regex_valid_privileges_format =
          boost::format(R"((.*=|)([%s]+)(/.*|))") % kValidPrivileges.data();
      std::string regex_valid_privileges = regex_valid_privileges_format.str();

      std::string match_string = std::string(permission);
      std::smatch matcher;
      if (!std::regex_match(match_string, matcher,
                            std::regex(regex_valid_privileges.c_str()))) {
        PQclear(res);
        error = ErrorCode::INVALID_PARAMETER;
        return error;
      }

      std::string privilege = matcher[2].str();
      for (int ordinal_position = 0; ordinal_position < nrows;
           ordinal_position++) {
        error = check_of_privilege(res, ordinal_position, privilege.c_str(),
                                   check_result);
        // Finish if an error occurs or if do not have permission.
        if ((error != ErrorCode::OK) || (check_result == false)) {
          break;
        }
      }
    }
  } else {
    // If the error code is "undefined_object", it is converted to an
    // undefined error.
    std::string error_code(PQresultErrorField(res, PG_DIAG_SQLSTATE));
    if (error_code == PgErrorCode::kUndefinedObject) {
      if (object_key == Metadata::ID) {
        error = ErrorCode::ID_NOT_FOUND;
      } else if (object_key == Metadata::NAME) {
        error = ErrorCode::NAME_NOT_FOUND;
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
 * @brief Checks for the presence of the specified permissions.
 * @param (res)               [in]  the result of a query.
 * @param (ordinal_position)  [in]  column ordinal position of PGresult.
 * @param (permission)        [in]  permissions.
 * @param (check_result)      [out] presence or absence of the
 *   specified permissions.
 * @return ErrorCode::OK if success, otherwise an error code.
 */
ErrorCode PrivilegesDAO::check_of_privilege(const PGresult* res,
                                            const int ordinal_position,
                                            const char* permission,
                                            bool& check_result) const {
  ErrorCode error = ErrorCode::UNKNOWN;

  check_result = true;
  for (; *permission != 0x00; permission++) {
    std::size_t find_position = kValidPrivileges.find(*permission);
    if (find_position != std::string_view::npos) {
      check_result = DbcUtils::str_to_boolean(
          PQgetvalue(res, ordinal_position, find_position));
      if (!check_result) {
        break;
      }
    }
  }

  error = ErrorCode::OK;
  return error;
}

/**
 * @brief Checks for the presence of the specified ID.
 * @param (auth_id)        [in]  auth_id.
 * @param (exists_result)  [out] presence or absence of the specified auth_id.
 * @return ErrorCode::OK if success, otherwise an error code.
 */
ErrorCode PrivilegesDAO::check_exists_authid(std::string_view auth_id,
                                             bool& exists_result) const {
  ErrorCode error = ErrorCode::UNKNOWN;
  std::vector<const char*> param_values;

  // Set the role ID as a parameter.
  param_values.emplace_back(auth_id.data());

  PGresult* res = nullptr;
  error = DbcUtils::exec_prepared(connection_,
                                  StatementName::PRIVILEGES_DAO_CHECK_EXISTS,
                                  param_values, res);

  if (error == ErrorCode::OK) {
    int nrows = PQntuples(res);
    if (nrows == 1) {
      int ordinal_position = 0;
      int column_position = 0;
      exists_result = DbcUtils::str_to_boolean(
          PQgetvalue(res, ordinal_position, column_position));
    }
  }

  PQclear(res);
  return error;
}

}  // namespace manager::metadata::db::postgresql
