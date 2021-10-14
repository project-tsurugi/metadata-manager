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
#include "manager/metadata/dao/postgresql/roles_dao.h"

#include <boost/format.hpp>
#include <iostream>
#include <string>
#include <string_view>

#include <libpq-fe.h>
#include "manager/metadata/dao/common/message.h"
#include "manager/metadata/dao/common/statement_name.h"
#include "manager/metadata/dao/postgresql/common.h"
#include "manager/metadata/dao/postgresql/dbc_utils.h"

// =============================================================================
namespace {

std::unordered_map<std::string, std::string> role_column_names;
std::unordered_map<std::string, std::string> statement_names_select;

namespace statement {

using manager::metadata::db::postgresql::PgCatalog;

/**
 * @brief  Returns a SELECT statement for role based on role id:
 *   select * from pg_authid where column_name = $1.
 * @param (column_name)  [in]  column name of metadata-table.
 * @return a SELECT statement:
 *    select * from pg_authid where column_name = $1.
 */
std::string select_equal_to(std::string_view column_name) {
  // SQL statement
  boost::format query =
      boost::format(
          "SELECT %2%, %3%, %4%, %5%, %6%, %7%, %8%, %9%"
          " , %10%, %11%, %12%, %13%"
          " FROM %1%"
          " WHERE %14% = $1") %
      PgCatalog::PgAuth::kTableName % PgCatalog::PgAuth::ColumnName::kOid %
      PgCatalog::PgAuth::ColumnName::kName %
      PgCatalog::PgAuth::ColumnName::kSuper %
      PgCatalog::PgAuth::ColumnName::kInherit %
      PgCatalog::PgAuth::ColumnName::kCreateRole %
      PgCatalog::PgAuth::ColumnName::kCreateDb %
      PgCatalog::PgAuth::ColumnName::kCanLogin %
      PgCatalog::PgAuth::ColumnName::kReplication %
      PgCatalog::PgAuth::ColumnName::kBypassRls %
      PgCatalog::PgAuth::ColumnName::kConnLimit %
      PgCatalog::PgAuth::ColumnName::kPassword %
      PgCatalog::PgAuth::ColumnName::kValidUntil % column_name.data();

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
 *  @brief  Constructor
 *  @param  (connection)  [in]  a connection to the metadata repository.
 *  @return none.
 */
RolesDAO::RolesDAO(DBSessionManager* session_manager)
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
  role_column_names.emplace(Roles::ROLE_OID,
                            PgCatalog::PgAuth::ColumnName::kOid);
  role_column_names.emplace(Roles::ROLE_ROLNAME,
                            PgCatalog::PgAuth::ColumnName::kName);

  // Creates a list of unique name
  // for the new prepared statement for each column names.
  for (auto column : role_column_names) {
    // Creates unique name for the new prepared statement.
    boost::format statement_name_select =
        boost::format("%1%-%2%-%3%") %
        static_cast<int>(StatementName::ROLES_DAO_SELECT) %
        PgCatalog::PgAuth::kTableName % column.first;

    // Addes this list to unique name for the new prepared statement.
    // key : column name
    // value : unique name for the new prepared statement.
    statement_names_select.emplace(column.first, statement_name_select.str());
  }
}

/**
 *  @brief  Defines all prepared statements.
 *  @param  none.
 *  @return ErrorCode::OK if success, otherwise an error code.
 */
ErrorCode RolesDAO::prepare() const {
  ErrorCode error = ErrorCode::UNKNOWN;

  // Set conditional SQL statement.
  for (auto column : role_column_names) {
    // Set SELECT statement.
    error =
        DbcUtils::prepare(connection_, statement_names_select.at(column.first),
                          statement::select_equal_to(column.second));
    if (error != ErrorCode::OK) {
      return error;
    }
  }

  return error;
}

/**
 * @brief Executes a SELECT statement to get role from the
 *   postgreSQL system catalog,
 *   where the given key equals the given value.
 * @param (object_key)    [in]  key. column name of a role metadata table.
 * @param (object_value)  [in]  value to be filtered.
 * @param (object)        [out] role to get,
 *   where the given key equals the given value.
 * @retval ErrorCode::OK if success.
 * @retval ErrorCode::ID_NOT_FOUND if the role id does not exist.
 * @retval ErrorCode::NAME_NOT_FOUND if the role name does not exist.
 * @retval otherwise an error code.
 */
ErrorCode RolesDAO::select_role_metadata(std::string_view object_key,
                                         std::string_view object_value,
                                         ptree& object) const {
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

    if (nrows == 1) {
      int ordinal_position = 0;
      error = convert_pgresult_to_ptree(res, ordinal_position, object);
    } else if (nrows == 0) {
      // Convert the error code.
      if (object_key == Roles::ROLE_OID) {
        error = ErrorCode::ID_NOT_FOUND;
      } else if (object_key == Roles::ROLE_ROLNAME) {
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
 * @brief Get the role by converting
 *   it from type PGresult to type ptree.
 * @brief Gets the ptree type role metadata
 *   converted from the given PGresult type value.
 * @param (res)               [in]  the result of a query.
 * @param (ordinal_position)  [in]  column ordinal position of PGresult.
 * @param (role)              [out] one role metadata.
 * @return ErrorCode::OK if success, otherwise an error code.
 */
ErrorCode RolesDAO::convert_pgresult_to_ptree(PGresult*& res,
                                              const int ordinal_position,
                                              ptree& role) {
  ErrorCode error = ErrorCode::UNKNOWN;

  // Set the value of the format_version to ptree.
  role.put(Roles::FORMAT_VERSION, Roles::format_version());

  // Set the value of the generation to ptree.
  role.put(Roles::GENERATION, Roles::generation());

  // Set the value of the oid column to ptree.
  role.put(Roles::ROLE_OID,
           PQgetvalue(res, ordinal_position,
                      static_cast<int>(OrdinalPosition::kOid)));

  // Set the value of the rolname column to ptree.
  role.put(Roles::ROLE_ROLNAME,
           PQgetvalue(res, ordinal_position,
                      static_cast<int>(OrdinalPosition::kName)));

  // Set the value of the rolsuper column to ptree.
  role.put(Roles::ROLE_ROLSUPER,
           PQgetvalue(res, ordinal_position,
                      static_cast<int>(OrdinalPosition::kSuper)));

  // Set the value of the rolinherit column to ptree.
  role.put(Roles::ROLE_ROLINHERIT,
           PQgetvalue(res, ordinal_position,
                      static_cast<int>(OrdinalPosition::kInherit)));

  // Set the value of the rolcreaterole column to ptree.
  role.put(Roles::ROLE_ROLCREATEROLE,
           PQgetvalue(res, ordinal_position,
                      static_cast<int>(OrdinalPosition::kCreateRole)));

  // Set the value of the rolcreatedb column to ptree.
  role.put(Roles::ROLE_ROLCREATEDB,
           PQgetvalue(res, ordinal_position,
                      static_cast<int>(OrdinalPosition::kCreateDb)));

  // Set the value of the rolcanlogin column to ptree.
  role.put(Roles::ROLE_ROLCANLOGIN,
           PQgetvalue(res, ordinal_position,
                      static_cast<int>(OrdinalPosition::kCanLogin)));

  // Set the value of the rolreplication column to ptree.
  role.put(Roles::ROLE_ROLREPLICATION,
           PQgetvalue(res, ordinal_position,
                      static_cast<int>(OrdinalPosition::kReplication)));

  // Set the value of the rolbypassrls column to ptree.
  role.put(Roles::ROLE_ROLBYPASSRLS,
           PQgetvalue(res, ordinal_position,
                      static_cast<int>(OrdinalPosition::kBypassRls)));

  // Set the value of the rolconnlimit column to ptree.
  role.put(Roles::ROLE_ROLCONNLIMIT,
           PQgetvalue(res, ordinal_position,
                      static_cast<int>(OrdinalPosition::kConnLimit)));

  // Set the value of the rolpassword column to ptree.
  role.put(Roles::ROLE_ROLPASSWORD,
           PQgetvalue(res, ordinal_position,
                      static_cast<int>(OrdinalPosition::kPassword)));

  // Set the value of the rolvaliduntil column to ptree.
  role.put(Roles::ROLE_ROLVALIDUNTIL,
           PQgetvalue(res, ordinal_position,
                      static_cast<int>(OrdinalPosition::kValidUntil)));

  error = ErrorCode::OK;
  return error;
}

}  // namespace manager::metadata::db::postgresql
