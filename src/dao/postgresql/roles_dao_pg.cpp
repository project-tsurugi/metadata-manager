/*
 * Copyright 2021-2023 Project Tsurugi.
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
#include "manager/metadata/dao/postgresql/roles_dao_pg.h"

#include <boost/format.hpp>

#include "manager/metadata/common/message.h"
#include "manager/metadata/helper/logging_helper.h"

// =============================================================================
namespace manager::metadata::db {

using boost::property_tree::ptree;

ErrorCode RolesDaoPg::select(
    const std::map<std::string_view, std::string_view>& keys,
    boost::property_tree::ptree& object) const {
  ErrorCode error = ErrorCode::UNKNOWN;

  if (keys.empty()) {
    LOG_ERROR << Message::INVALID_STATEMENT_KEY << "Keys is empty.";
    error = ErrorCode::INVALID_PARAMETER;
    return error;
  }

  const auto& it = keys.begin();
  // Set SELECT statement.
  SelectStatement statement;
  try {
    statement = select_statements_.at(it->first.data());
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

/* =============================================================================
 * Private method area
 */

void RolesDaoPg::create_prepared_statements() {
  // SELECT statements with oid specified.
  SelectStatement select_statement_oid{
      this->get_source_name(),
      this->get_select_statement(PgCatalog::PgAuth::ColumnName::kOid),
      PgCatalog::PgAuth::ColumnName::kOid};
  select_statements_.emplace(Roles::ROLE_OID, select_statement_oid);

  // SELECT statements with name specified.
  SelectStatement select_statement_name{
      this->get_source_name(),
      this->get_select_statement(PgCatalog::PgAuth::ColumnName::kName),
      PgCatalog::PgAuth::ColumnName::kName};
  select_statements_.emplace(Roles::ROLE_ROLNAME, select_statement_name);
}

std::string RolesDaoPg::get_select_statement(std::string_view key) const {
  // SQL statement
  boost::format query = boost::format(
                            "SELECT %2%, %3%, %4%, %5%, %6%, %7%, %8%, %9%"
                            " , %10%, %11%, %12%, %13%"
                            " FROM %1%"
                            " WHERE %14% = $1") %
                        PgCatalog::PgAuth::kTableName %
                        PgCatalog::PgAuth::ColumnName::kOid %
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
                        PgCatalog::PgAuth::ColumnName::kValidUntil % key;

  return query.str();
}

boost::property_tree::ptree RolesDaoPg::convert_pgresult_to_ptree(
    const PGresult* pg_result, const int row_number) const {
  boost::property_tree::ptree object;

  // Set the value of the format_version to ptree.
  object.put(Roles::FORMAT_VERSION, Roles::format_version());

  // Set the value of the generation to ptree.
  object.put(Roles::GENERATION, Roles::generation());

  // Set the value of the oid column to ptree.
  object.put(Roles::ROLE_OID,
             get_result_value(pg_result, row_number, OrdinalPosition::kOid));

  // Set the value of the rolname column to ptree.
  object.put(Roles::ROLE_ROLNAME,
             get_result_value(pg_result, row_number, OrdinalPosition::kName));

  // Set the value of the rolsuper column to ptree.
  object.put(
      Roles::ROLE_ROLSUPER,
      get_result_value<bool>(pg_result, row_number, OrdinalPosition::kSuper));

  // Set the value of the rolinherit column to ptree.
  object.put(
      Roles::ROLE_ROLINHERIT,
      get_result_value<bool>(pg_result, row_number, OrdinalPosition::kInherit));

  // Set the value of the rolcreaterole column to ptree.
  object.put(Roles::ROLE_ROLCREATEROLE,
             get_result_value<bool>(pg_result, row_number,
                                    OrdinalPosition::kCreateRole));

  // Set the value of the rolcreatedb column to ptree.
  object.put(Roles::ROLE_ROLCREATEDB,
             get_result_value<bool>(pg_result, row_number,
                                    OrdinalPosition::kCreateDb));

  // Set the value of the rolcanlogin column to ptree.
  object.put(Roles::ROLE_ROLCANLOGIN,
             get_result_value<bool>(pg_result, row_number,
                                    OrdinalPosition::kCanLogin));

  // Set the value of the rolreplication column to ptree.
  object.put(Roles::ROLE_ROLREPLICATION,
             get_result_value<bool>(pg_result, row_number,
                                    OrdinalPosition::kReplication));

  // Set the value of the rolbypassrls column to ptree.
  object.put(Roles::ROLE_ROLBYPASSRLS,
             get_result_value<bool>(pg_result, row_number,
                                    OrdinalPosition::kBypassRls));

  // Set the value of the rolconnlimit column to ptree.
  object.put(
      Roles::ROLE_ROLCONNLIMIT,
      get_result_value(pg_result, row_number, OrdinalPosition::kConnLimit));

  // Set the value of the rolpassword column to ptree.
  object.put(
      Roles::ROLE_ROLPASSWORD,
      get_result_value(pg_result, row_number, OrdinalPosition::kPassword));

  // Set the value of the rolvaliduntil column to ptree.
  object.put(
      Roles::ROLE_ROLVALIDUNTIL,
      get_result_value(pg_result, row_number, OrdinalPosition::kValidUntil));

  return object;
}

}  // namespace manager::metadata::db
