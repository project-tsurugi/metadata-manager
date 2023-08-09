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
#include "manager/metadata/dao/postgresql/tables_dao_pg.h"

#include <regex>

#include <boost/format.hpp>

#include "manager/metadata/common/message.h"
#include "manager/metadata/common/utility.h"
#include "manager/metadata/dao/postgresql/dbc_utils_pg.h"
#include "manager/metadata/helper/logging_helper.h"
#include "manager/metadata/helper/ptree_helper.h"

// =============================================================================
namespace manager::metadata::db {

using boost::property_tree::ptree;

ErrorCode TablesDaoPg::insert(const boost::property_tree::ptree& object,
                              ObjectId& object_id) const {
  ErrorCode error = ErrorCode::UNKNOWN;

  std::vector<const char*> params;
  // format_version
  std::string s_format_version = std::to_string(Tables::format_version());
  params.emplace_back(s_format_version.c_str());

  // generation
  std::string s_generation = std::to_string(Tables::generation());
  params.emplace_back(s_generation.c_str());

  // name
  auto name =
      ptree_helper::ptree_value_to_string<std::string>(object, Table::NAME);
  params.emplace_back(name.c_str());

  // namespace
  auto namespace_name = ptree_helper::ptree_value_to_string<std::string>(
      object, Table::NAMESPACE);
  params.emplace_back(namespace_name.c_str());

  // number_of_tuples
  auto reltuples = ptree_helper::ptree_value_to_string<std::string>(
      object, Table::NUMBER_OF_TUPLES);
  params.emplace_back((!reltuples.empty() ? reltuples.c_str() : nullptr));

  // Set INSERT statement.
  InsertStatement statement;
  try {
    statement = insert_statements_.at(Statement::kDefaultKey);
  } catch (...) {
    LOG_ERROR << Message::INVALID_STATEMENT_KEY << Statement::kDefaultKey;
    return ErrorCode::INVALID_PARAMETER;
  }

  PGresult* res = nullptr;
  // Execute a prepared statement.
  error = DbcUtils::exec_prepared(pg_conn_, statement.name(), params, res);

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

ErrorCode TablesDaoPg::select_all(
    std::vector<boost::property_tree::ptree>& objects) const {
  ErrorCode error = ErrorCode::UNKNOWN;
  std::vector<const char*> params;

  // Set SELECT-all statement.
  SelectAllStatement statement;
  try {
    statement = select_all_statements_.at(Statement::kDefaultKey);
  } catch (...) {
    LOG_ERROR << Message::INVALID_STATEMENT_KEY << Statement::kDefaultKey;
    return ErrorCode::INVALID_PARAMETER;
  }

  PGresult* res = nullptr;
  // Execute a prepared statement.
  error = DbcUtils::exec_prepared(pg_conn_, statement.name(), params, res);

  if (error == ErrorCode::OK) {
    int nrows = PQntuples(res);
    if (nrows >= 0) {
      for (int row_number = 0; row_number < nrows; row_number++) {
        objects.emplace_back(convert_pgresult_to_ptree(res, row_number));
      }
    } else {
      error = ErrorCode::INVALID_PARAMETER;
    }
  }

  PQclear(res);
  return error;
}

ErrorCode TablesDaoPg::select(std::string_view key,
                              const std::vector<std::string_view>& values,
                              boost::property_tree::ptree& object) const {
  ErrorCode error = ErrorCode::UNKNOWN;

  std::vector<const char*> params;
  // Set key value.
  std::transform(values.begin(), values.end(), std::back_inserter(params),
                 [](std::string_view value) { return value.data(); });

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
  error = DbcUtils::exec_prepared(pg_conn_, statement.name(), params, res);

  if (error == ErrorCode::OK) {
    object.clear();

    int nrows = PQntuples(res);
    if (nrows >= 1) {
      for (int row_number = 0; row_number < nrows; row_number++) {
        // Convert acquired data to ptree type.
        object.push_back(
            std::make_pair("", convert_pgresult_to_ptree(res, row_number)));
      }
      error = ErrorCode::OK;
    } else {
      // Get a NOT_FOUND error code corresponding to the key.
      error = get_not_found_error_code(key);
    }
  }
  PQclear(res);

  return error;
}

ErrorCode TablesDaoPg::update(std::string_view key,
                              const std::vector<std::string_view>& values,
                              const boost::property_tree::ptree& object) const {
  ErrorCode error = ErrorCode::UNKNOWN;
  std::vector<const char*> params;

  // name
  auto name =
      ptree_helper::ptree_value_to_string<std::string>(object, Table::NAME);
  params.emplace_back(name.c_str());

  // namespace
  auto namespace_name = ptree_helper::ptree_value_to_string<std::string>(
      object, Table::NAMESPACE);
  params.emplace_back(namespace_name.c_str());

  // number_of_tuples
  auto reltuples = ptree_helper::ptree_value_to_string<std::string>(
      object, Table::NUMBER_OF_TUPLES);
  params.emplace_back((!reltuples.empty() ? reltuples.c_str() : nullptr));

  // Set key value.
  std::transform(values.begin(), values.end(), std::back_inserter(params),
                 [](std::string_view value) { return value.data(); });

  // Set UPDATE statement.
  UpdateStatement statement;
  try {
    statement = update_statements_.at(key.data());
  } catch (...) {
    LOG_ERROR << Message::INVALID_STATEMENT_KEY << key;
    return ErrorCode::INVALID_PARAMETER;
  }

  PGresult* res = nullptr;
  // Execute a prepared statement.
  error = DbcUtils::exec_prepared(pg_conn_, statement.name(), params, res);

  if (error == ErrorCode::OK) {
    uint64_t number_of_rows_affected = 0;
    ErrorCode error_get =
        DbcUtils::get_number_of_rows_affected(res, number_of_rows_affected);

    if (error_get != ErrorCode::OK) {
      error = error_get;
    } else if (number_of_rows_affected == 0) {
      // Convert the error code.
      error = get_not_found_error_code(key);
    }
  }
  PQclear(res);

  return error;
}

ErrorCode TablesDaoPg::remove(std::string_view key,
                              const std::vector<std::string_view>& values,
                              ObjectId& object_id) const {
  ErrorCode error = ErrorCode::UNKNOWN;

  std::vector<const char*> params;
  // Set key value.
  std::transform(values.begin(), values.end(), std::back_inserter(params),
                 [](std::string_view value) { return value.data(); });

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
  error = DbcUtils::exec_prepared(pg_conn_, statement.name(), params, res);

  if (error == ErrorCode::OK) {
    uint64_t number_of_rows_affected = 0;
    ErrorCode error_get =
        DbcUtils::get_number_of_rows_affected(res, number_of_rows_affected);

    if (error_get != ErrorCode::OK) {
      error = error_get;
    } else if (number_of_rows_affected == 1) {
      // Obtain the object ID of the added metadata object.
      std::string result_value = PQgetvalue(res, kFirstRow, kFirstColumn);
      error = Utility::str_to_numeric(result_value, object_id);
    } else if (number_of_rows_affected == 0) {
      // Convert the error code.
      error = get_not_found_error_code(key);
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

std::string TablesDaoPg::get_insert_statement() const {
  // SQL statement
  boost::format query = boost::format(
                            "INSERT INTO %1%.%2% (%3%, %4%, %5%, %6%, %7%)"
                            " VALUES ($1, $2, $3, $4, $5)"
                            " RETURNING %8%") %
                        kSchemaTsurugiCatalog % kTableName %
                        ColumnName::kFormatVersion % ColumnName::kGeneration %
                        ColumnName::kName % ColumnName::kNamespace %
                        ColumnName::kTuples % ColumnName::kId;

  return query.str();
}

std::string TablesDaoPg::get_select_all_statement() const {
  // SQL statement
  boost::format query =
      boost::format(
          "SELECT"
          " tbl.%3%, tbl.%4%, tbl.%5%, tbl.%6%, tbl.%7%, tbl.%8%,"
          " cls.%10%, cls.%11%"
          " FROM %1%.%2% tbl LEFT JOIN %9% cls ON (tbl.%6% = cls.%12%)"
          " ORDER BY %5%") %
      kSchemaTsurugiCatalog % kTableName % ColumnName::kFormatVersion %
      ColumnName::kGeneration % ColumnName::kId % ColumnName::kName %
      ColumnName::kNamespace % ColumnName::kTuples %
      PgCatalog::PgClass::kTableName % PgCatalog::PgClass::ColumnName::kOwner %
      PgCatalog::PgClass::ColumnName::kAcl %
      PgCatalog::PgClass::ColumnName::kName;

  return query.str();
}

std::string TablesDaoPg::get_select_statement(std::string_view key) const {
  // SQL statement
  boost::format query =
      boost::format(
          "SELECT"
          " tbl.%3%, tbl.%4%, tbl.%5%, tbl.%6%, tbl.%7%, tbl.%8%"
          " , cls.%10%, cls.%11%"
          " FROM %1%.%2% tbl LEFT JOIN %9% cls ON (tbl.%6% = cls.%12%)"
          " WHERE tbl.%13% = $1") %
      kSchemaTsurugiCatalog % kTableName % ColumnName::kFormatVersion %
      ColumnName::kGeneration % ColumnName::kId % ColumnName::kName %
      ColumnName::kNamespace % ColumnName::kTuples %
      PgCatalog::PgClass::kTableName % PgCatalog::PgClass::ColumnName::kOwner %
      PgCatalog::PgClass::ColumnName::kAcl %
      PgCatalog::PgClass::ColumnName::kName % key;

  return query.str();
}

std::string TablesDaoPg::get_update_statement(std::string_view key) const {
  // SQL statement
  boost::format query = boost::format(
                            "UPDATE %1%.%2%"
                            " SET %3% = $1, %4% = $2, %5% = $3"
                            " WHERE %6% = $4") %
                        kSchemaTsurugiCatalog % kTableName % ColumnName::kName %
                        ColumnName::kNamespace % ColumnName::kTuples % key;

  return query.str();
}

std::string TablesDaoPg::get_delete_statement(std::string_view key) const {
  // SQL statement
  boost::format query =
      boost::format("DELETE FROM %1%.%2% WHERE %3% = $1 RETURNING %4%") %
      kSchemaTsurugiCatalog % kTableName % key % ColumnName::kId;

  return query.str();
}

boost::property_tree::ptree TablesDaoPg::convert_pgresult_to_ptree(
    const PGresult* pg_result, const int row_number) const {
  ErrorCode error = ErrorCode::UNKNOWN;
  boost::property_tree::ptree object;

  // Set the value of the format_version column to ptree.
  object.put(
      Table::FORMAT_VERSION,
      get_result_value(pg_result, row_number, OrdinalPosition::kFormatVersion));

  // Set the value of the generation column to ptree.
  object.put(Table::GENERATION, get_result_value(pg_result, row_number,
                                                 OrdinalPosition::kGeneration));

  // Set the value of the id column to ptree.
  object.put(Table::ID,
             get_result_value(pg_result, row_number, OrdinalPosition::kId));

  // Set the value of the name column to ptree.
  object.put(Table::NAME,
             get_result_value(pg_result, row_number, OrdinalPosition::kName));

  // Set the value of the namespace column to ptree.
  object.put(Table::NAMESPACE, get_result_value(pg_result, row_number,
                                                OrdinalPosition::kNamespace));

  // Set the value of the number of tuples column to ptree.
  auto tuples =
      get_result_value(pg_result, row_number, OrdinalPosition::kTuples);
  object.put(Table::NUMBER_OF_TUPLES, (tuples.empty() ? "0" : tuples.c_str()));

  // Set the value of the owner_role_id column to ptree.
  object.put(
      Table::OWNER_ROLE_ID,
      get_result_value(pg_result, row_number, OrdinalPosition::kOwnerRoleId));

  // Set the value of the acl column to ptree.
  auto acl_db_array =
      get_result_value(pg_result, row_number, OrdinalPosition::kAcl);
  std::regex regex("[{}]");
  acl_db_array = std::regex_replace(acl_db_array, regex, "");

  // Convert aclitem[] to ptree.
  ptree ptree_acls;
  for (const auto acl : Utility::split(acl_db_array, ',')) {
    ptree p_value;
    p_value.put("", acl);
    ptree_acls.push_back(std::make_pair("", p_value));
  }
  // NOTICE:
  //   If it is not set, MUST add an empty ptree.
  //   ogawayama-server read key Table::ACL.
  object.add_child(Table::ACL, ptree_acls);

  return object;
}

}  // namespace manager::metadata::db
