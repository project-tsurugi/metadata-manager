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
#include "manager/metadata/dao/postgresql/datatypes_dao_pg.h"

#include <boost/format.hpp>

#include "manager/metadata/common/message.h"
#include "manager/metadata/dao/postgresql/dbc_utils_pg.h"
#include "manager/metadata/helper/logging_helper.h"

// =============================================================================
namespace manager::metadata::db {

using boost::property_tree::ptree;

ErrorCode DataTypesDaoPg::select(std::string_view key,
                                 const std::vector<std::string_view>& values,
                                 boost::property_tree::ptree& object) const {
  ErrorCode error = ErrorCode::UNKNOWN;

  std::vector<const char*> params;
  // Set key value.
  std::transform(values.begin(), values.end(),
                  std::back_inserter(params),
                  [](std::string_view value) {
                    return value.data();
                  });

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
    int nrows = PQntuples(res);
    if (nrows == 1) {
      object = convert_pgresult_to_ptree(res, kFirstRow);
    } else if (nrows == 0) {
      // Get a NOT_FOUND error code corresponding to the key.
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

/**
 * @brief Get a SELECT statement to retrieve metadata matching the criteria
 *   from the metadata table.
 * @param key  [in]  column name of metadata table.
 * @return SELECT statement.
 */
std::string DataTypesDaoPg::get_select_statement(std::string_view key) const {
  // SQL statement
  boost::format query = boost::format(
                            "SELECT %3%, %4%, %5%, %6%, %7%, %8%, %9%"
                            " FROM %1%.%2%"
                            " WHERE %10% = $1") %
                        kSchemaTsurugiCatalog % kTableName %
                        ColumnName::kFormatVersion % ColumnName::kGeneration %
                        ColumnName::kId % ColumnName::kName %
                        ColumnName::kPgDataType % ColumnName::kPgDataTypeName %
                        ColumnName::kPgDataTypeQualifiedName % key;

  return query.str();
}

boost::property_tree::ptree DataTypesDaoPg::convert_pgresult_to_ptree(
    const PGresult* pg_result, const int row_number) const {
  boost::property_tree::ptree object;

  // Set the value of the format_version column to ptree.
  object.put(
      DataTypes::FORMAT_VERSION,
      get_result_value(pg_result, row_number, OrdinalPosition::kFormatVersion));

  // Set the value of the generation column to ptree.
  object.put(
      DataTypes::GENERATION,
      get_result_value(pg_result, row_number, OrdinalPosition::kGeneration));

  // Set the value of the id to ptree.
  object.put(DataTypes::ID,
             get_result_value(pg_result, row_number, OrdinalPosition::kId));

  // Set the value of the name column to ptree.
  object.put(DataTypes::NAME,
             get_result_value(pg_result, row_number, OrdinalPosition::kName));

  // Set the value of the pg_data_type column to ptree.
  object.put(
      DataTypes::PG_DATA_TYPE,
      get_result_value(pg_result, row_number, OrdinalPosition::kPgDataType));

  // Set the value of the pg_data_type_name column to ptree.
  object.put(DataTypes::PG_DATA_TYPE_NAME,
             get_result_value(pg_result, row_number,
                              OrdinalPosition::kPgDataTypeName));

  // Set the value of the pg_data_type_qualified_name column to ptree.
  object.put(DataTypes::PG_DATA_TYPE_QUALIFIED_NAME,
             get_result_value(pg_result, row_number,
                              OrdinalPosition::kPgDataTypeQualifiedName));

  return object;
}

}  // namespace manager::metadata::db
