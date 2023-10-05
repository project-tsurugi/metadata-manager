/*
 * Copyright 2020-2023 Project Tsurugi.
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
#include "manager/metadata/helper/logging_helper.h"

// =============================================================================
namespace manager::metadata::db {

using boost::property_tree::ptree;

ErrorCode DataTypesDaoPg::select(
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

void DataTypesDaoPg::create_prepared_statements() {
  DaoPg::create_prepared_statements();

  // SELECT statement with pg_dataType specified.
  SelectStatement statement_type{
      this->get_source_name(),
      this->get_select_statement(ColumnName::kPgDataType),
      DataType::PG_DATA_TYPE};
  select_statements_.emplace(DataType::PG_DATA_TYPE, statement_type);

  // SELECT statement with pg_dataTypeName specified.
  SelectStatement statement_type_name{
      this->get_source_name(),
      this->get_select_statement(ColumnName::kPgDataTypeName),
      DataType::PG_DATA_TYPE_NAME};
  select_statements_.emplace(DataType::PG_DATA_TYPE_NAME, statement_type_name);

  // SELECT statement with pg_data_type_qualified_name specified.
  SelectStatement statement_qualified{
      this->get_source_name(),
      this->get_select_statement(ColumnName::kPgDataTypeQualifiedName),
      DataType::PG_DATA_TYPE_QUALIFIED_NAME};
  select_statements_.emplace(DataType::PG_DATA_TYPE_QUALIFIED_NAME,
                             statement_qualified);
}

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
