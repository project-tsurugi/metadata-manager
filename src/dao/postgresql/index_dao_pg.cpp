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
#include "manager/metadata/dao/postgresql/index_dao_pg.h"

#include <boost/format.hpp>

#include "manager/metadata/common/message.h"
#include "manager/metadata/common/utility.h"
#include "manager/metadata/dao/postgresql/dbc_utils_pg.h"
#include "manager/metadata/helper/logging_helper.h"
#include "manager/metadata/helper/ptree_helper.h"

namespace manager::metadata::db {

using boost::property_tree::ptree;

// =============================================================================
//  IndexDaoPg class methods.

ErrorCode IndexDaoPg::insert(const boost::property_tree::ptree& object,
                             ObjectIdType& object_id) const {
  ErrorCode error = ErrorCode::UNKNOWN;
  std::vector<const char*> params;

  // format_version
  std::string s_format_version = std::to_string(Indexes::format_version());
  params.emplace_back(s_format_version.c_str());

  // generation
  std::string s_generation = std::to_string(Indexes::generation());
  params.emplace_back(s_generation.c_str());

  // name
  auto name = object.get_optional<std::string>(Index::NAME);
  params.emplace_back((name ? name.value().c_str() : nullptr));

  // namespace
  auto namespace_name = object.get_optional<std::string>(Index::NAMESPACE);
  params.emplace_back(
      (namespace_name ? namespace_name.value().c_str() : nullptr));

  // ownerId
  auto owner_id =
      ptree_helper::ptree_value_to_string<ObjectId>(object, Index::OWNER_ID);
  params.emplace_back(!owner_id.empty() ? owner_id.c_str() : nullptr);

  // acl
  auto acl = object.get_optional<std::string>(Index::ACL);
  params.emplace_back((acl ? acl.value().c_str() : nullptr));

  // tableId
  auto table_id =
      ptree_helper::ptree_value_to_string<ObjectId>(object, Index::TABLE_ID);
  params.emplace_back(!table_id.empty() ? table_id.c_str() : nullptr);

  // accessMethod
  auto access_method = ptree_helper::ptree_value_to_string<int64_t>(
      object, Index::ACCESS_METHOD);
  params.emplace_back(!access_method.empty() ? access_method.c_str() : nullptr);

  // IsUnique
  auto is_unique =
      ptree_helper::ptree_value_to_string<bool>(object, Index::IS_UNIQUE);
  params.emplace_back(!is_unique.empty() ? is_unique.c_str() : nullptr);

  // IsPrimary
  auto is_primary =
      ptree_helper::ptree_value_to_string<bool>(object, Index::IS_PRIMARY);
  params.emplace_back(!is_primary.empty() ? is_primary.c_str() : nullptr);

  // numberOfKeyColumns
  auto number_of_key_column = ptree_helper::ptree_value_to_string<int64_t>(
      object, Index::NUMBER_OF_KEY_COLUMNS);
  params.emplace_back(
      !number_of_key_column.empty() ? number_of_key_column.c_str() : nullptr);

  // columns
  auto o_columns = object.get_child_optional(Index::KEYS);
  std::string columns_json;
  if (o_columns) {
    ptree pt_columns;

    if (o_columns.value().empty()) {
      // Attempt to obtain by numeric.
      auto optional_number = object.get_optional<int64_t>(Index::KEYS);
      if (optional_number) {
        pt_columns = ptree_helper::make_array_ptree({optional_number.value()});
      }
    } else {
      pt_columns = o_columns.value();
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
  auto o_columns_id = object.get_child_optional(Index::KEYS_ID);
  std::string columns_id_json;
  if (o_columns_id) {
    ptree pt_columns_id;

    if (o_columns_id.value().empty()) {
      // Attempt to obtain by numeric.
      auto optional_number = object.get_optional<int64_t>(Index::KEYS_ID);
      if (optional_number) {
        pt_columns_id =
            ptree_helper::make_array_ptree({optional_number.value()});
      }
    } else {
      pt_columns_id = o_columns_id.value();
    }

    // Converts a property_tree to a JSON string.
    error = ptree_helper::ptree_to_json(pt_columns_id, columns_id_json);
    if (error != ErrorCode::OK) {
      return error;
    }
  }
  params.emplace_back(
      (!columns_id_json.empty() ? columns_id_json.c_str() : kEmptyStringJson));

  // options
  auto o_options = object.get_child_optional(Index::OPTIONS);
  std::string options_json;
  if (o_options) {
    ptree pt_options;

    if (o_options.value().empty()) {
      // Attempt to obtain by numeric.
      auto optional_number = object.get_optional<int64_t>(Index::OPTIONS);
      if (optional_number) {
        pt_options = ptree_helper::make_array_ptree({optional_number.value()});
      }
    } else {
      pt_options = o_options.value();
    }

    // Converts a property_tree to a JSON string.
    error = ptree_helper::ptree_to_json(pt_options, options_json);
    if (error != ErrorCode::OK) {
      return error;
    }
  }
  params.emplace_back(
      (!options_json.empty() ? options_json.c_str() : kEmptyStringJson));

  // Set INSERT statement.
  InsertStatement statement;
  try {
    statement = insert_statements_.at(Statement::kDefaultKey);
  } catch (...) {
    LOG_ERROR << Message::INVALID_STATEMENT_KEY << Statement::kDefaultKey;
    return ErrorCode::INVALID_PARAMETER;
  }

  PGresult* res = nullptr;
  // Executes a prepared statement.
  error = DbcUtils::execute_statement(pg_conn_, statement.name(), params, res);
  if (error == ErrorCode::OK) {
    int64_t number_of_tuples = PQntuples(res);
    if (number_of_tuples == 1) {
      // Obtain the object ID of the added metadata object.
      std::string result_value = PQgetvalue(res, kFirstRow, kFirstColumn);
      error = Utility::str_to_numeric(result_value, object_id);
    } else {
      error = ErrorCode::RESULT_MULTIPLE_ROWS;
    }
  }
  PQclear(res);

  return error;
}

ErrorCode IndexDaoPg::select_all(
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
  // Executes a prepared statement
  error = DbcUtils::execute_statement(pg_conn_, statement.name(), params, res);

  if (error == ErrorCode::OK) {
    int64_t number_of_tuples = PQntuples(res);
    if (number_of_tuples >= 0) {
      for (int64_t num = 0; num < number_of_tuples; num++) {
        objects.emplace_back(convert_pgresult_to_ptree(res, num));
      }
    } else {
      error = ErrorCode::INVALID_PARAMETER;
    }
  }
  PQclear(res);

  return error;
}

ErrorCode IndexDaoPg::select(std::string_view key,
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
  // Executes a prepared statement
  error = DbcUtils::execute_statement(pg_conn_, statement.name(), params, res);
  if (error == ErrorCode::OK) {
    object.clear();

    int64_t number_of_tuples = PQntuples(res);
    if (number_of_tuples >= 1) {
      for (int row_number = 0; row_number < number_of_tuples; row_number++) {
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

ErrorCode IndexDaoPg::update(std::string_view key,
                             const std::vector<std::string_view>& values,
                             const boost::property_tree::ptree& object) const {
  ErrorCode error = ErrorCode::UNKNOWN;
  std::vector<const char*> params;

  auto name = object.get_optional<std::string>(Index::NAME);
  params.emplace_back((name ? name.value().c_str() : nullptr));

  auto namespace_name = object.get_optional<std::string>(Index::NAMESPACE);
  params.emplace_back(
      (namespace_name ? namespace_name.value().c_str() : nullptr));

  auto owner_id =
      ptree_helper::ptree_value_to_string<ObjectId>(object, Index::OWNER_ID);
  params.emplace_back(!owner_id.empty() ? owner_id.c_str() : nullptr);

  auto acl = object.get_optional<std::string>(Index::ACL);
  params.emplace_back((acl ? acl.value().c_str() : nullptr));

  auto table_id =
      ptree_helper::ptree_value_to_string<ObjectId>(object, Index::TABLE_ID);
  params.emplace_back(!table_id.empty() ? table_id.c_str() : nullptr);

  auto access_method = ptree_helper::ptree_value_to_string<int64_t>(
      object, Index::ACCESS_METHOD);
  params.emplace_back(!access_method.empty() ? access_method.c_str() : nullptr);

  auto is_unique =
      ptree_helper::ptree_value_to_string<bool>(object, Index::IS_UNIQUE);
  params.emplace_back(!is_unique.empty() ? is_unique.c_str() : nullptr);

  auto is_primary =
      ptree_helper::ptree_value_to_string<bool>(object, Index::IS_PRIMARY);
  params.emplace_back(!is_primary.empty() ? is_primary.c_str() : nullptr);

  auto number_of_key_column = ptree_helper::ptree_value_to_string<int64_t>(
      object, Index::NUMBER_OF_KEY_COLUMNS);
  params.emplace_back(
      !number_of_key_column.empty() ? number_of_key_column.c_str() : nullptr);

  auto columns = object.get_child_optional(Index::KEYS);
  std::string columns_json;
  if (columns) {
    // Converts a property_tree to a JSON string.
    error = ptree_helper::ptree_to_json(columns.value(), columns_json);
    if (error != ErrorCode::OK) {
      return error;
    }
  }
  params.emplace_back((!columns_json.empty() ? columns_json.c_str() : "{}"));

  auto columns_id = object.get_child_optional(Index::KEYS_ID);
  std::string columns_id_json;
  if (columns_id) {
    // Converts a property_tree to a JSON string.
    error = ptree_helper::ptree_to_json(columns_id.value(), columns_id_json);
    if (error != ErrorCode::OK) {
      return error;
    }
  }
  params.emplace_back(
      (!columns_id_json.empty() ? columns_id_json.c_str() : "{}"));

  auto options = object.get_child_optional(Index::OPTIONS);
  std::string options_json;
  if (options) {
    // Converts a property_tree to a JSON string.
    error = ptree_helper::ptree_to_json(options.value(), options_json);
    if (error != ErrorCode::OK) {
      return error;
    }
  }
  params.emplace_back((!options_json.empty() ? options_json.c_str() : "{}"));

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
  // Executes a prepared statement
  error = DbcUtils::execute_statement(pg_conn_, statement.name(), params, res);

  if (error == ErrorCode::OK) {
    uint64_t number_of_rows_affected = 0;
    ErrorCode error_get =
        DbcUtils::get_number_of_rows_affected(res, number_of_rows_affected);
    if (error_get != ErrorCode::OK) {
      error = error_get;
    } else if (number_of_rows_affected == 0) {
      // Not found.
      error = Dao::get_not_found_error_code(key);
    }
  }
  PQclear(res);

  return error;
}

ErrorCode IndexDaoPg::remove(std::string_view key,
                             const std::vector<std::string_view>& values,
                             ObjectIdType& object_id) const {
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
  // Executes a prepared statement
  error = DbcUtils::execute_statement(pg_conn_, statement.name(), params, res);

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
      // Not found.
      error = Dao::get_not_found_error_code(key);
    }
  }
  PQclear(res);

  return error;
}

/* =============================================================================
 * Private method area
 */

std::string IndexDaoPg::get_insert_statement() const {
  boost::format query =
      boost::format(
          "INSERT INTO %1%.%2%"
          " (%3%, %4%, %5%, %6%, %7%, %8%, %9%, %10%, %11%, %12%, %13%, %14%,"
          " %15%, %16%)"
          " VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13,"
          " $14)"
          " RETURNING %17%") %
      kSchemaTsurugiCatalog % kTableName % ColumnName::kFormatVersion %
      ColumnName::kGeneration % ColumnName::kName % ColumnName::kNamespace %
      ColumnName::kOwnerId % ColumnName::kAcl % ColumnName::kTableId %
      ColumnName::kAccessMethod % ColumnName::kIsUnique %
      ColumnName::kIsPrimary % ColumnName::kNumKeyColumn %
      ColumnName::kColumns % ColumnName::kColumnsId % ColumnName::kOptions %
      ColumnName::kId;

  return query.str();
}

std::string IndexDaoPg::get_select_all_statement() const {
  // SQL statement
  boost::format query =
      boost::format(
          "SELECT %3%, %4%, %5%, %6%, %7%, %8%, %9%, %10%, %11%, %12%, %13%, "
          "%14%, %15%, %16%, %17%"
          " FROM %1%.%2%"
          " ORDER BY %5%") %
      kSchemaTsurugiCatalog % kTableName % ColumnName::kFormatVersion %
      ColumnName::kGeneration % ColumnName::kId % ColumnName::kName %
      ColumnName::kNamespace % ColumnName::kOwnerId % ColumnName::kAcl %
      ColumnName::kTableId % ColumnName::kAccessMethod % ColumnName::kIsUnique %
      ColumnName::kIsPrimary % ColumnName::kNumKeyColumn %
      ColumnName::kColumns % ColumnName::kColumnsId % ColumnName::kOptions;

  return query.str();
}

std::string IndexDaoPg::get_select_statement(std::string_view key) const {
  boost::format query =
      boost::format(
          "SELECT %3%, %4%, %5%, %6%, %7%, %8%, %9%, %10%, %11%, %12%, %13%, "
          "%14%, %15%, %16%, %17%"
          " FROM %1%.%2%"
          " WHERE %18% = $1"
          " ORDER BY %5%") %
      kSchemaTsurugiCatalog % kTableName % ColumnName::kFormatVersion %
      ColumnName::kGeneration % ColumnName::kId % ColumnName::kName %
      ColumnName::kNamespace % ColumnName::kOwnerId % ColumnName::kAcl %
      ColumnName::kTableId % ColumnName::kAccessMethod % ColumnName::kIsUnique %
      ColumnName::kIsPrimary % ColumnName::kNumKeyColumn %
      ColumnName::kColumns % ColumnName::kColumnsId % ColumnName::kOptions %
      key;

  return query.str();
}

std::string IndexDaoPg::get_update_statement(std::string_view key) const {
  boost::format query = boost::format(
                            "UPDATE %1%.%2%"
                            " SET %3% = $1, %4% = $2, %5% = $3, %6% = $4, %7% "
                            "= $5, %8% = $6, %9% = $7, %10% = $8,"
                            " %11% = $9, %12% = $10, %13% = $11, %14% = $12"
                            " WHERE %15% = $13") %
                        kSchemaTsurugiCatalog % kTableName % ColumnName::kName %
                        ColumnName::kNamespace % ColumnName::kOwnerId %
                        ColumnName::kAcl % ColumnName::kTableId %
                        ColumnName::kAccessMethod % ColumnName::kIsUnique %
                        ColumnName::kIsPrimary % ColumnName::kNumKeyColumn %
                        ColumnName::kColumns % ColumnName::kColumnsId %
                        ColumnName::kOptions % key;

  return query.str();
}

std::string IndexDaoPg::get_delete_statement(std::string_view key) const {
  boost::format query = boost::format(
                            "DELETE FROM %1%.%2%"
                            " WHERE %3% = $1"
                            " RETURNING %4%") %
                        kSchemaTsurugiCatalog % kTableName % key.data() %
                        ColumnName::kId;

  return query.str();
}

boost::property_tree::ptree IndexDaoPg::convert_pgresult_to_ptree(
    const PGresult* pg_result, const int row_number) const {
  boost::property_tree::ptree object;

  object.put(
      Object::FORMAT_VERSION,
      get_result_value(pg_result, row_number, OrdinalPosition::kFormatVersion));

  object.put(
      Object::GENERATION,
      get_result_value(pg_result, row_number, OrdinalPosition::kGeneration));

  object.put(Object::ID,
             get_result_value(pg_result, row_number, OrdinalPosition::kId));

  object.put(Object::NAME,
             get_result_value(pg_result, row_number, OrdinalPosition::kName));

  object.put(Index::NAMESPACE, get_result_value(pg_result, row_number,
                                                OrdinalPosition::kNamespace));

  object.put(Index::OWNER_ID, get_result_value(pg_result, row_number,
                                               OrdinalPosition::kOwnerId));

  object.put(Index::ACL,
             get_result_value(pg_result, row_number, OrdinalPosition::kAcl));

  object.put(Index::TABLE_ID, get_result_value(pg_result, row_number,
                                               OrdinalPosition::kTableId));

  object.put(
      Index::ACCESS_METHOD,
      get_result_value(pg_result, row_number, OrdinalPosition::kAccessMethod));

  object.put(Index::IS_UNIQUE,
             get_result_value<bool>(pg_result, row_number,
                                    OrdinalPosition::kIsUnique));

  object.put(Index::IS_PRIMARY,
             get_result_value<bool>(pg_result, row_number,
                                    OrdinalPosition::kIsPrimary));

  object.put(
      Index::NUMBER_OF_KEY_COLUMNS,
      get_result_value(pg_result, row_number, OrdinalPosition::kNumKeyColumn));

  ptree columns;
  // Converts a JSON string to a property_tree.
  // Set the boolean value converted to a string to property_tree.
  ptree_helper::json_to_ptree(
      get_result_value(pg_result, row_number, OrdinalPosition::kColumns),
      columns);
  object.add_child(Index::KEYS, columns);

  ptree columns_id;
  // Converts a JSON string to a property_tree.
  ptree_helper::json_to_ptree(
      get_result_value(pg_result, row_number, OrdinalPosition::kColumnsId),
      columns_id);
  object.add_child(Index::KEYS_ID, columns_id);

  ptree options;
  // Converts a JSON string to a property_tree.
  ptree_helper::json_to_ptree(
      get_result_value(pg_result, row_number, OrdinalPosition::kOptions),
      options);
  object.add_child(Index::OPTIONS, options);

  return object;
}

}  // namespace manager::metadata::db
