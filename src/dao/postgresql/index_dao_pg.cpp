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
#include "manager/metadata/dao/postgresql/index_dao_pg.h"

#include <libpq-fe.h>

#include <iostream>
#include <regex>
#include <string>
#include <string_view>

#include <boost/format.hpp>

#include "manager/metadata/common/utility.h"
#include "manager/metadata/dao/postgresql/common_pg.h"
#include "manager/metadata/dao/postgresql/dbc_utils_pg.h"
#include "manager/metadata/helper/logging_helper.h"
#include "manager/metadata/helper/ptree_helper.h"
#include "manager/metadata/indexes.h"

namespace manager::metadata::db {

using boost::property_tree::ptree;
using manager::metadata::db::postgresql::DbcUtils;

// =============================================================================
//  IndexDaoPg class methods.
/**
 * @brief Returns an INSERT statement for table metadata.
 * @param none.
 * @return an INSERT statement to insert table metadata.
 */
std::string IndexDaoPg::get_insert_statement() const {
  boost::format query =
      boost::format(
          "INSERT INTO %1%.%2%"
          " (%3%, %4%, %5%, %6%, %7%, %8%, %9%, %10%, %11%, %12%, %13%, %14%, %15%, %16%)"
          " VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14)"
          " RETURNING %17%") %
      SCHEMA_TSURUGI_CATALOG % this->get_source_name() %
      Column::kFormatVersion % Column::kGeneration % Column::kName %
      Column::kNamespace % Column::kOwnerId % Column::kAcl % Column::kTableId %
      Column::kAccessMethod % Column::kIsUnique % Column::kIsPrimary %
      Column::kNumKeyColumn % Column::kColumns % Column::kColumnsId %
      Column::kOptions % Column::kId;

  return query.str();
}

/**
 * @brief Returns a SELECT statement to get metadata:
 *   select * from table_name.
 * @return a SELECT statement:
 *   select * from table_name.
 */
std::string IndexDaoPg::get_select_all_statement() const {
  // SQL statement
  boost::format query =
      boost::format(
          "SELECT %3%, %4%, %5%, %6%, %7%, %8%, %9%, %10%, %11%, %12%, %13%, %14%, %15%, %16%, %17%"
          " FROM %1%.%2%"
          " ORDER BY %5%") %
      SCHEMA_TSURUGI_CATALOG % this->get_source_name() %
      Column::kFormatVersion % Column::kGeneration % Column::kId %
      Column::kName % Column::kNamespace % Column::kOwnerId % Column::kAcl %
      Column::kTableId % Column::kAccessMethod % Column::kIsUnique %
      Column::kIsPrimary % Column::kNumKeyColumn % Column::kColumns %
      Column::kColumnsId % Column::kOptions;

  return query.str();
}

/**
 * @brief Returns a SELECT statement to get metadata:
 *   select * from table_name where column_name = $1.
 * @param key  [in]  column name of metadata-table.
 * @return a SELECT statement:
 *   select * from table_name where column_name = $1.
 */
std::string IndexDaoPg::get_select_statement(std::string_view key) const {
  boost::format query =
      boost::format(
          "SELECT %3%, %4%, %5%, %6%, %7%, %8%, %9%, %10%, %11%, %12%, %13%, %14%, %15%, %16%, %17%"
          " FROM %1%.%2%"
          " WHERE %18% = $1"
          " ORDER BY %5%") %
      SCHEMA_TSURUGI_CATALOG % this->get_source_name() %
      Column::kFormatVersion % Column::kGeneration % Column::kId %
      Column::kName % Column::kNamespace % Column::kOwnerId % Column::kAcl %
      Column::kTableId % Column::kAccessMethod % Column::kIsUnique %
      Column::kIsPrimary % Column::kNumKeyColumn % Column::kColumns %
      Column::kColumnsId % Column::kOptions % key.data();

  return query.str();
}

/**
 * @brief Returns an UPDATE statement for table metadata.
 * @param none.
 * @return an UPDATE statement to insert table metadata.
 */
std::string IndexDaoPg::get_update_statement(std::string_view key) const {
  boost::format query =
      boost::format(
          "UPDATE %1%.%2%"
          " SET %3% = $1, %4% = $2, %5% = $3, %6% = $4, %7% = $5, %8% = $6, %9% = $7, %10% = $8,"
          " %11% = $9, %12% = $10, %13% = $11, %14% = $12"
          " WHERE %15% = $13") %
      SCHEMA_TSURUGI_CATALOG % this->get_source_name() %
      Column::kName % Column::kNamespace % Column::kOwnerId % Column::kAcl %
      Column::kTableId % Column::kAccessMethod % Column::kIsUnique %
      Column::kIsPrimary % Column::kNumKeyColumn % Column::kColumns %
      Column::kColumnsId % Column::kOptions % key.data();

  return query.str();
}

/**
 * @brief Returns a SELECT statement to get metadata:
 *   delete from table_name where column_name = $1.
 * @param (column_name)  [in]  column name of metadata-table.
 * @return a DELETE statement:
 *   delete from table_name where column_name = $1.
 */
std::string IndexDaoPg::get_delete_statement(std::string_view key) const {
  boost::format query = boost::format(
                            "DELETE FROM %1%.%2%"
                            " WHERE %3% = $1"
                            " RETURNING %4%") %
                        SCHEMA_TSURUGI_CATALOG % this->get_source_name() %
                        key.data() % Column::kId;

  return query.str();
}

/**
 * @brief
 */
void IndexDaoPg::create_prepared_statements() {

  // INSERT statements
  insert_statement_.set(this->get_source_name(),
                        this->get_insert_statement());

  // SELECT statements
  select_all_statement_.set(this->get_source_name().data(),
                            this->get_select_all_statement());

  SelectStatement select_by_id_statement{
      this->get_source_name(),
      this->get_select_statement(Object::ID),
      Object::ID};
  select_statements_.emplace(Object::ID, select_by_id_statement );

  SelectStatement select_by_name_statement{
      this->get_source_name(),
      this->get_select_statement(Object::NAME),
      Object::NAME};
  select_statements_.emplace(Object::NAME, select_by_name_statement);

  // UPDATE statements
  UpdateStatement update_by_id_statement{
      this->get_source_name(),
      this->get_update_statement(Object::ID),
      Object::ID};
  update_statements_.emplace(Object::ID, update_by_id_statement);

  UpdateStatement update_by_name_statement{
      this->get_source_name(),
      this->get_update_statement(Object::NAME),
      Object::NAME};
  update_statements_.emplace(Object::NAME, update_by_name_statement);

  // DELETE statements
  DeleteStatement delete_by_id_statement{
      this->get_source_name(),
      this->get_delete_statement(Object::ID),
      Object::ID};
  delete_statements_.emplace(Object::ID, delete_by_id_statement);

  DeleteStatement delete_by_name_statement{
      this->get_source_name(),
      this->get_delete_statement(Object::NAME),
      Object::NAME};
  delete_statements_.emplace(Object::NAME, delete_by_name_statement);
}

/**
 * @brief Defines all prepared statements.
 * @param none.
 * @return ErrorCode::OK if success, otherwise an error code.
 */
ErrorCode IndexDaoPg::prepare() {

  ErrorCode error = ErrorCode::UNKNOWN;

  this->create_prepared_statements();

  // Set prepared statements.
  error = DbcUtils::prepare(pg_conn_,
                            insert_statement_.name(),
                            insert_statement_.statement());
  if (error != ErrorCode::OK) {
    return error;
  }

  error = DbcUtils::prepare(pg_conn_,
                            select_all_statement_.name(),
                            select_all_statement_.statement());
  if (error != ErrorCode::OK) {
    return error;
  }

  for (const auto& element : select_statements_) {
    const SelectStatement& select_statement = element.second;
    error = DbcUtils::prepare(pg_conn_,
                              select_statement.name(),
                              select_statement.statement());
    if (error != ErrorCode::OK) {
      return error;
    }
  }

  for (const auto& element : update_statements_) {
    const UpdateStatement& update_statement = element.second;
    error = DbcUtils::prepare(pg_conn_,
                              update_statement.name(),
                              update_statement.statement());
    if (error != ErrorCode::OK) {
      return error;
    }
  }

  for (const auto& element : delete_statements_) {
    const DeleteStatement& delete_statement = element.second;
    error = DbcUtils::prepare(pg_conn_,
                              delete_statement.name(),
                              delete_statement.statement());
    if (error != ErrorCode::OK) {
      return error;
    }
  }

  return error;
}

/**
 * @brief Check the object which has specified name exists
 * in the metadata table.
 * @param name  [in] object name.
 * @return  true if it exists, otherwise false.
 */
bool IndexDaoPg::exists(std::string_view name) const {

  bool exists = false;

  ptree object;
  ErrorCode error = this->select(Object::NAME, name, object);
  if (error == ErrorCode::OK) {
    exists = true;
  }

  return exists;
}

/**
 * @brief Check the object which has specified name exists
 * in the metadata table.
 * @param object  [in] metadata object which has name.
 * @return  true if it exists, otherwise false.
 */
bool IndexDaoPg::exists(const boost::property_tree::ptree& object) const {

  bool exists = false;

  auto name = object.get_optional<std::string>(Object::NAME);
  if (name) {
    exists = this->exists(name.get());
  }

  return exists;
}

/**
 * @brief Insert a metadata object into the metadata table.
 * @param object    [in]  metadata object.
 * @param object_id [out] object ID.
 * @return  If success ErrorCode::OK, otherwise error code.
 */
ErrorCode IndexDaoPg::insert(
    const boost::property_tree::ptree& object,
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
  params.emplace_back((namespace_name ? namespace_name.value().c_str() : nullptr));

  // ownerId
  auto owner_id = get_string_value<ObjectId>(object, Index::OWNER_ID);
  params.emplace_back(!owner_id.empty() ? owner_id.c_str() : nullptr);

  // acl
  auto acl = object.get_optional<std::string>(Index::ACL);
  params.emplace_back((acl ? acl.value().c_str() : nullptr));

  // tableId
  auto table_id = get_string_value<ObjectId>(object, Index::TABLE_ID);
  params.emplace_back(!table_id.empty() ? table_id.c_str() : nullptr);

  // accessMethod
  auto access_method = get_string_value<int64_t>(object, Index::ACCESS_METHOD);
  params.emplace_back(!access_method.empty() ? access_method.c_str() : nullptr);

  // IsUnique
  auto is_unique = get_string_value<bool>(object, Index::IS_UNIQUE);
  params.emplace_back(!is_unique.empty() ? is_unique.c_str() : nullptr);

  // IsPrimary
  auto is_primary = get_string_value<bool>(object, Index::IS_PRIMARY);
  params.emplace_back(!is_primary.empty() ? is_primary.c_str() : nullptr);

  // numberOfKeyColumns
  auto number_of_key_column = get_string_value<int64_t>(object, Index::NUMBER_OF_KEY_COLUMNS);
  params.emplace_back(!number_of_key_column.empty() ? number_of_key_column.c_str() : nullptr);

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
    error = Utility::ptree_to_json(pt_columns, columns_json);
    if (error != ErrorCode::OK) {
      return error;
    }
  }
  params.emplace_back((!columns_json.empty() ? columns_json.c_str()
                                             : postgresql::EMPTY_STRING_JSON));

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
    error = Utility::ptree_to_json(pt_columns_id, columns_id_json);
    if (error != ErrorCode::OK) {
      return error;
    }
  }
  params.emplace_back((!columns_id_json.empty()
                           ? columns_id_json.c_str()
                           : postgresql::EMPTY_STRING_JSON));

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
    error = Utility::ptree_to_json(pt_options, options_json);
    if (error != ErrorCode::OK) {
      return error;
    }
  }
  params.emplace_back((!options_json.empty() ? options_json.c_str()
                                             : postgresql::EMPTY_STRING_JSON));

  PGresult* res = nullptr;
  // Executes a prepared statement.
  error = DbcUtils::execute_statement(pg_conn_, insert_statement_.name(),
                                      params, res);
  if (error == ErrorCode::OK) {
    int64_t number_of_tuples = PQntuples(res);
    if (number_of_tuples == 1) {
      // Obtain the object ID of the added metadata object.
      std::string str = PQgetvalue(res, FIRST_ROW, FIRST_COLUMN);
      error = DbcUtils::str_to_integral<ObjectIdType>(str.data(), object_id);
    } else {
      error = ErrorCode::RESULT_MULTIPLE_ROWS;
    }
  }
  PQclear(res);

  return error;
}

/**
 * @brief Select a metadata object from the metadata table..
 * @param key     [in]  key name of the metadata object.
 * @param value   [in]  value of key.
 * @param object  [out] a selected metadata object.
 * @retval ErrorCode::OK if success.
 * @retval ErrorCode::ID_NOT_FOUND if the table id does not exist.
 * @retval otherwise an error code.
 */
ErrorCode IndexDaoPg::select(
    std::string_view key, std::string_view value,
    boost::property_tree::ptree& object) const {

  ErrorCode error = ErrorCode::UNKNOWN;
  std::vector<const char*> params;

  params.emplace_back(value.data());

  // Set SELECT statement.
  SelectStatement select_statement;
  try {
    select_statement = select_statements_.at(key.data());
  }
  catch (std::out_of_range& e) {
    return ErrorCode::NOT_FOUND;
  }
  catch (...) {
    return ErrorCode::INTERNAL_ERROR;
  }

  PGresult* res = nullptr;
  // Executes a prepared statement
  error = DbcUtils::execute_statement(pg_conn_,
                                      select_statement.name(),
                                      params, res);
  if (error == ErrorCode::OK) {
    int64_t number_of_tuples = PQntuples(res);
    if (number_of_tuples == 1) {
      // Obtain data.
      error = this->convert_pgresult_to_ptree(res, FIRST_ROW, object);
    } else if (number_of_tuples == 0) {
      // Not found.
      error = Dao::get_not_found_error_code(key);
    } else {
      error = ErrorCode::RESULT_MULTIPLE_ROWS;
    }
  }
  PQclear(res);

  return error;
}

/**
 * @brief Select all metadata objects from the metadata table.
 * @param objects  [out] all metadata objects.
 * @return  If success ErrorCode::OK, otherwise error codes.
 */
ErrorCode IndexDaoPg::select_all(
    std::vector<boost::property_tree::ptree>& objects) const {

  ErrorCode error = ErrorCode::UNKNOWN;
  std::vector<const char*> params;

  PGresult* res = nullptr;
  // Executes a prepared statement
  error = DbcUtils::execute_statement(pg_conn_,
                                      select_all_statement_.name(),
                                      params, res);

  if (error == ErrorCode::OK) {
    int64_t number_of_tuples = PQntuples(res);
    if (number_of_tuples >= 0) {
      for (int64_t num = 0; num < number_of_tuples; num++) {
        ptree object;
        error = convert_pgresult_to_ptree(res, num, object);
        if (error != ErrorCode::OK) {
          break;
        }
        objects.emplace_back(object);
      }
    } else {
      error = ErrorCode::INVALID_PARAMETER;
    }
  }
  PQclear(res);

  return error;
}

/**
 * @brief Update a metadata object into the metadata table.
 * @param key     [in] key name of the metadata object.
 * @param value   [in] value of key.
 * @param object  [in] metadata object.
 * @retval ErrorCode::OK if success.
 * @retval ErrorCode::ID_NOT_FOUND if the index id does not exist.
 * @retval otherwise an error code.
 */
ErrorCode IndexDaoPg::update(
    std::string_view key, std::string_view value,
    const boost::property_tree::ptree& object) const {

  ErrorCode error = ErrorCode::UNKNOWN;
  std::vector<const char*> params;

  auto name = object.get_optional<std::string>(Index::NAME);
  params.emplace_back((name ? name.value().c_str() : nullptr));

  auto namespace_name = object.get_optional<std::string>(Index::NAMESPACE);
  params.emplace_back(
      (namespace_name ? namespace_name.value().c_str() : nullptr));

  auto owner_id = get_string_value<ObjectId>(object, Index::OWNER_ID);
  params.emplace_back(!owner_id.empty() ? owner_id.c_str() : nullptr);

  auto acl = object.get_optional<std::string>(Index::ACL);
  params.emplace_back((acl ? acl.value().c_str() : nullptr));

  auto table_id = get_string_value<ObjectId>(object, Index::TABLE_ID);
  params.emplace_back(!table_id.empty() ? table_id.c_str() : nullptr);

  auto access_method = get_string_value<int64_t>(object, Index::ACCESS_METHOD);
  params.emplace_back(!access_method.empty() ? access_method.c_str() : nullptr);

  auto is_unique = get_string_value<bool>(object, Index::IS_UNIQUE);
  params.emplace_back(!is_unique.empty() ? is_unique.c_str() : nullptr);

  auto is_primary = get_string_value<bool>(object, Index::IS_PRIMARY);
  params.emplace_back(!is_primary.empty() ? is_primary.c_str() : nullptr);

  auto number_of_key_column =
      get_string_value<int64_t>(object, Index::NUMBER_OF_KEY_COLUMNS);
  params.emplace_back(
      !number_of_key_column.empty() ? number_of_key_column.c_str() : nullptr);

  auto columns = object.get_child_optional(Index::KEYS);
  std::string columns_json;
  if (columns) {
    // Converts a property_tree to a JSON string.
    error = Utility::ptree_to_json(columns.value(), columns_json);
    if (error != ErrorCode::OK) {
      return error;
    }
  }
  params.emplace_back((!columns_json.empty() ? columns_json.c_str() : "{}"));

  auto columns_id = object.get_child_optional(Index::KEYS_ID);
  std::string columns_id_json;
  if (columns_id) {
    // Converts a property_tree to a JSON string.
    error = Utility::ptree_to_json(columns_id.value(), columns_id_json);
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
    error = Utility::ptree_to_json(options.value(), options_json);
    if (error != ErrorCode::OK) {
      return error;
    }
  }
  params.emplace_back((!options_json.empty() ? options_json.c_str() : "{}"));

  // Set key value.
  params.emplace_back(value.data());

  // Set UPDATE statement.
  UpdateStatement update_statement;
  try {
    update_statement = update_statements_.at(key.data());
  }
  catch (std::out_of_range& e) {
    return ErrorCode::NOT_FOUND;
  }
  catch (...) {
    return ErrorCode::INTERNAL_ERROR;
  }

  PGresult* res = nullptr;
  // Executes a prepared statement
  error = DbcUtils::execute_statement(pg_conn_,
                                      update_statement.name(),
                                      params, res);

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

/**
 * @brief Delete a metadata object from the metadata table.
 * @param key       [in] key name of the metadata object.
 * @param value     [in] value of key.
 * @param object_id [out] removed metadata objects.
 * @return  If success ErrorCode::OK, otherwise error codes.
 * @retval ErrorCode::ID_NOT_FOUND if the table id does not exist.
 * @retval ErrorCode::NAME_NOT_FOUND if the table name does not exist.
 * @retval otherwise an error code.
 */
ErrorCode IndexDaoPg::remove(std::string_view key,
                             std::string_view value,
                             ObjectIdType& object_id) const {

  ErrorCode error = ErrorCode::UNKNOWN;
  std::vector<const char*> params;

  params.emplace_back(value.data());
  DeleteStatement delete_statement;
  try {
    delete_statement = delete_statements_.at(key.data());
  }
  catch (std::out_of_range& e) {
    return ErrorCode::NOT_FOUND;
  }
  catch (...) {
    return ErrorCode::INTERNAL_ERROR;
  }

  PGresult* res = nullptr;
  // Executes a prepared statement
  error = DbcUtils::execute_statement(pg_conn_,
                                      delete_statement.name(),
                                      params, res);

  if (error == ErrorCode::OK) {
    uint64_t number_of_rows_affected = 0;
    ErrorCode error_get =
        DbcUtils::get_number_of_rows_affected(res, number_of_rows_affected);

    if (error_get != ErrorCode::OK) {
      error = error_get;
    } else if (number_of_rows_affected == 1) {
      // Obtain the object ID of the deleted metadata object.
      std::string str = PQgetvalue(res, FIRST_ROW, FIRST_COLUMN);
      error = DbcUtils::str_to_integral<ObjectIdType>(str.data(), object_id);
    } else if (number_of_rows_affected == 0) {
      // Not found.
      error = Dao::get_not_found_error_code(key);
    } else {
      error = ErrorCode::INVALID_PARAMETER;
    }
  }
  PQclear(res);

  return error;
}

/**
 * @brief Gets the ptree type table metadata
 *   converted from the given PGresult type value.
 * @param res         [in]  pointer to PGresult.
 * @param row_number  [in]  row number of the PGresult.
 * @param object      [out] metadata object.
 * @return ErrorCode::OK if success, otherwise an error code.
 */
ErrorCode IndexDaoPg::convert_pgresult_to_ptree(
    const PGresult* res, const int row_number,
    boost::property_tree::ptree& object) const {

  ErrorCode error = ErrorCode::UNKNOWN;
  object.clear();

  object.put(Object::FORMAT_VERSION,
      PQgetvalue(res, row_number, static_cast<int>(OrdinalPosition::kFormatVersion)));

  object.put(Object::GENERATION,
      PQgetvalue(res, row_number, static_cast<int>(OrdinalPosition::kGeneration)));

  object.put(Object::ID,
      PQgetvalue(res, row_number, static_cast<int>(OrdinalPosition::kId)));

  object.put(Object::NAME,
      PQgetvalue(res, row_number, static_cast<int>(OrdinalPosition::kName)));

  object.put(Index::NAMESPACE,
      PQgetvalue(res, row_number, static_cast<int>(OrdinalPosition::kNamespace)));

  object.put(Index::OWNER_ID,
      PQgetvalue(res, row_number, static_cast<int>(OrdinalPosition::kOwnerId)));

  object.put(Index::ACL,
      PQgetvalue(res, row_number, static_cast<int>(OrdinalPosition::kAcl)));

  object.put(Index::TABLE_ID,
      PQgetvalue(res, row_number, static_cast<int>(OrdinalPosition::kTableId)));

  object.put(Index::ACCESS_METHOD,
      PQgetvalue(res, row_number, static_cast<int>(OrdinalPosition::kAccessMethod)));

  // Set the boolean value converted to a string to property_tree.
  std::string is_unique =
      DbcUtils::convert_boolean_expression(
          PQgetvalue(res, row_number, static_cast<int>(OrdinalPosition::kIsUnique)));
  if (!is_unique.empty()) {
    object.put(Index::IS_UNIQUE, is_unique);
  }

  // Set the boolean value converted to a string to property_tree.
  std::string is_primary =
      DbcUtils::convert_boolean_expression(
          PQgetvalue(res, row_number, static_cast<int>(OrdinalPosition::kIsPrimary)));
  if (!is_primary.empty()) {
    object.put(Index::IS_PRIMARY, is_primary);
  }

  object.put(Index::NUMBER_OF_KEY_COLUMNS,
      PQgetvalue(res, row_number, static_cast<int>(OrdinalPosition::kNumKeyColumn)));

  ptree columns;
  // Converts a JSON string to a property_tree.
  // Set the boolean value converted to a string to property_tree.
  Utility::json_to_ptree(
      PQgetvalue(res, row_number, static_cast<int>(OrdinalPosition::kColumns)),
      columns);
  object.add_child(Index::KEYS, columns);

  ptree columns_id;
  // Converts a JSON string to a property_tree.
  Utility::json_to_ptree(
      PQgetvalue(res, row_number, static_cast<int>(OrdinalPosition::kColumnsId)),
      columns_id);
  object.add_child(Index::KEYS_ID, columns_id);

  ptree options;
  // Converts a JSON string to a property_tree.
  Utility::json_to_ptree(
      PQgetvalue(res, row_number, static_cast<int>(OrdinalPosition::kOptions)),
      options);
  object.add_child(Index::OPTIONS, options);

  error = ErrorCode::OK;
  return error;
}

/**
 * Split a string with a specified delimiter.
 * @param source    [in]  Source string to be split.
 * @param delimiter [in]  Delimiter to split.
 * @return Vector of the result of the split.
 */
std::vector<std::string> IndexDaoPg::split(const std::string& source,
                                           const char& delimiter) const {
  std::vector<std::string> result;
  std::stringstream stream(source);
  std::string buffer;

  while (std::getline(stream, buffer, delimiter)) {
    result.push_back(buffer);
  }

  return result;
}

/**
 * The value for a key is extracted from the ptree and returned as a string.
 * @param object    [in]  ptree object.
 * @param key_name  [in]  key name.
 * @return String of extracted values.
 */
template <typename T>
std::string IndexDaoPg::get_string_value(
    const boost::property_tree::ptree& object, const char* key_name) const {
  auto value = object.get_optional<T>(key_name);
  return (value ? std::to_string(value.value()) : "");
}

}  // namespace manager::metadata::db
