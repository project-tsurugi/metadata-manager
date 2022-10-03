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

#include "manager/metadata/common/message.h"
#include "manager/metadata/common/utility.h"
#include "manager/metadata/dao/postgresql/pg_common.h"
#include "manager/metadata/dao/postgresql/dbc_utils_pg.h"
#include "manager/metadata/tables.h"

namespace manager::metadata::db {

using boost::property_tree::ptree;
using manager::metadata::ErrorCode;
using manager::metadata::db::StatementName;
using manager::metadata::db::postgresql::DbcUtils;

// =============================================================================
//  IndexDaoPg class methods.
/**
 * @brief Returns an INSERT statement for table metadata.
 * @param none.
 * @return an INSERT statement to insert table metadata.
 */
std::string IndexDaoPg::get_insert_statement() const{
  boost::format query =
      boost::format(
          "INSERT INTO %1%.%2% (%3%, %4%, %5%)"
          "VALUES ($1, $2, $3)"
          "RETURNING %6%") %
      SCHEMA_TSURUGI_CATALOG % 
      this->get_source_name() %
      Column::kFormatVersion %
      Column::kGeneration % 
      Column::kName %
      Column::kId; 

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
          "SELECT %3%, %4%, %5%, %6%"
          "FROM %1%.%2%"
          "ORDER BY %5%") %
      SCHEMA_TSURUGI_CATALOG % 
      this->get_source_name() %
      Column::kFormatVersion %
      Column::kGeneration % 
      Column::kId %
      Column::kName;

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
          "SELECT tbl.%3%, tbl.%4%, tbl.%5%, tbl.%6%"
          "FROM %1%.%2%"
          "WHERE %7% = $1") %
      SCHEMA_TSURUGI_CATALOG % 
      this->get_source_name() %
      Column::kFormatVersion %
      Column::kGeneration % 
      Column::kId %
      Column::kName % 
      key.data();

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
          "SET %3% = $1"
          "WHERE %4% = $2") %
      SCHEMA_TSURUGI_CATALOG % 
      this->get_source_name() % 
      Column::kName %
      Column::kId;

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

  boost::format query =
      boost::format("DELETE "
                    "FROM %1%.%2% "
                    "WHERE %3% = $1 "
                    "RETURNING %4%") %
      SCHEMA_TSURUGI_CATALOG % 
      this->get_source_name() % 
      key.data() %
      Column::kId;

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
  select_statements_.emplace(Object::ID, select_by_id_statement);

  UpdateStatement update_by_name_statement{
      this->get_source_name(), 
      this->get_update_statement(Object::NAME),
      Object::NAME};
  select_statements_.emplace(Object::NAME, select_by_name_statement);

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
    const DeleteStatement& deletet_statement = element.second;
    error = DbcUtils::prepare(pg_conn_,
                              deletet_statement.name(),
                              deletet_statement.statement());
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

  std::string s_format_version = std::to_string(Tables::format_version());
  params.emplace_back(s_format_version.c_str());

  std::string s_generation = std::to_string(Tables::generation());
  params.emplace_back(s_generation.c_str());

  auto name = object.get_optional<std::string>(Tables::NAME);
  params.emplace_back((name ? name.value().c_str() : nullptr));

  PGresult* res = nullptr;
  error = DbcUtils::execute_statement(pg_conn_, 
                                      insert_statement_.name(),
                                      params, res);
  if (error == ErrorCode::OK) {
    int64_t number_of_tuples = PQntuples(res);
    if (number_of_tuples == 1) {
      // obtain object ID.
      std::string str = PQgetvalue(res, 
                                  FIRST_TUPLE_NUMBER, 
                                  FIRST_COLUMN_NUMBER);
      error = DbcUtils::str_to_integral<int64_t>(str.data(), object_id);
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
 * @return If success ErrorCode::OK, otherwise error codes.
 * @retval ErrorCode::ID_NOT_FOUND if the table id does not exist.
 * @retval ErrorCode::NAME_NOT_FOUND if the table name does not exist.
 * @retval otherwise an error code.
 */
ErrorCode IndexDaoPg::select(
    std::string_view key, std::string_view value,
    boost::property_tree::ptree& object) const {

  ErrorCode error = ErrorCode::UNKNOWN;
  std::vector<const char*> params;

  params.emplace_back(value.data());
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
  error = DbcUtils::execute_statement(pg_conn_, 
                                      select_statement.name(), 
                                      params, res);
  if (error == ErrorCode::OK) {
    int64_t number_of_tuples = PQntuples(res);
    if (number_of_tuples == 1) {
      // Obtain data.
      error = this->convert_pgresult_to_ptree(res, FIRST_TUPLE_NUMBER, object);
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
  error = DbcUtils::execute_statement(
      pg_conn_, select_all_statement_.name(),
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
 * @param key       [in] key name of the metadata object.
 * @param value     [in] value of key.
 * @param object    [in]  metadata object.
 * @return  If success ErrorCode::OK, otherwise error code.
 */
ErrorCode IndexDaoPg::update(
    std::string_view key, std::string_view value,
    const boost::property_tree::ptree& object) const {

  ErrorCode error = ErrorCode::UNKNOWN;
  std::vector<char const*> params;

  params.emplace_back(value.data());
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
      error = ErrorCode::ID_NOT_FOUND;
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
      int ordinal_position = 0;
      error = DbcUtils::str_to_integral<ObjectIdType>(
          PQgetvalue(res, ordinal_position, 0), object_id);
    } else if (number_of_rows_affected == 0) {
      // Convert the error code.
      if (key == Tables::ID) {
        error = ErrorCode::ID_NOT_FOUND;
      } else if (key == Tables::NAME) {
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

/**
 * @brief Gets the ptree type table metadata
 *   converted from the given PGresult type value.
 * @param res         [in]  pointer to PGresult.
 * @param tuple_num   [in]  row number of PGresult.
 * @param object      [out] metadata object.
 * @return ErrorCode::OK if success, otherwise an error code.
 */
ErrorCode IndexDaoPg::convert_pgresult_to_ptree(
    const PGresult* res, const int tuple_num,
    boost::property_tree::ptree& object) const {

  ErrorCode error = ErrorCode::UNKNOWN;
  object.clear();

  object.put(Object::FORMAT_VERSION,
            PQgetvalue(res, tuple_num,
                      static_cast<int>(OrdinalPosition::kFormatVersion)));

  object.put(Object::GENERATION,
            PQgetvalue(res, tuple_num,
                      static_cast<int>(OrdinalPosition::kGeneration)));

  object.put(Object::ID,
            PQgetvalue(res, tuple_num,
                      static_cast<int>(OrdinalPosition::kId)));

  object.put(Object::NAME,
            PQgetvalue(res, tuple_num,
                      static_cast<int>(OrdinalPosition::kName)));

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

}  // namespace manager::metadata::db
