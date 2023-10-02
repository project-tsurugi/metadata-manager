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
#include "manager/metadata/dao/postgresql/dao_pg.h"

#include "manager/metadata/dao/postgresql/dbc_utils_pg.h"

// =============================================================================
namespace manager::metadata::db {

ErrorCode DaoPg::prepare() {
  ErrorCode error = ErrorCode::UNKNOWN;

  // Create prepared statements.
  this->create_prepared_statements();

  // Set the prepared INSERT statements.
  error = exec_prepare(insert_statements_);

  // Set the prepared SELECT statements.
  if (error == ErrorCode::OK) {
    exec_prepare(select_statements_);
  }

  // Set the prepared UPDATE statements.
  if (error == ErrorCode::OK) {
    exec_prepare(update_statements_);
  }

  // Set the prepared DELETE statements.
  if (error == ErrorCode::OK) {
    exec_prepare(delete_statements_);
  }

  return error;
}

void DaoPg::create_prepared_statements() {
  // INSERT statements.
  InsertStatement insert_statement{this->get_source_name(),
                                   this->get_insert_statement(),
                                   Statement::kDefaultKey};
  insert_statements_.emplace(Statement::kDefaultKey, insert_statement);

  // SELECT statements.
  SelectStatement select_statement{this->get_source_name(),
                                   this->get_select_all_statement(),
                                   Statement::kDefaultKey};
  select_statements_.emplace(Statement::kDefaultKey, select_statement);

  // SELECT statements by ID.
  SelectStatement select_by_id_statement{this->get_source_name(),
                                         this->get_select_statement(Object::ID),
                                         Object::ID};
  select_statements_.emplace(Object::ID, select_by_id_statement);

  // SELECT statements by name.
  SelectStatement select_by_name_statement{
      this->get_source_name(), this->get_select_statement(Object::NAME),
      Object::NAME};
  select_statements_.emplace(Object::NAME, select_by_name_statement);

  // UPDATE statements by ID.
  UpdateStatement update_by_id_statement{this->get_source_name(),
                                         this->get_update_statement(Object::ID),
                                         Object::ID};
  update_statements_.emplace(Object::ID, update_by_id_statement);

  // UPDATE statements by name.
  UpdateStatement update_by_name_statement{
      this->get_source_name(), this->get_update_statement(Object::NAME),
      Object::NAME};
  update_statements_.emplace(Object::NAME, update_by_name_statement);

  // DELETE statements by ID.
  DeleteStatement delete_by_id_statement{this->get_source_name(),
                                         this->get_delete_statement(Object::ID),
                                         Object::ID};
  delete_statements_.emplace(Object::ID, delete_by_id_statement);

  // DELETE statements by name.
  DeleteStatement delete_by_name_statement{
      this->get_source_name(), this->get_delete_statement(Object::NAME),
      Object::NAME};
  delete_statements_.emplace(Object::NAME, delete_by_name_statement);
}

template <typename T,
          typename = std::enable_if_t<std::is_base_of_v<Statement, T>>>
ErrorCode DaoPg::exec_prepare(
    const std::unordered_map<std::string, T>& statements) const {
  ErrorCode error = ErrorCode::UNKNOWN;

  if (!statements.empty()) {
    // Set the prepared statements.
    for (const auto element : statements) {
      const auto& statement = element.second;

      error =
          DbcUtils::prepare(pg_conn_, statement.name(), statement.statement());
      if (error != ErrorCode::OK) {
        break;
      }
    }
  } else {
    error = ErrorCode::OK;
  }

  return error;
}

}  // namespace manager::metadata::db
