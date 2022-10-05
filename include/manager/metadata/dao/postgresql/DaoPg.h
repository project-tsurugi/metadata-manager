/*
 * Copyright 2020-2022 tsurugi project.
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
#pragma once

#include <string_view>
#include <memory>
#include <boost/property_tree/ptree.hpp>
#include "manager/metadata/metadata.h"
#include "manager/metadata/error_code.h"
#include "manager/metadata/dao/db_session_manager.h"
#include "manager/metadata/dao/postgresql/statements.h"

namespace manager::metadata::db {
class DaoPg {
 public:
  explicit DaoPg() {}
  virtual ~DaoPg() {}

protected:
  InsertStatement insert_statement_;
  SelectAllStatement select_all_statement_;
  std::unordered_map<std::string, SelectStatement> select_statements_;
  std::unordered_map<std::string, UpdateStatement> update_statements_;
  std::unordered_map<std::string, DeleteStatement> delete_statements_;

  virtual std::string get_insert_statement() const = 0;
  virtual std::string get_select_all_statement() const = 0;
  virtual std::string get_select_statement(std::string_view key) const = 0;
  virtual std::string get_update_statement(std::string_view key) const = 0;
  virtual std::string get_delete_statement(std::string_view key) const = 0;

  virtual void create_prepared_statements() = 0;


};  // class DaoPg

} // namespace manager::metadata::db
