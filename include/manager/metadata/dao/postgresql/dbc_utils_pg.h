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
#ifndef MANAGER_METADATA_DAO_POSTGRESQL_DBC_UTILS_PG_H_
#define MANAGER_METADATA_DAO_POSTGRESQL_DBC_UTILS_PG_H_

#include <string>
#include <string_view>
#include <unordered_map>
#include <vector>

#include "manager/metadata/dao/common/statement_name.h"
#include "manager/metadata/dao/postgresql/pg_common.h"
#include "manager/metadata/error_code.h"

namespace manager::metadata::db {

class DbcUtils {
 public:
  static bool is_open(const PgConnectionPtr& connection);
  static std::string convert_boolean_expression(const char* string);

  static manager::metadata::ErrorCode get_number_of_rows_affected(
      PGresult*& pgres, uint64_t& return_value);

  static PgConnectionPtr make_connection_sptr(PGconn* pgconn);
  static ResultPtr make_result_uptr(PGresult* pgres);

  static manager::metadata::ErrorCode prepare(
      const PgConnectionPtr& connection, const StatementName& statement_name,
      std::string_view statement, std::vector<Oid>* param_types = nullptr);
  static manager::metadata::ErrorCode prepare(
      const PgConnectionPtr& connection, std::string_view statement_name,
      std::string_view statement, std::vector<Oid>* param_types = nullptr);

  static manager::metadata::ErrorCode exec_prepared(
      const PgConnectionPtr& connection, const StatementName& statement_name,
      const std::vector<const char*>& param_values, PGresult*& res);
  static manager::metadata::ErrorCode exec_prepared(
      const PgConnectionPtr& connection, std::string_view statement_name,
      const std::vector<const char*>& param_values, PGresult*& res);
  static ErrorCode execute_statement(
      const PgConnectionPtr& connection, std::string_view statement_name,
      const std::vector<const char*>& param_values, PGresult*& res);

  static manager::metadata::ErrorCode find_statement_name(
      const std::unordered_map<std::string, std::string>& statement_names_map,
      std::string_view key_value, std::string& statement_name);
};  // class DbcUtils

}  // namespace manager::metadata::db

#endif  // MANAGER_METADATA_DAO_POSTGRESQL_DBC_UTILS_PG_H_
