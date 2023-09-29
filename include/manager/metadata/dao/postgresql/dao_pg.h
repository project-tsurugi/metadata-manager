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
#ifndef MANAGER_METADATA_DAO_POSTGRESQL_DAO_PG_H_
#define MANAGER_METADATA_DAO_POSTGRESQL_DAO_PG_H_

#include <string>
#include <string_view>
#include <type_traits>
#include <unordered_map>

#include "manager/metadata/dao/common/statements.h"
#include "manager/metadata/dao/dao.h"
#include "manager/metadata/dao/postgresql/db_session_manager_pg.h"
#include "manager/metadata/dao/postgresql/dbc_utils_pg.h"

namespace manager::metadata::db {

/**
 * @brief DAO base class for PostgreSQL.
 */
class DaoPg : public Dao {
 public:
  /**
   * @brief Construct a new DAO class for PostgreSQL.
   * @param session pointer to DB session manager for PostgreSQL.
   */
  explicit DaoPg(DbSessionManagerPg* session)
      : session_(session), pg_conn_(session->connection().pg_conn) {}
  virtual ~DaoPg() {}

  /**
   * @brief Defines all prepared statements.
   * @return If success ErrorCode::OK, otherwise error code.
   */
  virtual ErrorCode prepare();

 protected:
  DbSessionManagerPg* session_;
  PgConnectionPtr pg_conn_;

  std::unordered_map<std::string, InsertStatement> insert_statements_;
  std::unordered_map<std::string, SelectStatement> select_statements_;
  std::unordered_map<std::string, UpdateStatement> update_statements_;
  std::unordered_map<std::string, DeleteStatement> delete_statements_;

  virtual std::string get_source_name() const = 0;

  virtual std::string get_insert_statement() const                     = 0;
  virtual std::string get_select_all_statement() const                 = 0;
  virtual std::string get_select_statement(std::string_view key) const = 0;
  virtual std::string get_update_statement(std::string_view key) const = 0;
  virtual std::string get_delete_statement(std::string_view key) const = 0;

  /**
   * @brief Create prepared statements.
   */
  virtual void create_prepared_statements();

  /**
   * @brief Get the values of the specified row and column from PGresult.
   * @tparam T1 DB value type (only bool is a string indicating a boolean value,
   *   others are strings)
   * @tparam T2 Enum class that specifies the column position.
   * @param pg_result        [in]  query results.
   * @param row_number       [in]  row number.
   * @param column_position  [in]  enum value indicating column position.
   * @return DB value string ("true" or "false" string for bool type).
   */
  template <typename T1 = std::string, typename T2,
            typename    = std::enable_if_t<std::is_enum_v<T2>>>
  std::string get_result_value(const PGresult* pg_result, const int row_number,
                               T2 column_position) const {
    std::string result_value(PQgetvalue(
        pg_result, row_number,
        static_cast<typename std::underlying_type<T2>::type>(column_position)));
    if (std::is_same_v<T1, bool>) {
      result_value = DbcUtils::convert_boolean_expression(result_value.c_str());
    }
    return result_value;
  }

 private:
  /**
   * @brief Execute the definition of a prepared statement.
   * @tparam T SQL statement management class (Statement) or derived class.
   * @param statements  [in]  statement management class.
   * @return ErrorCode::OK if success, otherwise an error code.
   */
  template <typename T,
            typename = std::enable_if_t<std::is_base_of_v<Statement, T>>>
  ErrorCode exec_prepare(
      const std::unordered_map<std::string, T>& statements) const;
};  // class DaoPg

}  // namespace manager::metadata::db

#endif  // MANAGER_METADATA_DAO_POSTGRESQL_DAO_PG_H_
