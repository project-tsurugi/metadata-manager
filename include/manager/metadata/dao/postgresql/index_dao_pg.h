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
#pragma once

#include <string>
#include <string_view>
#include <vector>
#include <boost/property_tree/ptree.hpp>

#include "manager/metadata/error_code.h"
#include "manager/metadata/dao/postgresql/pg_common.h"
#include "manager/metadata/dao/postgresql/dbc_utils_pg.h"
#include "manager/metadata/dao/dao.h"
#include "manager/metadata/dao/postgresql/statements.h"
#include "manager/metadata/dao/postgresql/db_session_manager_pg.h"

namespace manager::metadata::db {

class IndexDaoPg : public Dao {
 public:
  /**
   * @brief Column name of the table metadata table in the metadata repository.
   */
  struct Column {
    // tsurugi_index table schema.
    static constexpr const char* const kFormatVersion = "format_version";
    static constexpr const char* const kGeneration    = "generation";
    static constexpr const char* const kId            = "id";
    static constexpr const char* const kName          = "name";
  };

  /**
   * @brief Column ordinal position of the table metadata table
   *   in the metadata repository.
   */
  enum class OrdinalPosition {
    kFormatVersion = 0,
    kGeneration,
    kId,
    kName,
  };  // enum class OrdinalPosition

  explicit IndexDaoPg(DbSessionManagerPg* session) 
      : Dao(session), pg_conn_(session->connection().pg_conn) {};

  std::string get_source_name() const override { return "indexes"; }

  manager::metadata::ErrorCode prepare() override;

  bool exists(std::string_view name) const override;
  bool exists(const boost::property_tree::ptree& object) const override;

  manager::metadata::ErrorCode insert(
      const boost::property_tree::ptree& object,
      ObjectIdType& object_id) const override;

  manager::metadata::ErrorCode select(
      std::string_view object_key, std::string_view object_value,
      boost::property_tree::ptree& object) const override;

  manager::metadata::ErrorCode select_all(
      std::vector<boost::property_tree::ptree>& objects) const override;

  manager::metadata::ErrorCode update(
      std::string_view key, std::string_view value,
      const boost::property_tree::ptree& object) const override;

  manager::metadata::ErrorCode remove(
      std::string_view key, std::string_view value,
      ObjectIdType& object) const override;

protected:
  SelectAllStatement select_all_statement_;
  InsertStatement insert_statement_;
  std::unordered_map<std::string, SelectStatement> select_statements_;
  std::unordered_map<std::string, UpdateStatement> update_statements_;
  std::unordered_map<std::string, DeleteStatement> delete_statements_;

  std::string get_insert_statement() const;
  std::string get_select_all_statement() const;
  std::string get_select_statement(std::string_view key) const;
  std::string get_update_statement(std::string_view key) const;
  std::string get_delete_statement(std::string_view key) const;

 private:
  PgConnectionPtr pg_conn_;

  manager::metadata::ErrorCode convert_pgresult_to_ptree(
      const PGresult* res, const int ordinal_position,
      boost::property_tree::ptree& table) const;
  std::vector<std::string> split(const std::string& source,
                                 const char& delimiter) const;
};  // class TablesDAO

}  // namespace manager::metadata::db
