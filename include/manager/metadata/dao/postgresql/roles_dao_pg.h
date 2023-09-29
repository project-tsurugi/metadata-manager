/*
 * Copyright 2021-2023 tsurugi project.
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
#ifndef MANAGER_METADATA_DAO_POSTGRESQL_ROLES_DAO_PG_H_
#define MANAGER_METADATA_DAO_POSTGRESQL_ROLES_DAO_PG_H_

#include <map>
#include <string>
#include <string_view>
#include <vector>

#include <boost/property_tree/ptree.hpp>

#include "manager/metadata/dao/postgresql/dao_pg.h"
#include "manager/metadata/roles.h"

namespace manager::metadata::db {

/**
 * @brief DAO class for accessing role metadata for PostgreSQL.
 */
class RolesDaoPg : public DaoPg {
 public:
  /**
   * @brief roles table name.
   */
  static constexpr const char* const kTableName = "roles";

  // Inheritance constructor.
  using DaoPg::DaoPg;

  /**
   * @brief Unsupported function.
   * @return Always ErrorCode::NOT_SUPPORTED.
   */
  ErrorCode insert(const boost::property_tree::ptree&,
                   ObjectId&) const override {
    // Do nothing and return of ErrorCode::NOT_SUPPORTED.
    return ErrorCode::NOT_SUPPORTED;
  }

  /**
   * @brief Select a role object from the PostgreSQL with the specified key
   * value.
   * @param keys    [in]  key name and value of the metadata object.
   * @param object  [out] a selected metadata object.
   * @return If success ErrorCode::OK, otherwise error code.
   */
  ErrorCode select(const std::map<std::string_view, std::string_view>& keys,
                   boost::property_tree::ptree& object) const override;

  /**
   * @brief Unsupported function.
   * @return Always ErrorCode::NOT_SUPPORTED.
   */
  ErrorCode update(const std::map<std::string_view, std::string_view>&,
                   const boost::property_tree::ptree&,
                   uint64_t&) const override {
    // Do nothing and return of ErrorCode::NOT_SUPPORTED.
    return ErrorCode::NOT_SUPPORTED;
  }

  /**
   * @brief Unsupported function.
   * @return Always ErrorCode::NOT_SUPPORTED.
   */
  ErrorCode remove(const std::map<std::string_view, std::string_view>&,
                   std::vector<ObjectId>&) const override {
    // Do nothing and return of ErrorCode::NOT_SUPPORTED.
    return ErrorCode::NOT_SUPPORTED;
  }

 private:
  /**
   * @brief Column ordinal position of the role metadata table
   *   in the PostgreSQL repository.
   */
  enum class OrdinalPosition {
    kOid = 0,
    kName,
    kSuper,
    kInherit,
    kCreateRole,
    kCreateDb,
    kCanLogin,
    kReplication,
    kBypassRls,
    kConnLimit,
    kPassword,
    kValidUntil,
  };  // enum class OrdinalPosition

  /**
   * @brief Create prepared statements.
   */
  void create_prepared_statements() override;

  /**
   * @brief Get the table source name.
   * @return table source name.
   */
  std::string get_source_name() const override { return kTableName; }

  /**
   * @brief Function defined for compatibility.
   * @return Always empty string.
   */
  std::string get_insert_statement() const {
    // Returns an unconditional empty string.
    return "";
  }

  /**
   * @brief Function defined for compatibility.
   * @return Always empty string.
   */
  std::string get_select_all_statement() const {
    // Returns an unconditional empty string.
    return "";
  }

  /**
   * @brief Get a SELECT statement to retrieve metadata matching the criteria
   *   from the metadata table.
   * @param key  [in]  column name of metadata table.
   * @return SELECT statement.
   */
  std::string get_select_statement(std::string_view key) const override;

  /**
   * @brief Function defined for compatibility.
   * @return Always empty string.
   */
  std::string get_update_statement(std::string_view) const {
    // Returns an unconditional empty string.
    return "";
  }

  /**
   * @brief Function defined for compatibility.
   * @return Always empty string.
   */
  std::string get_delete_statement(std::string_view) const {
    // Returns an unconditional empty string.
    return "";
  }

  /**
   * @brief Converts from PGresult type values to ptree type data.
   * @param pg_result   [in]  the result of a query.
   * @param row_number  [in]  row number of the PGresult.
   * @return metadata object.
   */
  boost::property_tree::ptree convert_pgresult_to_ptree(
      const PGresult* pg_result, const int row_number) const;
};  // class RolesDaoPg

}  // namespace manager::metadata::db

#endif  // MANAGER_METADATA_DAO_POSTGRESQL_ROLES_DAO_PG_H_
