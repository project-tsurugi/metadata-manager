/*
 * Copyright 2021 tsurugi project.
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
#ifndef MANAGER_METADATA_DAO_POSTGRESQL_PRIVILEGES_DAO_PG_H_
#define MANAGER_METADATA_DAO_POSTGRESQL_PRIVILEGES_DAO_PG_H_

#include <string>
#include <string_view>
#include <vector>

#include <boost/property_tree/ptree.hpp>

#include "manager/metadata/dao/postgresql/dao_pg.h"
#include "manager/metadata/error_code.h"

namespace manager::metadata::db {

/**
 * @brief DAO class for accessing privileges metadata for PostgreSQL.
 */
class PrivilegesDaoPg : public DaoPg {
 public:
  /**
   * @brief privileges table name.
   */
  static constexpr const char* const kTableName = "privileges";

  // Inheritance constructor.
  using DaoPg::DaoPg;

  /**
   * @brief Defines all prepared statements.
   * @return If success ErrorCode::OK, otherwise error code.
   */
  manager::metadata::ErrorCode prepare() override;

  /**
   * @brief Checks whether a role exists with the specified object ID.
   * @param object_id  [in]  object id of the role.
   * @return true if it exists, otherwise false.
   */
  bool exists(ObjectId object_id) const override;

  /**
   * @brief Function defined for compatibility.
   * @return Always ErrorCode::OK.
   */
  manager::metadata::ErrorCode insert(const boost::property_tree::ptree&,
                                      ObjectId&) const override {
    // Do nothing and return of ErrorCode::OK.
    return ErrorCode::OK;
  }

  /**
   * @brief Function defined for compatibility.
   * @return Always ErrorCode::OK.
   */
  manager::metadata::ErrorCode select_all(
      std::vector<boost::property_tree::ptree>&) const override {
    // Do nothing and return of ErrorCode::OK.
    return ErrorCode::OK;
  }

  /**
   * @brief Get table privileges for the specified role.
   * @param key     [in]  key. column name of a role.
   * @param values  [in]  value to be filtered.
   * @param object  [out] privileges to get, where the given
   *   key equals the given value.
   * @retval ErrorCode::OK if success.
   * @retval ErrorCode::ID_NOT_FOUND if the object id does not exist.
   * @retval ErrorCode::NAME_NOT_FOUND if the object name does not exist.
   * @retval otherwise an error code.
   */
  manager::metadata::ErrorCode select(
      std::string_view key, const std::vector<std::string_view>& values,
      boost::property_tree::ptree& object) const override;

  /**
   * @brief Function defined for compatibility.
   * @return Always ErrorCode::OK.
   */
  manager::metadata::ErrorCode update(
      std::string_view, const std::vector<std::string_view>&,
      const boost::property_tree::ptree&) const override {
    // Do nothing and return of ErrorCode::OK.
    return ErrorCode::OK;
  }

  /**
   * @brief Function defined for compatibility.
   * @return Always ErrorCode::OK.
   */
  manager::metadata::ErrorCode remove(std::string_view,
                                      const std::vector<std::string_view>&,
                                      ObjectId&) const override {
    // Do nothing and return of ErrorCode::OK.
    return ErrorCode::OK;
  }

 private:
  /**
   * @brief EXIST statement key.
   */
  static constexpr const char* const kStatementKeyExists = "Exists";

  /**
   * @brief Column ordinal position.
   */
  enum class OrdinalPosition {
    kTableName = 0,
    kSelect,
    kInsert,
    kUpdate,
    kDelete,
    kTruncate,
    kReferences,
    kTrigger
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
  std::string get_insert_statement() const override {
    // Returns an unconditional empty string.
    return "";
  }

  /**
   * @brief Function defined for compatibility.
   * @return Always empty string.
   */
  std::string get_select_all_statement() const override {
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
  std::string get_update_statement(std::string_view) const override {
    // Returns an unconditional empty string.
    return "";
  }

  /**
   * @brief Function defined for compatibility.
   * @return Always empty string.
   */
  std::string get_delete_statement(std::string_view) const override {
    // Returns an unconditional empty string.
    return "";
  }

  /**
   * @brief Returns a SELECT statement to check for the existence of an auth-id.
   * @return a SELECT statement.
   */
  std::string get_exists_statement() const;

  /**
   * @brief Converts from PGresult type values to ptree type data.
   * @param pg_result   [in]  the result of a query.
   * @param row_number  [in]  row number of the PGresult.
   * @return privilege object.
   */
  boost::property_tree::ptree convert_pgresult_to_ptree(
      const PGresult* pg_result, const int row_number) const;
};  // class PrivilegesDaoPg

}  // namespace manager::metadata::db

#endif  // MANAGER_METADATA_DAO_POSTGRESQL_PRIVILEGES_DAO_PG_H_
