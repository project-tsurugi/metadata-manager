/*
 * Copyright 2022-2023 Project Tsurugi.
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
#ifndef MANAGER_METADATA_DAO_POSTGRESQL_CONSTRAINTS_DAO_PG_H_
#define MANAGER_METADATA_DAO_POSTGRESQL_CONSTRAINTS_DAO_PG_H_

#include <map>
#include <string>
#include <string_view>
#include <vector>

#include <boost/property_tree/ptree.hpp>

#include "manager/metadata/constraints.h"
#include "manager/metadata/dao/postgresql/dao_pg.h"
#include "manager/metadata/error_code.h"

namespace manager::metadata::db {

/**
 * @brief DAO class for accessing constraint metadata for PostgreSQL.
 */
class ConstraintsDaoPg : public DaoPg {
 public:
  /**
   * @brief INSERT statement key by id.
   */
  static constexpr const char* const kStatementKeyInsertById = "ConstraintId";

  /**
   * @brief constraint metadata table name.
   */
  static constexpr const char* const kTableName = "constraints";

  /**
   * @brief Column name of the constraint metadata table
   *   in the metadata repository.
   */
  class ColumnName {
   public:
    static constexpr const char* const kFormatVersion  = "format_version";
    static constexpr const char* const kGeneration     = "generation";
    static constexpr const char* const kId             = "id";
    static constexpr const char* const kName           = "name";
    static constexpr const char* const kTableId        = "table_id";
    static constexpr const char* const kType           = "type";
    static constexpr const char* const kColumns        = "columns";
    static constexpr const char* const kColumnsId      = "columns_id";
    static constexpr const char* const kIndexId        = "index_id";
    static constexpr const char* const kExpression     = "expression";
    static constexpr const char* const kPkTable        = "pk_table";
    static constexpr const char* const kPkColumns      = "pk_columns";
    static constexpr const char* const kPkColumnsId    = "pk_columns_id";
    static constexpr const char* const kFkMatchType    = "fk_match_type";
    static constexpr const char* const kFkDeleteAction = "fk_delete_action";
    static constexpr const char* const kFkUpdateAction = "fk_update_action";
  };  // class ColumnName

  // Inheritance constructor.
  using DaoPg::DaoPg;

  /**
   * @brief Insert a metadata object into the metadata table.
   * @param object     [in]  metadata object.
   * @param object_id  [out] object id of the added row.
   * @return If success ErrorCode::OK, otherwise error code.
   * @note  If success, metadata object is added management metadata.
   *   e.g. format version, generation, etc...
   */
  ErrorCode insert(const boost::property_tree::ptree& object,
                   ObjectId& object_id) const override;

  /**
   * @brief Select a metadata object from the metadata table.
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
   * @brief Remove a metadata object from a metadata table file.
   * @param keys        [in]  key name and value of the metadata object.
   * @param object_ids  [out] object id of the deleted rows.
   * @return If success ErrorCode::OK, otherwise error code.
   */
  ErrorCode remove(const std::map<std::string_view, std::string_view>& keys,
                   std::vector<ObjectId>& object_ids) const override;

 private:
  /**
   * @brief Column ordinal position of the constraint metadata table
   *   in the metadata repository.
   */
  enum class OrdinalPosition {
    kFormatVersion = 0,
    kGeneration,
    kId,
    kName,
    kTableId,
    kType,
    kColumns,
    kColumnsId,
    kIndexId,
    kExpression,
    kPkTable,
    kPkColumns,
    kPkColumnsId,
    kFkMatchType,
    kFkDeleteAction,
    kFkUpdateAction
  };  // enum class OrdinalPosition

  /**
   * @brief Get the table source name.
   * @return table source name.
   */
  std::string get_source_name() const override { return kTableName; }

  /**
   * @brief Create prepared statements.
   */
  void create_prepared_statements() override;

  /**
   * @brief Get an INSERT statement for metadata table.
   * @return INSERT statement.
   */
  std::string get_insert_statement() const override;

  /**
   * @brief Get an INSERT statement for metadata with a specified ID.
   * @return INSERT statement.
   */
  std::string get_insert_statement_id() const;

  /**
   * @brief Get a SELECT statement to retrieve all metadata from the
   *   metadata table.
   * @return SELECT statement.
   */
  std::string get_select_all_statement() const override;

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
   * @brief Get a DELETE statement to delete metadata from the metadata table.
   * @param key  [in]  column name of metadata table.
   * @return DELETE statement.
   */
  std::string get_delete_statement(std::string_view key) const override;

  /**
   * @brief Gets the ptree type constraint metadata
   *   converted from the given PGresult type value.
   * @param pg_result   [in]  the result of a query.
   * @param row_number  [in]  row number of the PGresult.
   * @return metadata object.
   */
  boost::property_tree::ptree convert_pgresult_to_ptree(
      const PGresult* pg_result, const int row_number) const;
};  // class ConstraintsDaoPg

}  // namespace manager::metadata::db

#endif  // MANAGER_METADATA_DAO_POSTGRESQL_CONSTRAINTS_DAO_PG_H_
