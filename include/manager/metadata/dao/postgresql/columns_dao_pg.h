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
#ifndef MANAGER_METADATA_DAO_POSTGRESQL_COLUMNS_DAO_PG_H_
#define MANAGER_METADATA_DAO_POSTGRESQL_COLUMNS_DAO_PG_H_

#include <string>
#include <string_view>
#include <vector>

#include <boost/property_tree/ptree.hpp>

#include "manager/metadata/column.h"
#include "manager/metadata/dao/postgresql/dao_pg.h"
#include "manager/metadata/error_code.h"

namespace manager::metadata::db {

/**
 * @brief DAO class for accessing column metadata for PostgreSQL.
 */
class ColumnsDaoPg : public DaoPg {
 public:
  /**
   * @brief column metadata table name.
   */
  static constexpr const char* const kTableName = "tsurugi_attribute";

  /**
   * @brief Column name of the column metadata table in the metadata repository.
   */
  class ColumnName {
   public:
    static constexpr const char* const kFormatVersion = "format_version";
    static constexpr const char* const kGeneration    = "generation";
    static constexpr const char* const kId            = "id";
    static constexpr const char* const kName          = "name";
    static constexpr const char* const kTableId       = "table_id";
    static constexpr const char* const kColumnNumber  = "column_number";
    static constexpr const char* const kDataTypeId    = "data_type_id";
    static constexpr const char* const kDataLength    = "data_length";
    static constexpr const char* const kVarying       = "varying";
    static constexpr const char* const kIsNotNull     = "is_not_null";
    static constexpr const char* const kDefaultExpr   = "default_expression";
    static constexpr const char* const kIsFuncExpr    = "is_funcexpr";
  };  // class ColumnName

  // Inheritance constructor.
  using DaoPg::DaoPg;

  /**
   * @brief Add metadata object to metadata table.
   * @param object     [in]  column metadata object to add.
   * @param object_id  [out] object id of the added row.
   * @return ErrorCode::OK if success, otherwise an error code.
   * @note  If success, metadata object is added management metadata.
   *   e.g. format version, generation, etc...
   */
  manager::metadata::ErrorCode insert(const boost::property_tree::ptree& object,
                                      ObjectId& object_id) const override;

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
   * @brief Get a metadata object from a metadata table.
   * @param key     [in]  key. column name of a column metadata table.
   * @param values  [in]  value to be filtered.
   * @param object  [out] constraint metadata to get, where the given
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
   * @brief Removes column metadata with the specified key value
   *   from the column metadata table.
   * @param key        [in]  key. column name of a column metadata table.
   * @param values     [in]  value to be filtered.
   * @param object_id  [out] object id of the deleted row.
   * @retval ErrorCode::OK if success.
   * @retval ErrorCode::ID_NOT_FOUND if the object id does not exist.
   * @retval ErrorCode::NAME_NOT_FOUND if the object name does not exist.
   * @retval otherwise an error code.
   */
  manager::metadata::ErrorCode remove(
      std::string_view key, const std::vector<std::string_view>& values,
      ObjectId& object_id) const override;

 private:
  /**
   * @brief INSERT statement key by id.
   */
  static constexpr const char* const kStatementKeyInsertById = "ColumnId";

  /**
   * @brief Column ordinal position of the column metadata table
   *   in the metadata repository.
   */
  enum class OrdinalPosition {
    kFormatVersion = 0,
    kGeneration,
    kId,
    kName,
    kTableId,
    kColumnNumber,
    kDataTypeId,
    kDataLength,
    kVarying,
    kIsNotNull,
    kDefaultExpr,
    kIsFuncExpr,
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
   * @brief Get a DELETE statement to delete metadata from the metadata table.
   * @param key  [in]  column name of metadata table.
   * @return DELETE statement.
   */
  std::string get_delete_statement(std::string_view key) const override;

  /**
   * @brief Gets the ptree type column metadata
   *   converted from the given PGresult type value.
   * @param pg_result   [in]  the result of a query.
   * @param row_number  [in]  row number of the PGresult.
   * @return metadata object.
   */
  boost::property_tree::ptree convert_pgresult_to_ptree(
      const PGresult* pg_result, const int row_number) const;
};  // class ColumnsDaoPg

}  // namespace manager::metadata::db

#endif  // MANAGER_METADATA_DAO_POSTGRESQL_COLUMNS_DAO_PG_H_
