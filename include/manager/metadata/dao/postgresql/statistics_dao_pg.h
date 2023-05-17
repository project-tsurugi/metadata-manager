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
#ifndef MANAGER_METADATA_DAO_POSTGRESQL_STATISTICS_DAO_PG_H_
#define MANAGER_METADATA_DAO_POSTGRESQL_STATISTICS_DAO_PG_H_

#include <string>
#include <string_view>
#include <vector>

#include <boost/property_tree/ptree.hpp>

#include "manager/metadata/dao/postgresql/dao_pg.h"
#include "manager/metadata/statistics.h"

namespace manager::metadata::db {

/**
 * @brief DAO class for accessing statistics metadata for PostgreSQL.
 */
class StatisticsDaoPg : public DaoPg {
 public:
  /**
   * @brief column metadata table name.
   */
  static constexpr const char* const kTableName = "tsurugi_statistic";

  /**
   * @brief Column name of the column statistics table in the
   *   metadata repository.
   */
  class ColumnName {
   public:
    static constexpr const char* const kFormatVersion   = "format_version";
    static constexpr const char* const kGeneration      = "generation";
    static constexpr const char* const kId              = "id";
    static constexpr const char* const kName            = "name";
    static constexpr const char* const kColumnId        = "column_id";
    static constexpr const char* const kColumnStatistic = "column_statistic";
  };  // class ColumnName

  // Inheritance constructor.
  using DaoPg::DaoPg;

  /**
   * @brief Add metadata object to metadata table.
   * @param object     [in]   metadata object to add.
   * @param object_id  [out] object id of the added row.
   * @return ErrorCode::OK if success, otherwise an error code.
   * @note  If success, metadata object is added management metadata.
   *   e.g. format version, generation, etc...
   */
  manager::metadata::ErrorCode insert(const boost::property_tree::ptree& object,
                                      ObjectId& object_id) const override;

  /**
   * @brief Get all metadata objects from a metadata table.
   *   If the table metadata does not exist, return the container as empty.
   * @param objects  [out] all statistics metadata.
   * @return ErrorCode::OK if success, otherwise an error code.
   */
  manager::metadata::ErrorCode select_all(
      std::vector<boost::property_tree::ptree>& objects) const override;

  /**
   * @brief Get all column statistics rows from the column statistics table
   *   based on the specified table ID.
   * @param table_id   [in]  table id.
   * @param objects    [out] all statistics metadata.
   * @retval ErrorCode::OK if success.
   * @retval ErrorCode::ID_NOT_FOUND if the table id does not exist.
   * @retval otherwise an error code.
   */
  manager::metadata::ErrorCode select_all(
      ObjectId table_id,
      std::vector<boost::property_tree::ptree>& objects) const;

  /**
   * @brief Get a metadata object from a metadata table.
   * @param key     [in]  key. column name of a column statistics table.
   * @param value   [in]  value to be filtered.
   * @param object  [out] constraint metadata to get, where the given
   *   key equals the given value.
   * @retval ErrorCode::OK if success.
   * @retval ErrorCode::ID_NOT_FOUND if the object id does not exist.
   * @retval ErrorCode::NAME_NOT_FOUND if the object name does not exist.
   * @retval otherwise an error code.
   */
  manager::metadata::ErrorCode select(
      std::string_view object_key, std::string_view object_value,
      boost::property_tree::ptree& object) const override;

  /**
   * @brief Get a column statistics row from the column statistics table
   *   based on the specified table id and the given column ordinal position.
   * @param table_id  [in]  table id.
   * @param key       [in]  column name of a column metadata table.
   * @param value     [in]  value to be filtered.
   * @param object    [out] get statistics metadata for which the specified key
   * and value are equal.
   * @return ErrorCode::OK if success, otherwise an error code.
   */
  manager::metadata::ErrorCode select(
      ObjectId table_id, std::string_view object_key,
      std::string_view object_value, boost::property_tree::ptree& object) const;

  /**
   * @brief Function defined for compatibility.
   * @return Always ErrorCode::OK.
   */
  manager::metadata::ErrorCode update(
      std::string_view, std::string_view,
      const boost::property_tree::ptree&) const override {
    return ErrorCode::OK;
  }

  /**
   * @brief Removes column statistic with the specified key value
   *   from the column statistics table.
   * @param key        [in]  column name of a column metadata table.
   * @param value      [in]  value to be filtered.
   * @param object_id  [out] object id of the deleted row.
   *   If multiple rows are deleted, the first column id.
   * @return ErrorCode::OK if success, otherwise an error code.
   */
  manager::metadata::ErrorCode remove(std::string_view key,
                                      std::string_view value,
                                      ObjectId& object) const override;

  /**
   * @brief Removes column statistics with the specified table id and key value
   *   from the column statistics table.
   * @param table_id   [in]  table id.
   * @param key        [in]  column name of a column metadata table.
   * @param value      [in]  value to be filtered.
   * @param object_id  [out] object id of the deleted row.
   *   If multiple rows are deleted, the first column id.
   * @return ErrorCode::OK if success, otherwise an error code.
   */
  manager::metadata::ErrorCode remove(ObjectId table_id, std::string_view key,
                                      std::string_view value,
                                      ObjectId& object) const;

 private:
  /**
   * @brief Statement key by table id.
   */
  static constexpr const char* const kStatementKeyTableId = "table_id";

  /**
   * @brief Column ordinal position of the column statistics table
   *   in the metadata repository.
   */
  enum class OrdinalPosition {
    kFormatVersion = 0,
    kGeneration,
    kId,
    kName,
    kColumnId,
    kColumnStatistic,
    kTableId,
    kColumnNumber,
    kColumnName
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
   * @brief Get an INSERT statement for column statistics table
   *   based on column id.
   * @return INSERT statement.
   */
  std::string get_insert_statement() const override;

  /**
   * @brief Get an INSERT statement for column statistics table
   *   based on table id and column info.
   * @param key  [in]  column name of statistics table.
   * @return INSERT statement.
   */
  std::string get_insert_statement_columns(std::string_view key) const;

  /**
   * @brief Get a SELECT statement to retrieve all data from the
   *   statistics table.
   * @return SELECT statement.
   */
  std::string get_select_all_statement() const override;

  /**
   * @brief Get a SELECT statement to retrieve all data from the
   *   statistics table based on table id.
   * @return SELECT statement.
   */
  std::string get_select_all_statement_tid() const;

  /**
   * @brief Get a SELECT statement to retrieve data matching the criteria from
   * the statistics table.
   * @param key  [in]  column name of statistics table.
   * @return SELECT statement.
   */
  std::string get_select_statement(std::string_view key) const override;

  /**
   * @brief Get a SELECT statement to retrieve data matching the criteria from
   * the statistics table based on the table id and column info.
   * @param key  [in]  column name of statistics table.
   * @return SELECT statement.
   */
  std::string get_select_statement_columns(std::string_view key) const;

  /**
   * @brief Function defined for compatibility.
   * @return Always empty string.
   */
  std::string get_update_statement(std::string_view) const override {
    return "";
  }

  /**
   * @brief Get a DELETE statement to delete data from the statistics table
   *   based on id or name or column id.
   * @param key  [in]  column name of metadata table.
   * @return DELETE statement.
   */
  std::string get_delete_statement(std::string_view key) const override;

  /**
   * @brief Get a DELETE statement to delete data from the statistics table
   *   based on table id.
   * @return DELETE statement.
   */
  std::string get_delete_statement_tid() const;

  /**
   * @brief Get a DELETE statement to delete data from the statistics table
   *   based on table id and column ordinal position or name.
   * @param key  [in]  column name of metadata table.
   * @return DELETE statement.
   */
  std::string get_delete_statement_columns(std::string_view key) const;

  /**
   * @brief Execute a SELECT statement to get column statistics rows
   *   from the column statistics table.
   * @param statement  [in]  statement name.
   * @param params     [in]  Parameters of the statement.
   * @param objects    [out] all column statistics.
   * @return ErrorCode::OK if success, otherwise an error code.
   */
  manager::metadata::ErrorCode get_column_statistics_rows(
      std::string_view statement_name, const std::vector<const char*>& params,
      std::vector<boost::property_tree::ptree>& objects) const;

  /**
   * @brief Get a NOT_FOUND error code corresponding to the key.
   * @param key  [in] key name of the metadata object.
   * @retval ErrorCode::ID_NOT_FOUND if the key is id.
   * @retval ErrorCode::NAME_NOT_FOUND if the key is name.
   * @retval ErrorCode::NOT_FOUND if the key is otherwise.
   */
  manager::metadata::ErrorCode get_not_found_error_code(
      std::string_view key) const override;

  /**
   * @brief Gets the ptree type column statistics
   *   converted from the given PGresult type value.
   * @param pg_result   [in]  the result of a query.
   * @param row_number  [in]  row number of the PGresult.
   * @return metadata object.
   */
  boost::property_tree::ptree convert_pgresult_to_ptree(
      const PGresult* pg_result, const int row_number) const;
};  // class StatisticsDaoPg

}  // namespace manager::metadata::db

#endif  // MANAGER_METADATA_DAO_POSTGRESQL_STATISTICS_DAO_PG_H_
