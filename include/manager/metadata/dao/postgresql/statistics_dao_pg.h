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
#ifndef MANAGER_METADATA_DAO_POSTGRESQL_STATISTICS_DAO_PG_H_
#define MANAGER_METADATA_DAO_POSTGRESQL_STATISTICS_DAO_PG_H_

#include <map>
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
  static constexpr const char* const kTableName = "statistics";

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
   * @return If success ErrorCode::OK, otherwise error code.
   * @note  If success, metadata object is added management metadata.
   *   e.g. format version, generation, etc...
   */
  manager::metadata::ErrorCode insert(const boost::property_tree::ptree& object,
                                      ObjectId& object_id) const override;

  /**
   * @brief Select a metadata object from the metadata table.
   * @param keys    [in]  key name and value of the metadata object.
   * @param object  [out] a selected metadata object.
   * @return If success ErrorCode::OK, otherwise error code.
   */
  manager::metadata::ErrorCode select(
      const std::map<std::string_view, std::string_view>& keys,
      boost::property_tree::ptree& object) const override;

  /**
   * @brief Unsupported function.
   * @return Always ErrorCode::NOT_SUPPORTED.
   */
  manager::metadata::ErrorCode update(
      const std::map<std::string_view, std::string_view>&,
      const boost::property_tree::ptree&, uint64_t&) const override {
    // Do nothing and return of ErrorCode::NOT_SUPPORTED.
    return ErrorCode::NOT_SUPPORTED;
  }

  /**
   * @brief Remove a metadata object from a metadata table file.
   * @param keys        [in]  key name and value of the metadata object.
   * @param object_ids  [out] object id of the deleted rows.
   * @return If success ErrorCode::OK, otherwise error code.
   */
  manager::metadata::ErrorCode remove(
      const std::map<std::string_view, std::string_view>& keys,
      std::vector<ObjectId>& object_ids) const override;

 private:
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
   * @brief Get a SELECT statement to retrieve data matching the criteria from
   * the statistics table.
   * @param key  [in]  column name of statistics table.
   * @return SELECT statement.
   */
  std::string get_select_statement(std::string_view key) const override;

  /**
   * @brief Get a SELECT statement to retrieve data matching the criteria from
   * the statistics table based on table id.
   * @return SELECT statement.
   */
  std::string get_select_statement_tid() const;

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
   * @return If success ErrorCode::OK, otherwise error code.
   */
  manager::metadata::ErrorCode get_column_statistics_rows(
      std::string_view statement_name, const std::vector<const char*>& params,
      boost::property_tree::ptree& objects) const;

  /**
   * @brief Gets the ptree type column statistics
   *   converted from the given PGresult type value.
   * @param pg_result   [in]  the result of a query.
   * @param row_number  [in]  row number of the PGresult.
   * @return metadata object.
   */
  boost::property_tree::ptree convert_pgresult_to_ptree(
      const PGresult* pg_result, const int row_number) const;

  /**
   * @brief Sets the specified key and key value.
   * @param keys      [in]  key name and value of the metadata object.
   * @param key_name  [out] key name.
   * @param params    [out] statement parameters.
   * @return If success ErrorCode::OK, otherwise error code.
   */
  ErrorCode set_key_params(
      const std::map<std::string_view, std::string_view>& keys,
      std::string& key_name, std::vector<const char*>& params) const;
};  // class StatisticsDaoPg

}  // namespace manager::metadata::db

#endif  // MANAGER_METADATA_DAO_POSTGRESQL_STATISTICS_DAO_PG_H_
