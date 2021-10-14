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
#ifndef MANAGER_METADATA_DAO_POSTGRESQL_STATISTICS_DAO_H_
#define MANAGER_METADATA_DAO_POSTGRESQL_STATISTICS_DAO_H_

#include <string>
#include <string_view>
#include <unordered_map>

#include "manager/metadata/dao/postgresql/db_session_manager.h"
#include "manager/metadata/dao/postgresql/dbc_utils.h"
#include "manager/metadata/dao/statistics_dao.h"

namespace manager::metadata::db::postgresql {

class StatisticsDAO : public manager::metadata::db::StatisticsDAO {
 public:
  /**
   * @brief Column name of the column statistics table in the
   *   metadata repository.
   */
  class ColumnName {
   public:
    static constexpr const char* const kFormatVersion = "format_version";
    static constexpr const char* const kGeneration = "generation";
    static constexpr const char* const kId = "id";
    static constexpr const char* const kName = "name";
    static constexpr const char* const kColumnId = "column_id";
    static constexpr const char* const kColumnStatistic = "column_statistic";
  };  // class ColumnName

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
    kOrdinalPosition,
    kColumnName
  };  // enum class OrdinalPosition

  /**
   * @brief column metadata table name.
   */
  static constexpr const char* const kTableName = "tsurugi_statistic";

  explicit StatisticsDAO(DBSessionManager* session_manager);

  manager::metadata::ErrorCode prepare() const override;

  manager::metadata::ErrorCode upsert_column_statistic(
      const ObjectIdType column_id, const std::string* column_name,
      boost::property_tree::ptree* column_statistic,
      ObjectIdType& statistic_id) const override;
  manager::metadata::ErrorCode upsert_column_statistic(
      const ObjectIdType table_id, std::string_view object_key,
      std::string_view object_value, const std::string* column_name,
      boost::property_tree::ptree* column_statistic,
      ObjectIdType& statistic_id) const override;

  manager::metadata::ErrorCode select_column_statistic(
      std::string_view object_key, std::string_view object_value,
      boost::property_tree::ptree& object) const override;
  manager::metadata::ErrorCode select_column_statistic(
      const ObjectIdType table_id, std::string_view object_key,
      std::string_view object_value,
      boost::property_tree::ptree& object) const override;
  manager::metadata::ErrorCode select_column_statistic(
      std::vector<boost::property_tree::ptree>& container) const override;
  manager::metadata::ErrorCode select_column_statistic(
      const ObjectIdType table_id,
      std::vector<boost::property_tree::ptree>& container) const override;

  manager::metadata::ErrorCode delete_column_statistic(
      std::string_view object_key, std::string_view object_value,
      ObjectIdType& statistic_id) const override;
  manager::metadata::ErrorCode delete_column_statistic(
      const ObjectIdType table_id) const override;
  manager::metadata::ErrorCode delete_column_statistic(
      const ObjectIdType table_id, std::string_view object_key,
      std::string_view object_value, ObjectIdType& statistic_id) const override;

 private:
  ConnectionSPtr connection_;

  static manager::metadata::ErrorCode convert_pgresult_to_ptree(
      PGresult*& res, const int ordinal_position,
      boost::property_tree::ptree& statistic);

  manager::metadata::ErrorCode get_column_statistics_rows(
      std::string_view statement_name,
      const std::vector<const char*>& param_values,
      std::vector<boost::property_tree::ptree>& container) const;
};  // class StatisticsDAO

}  // namespace manager::metadata::db::postgresql

#endif  // MANAGER_METADATA_DAO_POSTGRESQL_STATISTICS_DAO_H_
