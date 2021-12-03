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
#ifndef MANAGER_METADATA_MANAGER_INCLUDE_MANAGER_METADATA_DAO_POSTGRESQL_TABLES_DAO_H_
#define MANAGER_METADATA_MANAGER_INCLUDE_MANAGER_METADATA_DAO_POSTGRESQL_TABLES_DAO_H_

#include <boost/property_tree/ptree.hpp>
#include <string>
#include <vector>

#include "manager/metadata/dao/postgresql/db_session_manager.h"
#include "manager/metadata/dao/postgresql/dbc_utils.h"
#include "manager/metadata/dao/tables_dao.h"
#include "manager/metadata/error_code.h"

namespace manager::metadata::db::postgresql {

class TablesDAO : public manager::metadata::db::TablesDAO {
 public:
  /**
   * @brief Column name of the table metadata table in the metadata repository.
   */
  class ColumnName {
   public:
    static constexpr const char* const kFormatVersion = "format_version";
    static constexpr const char* const kGeneration = "generation";
    static constexpr const char* const kId = "id";
    static constexpr const char* const kName = "name";
    static constexpr const char* const kNamespace = "namespace";
    static constexpr const char* const kPrimaryKey = "primary_key";
    static constexpr const char* const kTuples = "tuples";
  };  // class ColumnName

  /**
   * @brief Column ordinal position of the table metadata table
   *   in the metadata repository.
   */
  enum class OrdinalPosition {
    kFormatVersion = 0,
    kGeneration,
    kId,
    kName,
    kNamespace,
    kPrimaryKey,
    kTuples,
    kOwnerRoleId,
    kAcl
  };  // enum class OrdinalPosition

  /**
   * @brief table metadata table name.
   */
  static constexpr const char* const kTableName = "tsurugi_class";

  explicit TablesDAO(DBSessionManager* session_manager);

  manager::metadata::ErrorCode prepare() const override;

  manager::metadata::ErrorCode insert_table_metadata(
      const boost::property_tree::ptree& table,
      ObjectIdType& table_id) const override;

  manager::metadata::ErrorCode select_table_metadata(
      std::string_view object_key, std::string_view object_value,
      boost::property_tree::ptree& object) const override;
  manager::metadata::ErrorCode select_table_metadata(
      std::vector<boost::property_tree::ptree>& container) const override;

  manager::metadata::ErrorCode update_reltuples(
      const float reltuples, std::string_view object_key,
      std::string_view object_value,
      ObjectIdType& table_id) const override;

  manager::metadata::ErrorCode delete_table_metadata(
      std::string_view object_key, std::string_view object_value,
      ObjectIdType& table_id) const override;

 private:
  ConnectionSPtr connection_;

  manager::metadata::ErrorCode convert_pgresult_to_ptree(
      const PGresult* res, const int ordinal_position,
      boost::property_tree::ptree& table) const;
  std::vector<std::string> split(const std::string& source,
                                        const char& delimiter) const;
};  // class TablesDAO

}  // namespace manager::metadata::db::postgresql

#endif  // MANAGER_METADATA_MANAGER_INCLUDE_MANAGER_METADATA_DAO_POSTGRESQL_TABLES_DAO_H_
