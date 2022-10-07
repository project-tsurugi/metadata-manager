/*
 * Copyright 2022 tsurugi project.
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

#include <vector>

#include <boost/property_tree/ptree.hpp>

#include "manager/metadata/dao/constraints_dao.h"
#include "manager/metadata/dao/postgresql/common_pg.h"
#include "manager/metadata/dao/postgresql/db_session_manager_pg.h"
#include "manager/metadata/error_code.h"

namespace manager::metadata::db::postgresql {

class ConstraintsDAO : public manager::metadata::db::ConstraintsDAO {
 public:
  /**
   * @brief Column name of the constraint metadata table in the metadata repository.
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
   * @brief column metadata table name.
   */
  static constexpr const char* const kTableName = "tsurugi_constraint";

  explicit ConstraintsDAO(DBSessionManager* session_manager);

  manager::metadata::ErrorCode prepare() const override;

  manager::metadata::ErrorCode insert_constraint_metadata(
      const boost::property_tree::ptree& constraint_metadata,
      ObjectId& constraint_id) const override;

  manager::metadata::ErrorCode select_constraint_metadata(
      std::string_view object_key, std::string_view object_value,
      boost::property_tree::ptree& constraint_metadata) const override;
  manager::metadata::ErrorCode select_constraint_metadata(
      std::vector<boost::property_tree::ptree>& constraint_container) const override;

  manager::metadata::ErrorCode delete_constraint_metadata(
      std::string_view object_key, std::string_view object_value) const override;

 private:
  ConnectionSPtr connection_;

  manager::metadata::ErrorCode convert_pgresult_to_ptree(
      const PGresult* res, const int ordinal_position,
      boost::property_tree::ptree& columns_metadata) const;
};  // class ConstraintsDAO

}  // namespace manager::metadata::db::postgresql

#endif  // MANAGER_METADATA_DAO_POSTGRESQL_CONSTRAINTS_DAO_PG_H_
