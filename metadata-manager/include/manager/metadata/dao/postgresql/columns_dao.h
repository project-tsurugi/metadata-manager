/*
 * Copyright 2020 tsurugi project.
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
#ifndef MANAGER_METADATA_DAO_POSTGRESQL_COLUMNS_DAO_H_
#define MANAGER_METADATA_DAO_POSTGRESQL_COLUMNS_DAO_H_

#include "manager/metadata/dao/columns_dao.h"
#include "manager/metadata/dao/postgresql/common.h"
#include "manager/metadata/dao/postgresql/db_session_manager.h"

namespace manager::metadata::db::postgresql {

class ColumnsDAO : public manager::metadata::db::ColumnsDAO {
 public:
  /**
   * @brief Column name of the column metadata table in the metadata repository.
   */
  class ColumnName {
   public:
    static constexpr const char* const kFormatVersion = "format_version";
    static constexpr const char* const kGeneration = "generation";
    static constexpr const char* const kId = "id";
    static constexpr const char* const kName = "name";
    static constexpr const char* const kTableId = "table_id";
    static constexpr const char* const kOrdinalPosition = "ordinal_position";
    static constexpr const char* const kDataTypeId = "data_type_id";
    static constexpr const char* const kDataLength = "data_length";
    static constexpr const char* const kVarying = "varying";
    static constexpr const char* const kNullable = "nullable";
    static constexpr const char* const kDefaultExpr = "default_expr";
    static constexpr const char* const kDirection = "direction";
  };

  /**
   * @brief Column ordinal position of the column metadata table
   *   in the metadata repository.
   */
  class OrdinalPosition {
   public:
    enum {
      kFormatVersion = 0,
      kGeneration,
      kId,
      kName,
      kTableId,
      kOrdinalPosition,
      kDataTypeId,
      kDataLength,
      kVarying,
      kNullable,
      kDefaultExpr,
      kDirection
    };
  };

  /**
   * @brief column metadata table name.
   */
  static constexpr const char* const kTableName = "tsurugi_attribute";

  explicit ColumnsDAO(DBSessionManager* session_manager);

  manager::metadata::ErrorCode prepare() const override;

  manager::metadata::ErrorCode insert_one_column_metadata(
      const ObjectIdType table_id,
      boost::property_tree::ptree& column) const override;

  manager::metadata::ErrorCode select_column_metadata(
      std::string_view object_key, std::string_view object_value,
      boost::property_tree::ptree& object) const override;

  manager::metadata::ErrorCode delete_column_metadata(
      std::string_view object_key,
      std::string_view object_value) const override;

 private:
  ConnectionSPtr connection_;

  manager::metadata::ErrorCode get_ptree_from_p_gresult(
      PGresult*& res, int ordinal_position,
      boost::property_tree::ptree& column) const;
};  // class ColumnsDAO

}  // namespace manager::metadata::db::postgresql

#endif  // MANAGER_METADATA_DAO_POSTGRESQL_COLUMNS_DAO_H_
