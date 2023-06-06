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

#include <boost/property_tree/ptree.hpp>

#include "manager/metadata/dao/columns_dao.h"
#include "manager/metadata/dao/postgresql/common_pg.h"
#include "manager/metadata/dao/postgresql/db_session_manager_pg.h"
#include "manager/metadata/error_code.h"

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
    static constexpr const char* const kColumnNumber = "column_number";
    static constexpr const char* const kDataTypeId = "data_type_id";
    static constexpr const char* const kDataLength = "data_length";
    static constexpr const char* const kVarying = "varying";
    static constexpr const char* const kIsNotNull = "is_not_null";
    static constexpr const char* const kDefaultExpr = "default_expression";
    static constexpr const char* const kIsFuncExpr = "is_funcexpr";
  };  // class ColumnName

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
   * @brief column metadata table name.
   */
  static constexpr const char* const kTableName = "tsurugi_attribute";

  explicit ColumnsDAO(DBSessionManager* session_manager);

  manager::metadata::ErrorCode prepare() const override;

  manager::metadata::ErrorCode insert_column_metadata(
      const ObjectIdType table_id,
      const boost::property_tree::ptree& columns_metadata) const override;

  manager::metadata::ErrorCode select_column_metadata(
      std::string_view object_key, std::string_view object_value,
      boost::property_tree::ptree& columns_metadata) const override;

  manager::metadata::ErrorCode delete_column_metadata(
      std::string_view object_key,
      std::string_view object_value) const override;

 private:
  ConnectionSPtr connection_;

  manager::metadata::ErrorCode convert_pgresult_to_ptree(
      const PGresult* res, const int row_number,
      boost::property_tree::ptree& columns_metadata) const;
};  // class ColumnsDAO

}  // namespace manager::metadata::db::postgresql

#endif  // MANAGER_METADATA_DAO_POSTGRESQL_COLUMNS_DAO_PG_H_
