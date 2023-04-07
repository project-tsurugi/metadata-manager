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
#ifndef MANAGER_METADATA_DAO_POSTGRESQL_DATATYPES_DAO_PG_H_
#define MANAGER_METADATA_DAO_POSTGRESQL_DATATYPES_DAO_PG_H_

#include <string>

#include <boost/property_tree/ptree.hpp>

#include "manager/metadata/dao/datatypes_dao.h"
#include "manager/metadata/dao/postgresql/common_pg.h"
#include "manager/metadata/dao/postgresql/db_session_manager_pg.h"
#include "manager/metadata/error_code.h"

namespace manager::metadata::db::postgresql {

class DataTypesDAO : public manager::metadata::db::DataTypesDAO {
 public:
  /**
   * @brief Column name of the datatype table in the metadata repository.
   */
  class ColumnName {
   public:
    static constexpr const char* const kFormatVersion = "format_version";
    static constexpr const char* const kGeneration = "generation";
    static constexpr const char* const kId = "id";
    static constexpr const char* const kName = "name";
    static constexpr const char* const kPgDataType = "pg_data_type";
    static constexpr const char* const kPgDataTypeName = "pg_data_type_name";
    static constexpr const char* const kPgDataTypeQualifiedName =
        "pg_data_type_qualified_name";
  };  // class ColumnName

  /**
   * @brief Column ordinal position of the datatype table
   *   in the metadata repository.
   */
  enum class OrdinalPosition {
    kFormatVersion = 0,
    kGeneration,
    kId,
    kName,
    kPgDataType,
    kPgDataTypeName,
    kPgDataTypeQualifiedName
  };  // enum class OrdinalPosition

  /**
   * @brief datatype table name.
   */
  static constexpr const char* const kTableName = "tsurugi_type";

  explicit DataTypesDAO(DBSessionManager* session_manager);

  manager::metadata::ErrorCode prepare() const override;

  manager::metadata::ErrorCode select_one_data_type_metadata(
      std::string_view object_key, std::string_view object_value,
      boost::property_tree::ptree& object) const override;

 private:
  ConnectionSPtr connection_;

  manager::metadata::ErrorCode convert_pgresult_to_ptree(
      const PGresult* res, const int row_number,
      boost::property_tree::ptree& object) const;
};  // class DataTypesDAO

}  // namespace manager::metadata::db::postgresql

#endif  // MANAGER_METADATA_DAO_POSTGRESQL_DATATYPES_DAO_PG_H_
