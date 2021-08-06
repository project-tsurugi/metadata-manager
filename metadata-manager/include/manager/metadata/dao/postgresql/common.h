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
#ifndef MANAGER_METADATA_DAO_POSTGRESQL_COMMON_H_
#define MANAGER_METADATA_DAO_POSTGRESQL_COMMON_H_

#include <functional>
#include <memory>

extern "C" {
typedef struct pg_conn PGconn;
typedef struct pg_result PGresult;
}

namespace manager::metadata::db::postgresql {

typedef std::shared_ptr<PGconn> ConnectionSPtr;
typedef std::unique_ptr<PGresult, std::function<void(PGresult*)>> ResultUPtr;

static constexpr const char* const SCHEMA_NAME = "tsurugi_catalog";
struct TableName {
  static constexpr const char* const TABLE_METADATA_TABLE =
      "tsurugi_class";  //!< @brief table metadata table.
  static constexpr const char* const COLUMN_METADATA_TABLE =
      "tsurugi_attribute";  //!< @brief column metadata table.
  static constexpr const char* const COLUMN_STATISTICS_TABLE =
      "tsurugi_statistic";  //!< @brief column statistics table.
  static constexpr const char* const DATA_TYPES_TABLE =
      "tsurugi_type";  //!< @brief  data type metadata table.
};                     // struct TableName

}  // namespace manager::metadata::db::postgresql

#endif  // MANAGER_METADATA_DAO_POSTGRESQL_COMMON_H_