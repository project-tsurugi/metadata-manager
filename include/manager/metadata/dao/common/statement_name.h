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

#ifndef MANAGER_METADATA_DAO_COMMON_STATEMENT_NAME_H_
#define MANAGER_METADATA_DAO_COMMON_STATEMENT_NAME_H_

namespace manager::metadata::db {

/**
 * @brief statement name : unique name for the new prepared statement.
 */
enum class StatementName {
  STATISTICS_DAO_UPSERT_COLUMN_STATISTIC_BY_COLUMN_INFO = 0,
  STATISTICS_DAO_UPSERT_COLUMN_STATISTIC_BY_COLUMN_ID,
  STATISTICS_DAO_SELECT_COLUMN_STATISTIC,
  STATISTICS_DAO_SELECT_COLUMN_STATISTIC_BY_COLUMN_INFO,
  STATISTICS_DAO_SELECT_COLUMN_STATISTIC_ALL,
  STATISTICS_DAO_SELECT_COLUMN_STATISTIC_ALL_BY_TABLE_ID,
  STATISTICS_DAO_DELETE_COLUMN_STATISTIC,
  STATISTICS_DAO_DELETE_COLUMN_STATISTIC_BY_TABLE_ID,
  STATISTICS_DAO_DELETE_COLUMN_STATISTIC_BY_COLUMN_INFO,
  TABLES_DAO_UPDATE_TUPLES,
  TABLES_DAO_INSERT_TABLE_METADATA,
  TABLES_DAO_SELECT_TABLE_METADATA_ALL,
  COLUMNS_DAO_INSERT_ONE_COLUMN_METADATA,
  COLUMNS_DAO_SELECT_ALL_COLUMN_METADATA,
  COLUMNS_DAO_DELETE_ALL_COLUMN_METADATA,
  ROLES_DAO_SELECT,
  PRIVILEGES_DAO_SELECT,
  PRIVILEGES_DAO_CHECK_EXISTS,
  DAO_SELECT_EQUAL_TO,
  DAO_DELETE_EQUAL_TO,
};  // enum StatementName

}  // namespace manager::metadata::db

#endif  // MANAGER_METADATA_DAO_COMMON_STATEMENT_NAME_H_
