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

#ifndef MANAGER_METADATA_DAO_COMMON_STATEMENT_NAME_H_
#define MANAGER_METADATA_DAO_COMMON_STATEMENT_NAME_H_

namespace manager::metadata::db {

/**
 * @brief statement name : unique name for the new prepared statement.
 */
enum StatementName {
  STATISTICS_DAO_UPSERT_ONE_COLUMN_STATISTIC_BY_TABLE_ID_COLUMN_ORDINAL_POSITION =
      0,
  STATISTICS_DAO_SELECT_ONE_COLUMN_STATISTIC_BY_TABLE_ID_COLUMN_ORDINAL_POSITION,
  STATISTICS_DAO_SELECT_ALL_COLUMN_STATISTIC_BY_TABLE_ID,
  STATISTICS_DAO_DELETE_ALL_COLUMN_STATISTIC_BY_TABLE_ID,
  STATISTICS_DAO_DELETE_ONE_COLUMN_STATISTIC_BY_TABLE_ID_COLUMN_ORDINAL_POSITION,
  TABLES_DAO_UPDATE_RELTUPLES_BY_TABLE_ID,
  TABLES_DAO_UPDATE_RELTUPLES_BY_TABLE_NAME,
  TABLES_DAO_SELECT_TABLE_STATISTIC_BY_TABLE_ID,
  TABLES_DAO_SELECT_TABLE_STATISTIC_BY_TABLE_NAME,
  TABLES_DAO_INSERT_TABLE_METADATA,
  COLUMNS_DAO_INSERT_ONE_COLUMN_METADATA,
  COLUMNS_DAO_SELECT_ALL_COLUMN_METADATA_BY_TABLE_ID,
  COLUMNS_DAO_DELETE_ALL_COLUMN_METADATA_BY_TABLE_ID,
  DAO_SELECT_EQUAL_TO,
  DAO_DELETE_EQUAL_TO,
};  // enum StatementName

}  // namespace manager::metadata::db

#endif  // MANAGER_METADATA_DAO_COMMON_STATEMENT_NAME_H_
