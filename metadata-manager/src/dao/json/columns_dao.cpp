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
#include "manager/metadata/dao/json/columns_dao.h"

// =============================================================================
namespace manager::metadata::db::json {

using manager::metadata::ErrorCode;

/**
 * @brief Defines all prepared statements.
 * @param none.
 * @return ErrorCode::OK if success, otherwise an error code.
 */
ErrorCode ColumnsDAO::prepare() const {
  // Do nothing and return of ErrorCode::OK.
  return ErrorCode::OK;
}

/**
 * @brief Execute INSERT statement to insert
 *   the given one column statistic
 *   into the column metadata table based on the given table id.
 * @param (table_id)  [in]  table id.
 * @param (column)    [in]  one column metadata to add.
 * @return ErrorCode::OK if success, otherwise an error code.
 */
ErrorCode ColumnsDAO::insert_one_column_metadata(
    ObjectIdType table_id, boost::property_tree::ptree& column) const {
  // Do nothing and return of ErrorCode::OK.
  return ErrorCode::OK;
}

/**
 * @brief Execute a SELECT statement to get column metadata rows
 *   from the column metadata table,
 *   where the given key equals the given value.
 * @param (object_key)    [in]  key. column name of a column metadata table.
 * @param (object_value)  [in]  value to be filtered.
 * @param (object)        [out] column metadata to get,
 *   where the given key equals the given value.
 * @return ErrorCode::OK if success, otherwise an error code.
 */
ErrorCode ColumnsDAO::select_column_metadata(
    std::string_view object_key, std::string_view object_value,
    boost::property_tree::ptree& object) const {
  // Do nothing and return of ErrorCode::OK.
  return ErrorCode::OK;
}

/**
 * @brief Execute DELETE statement to delete column metadata
 *   from the column metadata table
 *   where the given key equals the given value.
 * @param (object_key)    [in]  key. column name of a column metadata table.
 * @param (object_value)  [in]  value to be filtered.
 * @return ErrorCode::OK if success, otherwise an error code.
 */
ErrorCode ColumnsDAO::delete_column_metadata(
    std::string_view object_key, std::string_view object_value) const {
  // Do nothing and return of ErrorCode::OK.
  return ErrorCode::OK;
}

}  // namespace manager::metadata::db::json
