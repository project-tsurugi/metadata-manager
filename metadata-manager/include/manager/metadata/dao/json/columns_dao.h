/*
 * Copyright 2021 tsurugi project.
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
#ifndef MANAGER_METADATA_MANAGER_INCLUDE_MANAGER_METADATA_DAO_JSON_COLUMNS_DAO_H_
#define MANAGER_METADATA_MANAGER_INCLUDE_MANAGER_METADATA_DAO_JSON_COLUMNS_DAO_H_

#include <boost/property_tree/ptree.hpp>
#include <string_view>

#include "manager/metadata/dao/columns_dao.h"
#include "manager/metadata/dao/json/db_session_manager.h"
#include "manager/metadata/error_code.h"

namespace manager::metadata::db::json {

class ColumnsDAO : public manager::metadata::db::ColumnsDAO {
 public:
  explicit ColumnsDAO([[maybe_unused]] DBSessionManager* session_manager) {}

  manager::metadata::ErrorCode prepare() const override {
    // Do nothing and return of ErrorCode::OK.
    return ErrorCode::OK;
  }

  manager::metadata::ErrorCode insert_one_column_metadata(
      [[maybe_unused]] const ObjectIdType table_id,
      [[maybe_unused]] const boost::property_tree::ptree& column)
      const override {
    // Do nothing and return of ErrorCode::OK.
    return ErrorCode::OK;
  }

  manager::metadata::ErrorCode select_column_metadata(
      [[maybe_unused]] std::string_view object_key,
      [[maybe_unused]] std::string_view object_value,
      [[maybe_unused]] boost::property_tree::ptree& object) const override {
    // Do nothing and return of ErrorCode::OK.
    return ErrorCode::OK;
  }

  manager::metadata::ErrorCode delete_column_metadata(
      [[maybe_unused]] std::string_view object_key,
      [[maybe_unused]] std::string_view object_value) const override {
    // Do nothing and return of ErrorCode::OK.
    return ErrorCode::OK;
  }
};  // class ColumnsDAO

}  // namespace manager::metadata::db::json

#endif  // MANAGER_METADATA_MANAGER_INCLUDE_MANAGER_METADATA_DAO_JSON_COLUMNS_DAO_H_
