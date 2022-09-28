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
#ifndef MANAGER_METADATA_DAO_JSON_CONSTRAINTS_DAO_JSON_H_
#define MANAGER_METADATA_DAO_JSON_CONSTRAINTS_DAO_JSON_H_

#include <vector>

#include <boost/property_tree/ptree.hpp>

#include "manager/metadata/dao/constraints_dao.h"
#include "manager/metadata/dao/json/db_session_manager_json.h"
#include "manager/metadata/error_code.h"

namespace manager::metadata::db::json {

class ConstraintsDAO : public manager::metadata::db::ConstraintsDAO {
 public:
  explicit ConstraintsDAO(DBSessionManager* session_manager) {}

  manager::metadata::ErrorCode prepare() const override {
    // Do nothing and return of ErrorCode::OK.
    return ErrorCode::OK;
  }

  manager::metadata::ErrorCode insert_constraint_metadata(
      [[maybe_unused]] const boost::property_tree::ptree& constraint,
      [[maybe_unused]] ObjectId& constraint_id) const override {
    // Do nothing and return of ErrorCode::OK.
    return ErrorCode::OK;
  }

  manager::metadata::ErrorCode select_constraint_metadata(
      [[maybe_unused]] std::string_view object_key, [[maybe_unused]] std::string_view object_value,
      [[maybe_unused]] boost::property_tree::ptree& object) const override {
    // Do nothing and return of ErrorCode::OK.
    return ErrorCode::OK;
  }

  manager::metadata::ErrorCode select_constraint_metadata(
      [[maybe_unused]] std::vector<boost::property_tree::ptree>& constraint_container)
      const override {
    // Do nothing and return of ErrorCode::OK.
    return ErrorCode::OK;
  }

  manager::metadata::ErrorCode delete_constraint_metadata(
      [[maybe_unused]] std::string_view object_key, [[maybe_unused]] std::string_view object_value,
      [[maybe_unused]] ObjectId& constraint_id) const override {
    // Do nothing and return of ErrorCode::OK.
    return ErrorCode::OK;
  }
};  // class ConstraintsDAO

}  // namespace manager::metadata::db::json

#endif  // MANAGER_METADATA_DAO_JSON_CONSTRAINTS_DAO_JSON_H_
