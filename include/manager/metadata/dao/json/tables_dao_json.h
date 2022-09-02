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
#ifndef MANAGER_METADATA_DAO_JSON_TABLES_DAO_JSON_H_
#define MANAGER_METADATA_DAO_JSON_TABLES_DAO_JSON_H_

#include <memory>
#include <string_view>
#include <vector>

#include <boost/property_tree/ptree.hpp>

#include "manager/metadata/dao/json/db_session_manager_json.h"
#include "manager/metadata/dao/tables_dao.h"
#include "manager/metadata/error_code.h"

namespace manager::metadata::db::json {

class TablesDAO : public manager::metadata::db::TablesDAO {
 public:
  explicit TablesDAO(DBSessionManager* session_manager)
      : session_manager_(session_manager) {}

  manager::metadata::ErrorCode prepare() const override;

  manager::metadata::ErrorCode insert_table_metadata(
      const boost::property_tree::ptree& table,
      ObjectIdType& table_id) const override;

  manager::metadata::ErrorCode select_table_metadata(
      std::string_view object_key, std::string_view object_value,
      boost::property_tree::ptree& object) const override;
  manager::metadata::ErrorCode select_table_metadata(
      std::vector<boost::property_tree::ptree>& container) const override;

  manager::metadata::ErrorCode update_reltuples(
      [[maybe_unused]] const float reltuples,
      [[maybe_unused]] std::string_view object_key,
      [[maybe_unused]] std::string_view object_value,
      [[maybe_unused]] ObjectIdType& table_id) const override {
    return ErrorCode::NOT_SUPPORTED;
  }

  manager::metadata::ErrorCode delete_table_metadata(
      std::string_view object_key, std::string_view object_value,
      ObjectIdType& table_id) const override;

 private:
  // root node.
  static constexpr const char* const TABLES_NODE = "tables";
  // table name.
  static constexpr const char* const TABLE_NAME = "tables";
  DBSessionManager* session_manager_;

  manager::metadata::ErrorCode get_metadata_object(
      std::string_view object_key, std::string_view object_value,
      boost::property_tree::ptree& object) const;
};  // class TablesDAO

}  // namespace manager::metadata::db::json

#endif  // MANAGER_METADATA_DAO_JSON_TABLES_DAO_JSON_H_
