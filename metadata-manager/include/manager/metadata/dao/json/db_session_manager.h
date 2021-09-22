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
#ifndef MANAGER_METADATA_DAO_JSON_DB_SESSION_MANAGER_H_
#define MANAGER_METADATA_DAO_JSON_DB_SESSION_MANAGER_H_

#include <boost/property_tree/ptree.hpp>

#include "manager/metadata/dao/db_session_manager.h"

namespace manager::metadata::db::json {

class DBSessionManager : public manager::metadata::db::DBSessionManager {
 public:
  DBSessionManager()
      : meta_object_(std::make_unique<boost::property_tree::ptree>()) {}

  manager::metadata::ErrorCode get_dao(
      const GenericDAO::TableName table_name,
      std::shared_ptr<GenericDAO>& gdao) override;

  manager::metadata::ErrorCode start_transaction() override;
  manager::metadata::ErrorCode commit() override;
  manager::metadata::ErrorCode rollback() override;

  manager::metadata::ErrorCode connect(std::string_view file_name,
                                       std::string_view initial_node);
  boost::property_tree::ptree* get_container() const;
  manager::metadata::ErrorCode load_object() const;

  DBSessionManager(const DBSessionManager&) = delete;
  DBSessionManager& operator=(const DBSessionManager&) = delete;

 private:
  std::string file_name_;
  std::unique_ptr<boost::property_tree::ptree> meta_object_;

  void init_meta_data();
  manager::metadata::ErrorCode save_object() const;
};  // class DBSessionManager

}  // namespace manager::metadata::db::json

#endif  // MANAGER_METADATA_DAO_JSON_DB_SESSION_MANAGER_H_
