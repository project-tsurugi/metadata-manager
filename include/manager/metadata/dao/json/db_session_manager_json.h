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
#pragma once

#include <memory>
#include <string>
#include <boost/property_tree/ptree.hpp>

#include "manager/metadata/dao/db_session_manager.h"
#include "manager/metadata/error_code.h"
#include "manager/metadata/dao/generic_dao.h"

namespace manager::metadata::db {
/**
 * @brief
 */
struct Connection {
  std::string json_file;
};

/**
 * @brief
 */
class DbSessionManagerJson : public DBSessionManager {
 public:
  DbSessionManagerJson()
      : contents_(std::make_unique<boost::property_tree::ptree>()) {}
  DbSessionManagerJson(const DBSessionManager&) = delete;
  DbSessionManagerJson& operator=(const DBSessionManager&) = delete;

  manager::metadata::ErrorCode get_dao(
      const GenericDAO::TableName,
      std::shared_ptr<GenericDAO>&) override { return ErrorCode::UNKNOWN; }
  std::shared_ptr<Dao> get_index_dao() override;

  Connection connection() const { return conn_; }

//  manager::metadata::ErrorCode connect() override { return ErrorCode::OK; }
  manager::metadata::ErrorCode connect(std::string_view file_name,
                                       std::string_view root_node);
  manager::metadata::ErrorCode start_transaction() override;
  manager::metadata::ErrorCode commit() override;
  manager::metadata::ErrorCode rollback() override;
 // void close() override {}

  manager::metadata::ErrorCode load_contents() const;
  boost::property_tree::ptree* get_contents() const {
    return contents_.get();
  }

 private:
  Connection conn_;
  std::unique_ptr<boost::property_tree::ptree> contents_;

  manager::metadata::ErrorCode save_contents() const;
  void clear_contents() { contents_->clear(); }
};  // class DbSessionManagerJson

} // manager//metadata::db


namespace manager::metadata::db::json {
/**
 * @brief
 */
class DBSessionManager : public manager::metadata::db::DBSessionManager {
 public:
  DBSessionManager()
      : file_name_(""), meta_object_(std::make_unique<boost::property_tree::ptree>()) {}

  manager::metadata::ErrorCode get_dao(
      const GenericDAO::TableName table_name,
      std::shared_ptr<GenericDAO>& gdao) override;

  std::shared_ptr<Dao> get_index_dao() override { return nullptr; }

 // Connection connection() const { return conn_; }

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
//  manager::metadata::db::Connection conn_;  // dummy for build

  void init_meta_data();
  manager::metadata::ErrorCode save_object() const;
};  // class DBSessionManager

}  // namespace manager::metadata::db::json
