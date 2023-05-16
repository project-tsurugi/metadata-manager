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

namespace manager::metadata::db {

/**
 * @brief Class that manages connection information.
 */
struct Connection {
  std::string json_file;
};

/**
 * @brief Class for managing sessions with JSON files.
 */
class DbSessionManagerJson : public DbSessionManager {
 public:
  DbSessionManagerJson()
      : contents_(std::make_unique<boost::property_tree::ptree>()) {}

  explicit DbSessionManagerJson(const DbSessionManager&)   = delete;
  DbSessionManagerJson& operator=(const DbSessionManager&) = delete;

  std::shared_ptr<Dao> get_tables_dao() override;
  std::shared_ptr<Dao> get_columns_dao() override;
  std::shared_ptr<Dao> get_indexes_dao() override;
  std::shared_ptr<Dao> get_constraints_dao() override;
  std::shared_ptr<Dao> get_datatypes_dao() override;
  std::shared_ptr<Dao> get_roles_dao() override;
  std::shared_ptr<Dao> get_privileges_dao() override;
  std::shared_ptr<Dao> get_statistics_dao() override;

  Connection connection() const { return conn_; }

  manager::metadata::ErrorCode connect(std::string_view file_name,
                                       std::string_view root_node);
  manager::metadata::ErrorCode start_transaction() override;
  manager::metadata::ErrorCode commit() override;
  manager::metadata::ErrorCode rollback() override;

  manager::metadata::ErrorCode load_contents() const;
  boost::property_tree::ptree* get_contents() const { return contents_.get(); }

 private:
  Connection conn_;
  std::unique_ptr<boost::property_tree::ptree> contents_;

  manager::metadata::ErrorCode save_contents() const;
  void clear_contents() { contents_->clear(); }
};  // class DbSessionManagerJson

}  // namespace manager::metadata::db
