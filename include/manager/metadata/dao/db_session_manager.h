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
#pragma once

#include <memory>
#include <string>

#include "manager/metadata/error_code.h"

namespace manager::metadata::db {

class Dao;

class DbSessionManager {
 public:
  /**
   * @brief Returns an instance of the DB session manager.
   * @return DB session manager instance.
   */
  static DbSessionManager& get_instance();

  DbSessionManager() {}
  explicit DbSessionManager(std::string database) : database_(database) {}

  virtual ~DbSessionManager() {}

  DbSessionManager(const DbSessionManager&)            = delete;
  DbSessionManager& operator=(const DbSessionManager&) = delete;

  virtual ErrorCode connect() = 0;

  virtual std::shared_ptr<Dao> get_tables_dao()      = 0;
  virtual std::shared_ptr<Dao> get_columns_dao()     = 0;
  virtual std::shared_ptr<Dao> get_indexes_dao()     = 0;
  virtual std::shared_ptr<Dao> get_constraints_dao() = 0;
  virtual std::shared_ptr<Dao> get_datatypes_dao()   = 0;
  virtual std::shared_ptr<Dao> get_roles_dao()       = 0;
  virtual std::shared_ptr<Dao> get_privileges_dao()  = 0;
  virtual std::shared_ptr<Dao> get_statistics_dao()  = 0;

  virtual ErrorCode start_transaction() = 0;
  virtual ErrorCode commit()            = 0;
  virtual ErrorCode rollback()          = 0;

 protected:
  std::string database_;
};  // class DbSessionManager

}  // namespace manager::metadata::db
