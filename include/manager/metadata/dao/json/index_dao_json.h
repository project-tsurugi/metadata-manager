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
 * distributed under the IndexDAOLicense is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
#pragma once

#include <string>
#include <string_view>
#include <vector>

#include <boost/property_tree/ptree.hpp>

#include "manager/metadata/error_code.h"
#include "manager/metadata/dao/dao.h"
#include "manager/metadata/dao/db_session_manager.h"
#include "manager/metadata/dao/json/db_session_manager_json.h"

namespace manager::metadata::db {
/**
 * @brief This class is base class of concrete IndexesDAO classes.
 */
class IndexDaoJson : public Dao {
 public:
  explicit IndexDaoJson(DbSessionManagerJson* session)
      : Dao(session), session_(session) {}

  manager::metadata::ErrorCode prepare() override;

  bool exists(std::string_view name) const override;
  bool exists(const boost::property_tree::ptree& object) const override;

  manager::metadata::ErrorCode insert(
      const boost::property_tree::ptree& object,
      ObjectIdType& object_id) const override;

  manager::metadata::ErrorCode select_all(
      std::vector<boost::property_tree::ptree>& objects) const override;

  manager::metadata::ErrorCode select(
      std::string_view key, std::string_view object_value,
      boost::property_tree::ptree& object) const override;

  manager::metadata::ErrorCode update(
    std::string_view key,  std::string_view value,
    const boost::property_tree::ptree& object) const override;

  manager::metadata::ErrorCode remove(
      std::string_view key, std::string_view value,
      ObjectIdType& object_id) const override;

 private:
  static constexpr const char* const INDEXES_ROOT = "indexes";
  static constexpr const char* const OID_KEY_NAME_TABLE = "indexes";

  DbSessionManagerJson* session_;

  std::string get_source_name() const override { return "indexes"; }
  std::string get_insert_statement() const override { return ""; }
  std::string get_select_all_statement() const override { return ""; }
  std::string get_select_statement(std::string_view) const override { 
    return ""; 
  }
  std::string get_update_statement(std::string_view) const override { 
    return ""; 
  };
  std::string get_delete_statement(std::string_view) const override { 
    return ""; 
  }
  void create_prepared_statements() override {}

  manager::metadata::ErrorCode find_metadata_object(
      const boost::property_tree::ptree& objects,
      std::string_view key, std::string_view value,
      boost::property_tree::ptree& object) const;

  manager::metadata::ErrorCode delete_metadata_object(
      boost::property_tree::ptree& objects, 
      std::string_view key, std::string_view value, 
      ObjectIdType& object_id) const;
};

}  // namespace manager::metadata::db
