/*
 * Copyright 2020-2022 tsurugi project.
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

#include <string_view>
#include <memory>
#include <boost/property_tree/ptree.hpp>
#include "manager/metadata/metadata.h"
#include "manager/metadata/error_code.h"
#include "manager/metadata/dao/db_session_manager.h"
#include "manager/metadata/dao/common/statements.h"

namespace manager::metadata::db {
class Dao {
 public:
  explicit Dao(DBSessionManager*) {}
  virtual ~Dao() {}

  virtual manager::metadata::ErrorCode prepare() = 0;

  /**
   * @brief Check the object which has specified name exists
   * in the metadata table.
   * @param name  [in] object name.
   * @return  true if it exists, otherwise false.
   */
  virtual bool exists(std::string_view name) const = 0;

  /**
   * @brief Check the object which has specified name exists
   * in the metadata table.
   * @param object  [in] metadata object which has name.
   * @return  true if it exists, otherwise false.
   */
  virtual bool exists(const boost::property_tree::ptree& object) const = 0;

  /**
   * @brief Insert a metadata object into the metadata table.
   * @param object    [in]  metadata object.
   * @param object_id [out] object ID.
   * @return  If success ErrorCode::OK, otherwise error code.
   */
  virtual manager::metadata::ErrorCode insert(
      const boost::property_tree::ptree& object,
      ObjectIdType& object_id) const = 0;

  /**
   * @brief Select all metadata objects from the metadata table.
   * @param object  [out] all metadata objects.
   * @return  If success ErrorCode::OK, otherwise error codes.
   */
  virtual manager::metadata::ErrorCode select_all(
      std::vector<boost::property_tree::ptree>& objects) const = 0;

  /**
   * @brief Select a metadata object from the metadata table..
   * @param key     [in] key name of the metadata object.
   * @param value   [in] value of key.
   * @param object  [out] a selected metadata object.
   * @return  If success ErrorCode::OK, otherwise error codes.
   * @retval  ErrorCode::ID_NOT_FOUND if the table id does not exist.
   * @retval  ErrorCode::NAME_NOT_FOUND if the table name does not exist.
   * @retval  otherwise an error code.
   */
  virtual manager::metadata::ErrorCode select(
      std::string_view key, std::string_view value,
      boost::property_tree::ptree& object) const = 0;

  /**
   * @brief Update a metadata object into the metadata table.
   * @param key     [in] key name of the metadata object.
   * @param value   [in] value of key.
   * @param object  [in]  metadata object.
   * @return  If success ErrorCode::OK, otherwise error code.
   */
  virtual manager::metadata::ErrorCode update(
      std::string_view key, std::string_view value,
      const boost::property_tree::ptree& object) const = 0;

  /**
   * @brief Delete a metadata object from the metadata table.
   * @param key       [in] key name of the metadata object.
   * @param value     [in] value of key.
   * @param object_id [out] removed metadata objects.
   * @return  If success ErrorCode::OK, otherwise error codes.
   */
  virtual manager::metadata::ErrorCode remove(
      std::string_view key, std::string_view value,
      ObjectIdType& object_id) const = 0;

protected:
  DBSessionManager* session_;

  InsertStatement insert_statement_;
  SelectAllStatement select_all_statement_;
  std::unordered_map<std::string, SelectStatement> select_statements_;
  std::unordered_map<std::string, UpdateStatement> update_statements_;
  std::unordered_map<std::string, DeleteStatement> delete_statements_;

  virtual std::string get_source_name() const = 0;
  virtual std::string get_insert_statement() const = 0;
  virtual std::string get_select_all_statement() const = 0;
  virtual std::string get_select_statement(std::string_view key) const = 0;
  virtual std::string get_update_statement(std::string_view key) const = 0;
  virtual std::string get_delete_statement(std::string_view key) const = 0;
  virtual void create_prepared_statements() = 0;

  /**
   * @brief
   */
  ErrorCode get_not_found_error_code(std::string_view key) const {
    if (key == Object::ID) {
      return ErrorCode::ID_NOT_FOUND;
    } else if (key == Object::NAME) {
      return ErrorCode::NAME_NOT_FOUND;
    } else {
      return ErrorCode::NOT_FOUND;
    }
  }
};  // class Dao

} // namespace manager::metadata::db
