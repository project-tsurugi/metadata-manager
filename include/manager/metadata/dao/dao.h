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

#include <memory>
#include <string>
#include <string_view>
#include <vector>

#include <boost/property_tree/ptree.hpp>

#include "manager/metadata/dao/db_session_manager.h"
#include "manager/metadata/error_code.h"
#include "manager/metadata/metadata.h"

namespace manager::metadata::db {
/**
 * @brief
 */
class Dao {
 public:
  /**
   * @brief Column name of the privilege.
   */
  class PrivilegeColumn {
   public:
    static constexpr const char* const kSelect     = "Select";
    static constexpr const char* const kInsert     = "Insert";
    static constexpr const char* const kUpdate     = "Update";
    static constexpr const char* const kDelete     = "Delete";
    static constexpr const char* const kTruncate   = "Truncate";
    static constexpr const char* const kReferences = "References";
    static constexpr const char* const kTrigger    = "Trigger";
  };  // class ColumnName

  Dao() {}
  virtual ~Dao() {}

  virtual manager::metadata::ErrorCode prepare() = 0;

  /**
   * @brief Verify that the object with the specified name exists
   *   in the metadata.
   * @param name  [in]  object name.
   * @return true if it exists, otherwise false.
   */
  virtual bool exists(std::string_view name) const {
    boost::property_tree::ptree object;
    return (this->select(Object::NAME, {name}, object) == ErrorCode::OK);
  }

  /**
   * @brief Verify that the object with the specified id exists
   *   in the metadata.
   * @param id  [in]  object id.
   * @return true if it exists, otherwise false.
   */
  virtual bool exists(ObjectId id) const {
    boost::property_tree::ptree object;
    return (this->select(Object::ID, {std::to_string(id)}, object) ==
            ErrorCode::OK);
  }

  /**
   * @brief Verify that the object with the specified name exists in the
   *   table metadata.
   * @param object  [in]  object.
   * @return true if it exists, otherwise false.
   */
  virtual bool exists(const boost::property_tree::ptree& object) const {
    auto opt_name_value = object.get_optional<std::string>(Object::NAME);
    if (opt_name_value) {
      return this->exists(opt_name_value.value());
    } else {
      return false;
    }
  }

  /**
   * @brief Insert a metadata object into the metadata table.
   * @param object     [in]  metadata object.
   * @param object_id  [out] object id of the added row.
   * @return If success ErrorCode::OK, otherwise error code.
   */
  virtual manager::metadata::ErrorCode insert(
      const boost::property_tree::ptree& object,
      ObjectIdType& object_id) const = 0;

  /**
   * @brief Select all metadata objects from the metadata table.
   * @param object  [out] all metadata objects.
   * @return If success ErrorCode::OK, otherwise error codes.
   */
  virtual manager::metadata::ErrorCode select_all(
      std::vector<boost::property_tree::ptree>& objects) const = 0;

  /**
   * @brief Select a metadata object from the metadata table.
   * @param key     [in]  key name of the metadata object.
   * @param values  [in]  value of key.
   * @param object  [out] a selected metadata object.
   * @retval ErrorCode::OK if success.
   * @retval ErrorCode::ID_NOT_FOUND if the table id does not exist.
   * @retval ErrorCode::NAME_NOT_FOUND if the table name does not exist.
   * @retval otherwise an error code.
   */
  virtual manager::metadata::ErrorCode select(
      std::string_view, const std::vector<std::string_view>& values,
      boost::property_tree::ptree& objects) const = 0;

  /**
   * @brief Update a metadata object into the metadata table.
   * @param key     [in]  key name of the metadata object.
   * @param values  [in]  value of key.
   * @param object  [in]  metadata object.
   * @return If success ErrorCode::OK, otherwise error code.
   */
  virtual manager::metadata::ErrorCode update(
      std::string_view key, const std::vector<std::string_view>& values,
      const boost::property_tree::ptree& object) const = 0;

  /**
   * @brief Delete a metadata object from the metadata table.
   * @param key        [in]  key name of the metadata object.
   * @param values     [in]  value of key.
   * @param object_id  [out] object id of the deleted row.
   * @return If success ErrorCode::OK, otherwise error codes.
   */
  virtual manager::metadata::ErrorCode remove(
      std::string_view key, const std::vector<std::string_view>& values,
      ObjectId& object_id) const = 0;

  /**
   * @brief Get a NOT_FOUND error code corresponding to the key.
   * @param key  [in]  key name of the metadata object.
   * @retval ErrorCode::ID_NOT_FOUND if the key is id.
   * @retval ErrorCode::NAME_NOT_FOUND if the key is name.
   * @retval ErrorCode::NOT_FOUND if the key is otherwise.
   */
  virtual manager::metadata::ErrorCode get_not_found_error_code(
      std::string_view key) const {
    if (key == Object::ID) {
      return ErrorCode::ID_NOT_FOUND;
    } else if (key == Object::NAME) {
      return ErrorCode::NAME_NOT_FOUND;
    } else {
      return ErrorCode::NOT_FOUND;
    }
  }
};  // class Dao

}  // namespace manager::metadata::db
