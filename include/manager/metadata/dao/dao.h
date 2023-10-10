/*
 * Copyright 2020-2023 Project Tsurugi.
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

#include <map>
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

  virtual ErrorCode prepare() = 0;

  /**
   * @brief Verify that the object with the specified name exists
   *   in the metadata.
   * @param name  [in]  object name.
   * @return true if it exists, otherwise false.
   */
  virtual bool exists(std::string_view name) const {
    boost::property_tree::ptree object;
    std::map<std::string_view, std::string_view> keys = {
        {Object::NAME, name}
    };

    auto error = this->select(keys, object);
    return ((error == ErrorCode::OK) && (object.size() >= 1));
  }

  /**
   * @brief Verify that the object with the specified id exists
   *   in the metadata.
   * @param id  [in]  object id.
   * @return true if it exists, otherwise false.
   */
  virtual bool exists(ObjectId id) const {
    boost::property_tree::ptree object;
    std::map<std::string_view, std::string_view> keys = {
        {Object::ID, std::to_string(id)}
    };

    auto error = this->select(keys, object);
    return ((error == ErrorCode::OK) && (object.size() >= 1));
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
   * @note  If success, metadata object is added management metadata.
   *   e.g. format version, generation, etc...
   */
  virtual ErrorCode insert(const boost::property_tree::ptree&,
                           ObjectIdType&) const = 0;

  /**
   * @brief Select a metadata object from the metadata table.
   * @param keys    [in]  key name and value of the metadata object.
   * @param object  [out] a selected metadata object.
   * @return If success ErrorCode::OK, otherwise error code.
   */
  virtual ErrorCode select(const std::map<std::string_view, std::string_view>&,
                           boost::property_tree::ptree&) const = 0;

  /**
   * @brief Update a metadata object into the metadata table.
   * @param keys    [in]  key name and value of the metadata object.
   * @param object  [in]  metadata object.
   * @param rows    [out] number of updated metadata object.
   * @return If success ErrorCode::OK, otherwise error code.
   */
  virtual ErrorCode update(const std::map<std::string_view, std::string_view>&,
                           const boost::property_tree::ptree&,
                           uint64_t&) const = 0;

  /**
   * @brief Update a metadata object into the metadata table.
   * @param keys       [in]  key name and value of the metadata object.
   * @param object_id  [out] object id of the deleted row.
   * @return If success ErrorCode::OK, otherwise error code.
   */
  virtual ErrorCode remove(const std::map<std::string_view, std::string_view>&,
                           std::vector<ObjectId>&) const = 0;
};  // class Dao

}  // namespace manager::metadata::db
