/*
 * Copyright 2020-2023 tsurugi project.
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

#include "manager/metadata/dao/json/dao_json.h"
#include "manager/metadata/error_code.h"
#include "manager/metadata/index.h"

namespace manager::metadata::db {

/**
 * @brief DAO class for accessing index metadata for JSON data.
 */
class IndexDaoJson : public DaoJson {
 public:
  // Root node name for index metadata.
  static constexpr const char* const kRootNode = "indexes";

  /**
   * @brief Construct a new Index Metadata DAO class for JSON data.
   * @param session pointer to DB session manager for JSON.
   */
  explicit IndexDaoJson(DbSessionManagerJson* session)
      : DaoJson(session, kTableName) {}

  /**
   * @brief Insert a metadata object into the metadata file.
   * @param object     [in]  metadata object.
   * @param object_id  [out] object id of the added row.
   * @return If success ErrorCode::OK, otherwise error code.
   * @note  If success, metadata object is added management metadata.
   *   e.g. format version, generation, etc...
   */
  manager::metadata::ErrorCode insert(const boost::property_tree::ptree& object,
                                      ObjectId& object_id) const override;

  /**
   * @brief Select a metadata object from the metadata file.
   * @param keys    [in]  key name and value of the metadata object.
   * @param object  [out] a selected metadata object.
   * @return If success ErrorCode::OK, otherwise error code.
   */
  manager::metadata::ErrorCode select(
      const std::map<std::string_view, std::string_view>& keys,
      boost::property_tree::ptree& object) const override;

  /**
   * @brief Update a metadata object into the metadata file.
   * @param keys    [in]  key name and value of the metadata object.
   * @param object  [in]  metadata object.
   * @param rows    [out] number of updated metadata object.
   * @return If success ErrorCode::OK, otherwise error code.
   */
  manager::metadata::ErrorCode update(
      const std::map<std::string_view, std::string_view>& keys,
      const boost::property_tree::ptree& object, uint64_t& rows) const override;

  /**
   * @brief Remove a metadata object from a metadata table file.
   * @param keys        [in]  key name and value of the metadata object.
   * @param object_ids  [out] object id of the deleted rows.
   * @return If success ErrorCode::OK, otherwise error code.
   */
  manager::metadata::ErrorCode remove(
      const std::map<std::string_view, std::string_view>& keys,
      std::vector<ObjectId>& object_ids) const override;

 private:
  // Name of the index metadata management file.
  static constexpr const char* const kTableName = "indexes";
  // Object ID key name for index ID.
  static constexpr const char* const kOidKeyNameIndex = "indexes";

  /**
   * @brief Find metadata object from metadata table.
   * @param objects  [in]  container of JSON object.
   * @param keys     [in]  key name and value of the metadata object.
   *   e.g. object ID, object name.
   * @param object   [out] metadata-object with the specified name.
   * @return If success ErrorCode::OK, otherwise error code.
   */
  ErrorCode find_metadata_object(
      const boost::property_tree::ptree& objects,
      const std::map<std::string_view, std::string_view>& keys,
      boost::property_tree::ptree& object) const;

  /**
   * @brief Delete a metadata object from a metadata table file.
   * @param objects     [in/out] metadata container.
   * @param keys        [in]     key name and value of a table metadata table.
   * @param object_ids  [out]    table id of the row deleted.
   * @return If success ErrorCode::OK, otherwise error code.
   */
  ErrorCode delete_metadata_object(
      boost::property_tree::ptree& objects,
      const std::map<std::string_view, std::string_view>& keys,
      std::vector<ObjectId>& object_ids) const;
};  // class IndexDaoJson

}  // namespace manager::metadata::db
