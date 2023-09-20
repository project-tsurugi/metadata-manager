/*
 * Copyright 2021-2023 tsurugi project.
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
#ifndef MANAGER_METADATA_DAO_JSON_COLUMNS_DAO_JSON_H_
#define MANAGER_METADATA_DAO_JSON_COLUMNS_DAO_JSON_H_

#include <map>
#include <string>
#include <string_view>
#include <vector>

#include <boost/property_tree/ptree.hpp>

#include "manager/metadata/dao/json/dao_json.h"
#include "manager/metadata/error_code.h"
#include "manager/metadata/tables.h"

namespace manager::metadata::db {

/**
 * @brief DAO class for accessing column metadata for JSON data.
 */
class ColumnsDaoJson : public DaoJson {
 public:
  // Root node name for constraint metadata.
  static constexpr const char* const kRootNode = "tables";

  /**
    * @brief Construct a new Column Metadata DAO class for JSON data.
    * @param session pointer to DB session manager for JSON.
    */
  explicit ColumnsDaoJson(DbSessionManagerJson* session)
      : DaoJson(session, kTableName) {}

  /**
   * @brief Insert a metadata object into the metadata table.
   * @param object     [in]  metadata object.
   * @param object_id  [out] object id of the added row.
   * @return If success ErrorCode::OK, otherwise error code.
   * @note  If success, metadata object is added management metadata.
   *   e.g. format version, generation, etc...
   */
  manager::metadata::ErrorCode insert(const boost::property_tree::ptree& object,
                                      ObjectId& object_id) const override;

  /**
   * @brief Select a metadata object from the metadata table.
   * @param keys    [in]  key name and value of the metadata object.
   * @param object  [out] a selected metadata object.
   * @return If success ErrorCode::OK, otherwise error code.
   */
  manager::metadata::ErrorCode select(
      const std::map<std::string_view, std::string_view>& keys,
      boost::property_tree::ptree& object) const override;

  /**
   * @brief Unsupported function.
   * @return Always ErrorCode::NOT_SUPPORTED.
   */
  manager::metadata::ErrorCode update(
      const std::map<std::string_view, std::string_view>&,
      const boost::property_tree::ptree&, uint64_t&) const override {
    // Do nothing and return of ErrorCode::NOT_SUPPORTED.
    return ErrorCode::NOT_SUPPORTED;
  }

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
  // Name of the column metadata management file.
  static constexpr const char* const kTableName = "tables";
  // Object ID key name for column ID.
  static constexpr const char* const kOidKeyNameColumn = "column";

  /**
   * @brief Find metadata object from metadata table.
   * @param objects  [in]  metadata container.
   * @param keys     [in]  key name and value of a table metadata table.
   * @param object   [out] metadata-object with the specified name.
   * @return If success ErrorCode::OK, otherwise error code.
   */
  manager::metadata::ErrorCode find_metadata_object(
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
  manager::metadata::ErrorCode delete_metadata_object(
      boost::property_tree::ptree& objects,
      const std::map<std::string_view, std::string_view>& keys,
      std::vector<ObjectId>& object_ids) const;
};  // class ColumnsDaoJson

}  // namespace manager::metadata::db

#endif  // MANAGER_METADATA_DAO_JSON_COLUMNS_DAO_JSON_H_
