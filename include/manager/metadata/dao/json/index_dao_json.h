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

#include <string>
#include <string_view>
#include <vector>

#include <boost/property_tree/ptree.hpp>

#include "manager/metadata/dao/json/dao_json.h"
#include "manager/metadata/error_code.h"
#include "manager/metadata/indexes.h"

namespace manager::metadata::db {

/**
 * @brief DAO class for accessing index metadata for JSON data.
 */
class IndexDaoJson : public DaoJson {
 public:
  // Root node name for index metadata.
  static constexpr const char* const kRootNode = "indexes";

  // Inheritance constructor.
  using DaoJson::DaoJson;

  /**
   * @brief Prepare to access the constraint metadata JSON file.
   * @return ErrorCode::OK if success, otherwise an error code.
   */
  manager::metadata::ErrorCode prepare() override;

  /**
   * @brief Add metadata object to metadata table file.
   * @param object     [in]  index metadata object to add.
   * @param object_id  [out] object id of the added row.
   * @return ErrorCode::OK if success, otherwise an error code.
   * @note  If success, metadata object is added management metadata.
   *   e.g. format version, generation, etc...
   */
  manager::metadata::ErrorCode insert(const boost::property_tree::ptree& object,
                                      ObjectId& object_id) const override;

  /**
   * @brief Get all metadata objects from a metadata table file.
   *   If the table metadata does not exist, return the container as empty.
   * @param objects  [out] all indexes metadata.
   * @return ErrorCode::OK if success, otherwise an error code.
   */
  manager::metadata::ErrorCode select_all(
      std::vector<boost::property_tree::ptree>& objects) const override;

  /**
   * @brief Get a metadata object from a metadata table file.
   * @param key     [in]  key. column name of a index metadata table.
   * @param value   [in]  value to be filtered.
   * @param object  [out] index metadata to get, where the given
   *   key equals the given value.
   * @retval ErrorCode::OK if success.
   * @retval ErrorCode::ID_NOT_FOUND if the object id does not exist.
   * @retval ErrorCode::NAME_NOT_FOUND if the object name does not exist.
   * @retval otherwise an error code.
   */
  manager::metadata::ErrorCode select(
      std::string_view key, std::string_view object_value,
      boost::property_tree::ptree& object) const override;

  /**
   * @brief Updates metadata objects in the metadata table.
   * @param key     [in]  key. column name of a index metadata table.
   * @param value   [in]  value to be filtered.
   * @param object  [in]  index metadata object to be update.
   * @return ErrorCode::OK if success, otherwise an error code.
   */
  manager::metadata::ErrorCode update(
      std::string_view key, std::string_view value,
      const boost::property_tree::ptree& object) const override;

  /**
   * @brief Remove a metadata object from a metadata table file.
   * @param key        [in]  key. column name of a index metadata table.
   * @param value      [in]  value to be filtered.
   * @param object_id  [out] object id of the deleted row.
   * @retval ErrorCode::OK if success.
   * @retval ErrorCode::ID_NOT_FOUND if the object id does not exist.
   * @retval ErrorCode::NAME_NOT_FOUND if the object name does not exist.
   * @retval otherwise an error code.
   */
  manager::metadata::ErrorCode remove(std::string_view key,
                                      std::string_view value,
                                      ObjectId& object_id) const override;

 private:
  // Name of the index metadata management file.
  static constexpr const char* const kIndexMetadataName = "indexes";
  // Object ID key name for index ID.
  static constexpr const char* const kOidKeyNameIndex = "indexes";

};  // class IndexDaoJson

}  // namespace manager::metadata::db
