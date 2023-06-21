/*
 * Copyright 2022-2023 tsurugi project.
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
#ifndef MANAGER_METADATA_DAO_JSON_CONSTRAINTS_DAO_JSON_H_
#define MANAGER_METADATA_DAO_JSON_CONSTRAINTS_DAO_JSON_H_

#include <string>
#include <string_view>
#include <vector>

#include <boost/property_tree/ptree.hpp>

#include "manager/metadata/constraint.h"
#include "manager/metadata/dao/json/dao_json.h"
#include "manager/metadata/error_code.h"

namespace manager::metadata::db {

/**
 * @brief DAO class for accessing constraint metadata for JSON data.
 */
class ConstraintsDaoJson : public DaoJson {
 public:
  // Root node name for constraint metadata.
  static constexpr const char* const kRootNode = "tables";

  /**
    * @brief Construct a new Constraint Metadata DAO class for JSON data.
    * @param session pointer to DB session manager for JSON.
    */
  explicit ConstraintsDaoJson(DbSessionManagerJson* session)
      : DaoJson(session, kTableName) {}

  /**
   * @brief Add metadata object to metadata table file.
   * @param object     [in]  constraint metadata object to add.
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
   * @param objects  [out] all constraints metadata.
   * @return ErrorCode::OK if success, otherwise an error code.
   */
  manager::metadata::ErrorCode select_all(
      std::vector<boost::property_tree::ptree>& objects) const override;

  /**
   * @brief Get a metadata object from a metadata table file.
   * @param key     [in]  key. column name of a constraint metadata table.
   * @param values  [in]  value to be filtered.
   * @param object  [out] constraint metadata to get, where the given
   *   key equals the given value.
   * @retval ErrorCode::OK if success.
   * @retval ErrorCode::ID_NOT_FOUND if the object id does not exist.
   * @retval ErrorCode::NAME_NOT_FOUND if the object name does not exist.
   * @retval otherwise an error code.
   */
  manager::metadata::ErrorCode select(
      std::string_view key, const std::vector<std::string_view>& values,
      boost::property_tree::ptree& object) const override;

  /**
   * @brief Function defined for compatibility.
   * @return Always ErrorCode::OK.
   */
  manager::metadata::ErrorCode update(
      std::string_view, const std::vector<std::string_view>&,
      const boost::property_tree::ptree&) const override {
    // Do nothing and return of ErrorCode::OK.
    return ErrorCode::OK;
  }

  /**
   * @brief Remove a metadata object from a metadata table file.
   * @param key        [in]  key. column name of a constraint metadata table.
   * @param values     [in]  value to be filtered.
   * @param object_id  [out] object id of the deleted row.
   * @retval ErrorCode::OK if success.
   * @retval ErrorCode::ID_NOT_FOUND if the object id does not exist.
   * @retval ErrorCode::NAME_NOT_FOUND if the object name does not exist.
   * @retval otherwise an error code.
   */
  manager::metadata::ErrorCode remove(
      std::string_view key, const std::vector<std::string_view>& values,
      ObjectId& object_id) const override;

 private:
  // Name of the constraint metadata management file.
  static constexpr const char* const kTableName = "tables";
  // Object ID key name for constraint ID.
  static constexpr const char* const kOidKeyNameConstraint = "constraint";

  /**
   * @brief Get metadata-object.
   * @param objects  [in]  metadata container.
   * @param key      [in]  key. column name of a constraint metadata table.
   * @param value    [in]  value to be filtered.
   * @param object   [out] metadata-object with the specified name.
   * @retval ErrorCode::OK if success.
   * @retval ErrorCode::ID_NOT_FOUND if the object id does not exist.
   * @retval ErrorCode::NAME_NOT_FOUND if the object name does not exist.
   * @retval otherwise an error code.
   */
  manager::metadata::ErrorCode get_metadata_object(
      const boost::property_tree::ptree& objects, std::string_view key,
      std::string_view value, boost::property_tree::ptree& object) const;

  /**
   * @brief Delete a metadata object from a metadata constraint file.
   * @param objects    [in/out] metadata container.
   * @param key        [in]     key. column name of a constraint metadata table.
   * @param value      [in]     value to be filtered.
   * @param object_id  [out]    table id of the row deleted.
   * @retval ErrorCode::OK if success.
   * @retval ErrorCode::ID_NOT_FOUND if the object id does not exist.
   * @retval ErrorCode::NAME_NOT_FOUND if the object name does not exist.
   * @retval otherwise an error code.
   */
  manager::metadata::ErrorCode delete_metadata_object(
      boost::property_tree::ptree& objects, std::string_view key,
      std::string_view value, ObjectId& object_id) const;
};  // class ConstraintsDaoJson

}  // namespace manager::metadata::db

#endif  // MANAGER_METADATA_DAO_JSON_CONSTRAINTS_DAO_JSON_H_
