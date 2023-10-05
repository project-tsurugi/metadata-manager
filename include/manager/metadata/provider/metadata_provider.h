/*
 * Copyright 2022-2023 Project Tsurugi.
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
#include <memory>
#include <string>
#include <vector>

#include <boost/property_tree/ptree.hpp>

#include "manager/metadata/dao/dao.h"
#include "manager/metadata/error_code.h"
#include "manager/metadata/metadata.h"

namespace manager::metadata::db {

class MetadataProvider {
 public:
  /**
   * @brief Returns an instance of the metadata provider.
   * @return metadata provider instance.
   */
  static MetadataProvider& get_instance() {
    static MetadataProvider instance;

    return instance;
  }

  /**
   * @brief Initialize and prepare to access the metadata repository.
   * @retval ErrorCode::OK if success.
   * @retval otherwise an error code.
   */
  ErrorCode init();

  // ============================================================================
  /**
   * @brief DB transaction control.
   * @param trans_function  [in]  function to perform transaction processing.
   * @retval ErrorCode::OK if success.
   * @retval otherwise an error code.
   */
  ErrorCode transaction(std::function<ErrorCode()> trans_function);

  // ============================================================================
  /**
   * @brief Add a table metadata object to the metadata table.
   *   If `object_id` is not nullptr, store the id of the added metadata.
   * @param object     [in]  table metadata to add.
   * @param object_id  [out] ID of the added table metadata.
   * @retval ErrorCode::OK if success.
   * @retval otherwise an error code.
   */
  ErrorCode add_table_metadata(const boost::property_tree::ptree& object,
                               ObjectId* object_id = nullptr);
  /**
   * @brief Add a column metadata object to the metadata table.
   *   If `object_id` is not nullptr, store the id of the added metadata.
   * @param object     [in]  constraints metadata to add.
   * @param object_id  [out] ID of the added constraint metadata.
   * @retval ErrorCode::OK if success.
   * @retval otherwise an error code.
   */
  ErrorCode add_column_metadata(const boost::property_tree::ptree& object,
                                ObjectId* object_id = nullptr);

  /**
   * @brief Add an index metadata object to the metadata table.
   *   If `object_id` is not nullptr, store the id of the added metadata.
   * @param object     [in]  index metadata to add.
   * @param object_id  [out] ID of the added index metadata.
   * @retval ErrorCode::OK if success.
   * @retval otherwise an error code.
   */
  ErrorCode add_index_metadata(const boost::property_tree::ptree& object,
                               ObjectId* object_id = nullptr);

  /**
   * @brief Add a constraint metadata object to the metadata table.
   *   If `object_id` is not nullptr, store the id of the added metadata.
   * @param object     [in]  constraints metadata to add.
   * @param object_id  [out] ID of the added constraint metadata.
   * @retval ErrorCode::OK if success.
   * @retval otherwise an error code.
   */
  ErrorCode add_constraint_metadata(const boost::property_tree::ptree& object,
                                    ObjectId* object_id = nullptr);

  /**
   * @brief Add a column statistic to the column statistics table.
   *   If column statistics already exist for the specified table ID and
   *   column information, they are updated.
   *   If `object_id` is not nullptr, store the id of the added statistic.
   * @param object     [in]  one column statistic to add or update.
   * @param object_id  [out] ID of the added column statistic.
   * @retval ErrorCode::OK if success.
   * @retval otherwise an error code.
   */
  ErrorCode add_column_statistic(const boost::property_tree::ptree& object,
                                 ObjectId* object_id = nullptr);

  // ============================================================================
  /**
   * @brief Get the table metadata object from the metadata table with the
   *   specified key value.
   * @param keys    [in]  key name and value of table metadata object.
   * @param object  [out] retrieved table metadata object.
   * @retval ErrorCode::OK if success.
   * @retval ErrorCode::ID_NOT_FOUND if the table id does not exist.
   * @retval ErrorCode::NAME_NOT_FOUND if the table name does not exist.
   * @retval ErrorCode::NOT_FOUND If the key does not exist.
   * @retval otherwise an error code.
   */
  ErrorCode get_table_metadata(
      const std::map<std::string_view, std::string_view>& keys,
      boost::property_tree::ptree& object);

  /**
   * @brief Get the column metadata object from the metadata table with the
   *   specified key value.
   * @param keys    [in]  key name and value of column metadata object.
   * @param object  [out] retrieved column metadata object.
   * @retval ErrorCode::OK if success.
   * @retval ErrorCode::ID_NOT_FOUND if the column id does not exist.
   * @retval ErrorCode::NAME_NOT_FOUND if the column name does not exist.
   * @retval ErrorCode::NOT_FOUND If the key does not exist.
   * @retval otherwise an error code.
   */
  ErrorCode get_column_metadata(
      const std::map<std::string_view, std::string_view>& keys,
      boost::property_tree::ptree& object);

  /**
   * @brief Get the index metadata object from the metadata table with the
   *   specified key value.
   * @param keys    [in]  key name and value of index metadata object.
   * @param object  [out] retrieved index metadata object.
   * @retval ErrorCode::OK if success.
   * @retval ErrorCode::ID_NOT_FOUND if the index id does not exist.
   * @retval ErrorCode::NAME_NOT_FOUND if the index name does not exist.
   * @retval ErrorCode::NOT_FOUND If the key does not exist.
   * @retval otherwise an error code.
   */
  ErrorCode get_index_metadata(
      const std::map<std::string_view, std::string_view>& keys,
      boost::property_tree::ptree& object);

  /**
   * @brief Get the constraint metadata object from the metadata table with the
   *   specified key value.
   * @param keys    [in]  key name and value of constraint metadata object.
   * @param object  [out] retrieved constraint metadata object.
   * @retval ErrorCode::OK if success.
   * @retval ErrorCode::ID_NOT_FOUND if the constraint id does not exist.
   * @retval ErrorCode::NAME_NOT_FOUND if the constraint name does not exist.
   * @retval ErrorCode::NOT_FOUND If the key does not exist.
   * @retval otherwise an error code.
   */
  ErrorCode get_constraint_metadata(
      const std::map<std::string_view, std::string_view>& keys,
      boost::property_tree::ptree& object);

  /**
   * @brief Get a column statistic from the column statistics table with the
   *   specified key value.
   * @param keys    [in]  key name and value of role object.
   * @param object  [out] retrieved column statistics object array.
   * @retval ErrorCode::OK if success.
   * @retval ErrorCode::ID_NOT_FOUND if the statistic id or column id
   *   does not exist.
   * @retval ErrorCode::NAME_NOT_FOUND if the statistic name does not exist.
   * @retval otherwise an error code.
   */
  ErrorCode get_column_statistic(
      const std::map<std::string_view, std::string_view>& keys,
      boost::property_tree::ptree& object);

  /**
   * @brief Get the datatype metadata object from the metadata table with the
   *   specified key value.
   * @param keys    [in]  key name and value of datatype metadata object.
   * @param object  [out] retrieved datatype metadata object.
   * @retval ErrorCode::OK if success.
   * @retval ErrorCode::ID_NOT_FOUND if the datatype id does not exist.
   * @retval ErrorCode::NAME_NOT_FOUND if the datatype name does not exist.
   * @retval ErrorCode::NOT_FOUND if the other datatype key does not exist.
   * @retval otherwise an error code.
   */
  ErrorCode get_datatype_metadata(
      const std::map<std::string_view, std::string_view>& keys,
      boost::property_tree::ptree& object);

  /**
   * @brief Get the role object from the PostgreSQL with the specified key value.
   * @param keys    [in]  key name and value of role object.
   * @param object  [out] retrieved role object array.
   * @retval ErrorCode::OK if success.
   * @retval ErrorCode::ID_NOT_FOUND if the role id does not exist.
   * @retval ErrorCode::NAME_NOT_FOUND if the role name does not exist.
   * @retval otherwise an error code.
   */
  ErrorCode get_role_metadata(
      const std::map<std::string_view, std::string_view>& keys,
      boost::property_tree::ptree& object);

  /**
   * @brief Get privileges from PostgreSQL for the role with the specified key.
   * @param keys    [in]  key name and value of role object.
   * @param object  [out] retrieved privilege object array.
   * @retval ErrorCode::OK if success.
   * @retval ErrorCode::ID_NOT_FOUND if the role id does not exist.
   * @retval ErrorCode::NAME_NOT_FOUND if the role name does not exist.
   * @retval otherwise an error code.
   */
  ErrorCode get_privileges(
      const std::map<std::string_view, std::string_view>& keys,
      boost::property_tree::ptree& object);

  // ============================================================================
  /**
   * @brief Update a table metadata table with the specified key value.
   * @param keys    [in]  key name and value of table metadata object.
   * @param object  [in]  table metadata object.
   * @param rows    [out] number of updated metadata object.
   * @retval ErrorCode::OK if success.
   * @retval ErrorCode::ID_NOT_FOUND if the table id does not exist.
   * @retval ErrorCode::NAME_NOT_FOUND if the table name does not exist.
   * @retval ErrorCode::NOT_FOUND If the key does not exist.
   * @retval otherwise an error code.
   */
  ErrorCode update_table_metadata(
      const std::map<std::string_view, std::string_view>& keys,
      const boost::property_tree::ptree& object, uint64_t* rows = nullptr);

  /**
   * @brief Update a column metadata table with the specified key value.
   * @param keys    [in]  key name and value of column metadata object.
   * @param object  [in]  column metadata object.
   * @param rows    [out] number of updated metadata object.
   * @retval ErrorCode::OK if success.
   * @retval otherwise an error code.
   */
  ErrorCode update_column_metadata(
      const std::map<std::string_view, std::string_view>& keys,
      const boost::property_tree::ptree& object, uint64_t* rows = nullptr);

  /**
   * @brief Update a index metadata table with the specified key value.
   * @param keys    [in]  key name and value of index metadata object.
   * @param object  [in]  index metadata object.
   * @param rows    [out] number of updated metadata object.
   * @retval ErrorCode::OK if success.
   * @retval otherwise an error code.
   */
  ErrorCode update_index_metadata(
      const std::map<std::string_view, std::string_view>& keys,
      const boost::property_tree::ptree& object, uint64_t* rows = nullptr);

  /**
   * @brief Update a constraint metadata table with the specified key value.
   * @param keys    [in]  key name and value of constraint metadata object.
   * @param object  [in]  constraint metadata object.
   * @param rows    [out] number of updated metadata object.
   * @retval ErrorCode::OK if success.
   * @retval otherwise an error code.
   */
  ErrorCode update_constraint_metadata(
      const std::map<std::string_view, std::string_view>& keys,
      const boost::property_tree::ptree& object, uint64_t* rows = nullptr);

  // ============================================================================
  /**
   * @brief Removes all metadata objects with the specified key value
   *   from the metadata table.
   *   (Metadata: table metadata, column metadata, index metadata,
   *   constraint metadata, column statistics)
   * @param keys        [in]  key name and value of table metadata object.
   * @param object_ids  [out] IDs of the removed table metadata.
   * @retval ErrorCode::OK if success.
   * @retval ErrorCode::ID_NOT_FOUND if the table id does not exist.
   * @retval ErrorCode::NAME_NOT_FOUND if the table name does not exist.
   * @retval ErrorCode::NOT_FOUND If the key does not exist.
   * @retval otherwise an error code.
   */
  ErrorCode remove_table_metadata(
      const std::map<std::string_view, std::string_view>& keys,
      std::vector<ObjectId>* object_ids = nullptr);

  /**
   * @brief Removes a column metadata object with the specified key value
   *   from the metadata table.
   * @param keys        [in]  key name and value of column metadata object.
   * @param object_ids  [out] IDs of the removed column metadata.
   * @retval ErrorCode::OK if success.
   * @retval ErrorCode::ID_NOT_FOUND if the column id does not exist.
   * @retval ErrorCode::NAME_NOT_FOUND if the column name does not exist.
   * @retval ErrorCode::NOT_FOUND If the key does not exist.
   * @retval otherwise an error code.
   */
  ErrorCode remove_column_metadata(
      const std::map<std::string_view, std::string_view>& keys,
      std::vector<ObjectId>* object_ids = nullptr);

  /**
   * @brief Removes a index metadata object with the specified key value
   *   from the metadata table.
   * @param keys        [in]  key name and value of index metadata object.
   * @param object_ids  [out] IDs of the removed index metadata.
   * @retval ErrorCode::OK if success.
   * @retval ErrorCode::ID_NOT_FOUND if the index id does not exist.
   * @retval ErrorCode::NAME_NOT_FOUND if the index name does not exist.
   * @retval ErrorCode::NOT_FOUND If the key does not exist.
   * @retval otherwise an error code.
   */
  ErrorCode remove_index_metadata(
      const std::map<std::string_view, std::string_view>& keys,
      std::vector<ObjectId>* object_ids = nullptr);

  /**
   * @brief Removes a constraint metadata object with the specified key value
   *   from the metadata table.
   * @param keys        [in]  key name and value of constraint metadata object.
   * @param object_ids  [out] IDs of the removed constraint metadata.
   * @retval ErrorCode::OK if success.
   * @retval ErrorCode::ID_NOT_FOUND if the constraint id does not exist.
   * @retval ErrorCode::NAME_NOT_FOUND if the constraint name does not exist.
   * @retval ErrorCode::NOT_FOUND If the key does not exist.
   * @retval otherwise an error code.
   */
  ErrorCode remove_constraint_metadata(
      const std::map<std::string_view, std::string_view>& keys,
      std::vector<ObjectId>* object_ids = nullptr);

  /**
   * @brief Removes a column statistics with the specified key value
   *   from the column statistic table.
   * @param keys        [in]  key name and value of column statistic object.
   * @param object_ids  [out] IDs of the removed column statistic.
   * @retval ErrorCode::OK if success.
   * @retval ErrorCode::ID_NOT_FOUND if the statistic id does not exist.
   * @retval ErrorCode::NAME_NOT_FOUND if the statistic name does not exist.
   * @retval ErrorCode::NOT_FOUND If the key does not exist.
   * @retval otherwise an error code.
   */
  ErrorCode remove_column_statistics(
      const std::map<std::string_view, std::string_view>& keys,
      std::vector<ObjectId>* object_ids = nullptr);

  // ============================================================================
  /**
   * @brief Returns whether the error is related to NotFound.
   * @param error  [in]  error code.
   * @retval true: not found.
   * @retval false: other.
   */
  static bool is_not_found(ErrorCode error) {
    return ((error == ErrorCode::ID_NOT_FOUND) ||
            (error == ErrorCode::NAME_NOT_FOUND) ||
            (error == ErrorCode::NOT_FOUND));
  }

  /**
   * @brief Get a NOT_FOUND error code corresponding to the key.
   * @param key  [in]  key name of the metadata object.
   * @retval ErrorCode::ID_NOT_FOUND if the key is id.
   * @retval ErrorCode::NAME_NOT_FOUND if the key is name.
   * @retval ErrorCode::NOT_FOUND if the key is otherwise.
   */
  static ErrorCode get_not_found_error_code(std::string_view key) {
    return get_not_found_error_code({{key, ""}});
  }

  /**
   * @brief Get a NOT_FOUND error code corresponding to the key.
   * @param keys  [in]  key name of the metadata object.
   * @retval ErrorCode::ID_NOT_FOUND if the key is id.
   * @retval ErrorCode::NAME_NOT_FOUND if the key is name.
   * @retval ErrorCode::NOT_FOUND if the key is otherwise.
   */
  static ErrorCode get_not_found_error_code(
      const std::map<std::string_view, std::string_view>& keys) {
    if (keys.find(Object::ID) != keys.end()) {
      return ErrorCode::ID_NOT_FOUND;
    } else if (keys.find(Object::NAME) != keys.end()) {
      return ErrorCode::NAME_NOT_FOUND;
    } else {
      return ErrorCode::NOT_FOUND;
    }
  }

 private:
  /**
   * @brief Mapping information between privilege codes and column names.
   */
  const std::map<std::string, std::string> privileges_map_ = {
      {    Dao::PrivilegeColumn::kSelect, "r"},
      {    Dao::PrivilegeColumn::kInsert, "a"},
      {    Dao::PrivilegeColumn::kUpdate, "w"},
      {    Dao::PrivilegeColumn::kDelete, "d"},
      {  Dao::PrivilegeColumn::kTruncate, "D"},
      {Dao::PrivilegeColumn::kReferences, "x"},
      {   Dao::PrivilegeColumn::kTrigger, "t"}
  };

  std::shared_ptr<Dao> table_dao_;
  std::shared_ptr<Dao> column_dao_;
  std::shared_ptr<Dao> index_dao_;
  std::shared_ptr<Dao> constraint_dao_;
  std::shared_ptr<Dao> privilege_dao_;
  std::shared_ptr<Dao> statistic_dao_;
  std::shared_ptr<Dao> datatype_dao_;
  std::shared_ptr<Dao> role_dao_;

  /**
   * @brief Constructor
   */
  MetadataProvider() {}

  /**
   * @brief Checks for the presence of specified privileges.
   * @param src  [in]  ptree object before conversion.
   * @param dst  [in]  ptree object after conversion.
   * @retval ErrorCode::OK if success.
   * @retval otherwise an error code.
   */
  void convert_privilege(const boost::property_tree::ptree& src,
                         boost::property_tree::ptree& dst) const;
};

}  // namespace manager::metadata::db
