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
   * @brief Add a table metadata object to the metadata table.
   * @param object     [in]  table metadata to add.
   * @param object_id  [out] ID of the added table metadata.
   * @retval ErrorCode::OK if success.
   * @retval otherwise an error code.
   */
  ErrorCode add_table_metadata(const boost::property_tree::ptree& object,
                               ObjectId& object_id);

  /**
   * @brief Add an index metadata object to the metadata table.
   * @param object     [in]  index metadata to add.
   * @param object_id  [out] ID of the added index metadata.
   * @retval ErrorCode::OK if success.
   * @retval otherwise an error code.
   */
  ErrorCode add_index_metadata(const boost::property_tree::ptree& object,
                               ObjectId& object_id);

  /**
   * @brief Add a constraint metadata object to the metadata table.
   * @param object     [in]  constraints metadata to add.
   * @param object_id  [out] ID of the added constraint metadata.
   * @retval ErrorCode::OK if success.
   * @retval otherwise an error code.
   */
  ErrorCode add_constraint_metadata(const boost::property_tree::ptree& object,
                                    ObjectId& object_id);

  /**
   * @brief Add a column statistic to the column statistics table.
   *   If column statistics already exist for the specified table ID and
   *   column information, they are updated.
   * @param object     [in]  one column statistic to add or update.
   * @param object_id  [out] ID of the added column statistic.
   * @retval ErrorCode::OK if success.
   * @retval otherwise an error code.
   */
  ErrorCode add_column_statistic(const boost::property_tree::ptree& object,
                                 ObjectId& object_id);

  // ============================================================================
  /**
   * @brief Get a table metadata object from the metadata table with the
   *   specified key value.
   * @param key     [in]  key of table metadata object.
   * @param value   [in]  value of table metadata object.
   * @param object  [out] retrieved constraint metadata object array.
   * @retval ErrorCode::OK if success.
   * @retval ErrorCode::ID_NOT_FOUND if the table id does not exist.
   * @retval ErrorCode::NAME_NOT_FOUND if the table name does not exist.
   * @retval otherwise an error code.
   */
  ErrorCode get_table_metadata(std::string_view key, std::string_view value,
                               boost::property_tree::ptree& object);

  /**
   * @brief Get all table metadata object from the metadata table.
   * @param objects  [out] table metadata object to get.
   * @retval ErrorCode::OK if success.
   * @retval otherwise an error code.
   */
  ErrorCode get_table_metadata(
      std::vector<boost::property_tree::ptree>& objects);

  /**
   * @brief Get a table statistic from the metadata table with the
   *   specified key value.
   * @param key     [in]  key of table metadata object.
   * @param value   [in]  value of table metadata object.
   * @param object  [out] one table metadata object to get, where key = value.
   * @retval ErrorCode::OK if success.
   * @retval ErrorCode::ID_NOT_FOUND if the table id does not exist.
   * @retval ErrorCode::NAME_NOT_FOUND if the table name does not exist.
   * @retval otherwise an error code.
   */
  ErrorCode get_table_statistic(std::string_view key, std::string_view value,
                                boost::property_tree::ptree& object);

  /**
   * @brief Get a index metadata object from the metadata table with the
   *   specified key value.
   * @param key     [in]  key of index metadata object. e.g. id or name.
   * @param value   [in]  key value.
   * @param object  [out] retrieved index metadata object.
   * @retval ErrorCode::OK              if success.
   * @retval ErrorCode::ID_NOT_FOUND    if the id does not exist.
   * @retval ErrorCode::NAME_NOT_FOUND  if the name does not exist.
   * @retval otherwise an error code.
   */
  ErrorCode get_index_metadata(std::string_view key, std::string_view value,
                               boost::property_tree::ptree& object);

  /**
   * @brief Get all index metadata object from the metadata table.
   * @param objects [out] table metadata objects.
   * @retval ErrorCode::OK if success.
   * @retval otherwise an error code.
   */
  ErrorCode get_index_metadata(
      std::vector<boost::property_tree::ptree>& objects);

  /**
   * @brief Get a constraint metadata object from the metadata table with the
   *   specified key value.
   * @param key     [in]  key of constraint metadata object.
   * @param value   [in]  key value.
   * @param object  [out] retrieved constraint metadata object array.
   * @retval ErrorCode::OK if success.
   * @retval ErrorCode::ID_NOT_FOUND if the constraint id does not exist.
   * @retval ErrorCode::NAME_NOT_FOUND if the constraint name does not exist.
   * @retval ErrorCode::NOT_FOUND if the table id does not exist.
   * @retval otherwise an error code.
   */
  ErrorCode get_constraint_metadata(std::string_view key,
                                    std::string_view value,
                                    boost::property_tree::ptree& object);

  /**
   * @brief Get all constraint metadata object from the metadata table.
   * @param objects  [out] constraint metadata object to get.
   * @retval ErrorCode::OK if success.
   * @retval otherwise an error code.
   */
  ErrorCode get_constraint_metadata(
      std::vector<boost::property_tree::ptree>& objects);

  /**
   * @brief Get a column statistic from the column statistics table with the
   *   specified key value.
   * @param key     [in]  key of column statistics object.
   * @param value   [in]  value of column statistics object.
   * @param object  [out] retrieved column statistics object array.
   * @retval ErrorCode::OK if success.
   * @retval ErrorCode::ID_NOT_FOUND if the statistic id or column id
   *   does not exist.
   * @retval ErrorCode::NAME_NOT_FOUND if the statistic name does not exist.
   * @retval otherwise an error code.
   */
  ErrorCode get_column_statistic(std::string_view key, std::string_view value,
                                 boost::property_tree::ptree& object);

  /**
   * @brief Get a column statistic from the column statistics table with the
   *   specified table id and column info.
   * @param table_id  [in]  table id.
   * @param key       [in]  key. column name of a column statistic table.
   * @param value     [in]  value to be filtered.
   * @param object    [out] retrieved column statistics object array.
   * @retval ErrorCode::OK if success.
   * @retval ErrorCode::ID_NOT_FOUND if the ordinal position does not exist.
   * @retval ErrorCode::NAME_NOT_FOUND if the statistic name does not exist.
   * @retval otherwise an error code.
   */
  ErrorCode get_column_statistic(const ObjectId table_id, std::string_view key,
                                 std::string_view value,
                                 boost::property_tree::ptree& object);

  /**
   * @brief Get all column statistics from the column statistics table.
   * @param objects  [out] all column statistics.
   * @retval ErrorCode::OK if success.
   * @retval otherwise an error code.
   */
  ErrorCode get_column_statistics(
      std::vector<boost::property_tree::ptree>& objects);

  /**
   * @brief Get all column statistics from the column statistics table
   *   with the specified table id.
   * @param table_id  [in]  table id.
   * @param objects   [out] all column statistics.
   * @retval ErrorCode::OK if success.
   * @retval ErrorCode::ID_NOT_FOUND if the table id does not exist.
   * @retval otherwise an error code.
   */
  ErrorCode get_column_statistics(
      const ObjectId table_id,
      std::vector<boost::property_tree::ptree>& objects);

  /**
   * @brief Get a datatype metadata object from the metadata table with the
   *   specified key value.
   * @param key     [in]  key of data type metadata object.
   * @param value   [in]  value of data type metadata object.
   * @param object  [out] retrieved data type metadata object array.
   * @retval ErrorCode::OK if success.
   * @retval ErrorCode::ID_NOT_FOUND if the data types id does not exist.
   * @retval ErrorCode::NAME_NOT_FOUND if the data types name does not exist.
   * @retval ErrorCode::NOT_FOUND if the other data types key does not exist.
   * @retval otherwise an error code.
   */
  ErrorCode get_datatype_metadata(std::string_view key, std::string_view value,
                                  boost::property_tree::ptree& object);

  // ============================================================================
  /**
   * @brief Update a table metadata table with the specified table id.
   * @param object_id  [in]  Table ID of the table metadata to be updated.
   * @param object     [in]  Table metadata object.
   * @retval ErrorCode::OK if success.
   * @retval ErrorCode::ID_NOT_FOUND if the table id does not exist.
   * @retval otherwise an error code.
   */
  ErrorCode update_table_metadata(const ObjectId object_id,
                                  const boost::property_tree::ptree& object);

  /**
   * @brief Update a index metadata table with the specified table id.
   * @param object_id  [in]  object ID of the index metadata to be updated.
   * @param object     [in]  Table metadata object.
   * @retval ErrorCode::OK if success.
   * @retval ErrorCode::ID_NOT_FOUND if the table id does not exist.
   * @retval otherwise an error code.
   */
  ErrorCode update_index_metadata(const ObjectId object_id,
                                  const boost::property_tree::ptree& object);

  // ============================================================================
  /**
   * @brief Removes all metadata objects with the specified table info
   *   from the metadata table.
   *   (Metadata: table metadata, column metadata, index metadata,
   *   constraint metadata, column statistics)
   * @param key        [in]  key of table metadata object.
   * @param value      [in]  value of table metadata object.
   * @param object_id  [out] ID of the removed table metadata.
   * @retval ErrorCode::OK if success.
   * @retval ErrorCode::ID_NOT_FOUND if the table id does not exist.
   * @retval ErrorCode::NAME_NOT_FOUND if the table name does not exist.
   * @retval otherwise an error code.
   */
  ErrorCode remove_table_metadata(std::string_view key, std::string_view value,
                                  ObjectId& object_id);

  /**
   * @brief Removes a index metadata object with the specified object id
   *   from the metadata table.
   * @param key       [in]  key of index metadata object.
   * @param value     [in]  value of index metadata object.
   * @param object_id [out] ID of the removed index metadata.
   * @retval ErrorCode::OK if success.
   * @retval ErrorCode::ID_NOT_FOUND if the index id does not exist.
   * @retval ErrorCode::NAME_NOT_FOUND if the index name does not exist.
   * @retval otherwise an error code.
   */
  ErrorCode remove_index_metadata(std::string_view key, std::string_view value,
                                  ObjectId& object_id);

  /**
   * @brief Removes a constraint metadata object with the specified object id
   *   from the metadata table.
   * @param object_id  [in]  constraint id.
   * @retval ErrorCode::OK if success.
   * @retval ErrorCode::ID_NOT_FOUND if the constraint id does not exist.
   * @retval otherwise an error code.
   */
  ErrorCode remove_constraint_metadata(const ObjectId object_id);

  /**
   * @brief Removes a column statistic with the specified key value
   *   from the column statistic table.
   * @param key        [in]  key of column statistics object.
   * @param value      [in]  value of column statistics object.
   * @param object_id  [out] statistic id of the row deleted.
   * @retval ErrorCode::OK if success.
   * @retval ErrorCode::ID_NOT_FOUND if the statistic id does not exist.
   * @retval ErrorCode::NAME_NOT_FOUND if the statistic name does not exist.
   * @retval otherwise an error code.
   */
  ErrorCode remove_column_statistic(std::string_view key,
                                    std::string_view value,
                                    ObjectId& object_id);

  /**
   * @brief Removes a column statistic with the specified table id
   *   from the column statistic table.
   * @param table_id  [in]  table id.
   * @retval ErrorCode::OK if success.
   * @retval ErrorCode::ID_NOT_FOUND if the table id does not exist.
   * @retval otherwise an error code.
   */
  ErrorCode remove_column_statistics(const ObjectId table_id);

  /**
   * @brief Removes a column statistic with the specified table id and
   *   column info from the column statistic table.
   * @param table_id   [in]  table id.
   * @param key        [in]  key. column name of a column statistic table.
   * @param value      [in]  value to be filtered.
   * @param object_id  [out] statistic id of the row deleted.
   * @retval ErrorCode::OK if success.
   * @retval ErrorCode::ID_NOT_FOUND if the ordinal position does not exist.
   * @retval ErrorCode::NAME_NOT_FOUND if the statistic name does not exist.
   * @retval otherwise an error code.
   */
  ErrorCode remove_column_statistic(const ObjectId table_id,
                                    std::string_view key,
                                    std::string_view value,
                                    ObjectId& object_id);

  // ============================================================================
  /**
   * @brief Updates table statistics with the specified table id.
   * @param object     [in]  Table statistic object.
   * @param object_id  [out] ID of the added table metadata.
   * @retval ErrorCode::OK if success.
   * @retval ErrorCode::ID_NOT_FOUND if the table id does not exist.
   * @retval ErrorCode::NAME_NOT_FOUND if the table name does not exist.
   * @retval otherwise an error code.
   */
  ErrorCode set_table_statistic(const boost::property_tree::ptree& object,
                                ObjectId& object_id);

  // ============================================================================
  /**
   * @brief Gets the presence or absence of the specified permission from the
   *   PostgreSQL system catalog.
   * @param key           [in]  key of role metadata object.
   * @param value         [in]  value of role metadata object.
   * @param permission    [in]  permissions.
   * @param check_result  [out] presence or absence of the specified
   *   permissions.
   * @retval ErrorCode::OK if success.
   * @retval ErrorCode::NOT_FOUND if the foreign table does not exist.
   * @retval ErrorCode::ID_NOT_FOUND if the role id does not exist.
   * @retval ErrorCode::NAME_NOT_FOUND if the role name does not exist.
   * @retval ErrorCode::NOT_SUPPORTED If the function is not supported.
   * @retval otherwise an error code.
   */
  ErrorCode confirm_permission(std::string_view key, std::string_view value,
                               std::string_view permission, bool& check_result);

 private:
  /**
   * @brief Mapping information between privilege codes and column names.
   */
  const std::map<char, std::string> privileges_map_ = {
      {'r',     Dao::PrivilegeColumn::kSelect},
      {'a',     Dao::PrivilegeColumn::kInsert},
      {'w',     Dao::PrivilegeColumn::kUpdate},
      {'d',     Dao::PrivilegeColumn::kDelete},
      {'D',   Dao::PrivilegeColumn::kTruncate},
      {'x', Dao::PrivilegeColumn::kReferences},
      {'t',    Dao::PrivilegeColumn::kTrigger}
  };

  std::shared_ptr<Dao> table_dao_;
  std::shared_ptr<Dao> column_dao_;
  std::shared_ptr<Dao> index_dao_;
  std::shared_ptr<Dao> constraint_dao_;
  std::shared_ptr<Dao> privilege_dao_;
  std::shared_ptr<Dao> statistic_dao_;
  std::shared_ptr<Dao> datatype_dao_;

  /**
   * @brief Constructor
   */
  MetadataProvider() {}

  /**
   * @brief Start DB transaction control.
   * @retval ErrorCode::OK if success.
   * @retval otherwise an error code.
   */
  ErrorCode start_transaction() const;

  /**
   * @brief End DB transaction control.
   * @param result processing result code.
   * @retval ErrorCode::OK if success.
   * @retval otherwise an error code.
   */
  ErrorCode end_transaction(const ErrorCode& result) const;

  /**
   * @brief Checks for the presence of specified privileges.
   * @param object      [in]  ptree of table privileges.
   * @param privileges  [in]  privileges to check.
   * @retval ErrorCode::OK if success.
   * @retval otherwise an error code.
   */
  bool check_of_privilege(const boost::property_tree::ptree& object,
                          std::string_view privileges) const;

  /**
   * @brief Get a one metadata object using the specified DAO.
   *   If more than one is retrieved, the first one is retrieved.
   * @tparam T Derived class of Dao.
   * @param dao     [in]  DAO of the metadata.
   * @param key     [in]  key of metadata object.
   * @param values  [in]  value of metadata object.
   * @param object  [out] retrieved metadata object.
   * @retval ErrorCode::OK if success.
   * @retval ErrorCode::RESULT_MULTIPLE_ROWS if success (multiple rows).
   * @retval otherwise an error code.
   */
  template <typename T, typename = std::enable_if_t<std::is_base_of_v<Dao, T>>>
  ErrorCode select_single(const T& dao, std::string_view key,
                          const std::vector<std::string_view> values,
                          boost::property_tree::ptree& object) const;

  /**
   * @brief Get all metadata objects that match the criteria using
   *   the specified DAO.
   * @tparam T Derived class of Dao.
   * @param dao      [in]  DAO of the metadata.
   * @param key      [in]  key of metadata object.
   * @param values   [in]  value of metadata object.
   * @param objects  [out] retrieved metadata object.
   * @retval ErrorCode::OK if success.
   * @retval otherwise an error code.
   */
  template <typename T, typename = std::enable_if_t<std::is_base_of_v<Dao, T>>>
  ErrorCode select_multiple(
      const T& dao, std::string_view key,
      const std::vector<std::string_view> values,
      std::vector<boost::property_tree::ptree>& objects) const;
};

}  // namespace manager::metadata::db
