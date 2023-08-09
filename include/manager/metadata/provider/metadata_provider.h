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

#include <memory>
#include <string_view>
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
   * @brief Update a index metadata table with the specified table id.
   * @param object_id  [in]  object ID of the index metadata to be updated.
   * @param object     [in]  Table metadata object.
   * @retval ErrorCode::OK if success.
   * @retval ErrorCode::ID_NOT_FOUND if the table id does not exist.
   * @retval otherwise an error code.
   */
  ErrorCode update_index_metadata(const ObjectId object_id,
                                  const boost::property_tree::ptree& object);

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

 private:
  std::shared_ptr<Dao> index_dao_;

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
};

}  // namespace manager::metadata::db
