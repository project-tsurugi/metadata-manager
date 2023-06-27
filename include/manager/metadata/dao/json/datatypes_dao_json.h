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
#ifndef MANAGER_METADATA_DAO_JSON_DATATYPES_DAO_JSON_H_
#define MANAGER_METADATA_DAO_JSON_DATATYPES_DAO_JSON_H_

#include <string>
#include <string_view>
#include <vector>

#include <boost/property_tree/ptree.hpp>

#include "manager/metadata/dao/json/dao_json.h"
#include "manager/metadata/datatype.h"
#include "manager/metadata/error_code.h"

namespace manager::metadata::db {

/**
 * @brief DAO class for accessing data type metadata for JSON data.
 */
class DataTypesDaoJson : public DaoJson {
 public:
  static constexpr const char* const kRootNode = "data_types";

  /**
    * @brief Construct a new DataType Metadata DAO class for JSON data.
    * @param session pointer to DB session manager for JSON.
    */
  explicit DataTypesDaoJson(DbSessionManagerJson* session)
      : DaoJson(session, "") {}

  /**
   * @brief Prepare to access the constraint metadata JSON file.
   * @return ErrorCode::OK if success, otherwise an error code.
   */
  manager::metadata::ErrorCode prepare() override;

  /**
   * @brief Unsupported function.
   * @return Always ErrorCode::NOT_SUPPORTED.
   */
  manager::metadata::ErrorCode insert(const boost::property_tree::ptree&,
                                      ObjectId&) const {
    // Do nothing and return of ErrorCode::NOT_SUPPORTED.
    return ErrorCode::NOT_SUPPORTED;
  }

  /**
   * @brief Get all metadata objects from a metadata table file.
   *   If the table metadata does not exist, return the container as empty.
   * @param objects  [out] all data-types metadata.
   * @return ErrorCode::OK if success, otherwise an error code.
   */
  manager::metadata::ErrorCode select_all(
      std::vector<boost::property_tree::ptree>& objects) const override;

  /**
   * @brief Get metadata object from a metadata table file.
   * @param key    [in]  key. column name of a table metadata table.
   * @param values [in]  value to be filtered.
   * @param object [out] datatype metadata to get, where the given key
   *   equals the given value.
   * @retval ErrorCode::OK if success.
   * @retval ErrorCode::ID_NOT_FOUND if the table id does not exist.
   * @retval ErrorCode::NAME_NOT_FOUND if the table name does not exist.
   * @retval otherwise an error code.
   */
  manager::metadata::ErrorCode select(
      std::string_view key, const std::vector<std::string_view>& values,
      boost::property_tree::ptree& object) const override;

  /**
   * @brief Unsupported function.
   * @return Always ErrorCode::NOT_SUPPORTED.
   */
  manager::metadata::ErrorCode update(
      std::string_view, const std::vector<std::string_view>&,
      const boost::property_tree::ptree&) const {
    // Do nothing and return of ErrorCode::NOT_SUPPORTED.
    return ErrorCode::NOT_SUPPORTED;
  }

  /**
   * @brief Unsupported function.
   * @return Always ErrorCode::NOT_SUPPORTED.
   */
  manager::metadata::ErrorCode remove(std::string_view,
                                      const std::vector<std::string_view>&,
                                      ObjectId&) const {
    // Do nothing and return of ErrorCode::NOT_SUPPORTED.
    return ErrorCode::NOT_SUPPORTED;
  }

 private:
  boost::property_tree::ptree datatype_contents_;
};  // class DataTypesDaoJson

}  // namespace manager::metadata::db

#endif  // MANAGER_METADATA_DAO_JSON_DATATYPES_DAO_JSON_H_
