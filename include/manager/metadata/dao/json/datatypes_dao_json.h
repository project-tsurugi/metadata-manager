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

#include <map>
#include <string>
#include <string_view>
#include <vector>

#include <boost/property_tree/ptree.hpp>

#include "manager/metadata/dao/json/dao_json.h"
#include "manager/metadata/datatypes.h"
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
   * @return If success ErrorCode::OK, otherwise error code.
   */
  manager::metadata::ErrorCode prepare() override;

  /**
   * @brief Unsupported function.
   * @return Always ErrorCode::NOT_SUPPORTED.
   */
  manager::metadata::ErrorCode insert(const boost::property_tree::ptree&,
                                      ObjectId&) const override {
    // Do nothing and return of ErrorCode::NOT_SUPPORTED.
    return ErrorCode::NOT_SUPPORTED;
  }

  /**
   * @brief Get metadata object from a metadata table file.
   * @param keys   [in]  key name and value of the metadata object.
   * @param object [out] datatype metadata to get, where the given key
   *   equals the given value.
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
   * @brief Unsupported function.
   * @return Always ErrorCode::NOT_SUPPORTED.
   */
  manager::metadata::ErrorCode remove(
      const std::map<std::string_view, std::string_view>&,
      std::vector<ObjectId>& objet_ids) const override {
    // Do nothing and return of ErrorCode::NOT_SUPPORTED.
    return ErrorCode::NOT_SUPPORTED;
  }

 private:
  boost::property_tree::ptree datatype_contents_;
};  // class DataTypesDaoJson

}  // namespace manager::metadata::db

#endif  // MANAGER_METADATA_DAO_JSON_DATATYPES_DAO_JSON_H_
