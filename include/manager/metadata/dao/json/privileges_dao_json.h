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
#ifndef MANAGER_METADATA_DAO_JSON_PRIVILEGES_DAO_JSON_H_
#define MANAGER_METADATA_DAO_JSON_PRIVILEGES_DAO_JSON_H_

#include <string>
#include <string_view>
#include <vector>

#include "manager/metadata/dao/json/dao_json.h"
#include "manager/metadata/error_code.h"

namespace manager::metadata::db {

/**
 * @brief DAO class defined for compatibility.
 *   This metadata is not supported in JSON.
 */
class PrivilegesDaoJson : public DaoJson {
 public:
  /**
    * @brief Construct a new Table Privilege DAO class for JSON data.
    * @param session pointer to DB session manager for JSON.
    */
  explicit PrivilegesDaoJson(DbSessionManagerJson* session)
        : DaoJson(session, "") {}

  /**
   * @brief Function defined for compatibility.
   * @return Always true.
   */
  bool exists(std::string_view) const override { return true; }

  /**
   * @brief Function defined for compatibility.
   * @return Always true.
   */
  bool exists(ObjectId) const override { return true; }

  /**
   * @brief Function defined for compatibility.
   * @return Always true.
   */
  bool exists(const boost::property_tree::ptree&) const override {
    return true;
  }

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
   * @brief Unsupported function.
   * @return Always ErrorCode::NOT_SUPPORTED.
   */
  manager::metadata::ErrorCode select_all(
      std::vector<boost::property_tree::ptree>&) const override {
    // Do nothing and return of ErrorCode::NOT_SUPPORTED.
    return ErrorCode::NOT_SUPPORTED;
  }

  /**
   * @brief Unsupported function.
   * @return Always ErrorCode::NOT_SUPPORTED.
   */
  manager::metadata::ErrorCode select(
      std::string_view, const std::vector<std::string_view>&,
      boost::property_tree::ptree&) const override {
    // Do nothing and return of ErrorCode::NOT_SUPPORTED.
    return ErrorCode::NOT_SUPPORTED;
  }

  /**
   * @brief Unsupported function.
   * @return Always ErrorCode::NOT_SUPPORTED.
   */
  manager::metadata::ErrorCode update(
      std::string_view, const std::vector<std::string_view>&,
      const boost::property_tree::ptree&) const override {
    // Do nothing and return of ErrorCode::NOT_SUPPORTED.
    return ErrorCode::NOT_SUPPORTED;
  }

  /**
   * @brief Unsupported function.
   * @return Always ErrorCode::NOT_SUPPORTED.
   */
  manager::metadata::ErrorCode remove(std::string_view,
                                      const std::vector<std::string_view>&,
                                      ObjectId&) const override {
    // Do nothing and return of ErrorCode::NOT_SUPPORTED.
    return ErrorCode::NOT_SUPPORTED;
  }
};  // class PrivilegesDaoJson

}  // namespace manager::metadata::db

#endif  // MANAGER_METADATA_DAO_JSON_PRIVILEGES_DAO_JSON_H_
