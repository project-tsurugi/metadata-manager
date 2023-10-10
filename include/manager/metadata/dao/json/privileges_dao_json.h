/*
 * Copyright 2021-2023 Project Tsurugi.
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

#include <map>
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
  ErrorCode insert(const boost::property_tree::ptree&,
                   ObjectId&) const override {
    // Do nothing and return of ErrorCode::NOT_SUPPORTED.
    return ErrorCode::NOT_SUPPORTED;
  }

  /**
   * @brief Function defined for compatibility.
   * @return Always ErrorCode::OK.
   */
  ErrorCode select(const std::map<std::string_view, std::string_view>&,
                   boost::property_tree::ptree&) const override {
    // Do nothing and return of ErrorCode::OK.
    return ErrorCode::OK;
  }

  /**
   * @brief Unsupported function.
   * @return Always ErrorCode::NOT_SUPPORTED.
   */
  ErrorCode update(const std::map<std::string_view, std::string_view>&,
                   const boost::property_tree::ptree&,
                   uint64_t&) const override {
    // Do nothing and return of ErrorCode::NOT_SUPPORTED.
    return ErrorCode::NOT_SUPPORTED;
  }

  /**
   * @brief Unsupported function.
   * @return Always ErrorCode::NOT_SUPPORTED.
   */
  ErrorCode remove(const std::map<std::string_view, std::string_view>&,
                   std::vector<ObjectId>&) const override {
    // Do nothing and return of ErrorCode::NOT_SUPPORTED.
    return ErrorCode::NOT_SUPPORTED;
  }
};  // class PrivilegesDaoJson

}  // namespace manager::metadata::db

#endif  // MANAGER_METADATA_DAO_JSON_PRIVILEGES_DAO_JSON_H_
