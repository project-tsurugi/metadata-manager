/*
 * Copyright 2021 tsurugi project.
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

#include <string>
#include <string_view>
#include <vector>

#include <boost/property_tree/ptree.hpp>

#include "manager/metadata/dao/json/dao_json.h"
#include "manager/metadata/error_code.h"

namespace manager::metadata::db {

/**
 * @brief Class defined for compatibility.
 *   The functionality of the class in question is encapsulated in the
 *   manager::metadata::db::TablesDaoJson class.
 */
class ColumnsDaoJson : public DaoJson {
 public:
  /**
    * @brief Construct a new Column Metadata DAO class for JSON data.
    * @param session pointer to DB session manager for JSON.
    */
  explicit ColumnsDaoJson(DbSessionManagerJson* session)
      : DaoJson(session, "") {}

  /**
   * @brief Function defined for compatibility.
   * @return Always ErrorCode::OK.
   */
  manager::metadata::ErrorCode prepare() override {
    // Do nothing and return of ErrorCode::OK.
    return ErrorCode::OK;
  }

  /**
   * @brief Function defined for compatibility.
   * @return Always ErrorCode::OK.
   */
  manager::metadata::ErrorCode insert(const boost::property_tree::ptree&,
                                      ObjectId&) const override {
    // Do nothing and return of ErrorCode::OK.
    return ErrorCode::OK;
  }

  /**
   * @brief Function defined for compatibility.
   * @return Always ErrorCode::OK.
   */
  manager::metadata::ErrorCode select_all(
      std::vector<boost::property_tree::ptree>&) const override {
    // Do nothing and return of ErrorCode::OK.
    return ErrorCode::OK;
  }

  /**
   * @brief Function defined for compatibility.
   * @return Always ErrorCode::OK.
   */
  manager::metadata::ErrorCode select(
      std::string_view, const std::vector<std::string_view>&,
      boost::property_tree::ptree&) const override {
    // Do nothing and return of ErrorCode::OK.
    return ErrorCode::OK;
  }

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
   * @brief Function defined for compatibility.
   * @return Always ErrorCode::OK.
   */
  manager::metadata::ErrorCode remove(std::string_view,
                                      const std::vector<std::string_view>&,
                                      ObjectId&) const override {
    // Do nothing and return of ErrorCode::OK.
    return ErrorCode::OK;
  }
};  // class ColumnsDaoJson

}  // namespace manager::metadata::db

#endif  // MANAGER_METADATA_DAO_JSON_COLUMNS_DAO_JSON_H_
