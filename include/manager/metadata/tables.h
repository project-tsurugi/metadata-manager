/*
 * Copyright 2020-2023 tsurugi project.
 *
 * Licensed under the Apache License, version 2.0 (the "License");
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
#ifndef MANAGER_METADATA_TABLES_H_
#define MANAGER_METADATA_TABLES_H_

#include <string>
#include <string_view>
#include <vector>

#include <boost/property_tree/ptree.hpp>

#include "manager/metadata/column.h"
#include "manager/metadata/error_code.h"
#include "manager/metadata/metadata.h"
#include "manager/metadata/table.h"

namespace manager::metadata {

/**
 * @brief Container of table metadata objects.
 */
class Tables : public Metadata {
 public:
  /**
   * @brief Field name constant indicating the namespace of the metadata.
   * @deprecated Deprecated in the future. Please use Table::NAMESPACE.
   */
  static constexpr const char* const NAMESPACE = Table::NAMESPACE;
  /**
   * @brief Field name constant indicating the columns node of the metadata.
   * @deprecated Deprecated in the future. Please use Table::NAMESPACE.
   */
  static constexpr const char* const COLUMNS_NODE = Table::COLUMNS_NODE;
  /**
   * @brief Field name constant indicating the constraints node of the metadata.
   * @deprecated Deprecated in the future. Please use Table::NAMESPACE.
   */
  static constexpr const char* const CONSTRAINTS_NODE = Table::CONSTRAINTS_NODE;
  /**
   * @brief Field name constant indicating the tuples of the metadata.
   * @deprecated Deprecated in the future. Please use Table::NUMBER_OF_TUPLES.
   */
  static constexpr const char* const NUMBER_OF_TUPLES = Table::NUMBER_OF_TUPLES;
  /**
   * @brief Field name constant indicating the owner role id of the metadata.
   * @deprecated Deprecated in the future. Please use Table::NAMESPACE.
   */
  static constexpr const char* const OWNER_ROLE_ID = Table::OWNER_ROLE_ID;
  /**
   * @brief Field name constant indicating the acl of the metadata.
   * @deprecated Deprecated in the future. Please use Table::NAMESPACE.
   */
  static constexpr const char* const ACL = Table::ACL;

  /**
   * @brief Class indicating the field name of the column metadata.
   * @deprecated Deprecated in the future. Please use Column class.
   */
  struct Column : public manager::metadata::Column {};

  explicit Tables(std::string_view database)
      : Tables(database, kDefaultComponent) {}
  Tables(std::string_view database, std::string_view component);

  Tables(const Tables&)            = delete;
  Tables& operator=(const Tables&) = delete;

  ErrorCode init() const override;

  ErrorCode add(const boost::property_tree::ptree& object) const override;
  ErrorCode add(const boost::property_tree::ptree& object,
                ObjectId* object_id) const override;

  ErrorCode get(const ObjectId object_id,
                boost::property_tree::ptree& object) const override;
  ErrorCode get(std::string_view object_name,
                boost::property_tree::ptree& object) const override;

  ErrorCode get_all(
      std::vector<boost::property_tree::ptree>& objects) const override;

  ErrorCode get_statistic(const ObjectId table_id,
                          boost::property_tree::ptree& object) const;
  ErrorCode get_statistic(std::string_view table_name,
                          boost::property_tree::ptree& object) const;
  ErrorCode set_statistic(boost::property_tree::ptree& container) const;

  ErrorCode update(const ObjectIdType object_id,
                   const boost::property_tree::ptree& object) const override;

  ErrorCode remove(const ObjectIdType object_id) const override;
  ErrorCode remove(std::string_view object_name,
                   ObjectId* object_id) const override;

  ErrorCode get_acls(std::string_view token,
                     boost::property_tree::ptree& acls) const;

  ErrorCode confirm_permission_in_acls(const ObjectId object_id,
                                       const char* permission,
                                       bool& check_result) const;
  ErrorCode confirm_permission_in_acls(std::string_view object_name,
                                       const char* permission,
                                       bool& check_result) const;

 private:
  manager::metadata::ErrorCode param_check_metadata_add(
      const boost::property_tree::ptree& object) const;
  manager::metadata::ErrorCode param_check_statistic_update(
      const boost::property_tree::ptree& object) const;
};  // class Tables

}  // namespace manager::metadata

#endif  // MANAGER_METADATA_TABLES_H_
