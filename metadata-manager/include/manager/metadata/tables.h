/*
 * Copyright 2020-2021 tsurugi project.
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
#ifndef MANAGER_METADATA_MANAGER_INCLUDE_MANAGER_METADATA_TABLES_H_
#define MANAGER_METADATA_MANAGER_INCLUDE_MANAGER_METADATA_TABLES_H_

#include <boost/property_tree/ptree.hpp>
#include <string_view>
#include <vector>

#include "manager/metadata/error_code.h"
#include "manager/metadata/metadata.h"

namespace manager::metadata {

class Tables : public Metadata {
 public:
  // table ACL object.
  static constexpr const char* const TABLE_ACL_NODE = "tables";

  // table metadata-object.
  // FORMAT_VERSION is defined in base class.
  // GENERATION is defined in base class.
  // ID is defined in base class.
  // NAME is defined in base class.
  static constexpr const char* const NAMESPACE = "namespace";
  static constexpr const char* const COLUMNS_NODE = "columns";
  static constexpr const char* const PRIMARY_KEY_NODE = "primaryKey";
  static constexpr const char* const TUPLES = "tuples";
  static constexpr const char* const OWNER_ROLE_ID = "ownerRoleId";
  static constexpr const char* const ACL = "acl";

  // column metadata-object.
  struct Column {
    static constexpr const char* const FORMAT_VERSION = "formatVersion";
    static constexpr const char* const GENERATION = "generation";
    static constexpr const char* const ID = "id";
    static constexpr const char* const TABLE_ID = "tableId";
    static constexpr const char* const NAME = "name";
    static constexpr const char* const ORDINAL_POSITION = "ordinalPosition";
    static constexpr const char* const DATA_TYPE_ID = "dataTypeId";
    static constexpr const char* const DATA_LENGTH = "dataLength";
    static constexpr const char* const VARYING = "varying";
    static constexpr const char* const NULLABLE = "nullable";
    static constexpr const char* const DEFAULT = "defaultExpr";
    static constexpr const char* const DIRECTION = "direction";

    /**
     * @brief represents sort direction of elements.
     */
    enum class Direction {
      /**
       * @brief default order.
       */
      DEFAULT = 0,

      /**
       * @brief ascendant order.
       */
      ASCENDANT,

      /**
       * @brief descendant order.
       */
      DESCENDANT,
    };
  };

  explicit Tables(std::string_view database)
      : Tables(database, kDefaultComponent) {}
  Tables(std::string_view database, std::string_view component);

  Tables(const Tables&) = delete;
  Tables& operator=(const Tables&) = delete;

  ErrorCode init() const override;

  ErrorCode add(const boost::property_tree::ptree& object) const override;
  ErrorCode add(const boost::property_tree::ptree& object,
                ObjectIdType* object_id) const override;

  ErrorCode get(const ObjectIdType object_id,
                boost::property_tree::ptree& object) const override;
  ErrorCode get(std::string_view object_name,
                boost::property_tree::ptree& object) const override;
  ErrorCode get_all(
      std::vector<boost::property_tree::ptree>& container) const override;

  ErrorCode get_statistic(const ObjectIdType table_id,
                          boost::property_tree::ptree& object) const;
  ErrorCode get_statistic(std::string_view table_name,
                          boost::property_tree::ptree& object) const;
  ErrorCode set_statistic(boost::property_tree::ptree& object) const;

  ErrorCode remove(const ObjectIdType object_id) const override;
  ErrorCode remove(std::string_view object_name,
                   ObjectIdType* object_id) const override;

  ErrorCode get_acls(std::string_view token,
                     boost::property_tree::ptree& acls) const;

  ErrorCode confirm_permission_in_acls(const ObjectIdType object_id,
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

#endif  // MANAGER_METADATA_MANAGER_INCLUDE_MANAGER_METADATA_TABLES_H_
