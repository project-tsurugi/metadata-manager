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
#ifndef MANAGER_METADATA_TABLES_H_
#define MANAGER_METADATA_TABLES_H_

#include <string_view>
#include <vector>

#include <boost/property_tree/ptree.hpp>

#include "manager/metadata/error_code.h"
#include "manager/metadata/metadata.h"
#include "manager/metadata/columns.h"
#include "manager/metadata/indexes.h"
#include "manager/metadata/constraints.h"

namespace manager::metadata {
/**
 * @brief Table metadata object.
 */
struct Table : public Object {
  Table() {}
  std::string             namespace_name;
  int64_t                 owner_id;
  std::string             acl;
  int64_t                 tuples;
  std::vector<int64_t>    primary_keys;
  std::vector<Column>	    columns;
  std::vector<Index>      indexes;
  std::vector<Constraint> constraints;
};

/**
 * @brief Container of table metadata objects.
 */
class Tables : public Metadata {
 public:
  /**
   * @brief Field name constant indicating the namespace of the metadata.
   */
  static constexpr const char* const NAMESPACE = "namespace";
  /**
   * @brief Field name constant indicating the columns node of the metadata.
   */
  static constexpr const char* const COLUMNS_NODE = "columns";
  /**
   * @brief Field name constant indicating the primary keys of the metadata.
   */
  static constexpr const char* const PRIMARY_KEY_NODE = "primaryKey";
  /**
   * @brief Field name constant indicating the tuples of the metadata.
   */
  static constexpr const char* const TUPLES = "tuples";
  /**
   * @brief Field name constant indicating the owner role id of the metadata.
   */
  static constexpr const char* const OWNER_ROLE_ID = "ownerRoleId";
  /**
   * @brief Field name constant indicating the acl of the metadata.
   */
  static constexpr const char* const ACL = "acl";

  // column metadata-object.
  struct Column {
    /**
     * @brief Field name constant indicating the format version of the metadata.
     */
    static constexpr const char* const FORMAT_VERSION = "formatVersion";
    /**
     * @brief Field name constant indicating the generation of the metadata.
     */
    static constexpr const char* const GENERATION = "generation";
    /**
     * @brief Field name constant indicating the object id of the metadata.
     */
    static constexpr const char* const ID = "id";
    /**
     * @brief Field name constant indicating the table id of the metadata.
     */
    static constexpr const char* const TABLE_ID = "tableId";
    /**
     * @brief Field name constant indicating the column name of the metadata.
     */
    static constexpr const char* const NAME = "name";
    /**
     * @brief Field name constant indicating the ordinal position of the metadata.
     */
    static constexpr const char* const ORDINAL_POSITION = "ordinalPosition";
    /**
     * @brief Field name constant indicating the data type id of the metadata.
     */
    static constexpr const char* const DATA_TYPE_ID = "dataTypeId";
    /**
     * @brief Field name constant indicating the data length of the metadata.
     */
    static constexpr const char* const DATA_LENGTH = "dataLength";
    /**
     * @brief Field name constant indicating the varying of the metadata.
     */
    static constexpr const char* const VARYING = "varying";
    /**
     * @brief Field name constant indicating the nullable of the metadata.
     */
    static constexpr const char* const NULLABLE = "nullable";
    /**
     * @brief Field name constant indicating the default expression of the metadata.
     */
    static constexpr const char* const DEFAULT = "defaultExpr";
    /**
     * @brief Field name constant indicating the direction of the metadata.
     */
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

  /**
   * @brief Field name constants for metadata indicating
   *    table authorization information.
   */
  static constexpr const char* const TABLE_ACL_NODE = "tables";

  explicit Tables(std::string_view database)
      : Tables(database, kDefaultComponent) {}
  Tables(std::string_view database, std::string_view component);

  Tables(const Tables&) = delete;
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
      std::vector<boost::property_tree::ptree>& container) const override;

  ErrorCode get_statistic(const ObjectId table_id,
                          boost::property_tree::ptree& object) const;
  ErrorCode get_statistic(std::string_view table_name,
                          boost::property_tree::ptree& object) const;
  ErrorCode set_statistic(boost::property_tree::ptree& object) const;

  ErrorCode remove(const ObjectId object_id) const override;
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

  ErrorCode add(const manager::metadata::Table& table) const;
  ErrorCode add(const manager::metadata::Table& table,
                ObjectId* object_id) const;
  
  ErrorCode get(const ObjectId object_id,
                manager::metadata::Table& table) const;
  ErrorCode get(std::string_view table_name,
                manager::metadata::Table& table) const;

 private:
  manager::metadata::ErrorCode param_check_metadata_add(
      const boost::property_tree::ptree& object) const;
  manager::metadata::ErrorCode param_check_statistic_update(
      const boost::property_tree::ptree& object) const;
};  // class Tables

}  // namespace manager::metadata

#endif  // MANAGER_METADATA_TABLES_H_
