/*
 * Copyright 2023 tsurugi project.
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
#ifndef MANAGER_METADATA_TABLE_H_
#define MANAGER_METADATA_TABLE_H_

#include <vector>

#include <boost/property_tree/ptree.hpp>

#include "manager/metadata/column.h"
#include "manager/metadata/constraint.h"
#include "manager/metadata/index.h"
#include "manager/metadata/object.h"

namespace manager::metadata {

/**
 * @brief Table metadata object.
 */
struct Table : public ClassObject {
  /**
   * @brief Number of rows (live raw) used in statistics.
   */
  int64_t number_of_tuples;
  /**
   * @brief List of columns belonging to the table.
   */
  std::vector<Column> columns;
  /**
   * @brief List of indexes belonging to the table.
   */
  std::vector<Index> indexes;
  /**
   * @brief List of constraints belonging to the table.
   */
  std::vector<Constraint> constraints;

  /**
   * @brief Field name constant indicating the namespace of the metadata.
   */
  static constexpr const char* const NAMESPACE = "namespace";
  /**
   * @brief Field name constant indicating the columns node of the metadata.
   */
  static constexpr const char* const COLUMNS_NODE = "columns";
  /**
   * @brief Field name constant indicating the constraints node of the metadata.
   */
  static constexpr const char* const CONSTRAINTS_NODE = "constraints";
  /**
   * @brief Field name constant indicating the number of tuples of the metadata.
   */
  static constexpr const char* const NUMBER_OF_TUPLES = "numberOfTuples";
  /**
   * @brief Field name constant indicating the owner role id of the metadata.
   */
  static constexpr const char* const OWNER_ROLE_ID = "ownerRoleId";
  /**
   * @brief Field name constant indicating the acl of the metadata.
   */
  static constexpr const char* const ACL = "acl";
  /**
   * @brief Field name constants for metadata indicating table authorization
   *   information.
   */
  static constexpr const char* const TABLE_ACL_NODE = "tables";

  Table() : ClassObject(), number_of_tuples(INVALID_VALUE) {}
  explicit Table(boost::property_tree::ptree pt);

  boost::property_tree::ptree convert_to_ptree() const override;
  void convert_from_ptree(const boost::property_tree::ptree& pt) override;
};  // class Table

}  // namespace manager::metadata

#endif  // MANAGER_METADATA_TABLE_H_
