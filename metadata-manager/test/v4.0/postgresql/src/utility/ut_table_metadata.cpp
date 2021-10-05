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
#include "test/utility/ut_table_metadata.h"

#include <utility>

#include "manager/metadata/tables.h"

using namespace boost::property_tree;

namespace manager::metadata::testing {

/**
 * @brief Generate ptree type table metadata
 * from UTTableMetadata fields.
 * @return none.
 */
void UTTableMetadata::generate_ptree() {
  // name
  tables.put(Tables::NAME, name);

  // namespace
  if (!namespace_name.empty()) {
    tables.put(Tables::NAMESPACE, namespace_name);
  }

  // primary keys
  if (primary_keys.size() > 0) {
    ptree p_primary_key;
    ptree p_primary_keys;

    for (int pkey_val : primary_keys) {
      p_primary_key.put("", pkey_val);
      p_primary_keys.push_back(std::make_pair("", p_primary_key));
    }

    tables.add_child(Tables::PRIMARY_KEY_NODE, p_primary_keys);
  }

  // columns
  ptree ptree_columns;
  for (UTColumnMetadata column : columns) {
    ptree ptree_column;

    // column name
    ptree_column.put(Tables::Column::NAME, column.name);

    // column ordinal position
    ptree_column.put(Tables::Column::ORDINAL_POSITION, column.ordinal_position);

    // column data type id
    ptree_column.put<ObjectIdType>(Tables::Column::DATA_TYPE_ID,
                                   column.data_type_id);

    // column nullable
    ptree_column.put<bool>(Tables::Column::NULLABLE, column.nullable);

    // add column data length to ptree
    // if UTTableMetadata data length is initialized
    if (column.data_length >= 0) {
      ptree_column.put(Tables::Column::DATA_LENGTH, column.data_length);
    }

    // add column data length array to ptree
    // if UTTableMetadata data length array is initialized
    if (!column.p_data_lengths.empty()) {
      ptree_column.add_child(Tables::Column::DATA_LENGTH,
                             column.p_data_lengths);
    }

    // add column varying to ptree
    // if UTTableMetadata varying is initialized
    if (column.varying >= 0) {
      ptree_column.put<bool>(Tables::Column::VARYING,
                             static_cast<bool>(column.varying));
    }

    // add column default expression to ptree
    // if UTTableMetadata default expression is initialized
    if (!column.default_expr.empty()) {
      ptree_column.put(Tables::Column::DEFAULT, column.default_expr);
    }

    // add column direction to ptree
    // if UTTableMetadata direction is initialized
    if (column.direction >=
        static_cast<int>(Tables::Column::Direction::DEFAULT)) {
      ptree_column.put(Tables::Column::DIRECTION, column.direction);
    }

    ptree_columns.push_back(std::make_pair("", ptree_column));
  }

  tables.add_child(Tables::COLUMNS_NODE, ptree_columns);
}

}  // namespace manager::metadata::testing
