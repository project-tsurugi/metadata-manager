/*
 * Copyright 2021 tsurugi project.
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

#include "manager/metadata/helper/ptree_helper.h"
#include "manager/metadata/tables.h"

namespace manager::metadata::testing {

using boost::property_tree::ptree;

/**
 * @brief Generate ptree type table metadata
 * from UTTableMetadata fields.
 * @return none.
 */
void UTTableMetadata::generate_ptree() {
  // format_version
  if (format_version != NOT_INITIALIZED) {
    tables.put(Tables::FORMAT_VERSION, format_version);
  }

  // generation
  if (generation != NOT_INITIALIZED) {
    tables.put(Tables::GENERATION, generation);
  }

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

  // tuples
  if (tuples != NOT_INITIALIZED) {
    tables.put(Tables::TUPLES, tuples);
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

    // add column data length array to ptree
    // if UTTableMetadata data length array is initialized
    if (!column.p_data_length.empty()) {
      ptree_column.add_child(Tables::Column::DATA_LENGTH, column.p_data_length);
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

    ptree_columns.push_back(std::make_pair("", ptree_column));
  }
  tables.add_child(Tables::COLUMNS_NODE, ptree_columns);

  // constraints
  ptree ptree_constraints;
  for (UTConstraintMetadata constraint : constraints) {
    ptree ptree_constraint;

    // constraint name
    ptree_constraint.put(Constraint::NAME, constraint.name);

    // constraint type
    ptree_constraint.put(Constraint::TYPE, constraint.type);

    // constraint columns
    if (constraint.columns >= 0) {
      ptree_constraint.put(Constraint::COLUMNS, constraint.columns);
    }
    if (!constraint.p_columns.empty()) {
      ptree_constraint.add_child(Constraint::COLUMNS, constraint.p_columns);
    }

    // constraint columns id
    if (constraint.columns_id >= 0) {
      ptree_constraint.put(Constraint::COLUMNS_ID, constraint.columns_id);
    }
    if (!constraint.p_columns_id.empty()) {
      ptree_constraint.add_child(Constraint::COLUMNS_ID,
                                 constraint.p_columns_id);
    }

    // constraint type
    ptree_constraint.put(Constraint::INDEX_ID, constraint.index_id);

    // constraint type
    ptree_constraint.put(Constraint::EXPRESSION, constraint.expression);

    ptree_constraints.push_back(std::make_pair("", ptree_constraint));
  }
  tables.add_child(Tables::CONSTRAINTS_NODE, ptree_constraints);
}

/**
 * @brief Generate table metadata
 * from UTTableMetadata fields.
 * @return none.
 */
void UTTableMetadata::generate_table() {
  table.format_version = 1;
  table.generation     = 1;
  table.id             = id;
  table.namespace_name = namespace_name;
  table.name           = name;
  table.primary_keys.emplace_back(1);
  table.primary_keys.emplace_back(3);
  table.tuples = tuples;

  // columns metadata
  for (const auto& column_meta : columns) {
    Column column;
    column.id               = column_meta.id;
    column.name             = column_meta.name;
    column.ordinal_position = column_meta.ordinal_position;
    column.data_type_id     = column_meta.data_type_id;
    column.data_length      = column_meta.data_length;
    column.nullable         = column_meta.nullable;
    column.varying          = column_meta.varying;
    column.default_expr     = column_meta.default_expr;

    table.columns.emplace_back(column);
  }

  // constraints metadata
  for (const auto& constraint_meta : constraints) {
    Constraint constraint;
    constraint.id       = constraint_meta.id;
    constraint.name     = constraint_meta.name;
    constraint.table_id = constraint_meta.table_id;
    constraint.type =
        static_cast<Constraint::ConstraintType>(constraint_meta.type);
    constraint.columns    = constraint_meta.columns_list;
    constraint.columns_id = constraint_meta.columns_id_list;
    constraint.index_id   = constraint_meta.index_id;
    constraint.expression = constraint_meta.expression;

    table.constraints.emplace_back(constraint);
  }
}

}  // namespace manager::metadata::testing
