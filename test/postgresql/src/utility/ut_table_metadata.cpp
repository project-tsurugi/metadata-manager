/*
 * Copyright 2020-2022 tsurugi project.
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
    tables.put(Table::FORMAT_VERSION, format_version);
  }

  // generation
  if (generation != NOT_INITIALIZED) {
    tables.put(Table::GENERATION, generation);
  }

  // name
  tables.put(Table::NAME, name);

  // namespace
  if (!namespace_name.empty()) {
    tables.put(Table::NAMESPACE, namespace_name);
  }

  // tuples
  if (tuples != NOT_INITIALIZED) {
    tables.put(Table::NUMBER_OF_TUPLES, tuples);
  }

  // columns
  ptree ptree_columns;
  for (UTColumnMetadata column : columns) {
    ptree ptree_column;

    // column name
    ptree_column.put(Column::NAME, column.name);

    // column ordinal position
    ptree_column.put(Column::COLUMN_NUMBER, column.column_number);

    // column data type id
    ptree_column.put<ObjectIdType>(Column::DATA_TYPE_ID, column.data_type_id);

    // column nullable
    ptree_column.put<bool>(Column::IS_NOT_NULL, column.is_not_null);

    // add column data length array to ptree
    // if UTTableMetadata data length array is initialized
    if (!column.p_data_length.empty()) {
      ptree_column.add_child(Column::DATA_LENGTH, column.p_data_length);
    }

    // add column varying to ptree
    // if UTTableMetadata varying is initialized
    ptree_column.put<bool>(Column::VARYING,
                            static_cast<bool>(column.varying));

    // add column default expression to ptree
    // if UTTableMetadata default expression is initialized
    if (!column.default_expr.empty()) {
      ptree_column.put(Column::DEFAULT_EXPR, column.default_expr);
    }

    ptree_columns.push_back(std::make_pair("", ptree_column));
  }
  tables.add_child(Table::COLUMNS_NODE, ptree_columns);

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
  tables.add_child(Table::CONSTRAINTS_NODE, ptree_constraints);
}

}  // namespace manager::metadata::testing
