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
#include "test/metadata/ut_table_metadata.h"

#include <utility>

#include <boost/foreach.hpp>

#include "manager/metadata/datatypes.h"
#include "manager/metadata/helper/ptree_helper.h"
#include "manager/metadata/tables.h"

namespace manager::metadata::testing {

using boost::property_tree::ptree;

/**
 * @brief Generate ptree type table metadata
 * from UTTableMetadata fields.
 * @return none.
 */
void UTTableMetadata::generate_test_metadata() {
  // Generate unique table name.
  std::string table_name =
      (table_name_.empty() ? "table_name_" + UTUtils::generate_narrow_uid()
                           : table_name_);

  metadata_struct_.format_version   = INVALID_VALUE;
  metadata_struct_.generation       = INVALID_VALUE;
  metadata_struct_.id               = INVALID_OBJECT_ID;
  metadata_struct_.name             = table_name;
  metadata_struct_.namespace_name   = "";
  metadata_struct_.number_of_tuples = INVALID_VALUE;

  manager::metadata::Column column;
  metadata_struct_.columns.clear();
  {
    column.id            = INVALID_OBJECT_ID;
    column.name          = "column_name_1_" + UTUtils::generate_narrow_uid();
    column.table_id      = INVALID_OBJECT_ID;
    column.column_number = 1;
    column.data_type_id  = static_cast<int64_t>(DataTypes::DataTypesId::INT64);
    column.data_length   = {};
    column.varying       = false;
    column.is_not_null   = true;
    column.default_expression = "auto number";
    metadata_struct_.columns.push_back(column);

    column.id            = INVALID_OBJECT_ID;
    column.name          = "column_name_2_" + UTUtils::generate_narrow_uid();
    column.table_id      = INVALID_OBJECT_ID;
    column.column_number = 2;
    column.data_type_id = static_cast<int64_t>(DataTypes::DataTypesId::VARCHAR);
    column.data_length  = {64};
    column.varying      = true;
    column.is_not_null  = false;
    column.default_expression = "";
    metadata_struct_.columns.push_back(column);

    column.id            = INVALID_OBJECT_ID;
    column.name          = "column_name_3_" + UTUtils::generate_narrow_uid();
    column.table_id      = INVALID_OBJECT_ID;
    column.column_number = 3;
    column.data_type_id  = static_cast<int64_t>(DataTypes::DataTypesId::CHAR);
    column.data_length   = {5};
    column.varying       = false;
    column.is_not_null   = false;
    column.default_expression = "";
    metadata_struct_.columns.push_back(column);
  }

  // constraints
  manager::metadata::Constraint constraint;
  metadata_struct_.constraints.clear();
  {
    constraint.id       = INVALID_OBJECT_ID;
    constraint.name     = "constraint_name_1_" + UTUtils::generate_narrow_uid();
    constraint.table_id = INVALID_OBJECT_ID;
    constraint.type     = Constraint::ConstraintType::PRIMARY_KEY;
    constraint.columns  = {1};
    constraint.columns_id = {1001};
    constraint.index_id   = 1;
    constraint.expression = "";
    metadata_struct_.constraints.push_back(constraint);

    constraint.id       = INVALID_OBJECT_ID;
    constraint.name     = "constraint_name_2_" + UTUtils::generate_narrow_uid();
    constraint.table_id = INVALID_OBJECT_ID;
    constraint.type     = Constraint::ConstraintType::UNIQUE;
    constraint.columns  = {1, 2};
    constraint.columns_id = {1001, 1002};
    constraint.index_id   = 2;
    constraint.expression = "";
    metadata_struct_.constraints.push_back(constraint);
  }
  metadata_ptree_ = metadata_struct_.convert_to_ptree();
}

/**
 * @brief Verifies that the actual table metadata equals expected one.
 * @param expected  [in]  expected table metadata.
 * @param actual    [in]  actual table metadata.
 * @param file      [in]  file name of the caller.
 * @param line      [in]  line number of the caller.
 * @return none.
 */
void UTTableMetadata::check_metadata_expected(
    const boost::property_tree::ptree& expected,
    const boost::property_tree::ptree& actual, const char* file,
    const int64_t line) const {
  Table expected_struct;
  Table actual_struct;

  expected_struct.convert_from_ptree(expected);
  actual_struct.convert_from_ptree(actual);

  this->check_metadata_expected(expected_struct, actual_struct, file, line);
}

/**
 * @brief Verifies that the actual table metadata equals expected one.
 * @param expected  [in]  expected table metadata.
 * @param actual    [in]  actual table metadata.
 * @param file      [in]  file name of the caller.
 * @param line      [in]  line number of the caller.
 * @return none.
 */
void UTTableMetadata::check_metadata_expected(
    const manager::metadata::Table& expected,
    const boost::property_tree::ptree& actual, const char* file,
    const int64_t line) const {
  Table actual_struct;
  actual_struct.convert_from_ptree(actual);

  this->check_metadata_expected(expected, actual_struct, file, line);
}

/**
 * @brief Verifies that the actual table metadata equals expected one.
 * @param expected  [in]  expected table metadata.
 * @param actual    [in]  actual table metadata.
 * @param file      [in]  file name of the caller.
 * @param line      [in]  line number of the caller.
 * @return none.
 */
void UTTableMetadata::check_metadata_expected(
    const boost::property_tree::ptree& expected,
    const manager::metadata::Table& actual, const char* file,
    const int64_t line) const {
  Table actual_struct;
  Table expected_struct;
  expected_struct.convert_from_ptree(expected);

  this->check_metadata_expected(expected_struct, actual, file, line);
}

/**
 * @brief Verifies that the actual table metadata equals expected one.
 * @param expected  [in]  expected table metadata.
 * @param actual    [in]  actual table metadata.
 * @param file      [in]  file name of the caller.
 * @param line      [in]  line number of the caller.
 * @return none.
 */
void UTTableMetadata::check_metadata_expected(
    const manager::metadata::Table& expected,
    const manager::metadata::Table& actual, const char* file,
    const int64_t line) const {
  // format version
  check_expected<int32_t>(Tables::format_version(), actual.format_version,
                          Table::FORMAT_VERSION, file, line);
  // generation
  check_expected(Tables::generation(), actual.generation, Table::GENERATION,
                 file, line);
  // table name
  check_expected(expected.name, actual.name, Table::NAME, file, line);
  // table id
  check_expected(expected.id, actual.id, Table::ID, file, line);
  // namespace
  check_expected(expected.namespace_name, actual.namespace_name,
                 Table::NAMESPACE, file, line);
  // number of tuples
  check_expected(expected.number_of_tuples, actual.number_of_tuples,
                 Table::NUMBER_OF_TUPLES, file, line);

  // column metadata
  check_expected(expected.columns.size(), actual.columns.size(),
                 Table::COLUMNS_NODE, file, line);
  if (!::testing::Test::HasFailure()) {
    for (size_t idx = 0; idx < expected.columns.size(); ++idx) {
      auto& column_expected = expected.columns[idx];
      auto& column_actual   = actual.columns[idx];

      // object id
      EXPECT_GT(column_actual.id, static_cast<ObjectId>(0));
      // table id
      check_expected(expected.id, column_actual.table_id, Column::TABLE_ID,
                     file, line);
      // name
      check_expected(column_expected.name, column_actual.name, Column::NAME,
                     file, line);
      // number
      check_expected(column_expected.column_number, column_actual.column_number,
                     Column::COLUMN_NUMBER, file, line);
      // data type id
      check_expected(column_expected.data_type_id, column_actual.data_type_id,
                     Column::DATA_TYPE_ID, file, line);
      // column data length
      check_child_expected(column_expected.data_length,
                           column_actual.data_length, Column::DATA_LENGTH, file,
                           line);
      // column varying
      check_expected(column_expected.varying, column_actual.varying,
                     Column::VARYING, file, line);
      // is not null
      check_expected(column_expected.is_not_null, column_actual.is_not_null,
                     Column::IS_NOT_NULL, file, line);
      // default expression
      check_expected(column_expected.default_expression,
                     column_actual.default_expression, Column::DEFAULT_EXPR,
                     file, line);
    }
  }

  // constraint metadata
  check_expected(expected.constraints.size(), actual.constraints.size(),
                 Table::CONSTRAINTS_NODE, file, line);
  if (!::testing::Test::HasFailure()) {
    for (size_t idx = 0; idx < expected.constraints.size(); ++idx) {
      auto& constraint_expected = expected.constraints[idx];
      auto& constraint_actual   = actual.constraints[idx];

      // object id
      EXPECT_GT(constraint_actual.id, static_cast<ObjectIdType>(0));
      // table id
      check_expected(expected.id, constraint_actual.table_id,
                     Constraint::TABLE_ID, file, line);
      // name
      check_expected(constraint_expected.name, constraint_actual.name,
                     Constraint::NAME, file, line);
      // type
      check_expected(constraint_expected.type, constraint_actual.type,
                     Constraint::TYPE, file, line);
      // column numbers
      check_child_expected(constraint_expected.columns,
                           constraint_actual.columns, Constraint::COLUMNS, file,
                           line);
      // column IDs
      check_child_expected(constraint_expected.columns_id,
                           constraint_actual.columns_id, Constraint::COLUMNS_ID,
                           file, line);
      // index id
      check_expected(constraint_expected.index_id, constraint_actual.index_id,
                     Constraint::INDEX_ID, file, line);
      // expression
      check_expected(constraint_expected.expression,
                     constraint_actual.expression, Constraint::EXPRESSION, file,
                     line);
    }
  }
}

}  // namespace manager::metadata::testing
