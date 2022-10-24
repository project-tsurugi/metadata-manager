/*
 * Copyright 2021-2022 tsurugi project.
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
#include "test/helper/table_metadata_helper.h"

#include <gtest/gtest.h>

#include <string>
#include <vector>

#include <boost/foreach.hpp>
#include <boost/property_tree/json_parser.hpp>

#include "manager/metadata/datatypes.h"
#include "manager/metadata/helper/ptree_helper.h"
#include "manager/metadata/metadata_factory.h"
#include "manager/metadata/tables.h"
#include "test/common/global_test_environment.h"
#include "test/common/ut_utils.h"

namespace manager::metadata::testing {

#define EXPECT_EQ_T(expected, actual, text)                 \
  if (expected != actual) std::cout << "[" << text << "] "; \
  EXPECT_EQ(expected, actual)

namespace json_parser = boost::property_tree::json_parser;

using boost::property_tree::json_parser_error;
using boost::property_tree::ptree;

/**
 * @brief Get the number of records in the current table metadata.
 * @return Current number of records.
 */
std::int64_t TableMetadataHelper::get_record_count() {
  // generate constraint metadata manager.
  auto tables = std::make_unique<Tables>(GlobalTestEnvironment::TEST_DB);

  // initialize tables metadata manager.
  ErrorCode error = tables->init();

  std::vector<ptree> container = {};
  if (error == ErrorCode::OK) {
    // get all tables metadata.
    tables->get_all(container);
  }

  return container.size();
}

/**
 * @brief Generate table metadata.
 * @param (testdata_table_metadata)    [out] table metadata used as test data.
 */
void TableMetadataHelper::generate_table_metadata(
    std::unique_ptr<UTTableMetadata>& testdata_table_metadata) {
  // generate unique table name.
  int s                   = time(NULL);
  std::string table_name  = "table_name" + std::to_string(s);
  testdata_table_metadata = std::make_unique<UTTableMetadata>(table_name);

  // generate namespace.
  testdata_table_metadata->namespace_name = "namespace";

  // generate number of tuples.
  testdata_table_metadata->tuples = 0;

  // generate three column metadata.
  {
    std::vector<std::string> col_names      = {"col1", "col2", "col3"};
    std::vector<ObjectIdType> column_number = {1, 2, 3};

    // first column metadata
    bool is_null = true;
    UTColumnMetadata column1{
        col_names[0], column_number[0],
        static_cast<ObjectIdType>(DataTypes::DataTypesId::FLOAT32), !is_null};

    // second column metadata
    UTColumnMetadata column2{
        col_names[1], column_number[1],
        static_cast<ObjectIdType>(DataTypes::DataTypesId::VARCHAR), !is_null};
    ptree data_length;
    data_length.put("", 8);
    column2.p_data_length.push_back(std::make_pair("", data_length));
    data_length.put("", 2);
    column2.p_data_length.push_back(std::make_pair("", data_length));
    column2.data_length.emplace_back(8);
    column2.data_length.emplace_back(2);

    column2.varying = true;
    column2.default_expr = "default2";

    // third column metadata
    UTColumnMetadata column3{
        col_names[2], column_number[2],
        static_cast<ObjectIdType>(DataTypes::DataTypesId::CHAR), is_null};
    column3.default_expr = "default3";
    column3.data_length  = {1};
    column3.varying      = false;

    // set table metadata to three column metadata
    testdata_table_metadata->columns.emplace_back(column1);
    testdata_table_metadata->columns.emplace_back(column2);
    testdata_table_metadata->columns.emplace_back(column3);
  }

  // generate three constraint metadata.
  {
    std::vector<Constraint::ConstraintType> constraint_types = {
        Constraint::ConstraintType::UNIQUE, Constraint::ConstraintType::CHECK};

    ptree columns;
    ptree columns_id;

    // first constraint metadata
    UTConstraintMetadata constraint1{"constraint1",
                                     Constraint::ConstraintType::UNIQUE};
    columns.put("", 1);
    constraint1.p_columns.push_back(std::make_pair("", columns));
    constraint1.columns_list.emplace_back(1);
    columns_id.put("", 1234);
    constraint1.p_columns.push_back(std::make_pair("", columns_id));
    constraint1.columns_id_list.emplace_back(1234);
    constraint1.index_id = 1L;

    // second column metadata
    // first constraint metadata
    UTConstraintMetadata constraint2{"constraint2",
                                     Constraint::ConstraintType::CHECK};
    columns.put("", 2);
    constraint2.p_columns.push_back(std::make_pair("", columns));
    constraint2.columns_list.emplace_back(5678);
    columns_id.put("", 5678);
    constraint2.p_columns_id.push_back(std::make_pair("", columns_id));
    constraint2.columns_id_list.emplace_back(5678);
    constraint2.expression = "expression-text";

    // set table metadata to two constraints metadata
    testdata_table_metadata->constraints.emplace_back(constraint1);
    testdata_table_metadata->constraints.emplace_back(constraint2);
  }

  // generate ptree from UTTableMetadata fields.
  testdata_table_metadata->generate_ptree();
  testdata_table_metadata->generate_table();
}

/**
 * @brief Make valid table metadata used as test data,
 * by reading a json file with table metadata.
 */
std::vector<boost::property_tree::ptree> TableMetadataHelper::make_valid_table_metadata() {
  std::vector<ptree> test_data_table_metadata;

  ptree pt;
  try {
    // read a json file with table metadata used as test data.
    json_parser::read_json(global->get_json_schema_file_name(), pt);
  } catch (boost::property_tree::json_parser_error& e) {
    UTUtils::print("could not read a json file with table metadata.",
                   e.what());
    return test_data_table_metadata;
  } catch (...) {
    UTUtils::print("could not read a json file with table metadata.");
    return test_data_table_metadata;
  }

  // Make valid table metadata used as test data.
  boost::optional<ptree&> o_tables = pt.get_child_optional("tables");
  if (o_tables) {
    ptree& tables = o_tables.value();
    BOOST_FOREACH (const ptree::value_type& node, tables) {
      ptree table = node.second;
      test_data_table_metadata.emplace_back(table);
    }
  }

  return test_data_table_metadata;
}

/**
 * @brief Add one new table metadata to table metadata table.
 * @param (table_name)    [in]  table name of new table metadata.
 * @param (ret_table_id)  [out] (optional) table id returned from the api to add
 *   new table metadata.
 * @return none.
 */
void TableMetadataHelper::add_table(std::string_view table_name,
                                    ObjectIdType* ret_table_id) {
  // prepare test data for adding table metadata.
  UTTableMetadata* testdata_table_metadata =
      global->testdata_table_metadata.get();

  ptree new_table = testdata_table_metadata->tables;
  new_table.put(Table::NAME, table_name);

  // add table metadata.
  add_table(new_table, ret_table_id);
}

/**
 * @brief Add one new table metadata to table metadata table.
 * @param (new_table)  [in]  new table metadata.
 * @param (table_id)   [out] (optional) table id returned from the api to add
 *   new table metadata.
 * @return none.
 */
void TableMetadataHelper::add_table(
    const boost::property_tree::ptree& new_table, ObjectIdType* table_id) {
  UTUtils::print("-- add table metadata --");
  UTUtils::print(" " + UTUtils::get_tree_string(new_table));

  auto tables = get_table_metadata(GlobalTestEnvironment::TEST_DB);

  ErrorCode error = tables->init();
  ASSERT_EQ(ErrorCode::OK, error);

  // add table metadata.
  ObjectIdType ret_table_id = INVALID_VALUE;
  error                     = tables->add(new_table, &ret_table_id);
  ASSERT_EQ(ErrorCode::OK, error);
  ASSERT_GT(ret_table_id, 0);

  UTUtils::print(" >> new table_id: ", ret_table_id);

  if (table_id != nullptr) {
    *table_id = ret_table_id;
  }
}

/**
 * @brief Add one new table metadata to table metadata table.
 * @param (new_table)  [in]  new table metadata.
 * @param (table_id)   [out] (optional) table id returned from the api to add
 *   new table metadata.
 * @return none.
 */
void TableMetadataHelper::add_table(const manager::metadata::Table& new_table,
                                    ObjectIdType* table_id) {
  UTUtils::print("-- add table metadata --");
  ptree pt_table = new_table.convert_to_ptree();
  UTUtils::print(" " + UTUtils::get_tree_string(pt_table));

  auto tables = get_table_metadata(GlobalTestEnvironment::TEST_DB);

  ErrorCode error = tables->init();
  ASSERT_EQ(ErrorCode::OK, error);

  // add table metadata.
  ObjectIdType ret_table_id = INVALID_VALUE;
  error                     = tables->add(new_table, &ret_table_id);
  ASSERT_EQ(ErrorCode::OK, error);
  ASSERT_GT(ret_table_id, 0);

  UTUtils::print(" >> new table_id: ", ret_table_id);

  if (table_id != nullptr) {
    *table_id = ret_table_id;
  }
}

/**
 * @brief Remove one table metadata to table metadata table.
 * @param (table_id)  [in]   table id of remove table metadata.
 * @return none.
 */
void TableMetadataHelper::remove_table(const ObjectIdType table_id) {
  UTUtils::print("-- remove table metadata --");
  UTUtils::print(" >> table_id: ", table_id);

  auto tables = get_table_metadata(GlobalTestEnvironment::TEST_DB);

  ErrorCode error = tables->init();
  ASSERT_EQ(ErrorCode::OK, error);

  // remove table metadata.
  error = tables->remove(table_id);
  ASSERT_EQ(ErrorCode::OK, error);
}

/**
 * @brief Remove one table metadata to table metadata table.
 * @param (table_name)  [in]  table name of remove table metadata.
 * @return none.
 */
void TableMetadataHelper::remove_table(std::string_view table_name) {
  UTUtils::print("-- remove table metadata --");
  UTUtils::print(" >> table_name: ", table_name);

  auto tables = get_table_metadata(GlobalTestEnvironment::TEST_DB);

  ErrorCode error = tables->init();
  ASSERT_EQ(ErrorCode::OK, error);

  // remove table metadata.
  error = tables->remove(table_name, nullptr);
  ASSERT_EQ(ErrorCode::OK, error);
}

/**
 * @brief Verifies that the actual table metadata equals expected one.
 * @param (expected)   [in]  expected table metadata.
 * @param (actual)     [in]  actual table metadata.
 * @return none.
 */
void TableMetadataHelper::check_table_metadata_expected(
    const manager::metadata::Table& expected,
    const boost::property_tree::ptree& actual) {
  // format version
  EXPECT_EQ(expected.format_version,
            actual.get<FormatVersionType>(Table::FORMAT_VERSION));

  // generation
  EXPECT_EQ(expected.generation, actual.get<GenerationType>(Table::GENERATION));

  // table name
  EXPECT_EQ(expected.name, actual.get<std::string>(Table::NAME));

  // table id
  ObjectIdType table_id_expected = expected.id;
  EXPECT_EQ(table_id_expected, actual.get<ObjectIdType>(Table::ID));

  // namespace
  boost::optional<std::string> o_namespace_actual =
      actual.get_optional<std::string>(Table::NAMESPACE);
  EXPECT_EQ(expected.namespace_name, o_namespace_actual.value_or(""));

  // number of tuples
  auto o_tuples_actual = actual.get_optional<int64_t>(Table::NUMBER_OF_TUPLES);
  EXPECT_EQ(expected.number_of_tuples, o_tuples_actual.value_or(INVALID_VALUE));

  // column metadata
  auto o_columns_actual = actual.get_child_optional(Table::COLUMNS_NODE);
  if (o_columns_actual) {
    std::vector<metadata::Column> p_columns_expected;
    std::vector<ptree> p_columns_actual;
    BOOST_FOREACH (const ptree::value_type& node, o_columns_actual.value()) {
      ptree column = node.second;
      p_columns_actual.emplace_back(column);
    }
    // Verifies that the number of column metadata is expected number.
    EXPECT_EQ(expected.columns.size(), p_columns_actual.size());

    auto column_expected = expected.columns.begin();
    for (int op = 0; static_cast<size_t>(op) < p_columns_expected.size();
         op++) {
      ptree column_actual = p_columns_actual[op];

      // column metadata id
      boost::optional<ObjectIdType> id_actual =
          column_actual.get<ObjectIdType>(Column::ID);
      EXPECT_GT(id_actual, static_cast<ObjectIdType>(0));

      // column metadata table id
      boost::optional<ObjectIdType> table_id_actual =
          column_actual.get<ObjectIdType>(Column::TABLE_ID);
      EXPECT_EQ(column_expected->table_id, table_id_actual);

      // column name
      auto name = column_actual.get_optional<std::string>(Column::NAME);
      if (name) {
        EXPECT_EQ(column_expected->name, name.get());
      }

      // column number
      auto column_number =
          column_actual.get_optional<int64_t>(Column::COLUMN_NUMBER);
      if (column_number) {
        EXPECT_EQ(column_expected->column_number, column_number.get());
      }

      // column data type id
      auto data_type_id =
          column_actual.get_optional<int64_t>(Column::DATA_TYPE_ID);
      if (data_type_id) {
        EXPECT_EQ(column_expected->data_type_id, data_type_id.get());
      }

      // column data length
      check_child_expected(column_expected->data_length, actual,
                           Column::DATA_LENGTH);

      // column varying
      auto varying = column_actual.get_optional<bool>(Column::VARYING);
      if (varying) {
        EXPECT_EQ(column_expected->varying, varying.get());
      }

      // is not null
      auto is_not_null = column_actual.get_optional<bool>(Column::IS_NOT_NULL);
      if (is_not_null) {
        EXPECT_EQ(column_expected->is_not_null, is_not_null.get());
      }

      // default expression
      auto default_expr =
          column_actual.get_optional<std::string>(Column::DEFAULT_EXPR);
      if (default_expr) {
        EXPECT_EQ(column_expected->default_expression, default_expr.get());
      }
    }
  } else {
    ASSERT_EQ(expected.columns.size() == 0, !o_columns_actual.is_initialized());
  }

  // constraint metadata
  {
    auto o_constraints_actual =
        actual.get_child_optional(Table::CONSTRAINTS_NODE);

    if (o_constraints_actual) {
      std::vector<metadata::Column> p_constraints_expected;
      std::vector<ptree> p_constraints_actual;
      BOOST_FOREACH (const ptree::value_type& node,
                     o_constraints_actual.value()) {
        ptree constraint = node.second;
        p_constraints_actual.emplace_back(constraint);
      }

      // Verifies that the number of constraint metadata is expected number.
      EXPECT_EQ(expected.constraints.size(), p_constraints_actual.size());

      auto constraint_expected = expected.constraints.begin();
      for (int op = 0; static_cast<size_t>(op) < p_constraints_expected.size();
           op++) {
        ptree constraint_actual = p_constraints_actual[op];

        // constraint metadata id
        auto id_actual = constraint_actual.get<ObjectIdType>(Constraint::ID);
        EXPECT_GT(id_actual, static_cast<ObjectIdType>(0));

        // constraint metadata table id
        auto table_id_actual =
            constraint_actual.get<ObjectIdType>(Constraint::TABLE_ID);
        EXPECT_EQ(constraint_expected->table_id, table_id_actual);

        // constraint name
        auto name =
            constraint_actual.get_optional<std::string>(Constraint::NAME);
        if (name) {
          EXPECT_EQ(constraint_expected->name, name.value());
        }

        // constraint type
        auto type = constraint_actual.get_optional<int64_t>(Constraint::TYPE);
        if (type) {
          EXPECT_EQ(constraint_expected->type,
                    static_cast<Constraint::ConstraintType>(type.value()));
        }

        // constraint column numbers
        check_child_expected(constraint_expected->columns, constraint_actual,
                             Constraint::COLUMNS);

        // constraint column IDs
        check_child_expected(constraint_expected->columns_id, constraint_actual,
                             Constraint::COLUMNS_ID);

        // constraint index id
        auto index_id =
            constraint_actual.get_optional<int64_t>(Constraint::INDEX_ID);
        if (index_id) {
          EXPECT_EQ(constraint_expected->index_id, index_id.value());
        }

        // constraint expression
        auto expression =
            constraint_actual.get_optional<std::string>(Constraint::EXPRESSION);
        if (expression) {
          EXPECT_EQ(constraint_expected->expression, expression.value());
        }
      }
    } else {
      EXPECT_EQ((expected.constraints.size() == 0),
                !o_constraints_actual.is_initialized());
    }
  }
}

/**
 * @brief Print column metadata fields used as test data.
 * @param (column_metadata)    [in] column metadata used as test data.
 */
void TableMetadataHelper::print_column_metadata(
    const UTColumnMetadata& column_metadata) {
  std::string data_length_string;
  for (auto value : column_metadata.data_length) {
    data_length_string = data_length_string +
                         (data_length_string.empty() ? "" : ",") +
                         std::to_string(value);
  }

  UTUtils::print(" id: ", column_metadata.id);
  UTUtils::print(" tableId: ", column_metadata.table_id);
  UTUtils::print(" name: ", column_metadata.name);
  UTUtils::print(" ordinalPosition: ", column_metadata.column_number);
  UTUtils::print(" dataTypeId: ", column_metadata.data_type_id);
  UTUtils::print(" dataLength: [", data_length_string, "]");
  UTUtils::print(" varying: ", column_metadata.varying);
  UTUtils::print(" nullable: ", column_metadata.is_not_null);
  UTUtils::print(" defaultExpr: ", column_metadata.default_expr);
}

void TableMetadataHelper::check_table_metadata_expected(
    const boost::property_tree::ptree& expected,
    const manager::metadata::Table& actual) {
  // format version
  EXPECT_EQ(Tables::format_version(), actual.format_version);

  // generation
  EXPECT_EQ(Tables::generation(), actual.generation);

  // table name
  EXPECT_EQ(expected.get<std::string>(Table::NAME), actual.name);

  // table id
  ObjectIdType table_id_expected = expected.get<ObjectIdType>(Table::ID);
  EXPECT_EQ(table_id_expected, actual.id);

  // namespace
  boost::optional<std::string> o_namespace_expected =
      expected.get_optional<std::string>(Table::NAMESPACE);
  boost::optional<std::string> o_namespace_actual = actual.namespace_name;

  if (o_namespace_expected && o_namespace_actual) {
    std::string& s_namespace_expected = o_namespace_expected.value();
    std::string& s_namespace_actual   = o_namespace_actual.value();
    EXPECT_EQ(s_namespace_expected, s_namespace_actual);
  } else {
    EXPECT_EQ(o_namespace_expected.is_initialized(),
              o_namespace_actual.is_initialized());
  }

  // number of tuples
  auto o_tuples_expected =
      expected.get_optional<int64_t>(Table::NUMBER_OF_TUPLES);
  EXPECT_EQ(o_tuples_expected.value_or(INVALID_VALUE), actual.number_of_tuples);

  // column metadata
  auto o_columns_expected = expected.get_child_optional(Table::COLUMNS_NODE);
  if (o_columns_expected) {
    std::vector<ptree> p_columns_expected;
    BOOST_FOREACH (const ptree::value_type& node, o_columns_expected.value()) {
      ptree column = node.second;
      p_columns_expected.emplace_back(column);
    }
    // Verifies that the number of column metadata is expected number.
    ASSERT_EQ(p_columns_expected.size(), actual.columns.size());

    auto column_actual = actual.columns.begin();
    for (int op = 0; static_cast<size_t>(op) < p_columns_expected.size();
         op++) {
      ptree column_expected = p_columns_expected[op];

      // column metadata id
      EXPECT_GT(column_actual->id, static_cast<ObjectIdType>(0));

      // column metadata table id
      EXPECT_EQ(table_id_expected, column_actual->table_id);

      // column name
      auto name = column_expected.get_optional<std::string>(Column::NAME);
      if (name) {
        EXPECT_EQ(name.get(), column_actual->name);
      }
      // column number
      auto column_number =
          column_expected.get_optional<int64_t>(Column::COLUMN_NUMBER);
      if (column_number) {
        EXPECT_EQ(column_number.get(), column_actual->column_number);
      }
      // column data type id
      auto data_type_id =
          column_expected.get_optional<int64_t>(Column::DATA_TYPE_ID);
      if (data_type_id) {
        EXPECT_EQ(data_type_id.get(), column_actual->data_type_id);
      }
      // column data length
      std::vector<int64_t> data_length_expected =
          ptree_helper::make_vector_int(column_expected, Column::DATA_LENGTH);
      EXPECT_EQ(data_length_expected, column_actual->data_length);
      // column varying
      auto varying = column_expected.get_optional<bool>(Column::VARYING);
      if (varying) {
        EXPECT_EQ(varying.get(), column_actual->varying);
      }
      // is not null
      auto is_not_null =
          column_expected.get_optional<bool>(Column::IS_NOT_NULL);
      if (is_not_null) {
        EXPECT_EQ(is_not_null.get(), column_actual->is_not_null);
      }
      // default
      auto default_expr =
          column_expected.get_optional<std::string>(Column::DEFAULT_EXPR);
      if (default_expr) {
        EXPECT_EQ(default_expr.get(), column_actual->default_expression);
      }
      column_actual++;
    }
  } else {
    EXPECT_FALSE(o_columns_expected.is_initialized());
  }
}

/**
 * @brief Print table statistic fields.
 * @param (table_statistics)    [in] table statistics used as test data.
 */
void TableMetadataHelper::print_table_statistics(
    const boost::property_tree::ptree& table_statistics) {
  auto metadata_id   = table_statistics.get_optional<ObjectIdType>(Table::ID);
  auto metadata_name = table_statistics.get_optional<std::string>(Table::NAME);
  auto metadata_namespace =
      table_statistics.get_optional<std::string>(Table::NAMESPACE);
  auto metadata_tuples =
      table_statistics.get_optional<int64_t>(Table::NUMBER_OF_TUPLES);

  UTUtils::print(" id: ", metadata_id.get_value_or(0));
  UTUtils::print(" name: ", metadata_name.get_value_or("<NULL>"));
  UTUtils::print(" namespace: ", metadata_namespace.get_value_or("<NULL>"));
  UTUtils::print(" tuples: ", metadata_tuples.get_value_or(0));
}

/**
 * @brief Verifies that the actual table metadata equals expected one.
 * @param (expected)   [in]  expected table metadata.
 * @param (actual)     [in]  actual table metadata.
 * @return none.
 */
void TableMetadataHelper::check_table_metadata_expected(
    const boost::property_tree::ptree& expected,
    const boost::property_tree::ptree& actual) {
  // format version
  EXPECT_EQ(Tables::format_version(),
            actual.get<FormatVersionType>(Table::FORMAT_VERSION));

  // generation
  EXPECT_EQ(Tables::generation(),
            actual.get<GenerationType>(Table::GENERATION));

  // table name
  ASSERT_EQ(expected.get<std::string>(Table::NAME),
            actual.get<std::string>(Table::NAME));

  // table id
  ObjectIdType table_id_expected = expected.get<ObjectIdType>(Table::ID);
  ASSERT_EQ(table_id_expected, actual.get<ObjectIdType>(Table::ID));

  // namespace
  check_expected<std::string>(expected, actual, Table::NAMESPACE);

  // number of tuples
  check_expected<int64_t>(expected, actual, Table::NUMBER_OF_TUPLES);

  // column metadata
  {
    auto o_expected = expected.get_child_optional(Table::COLUMNS_NODE);
    auto o_actual   = actual.get_child_optional(Table::COLUMNS_NODE);

    if (o_expected && o_actual) {
      std::vector<ptree> p_expected;
      std::vector<ptree> p_actual;
      BOOST_FOREACH (const ptree::value_type& node, o_expected.value()) {
        ptree column = node.second;
        p_expected.emplace_back(column);
      }
      BOOST_FOREACH (const ptree::value_type& node, o_actual.value()) {
        ptree column = node.second;
        p_actual.emplace_back(column);
      }

      // Verifies that the number of column metadata is expected number.
      EXPECT_EQ(p_expected.size(), p_actual.size());

      for (std::size_t op = 0; op < p_expected.size(); op += 1) {
        ptree column_expected = p_expected[op];
        ptree column_actual   = p_actual[op];

        // column metadata id
        boost::optional<ObjectIdType> id_actual =
            column_actual.get<ObjectIdType>(Column::ID);
        EXPECT_GT(id_actual.value(), static_cast<ObjectIdType>(0));
        // column metadata table id
        boost::optional<ObjectIdType> table_id_actual =
            column_actual.get<ObjectIdType>(Column::TABLE_ID);
        EXPECT_EQ(table_id_expected, table_id_actual.value());
        // column name
        check_expected<std::string>(column_expected, column_actual,
                                    Column::NAME);
        // column number
        check_expected<ObjectIdType>(column_expected, column_actual,
                                     Column::COLUMN_NUMBER);
        // column data type id
        check_expected<ObjectIdType>(column_expected, column_actual,
                                     Column::DATA_TYPE_ID);
        // column data length
        check_child_expected(column_expected, column_actual,
                             Column::DATA_LENGTH);
        // column varying
        check_expected<bool>(column_expected, column_actual, Column::VARYING);
        // column is not null
        check_expected<bool>(column_expected, column_actual,
                             Column::IS_NOT_NULL);
        // column default expression
        check_expected<std::string>(column_expected, column_actual,
                                    Column::DEFAULT_EXPR);
      }
    } else {
      EXPECT_EQ(o_expected.is_initialized(), o_actual.is_initialized());
    }
  }

  // constraint metadata
  {
    auto o_expected = expected.get_child_optional(Table::CONSTRAINTS_NODE);
    auto o_actual   = actual.get_child_optional(Table::CONSTRAINTS_NODE);

    if (o_expected && o_actual) {
      std::vector<ptree> p_expected;
      std::vector<ptree> p_actual;

      BOOST_FOREACH (const ptree::value_type& node, o_expected.value()) {
        ptree constraint = node.second;
        p_expected.emplace_back(constraint);
      }
      BOOST_FOREACH (const ptree::value_type& node, o_actual.value()) {
        ptree constraint = node.second;
        p_actual.emplace_back(constraint);
      }

      // Verifies that the number of constraint metadata is expected number.
      ASSERT_EQ(p_expected.size(), p_actual.size());

      for (std::size_t op = 0; op < p_expected.size(); op += 1) {
        ptree constraints_expected = p_expected[op];
        ptree constraints_actual   = p_actual[op];

        // constraint metadata id
        boost::optional<ObjectIdType> id_actual =
            constraints_actual.get<ObjectIdType>(Constraint::ID);
        EXPECT_GT(id_actual.value(), static_cast<ObjectIdType>(0));
        // constraint metadata table id
        boost::optional<ObjectIdType> table_id_actual =
            constraints_actual.get<ObjectIdType>(Constraint::TABLE_ID);
        EXPECT_EQ(table_id_expected, table_id_actual.value());

        // constraint name
        check_expected<std::string>(constraints_expected, constraints_actual,
                                    Constraint::NAME);
        // constraint type
        check_expected<ObjectIdType>(constraints_expected, constraints_actual,
                                     Constraint::TYPE);
        // constraint column numbers
        check_child_expected(constraints_expected, constraints_actual,
                             Constraint::COLUMNS);
        // constraint column IDs
        check_child_expected(constraints_expected, constraints_actual,
                             Constraint::COLUMNS_ID);
        // constraint index id
        check_expected<ObjectIdType>(constraints_expected, constraints_actual,
                                     Constraint::INDEX_ID);
        // constraint expression
        check_expected<std::string>(constraints_expected, constraints_actual,
                                    Constraint::EXPRESSION);
      }
    } else if (o_expected) {
      EXPECT_EQ(o_expected.value().empty(), !o_actual.is_initialized());
    } else if (o_actual) {
      EXPECT_EQ(!o_expected.is_initialized(), o_actual.value().empty());
    } else {
      EXPECT_EQ(!o_expected.is_initialized(), !o_actual.is_initialized());
    }
  }
}

/**
 * @brief Verifies that the actual table metadata equals expected one.
 * @param (expected)   [in]  expected table metadata.
 * @param (actual)     [in]  actual table metadata.
 * @return none.
 */
void TableMetadataHelper::check_table_acls_expected(
    const std::map<std::string_view, std::string_view>& expected,
    const boost::property_tree::ptree& actual) {
  auto expected_check = expected;

  auto o_acls_actual = actual.get_child_optional(Table::TABLE_ACL_NODE);
  if (o_acls_actual) {
    std::vector<ptree> p_columns_expected;
    std::vector<ptree> p_columns_actual;

    BOOST_FOREACH (const ptree::value_type& node, o_acls_actual.value()) {
      auto actual_table_name   = node.first;
      std::string actual_value = actual_table_name + "|" + node.second.data();

      auto expected_item = expected.find(actual_table_name);
      if (expected_item != expected.end()) {
        std::string expected_value = std::string(expected_item->first) + "|" +
                                     std::string(expected_item->second);
        EXPECT_EQ(expected_value, actual_value);

        expected_check.erase(actual_table_name);
      }
    }

    for (auto& check : expected_check) {
      auto table_name = check.first;
      auto acl_value  = check.second;

      std::string actual_value   = "";
      std::string expected_value = "";
      if (acl_value.empty()) {
        // Normal due to no authority.
        EXPECT_TRUE(true);
      } else {
        // Abnormal because the expected authorization cannot be obtained.
        actual_value   = "";
        expected_value = std::string(table_name) + "|" + std::string(acl_value);
        EXPECT_EQ(expected_value, actual_value);
      }
    }
  } else {
    FAIL();
  }
}

/**
 * @brief Verifies that the actual metadata equals expected one.
 * @param (expected)   [in]  expected metadata.
 * @param (actual)     [in]  actual metadata.
 * @param (meta_name)  [in]  name of metadata table.
 * @return none.
 */
void TableMetadataHelper::check_child_expected(
    const boost::property_tree::ptree& expected,
    const boost::property_tree::ptree& actual, const char* meta_name) {
  auto o_expected = expected.get_child_optional(meta_name);
  auto o_actual   = actual.get_child_optional(meta_name);

  if (o_expected && o_actual) {
    auto expected_value = UTUtils::get_tree_string(o_expected.value());
    auto actual_value   = UTUtils::get_tree_string(o_actual.value());
    EXPECT_EQ_T(expected_value, actual_value, meta_name);
  } else if (o_expected) {
    EXPECT_EQ_T(o_expected.value().empty(), !o_actual.is_initialized(),
                meta_name);
  } else if (o_actual) {
    EXPECT_EQ_T(!o_expected.is_initialized(), o_actual.value().empty(),
                meta_name);
  } else {
    EXPECT_EQ_T(o_expected.is_initialized(), o_actual.is_initialized(),
                meta_name);
  }
}

/**
 * @brief Verifies that the actual metadata equals expected one.
 * @param (expected)   [in]  expected metadata.
 * @param (actual)     [in]  actual metadata.
 * @param (meta_name)  [in]  name of metadata table.
 * @return none.
 */
template <typename T>
void TableMetadataHelper::check_child_expected(
    const std::vector<T>& expected, const boost::property_tree::ptree& actual,
    const char* meta_name) {
  auto o_actual = actual.get_child_optional(meta_name);

  if ((expected.size() != 0) && o_actual) {
    std::vector<T> actual_array;
    std::transform(o_actual.get().begin(), o_actual.get().end(),
                   std::back_inserter(actual_array),
                   [](boost::property_tree::ptree::value_type v) {
                     return v.second.get_optional<T>("").get();
                   });
    EXPECT_EQ_T(expected, actual_array, meta_name);
  } else if (o_actual) {
    EXPECT_EQ_T((expected.size() == 0), o_actual.value().empty(), meta_name);
  } else {
    EXPECT_EQ_T((expected.size() == 0), !o_actual.is_initialized(), meta_name);
  }
}

/**
 * @brief Verifies that the actual metadata equals expected one.
 * @param (expected)   [in]  expected column metadata.
 * @param (actual)     [in]  actual column metadata.
 * @param (meta_name)  [in]  name of column metadata table.
 * @return none.
 */
template <typename T>
void TableMetadataHelper::check_expected(
    const boost::property_tree::ptree& expected,
    const boost::property_tree::ptree& actual, const char* meta_name) {
  auto value_expected = expected.get_optional<T>(meta_name);
  auto value_actual   = actual.get_optional<T>(meta_name);

  if (value_expected && value_actual) {
    EXPECT_EQ_T(value_expected.value(), value_actual.value(), meta_name);
  } else {
    if (value_expected) {
      const auto& value_expected = expected.get<std::string>(meta_name);
      EXPECT_EQ_T(value_expected.empty(), !value_actual.is_initialized(),
                  meta_name);
    } else if (value_actual) {
      const auto& value_actual = actual.get<std::string>(meta_name);
      EXPECT_EQ_T(!value_expected.is_initialized(), value_actual.empty(),
                  meta_name);
    } else {
      EXPECT_EQ_T(value_expected.is_initialized(),
                  value_actual.is_initialized(), meta_name);
    }
  }
}

}  // namespace manager::metadata::testing
