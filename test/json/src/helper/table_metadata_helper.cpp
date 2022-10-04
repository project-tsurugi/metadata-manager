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
#include "test/helper/table_metadata_helper.h"

#include <gtest/gtest.h>

#include <string>
#include <vector>

#include <boost/foreach.hpp>
#include <boost/property_tree/json_parser.hpp>
#include <boost/property_tree/ptree.hpp>

#include "manager/metadata/datatypes.h"
#include "manager/metadata/tables.h"
#include "test/global_test_environment.h"
#include "test/utility/ut_utils.h"

namespace manager::metadata::testing {

namespace json_parser = boost::property_tree::json_parser;

using boost::property_tree::json_parser_error;
using boost::property_tree::ptree;

/**
 * @brief Generate table metadata.
 * @param (testdata_table_metadata)    [out] table metadata used as test data.
 */
void TableMetadataHelper::generate_table_metadata(
    std::unique_ptr<UTTableMetadata>& testdata_table_metadata) 
{
  // generate unique table name.
  int s = time(NULL);
  std::string table_name = "table_name" + std::to_string(s);
  testdata_table_metadata = std::make_unique<UTTableMetadata>(table_name);

  // generate namespace.
  testdata_table_metadata->namespace_name = "namespace";

  // generate three column metadata.
  std::vector<ObjectIdType> ordinal_positions = {1, 2, 3};
  std::vector<std::string> col_names = {"col1", "col2", "col3"};

  // generate primary keys.
  testdata_table_metadata->primary_keys.push_back(ordinal_positions[0]);
  testdata_table_metadata->primary_keys.push_back(ordinal_positions[1]);

  // generate tuples.
  testdata_table_metadata->tuples = 0;

  // first column metadata
  bool is_null = true;
  UTColumnMetadata column1{
      col_names[0], ordinal_positions[0],
      static_cast<ObjectIdType>(DataTypes::DataTypesId::FLOAT32), !is_null};

  // second column metadata
  UTColumnMetadata column2{
      col_names[1], ordinal_positions[1],
      static_cast<ObjectIdType>(DataTypes::DataTypesId::VARCHAR), !is_null};
  ptree data_length;
  data_length.put("", 8);
  column2.p_data_lengths.push_back(std::make_pair("", data_length));
  data_length.put("", 2);
  column2.p_data_lengths.push_back(std::make_pair("", data_length));
  column2.data_lengths.emplace_back(8);
  column2.data_lengths.emplace_back(2);

  column2.varying = true;

  // third column metadata
  UTColumnMetadata column3{
      col_names[2], ordinal_positions[2],
      static_cast<ObjectIdType>(DataTypes::DataTypesId::CHAR), is_null};
  column3.default_expr = "default";
  column3.data_length = 1;
  column3.varying = false;

  // set table metadata to three column metadata
  testdata_table_metadata->columns.emplace_back(column1);
  testdata_table_metadata->columns.emplace_back(column2);
  testdata_table_metadata->columns.emplace_back(column3);

  // generate ptree from UTTableMetadata fields.
  testdata_table_metadata->generate_ptree();
  testdata_table_metadata->generate_table();
}

/**
 * @brief Make valid table metadata used as test data,
 * by reading a json file with table metadata.
 */
std::vector<boost::property_tree::ptree>
TableMetadataHelper::make_valid_table_metadata() 
{
  std::vector<ptree> test_data_table_metadata;

  ptree pt;
  try {
    // read a json file with table metadata used as test data.
    read_json(global->get_json_schema_file_name(), pt);
  } catch (json_parser_error& e) {
    UTUtils::print("could not read a json file with table metadata.", e.what());
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
 * @param (table_name)       [in]   table name of new table metadata.
 * @param (ret_table_id)     [out]  table id returned from the api to add new
 * table metadata.
 * @return none.
 */
void TableMetadataHelper::add_table(std::string_view table_name,
                                    ObjectIdType* ret_table_id) 
{
  // prepare test data for adding table metadata.
  UTTableMetadata* testdata_table_metadata =
      global->testdata_table_metadata.get();
  ptree new_table = testdata_table_metadata->tables;
  new_table.put(Tables::NAME, table_name);

  // add table metadata.
  add_table(new_table, ret_table_id);
}

/**
 * @brief Add one new table metadata to table metadata table.
 * @param (new_table)  [in]   new table metadata.
 * @param (table_id)   [out]  table id returned from the api to
 *   add new table metadata.
 * @return none.
 */
void TableMetadataHelper::add_table(
    const boost::property_tree::ptree& new_table, ObjectIdType* table_id) 
{
  auto tables = std::make_unique<Tables>(GlobalTestEnvironment::TEST_DB);

  ErrorCode error = tables->init();
  EXPECT_EQ(ErrorCode::OK, error);

  // add table metadata.
  ObjectIdType ret_table_id = INVALID_VALUE;
  error = tables->add(new_table, &ret_table_id);
  EXPECT_EQ(ErrorCode::OK, error);
  EXPECT_GT(ret_table_id, 0);

  UTUtils::print("-- add table metadata --");
  UTUtils::print("new table id:", ret_table_id);
  UTUtils::print(UTUtils::get_tree_string(new_table));

  if (table_id != nullptr) {
    *table_id = ret_table_id;
  }
}

/**
 * @brief Add one new table metadata to table metadata table.
 * @param (new_table)  [in]   new table metadata.
 * @param (table_id)   [out]  table id returned from the api to
 *   add new table metadata.
 * @return none.
 */
void TableMetadataHelper::add_table(
    const manager::metadata::Table& new_table, ObjectIdType* table_id) 
{
  auto tables = std::make_unique<Tables>(GlobalTestEnvironment::TEST_DB);

  ErrorCode error = tables->init();
  EXPECT_EQ(ErrorCode::OK, error);

  // add table metadata.
  ObjectIdType ret_table_id = INVALID_VALUE;
  error = tables->add(new_table, &ret_table_id);
  EXPECT_EQ(ErrorCode::OK, error);
  EXPECT_GT(ret_table_id, 0);

  UTUtils::print("-- add table metadata --");
  UTUtils::print("new table id:", ret_table_id);
//  UTUtils::print(new_table);

  if (table_id != nullptr) {
    *table_id = ret_table_id;
  }
}

/**
 * @brief Verifies that the actual table metadata equals expected one.
 * @param (expected)   [in]  expected table metadata.
 * @param (actual)     [in]  actual table metadata.
 * @return none.
 */
void TableMetadataHelper::check_table_metadata_expected(
    const manager::metadata::Table & expected,
    const boost::property_tree::ptree& actual)
{
  // format version
  EXPECT_EQ(expected.format_version,
            actual.get<FormatVersionType>(Tables::FORMAT_VERSION));

  // generation
  EXPECT_EQ(expected.generation,
            actual.get<GenerationType>(Tables::GENERATION));

  // table name
  EXPECT_EQ(expected.name,
            actual.get<std::string>(Tables::NAME));

  // table id
  ObjectIdType table_id_expected = expected.id;
  EXPECT_EQ(table_id_expected, actual.get<ObjectIdType>(Tables::ID));

  // namespace
  boost::optional<std::string> o_namespace_expected = expected.namespace_name;
  boost::optional<std::string> o_namespace_actual =
      actual.get_optional<std::string>(Tables::NAMESPACE);

  if (o_namespace_actual) {
    std::string& s_namespace_expected = o_namespace_expected.value();
    std::string& s_namespace_actual = o_namespace_actual.value();
    EXPECT_EQ(s_namespace_expected, s_namespace_actual);
  } else if (!o_namespace_expected && !o_namespace_actual) {
    ASSERT_TRUE(true);
  } else {
    ASSERT_TRUE(false);
  }

  // primary keys
#if 0   // ToDo:
    check_metadata_expected(expected, actual, Tables::PRIMARY_KEY_NODE);
#endif

  // tuples
  auto o_tuples_expected = expected.tuples;
  auto o_tuples_actual = actual.get_optional<float>(Tables::TUPLES);
  if (o_tuples_actual) {
    EXPECT_EQ(o_tuples_expected, o_tuples_actual.value());
  } else if (!o_tuples_expected && !o_tuples_actual) {
    ASSERT_TRUE(true);
  } else {
    ASSERT_TRUE(false);
  }

  // column metadata
  auto columns_expected = expected.columns;
  auto o_columns_actual = actual.get_child_optional(Tables::COLUMNS_NODE);

  if (o_columns_actual) {
    std::vector<metadata::Column> p_columns_expected;
    std::vector<ptree> p_columns_actual;
    BOOST_FOREACH (const ptree::value_type& node, o_columns_actual.value()) {
      ptree column = node.second;
      p_columns_actual.emplace_back(column);
    }
    // Verifies that the number of column metadata is expected number.
    EXPECT_EQ(columns_expected.size(), p_columns_actual.size());

    auto column_expected = columns_expected.begin();
    for (int op = 0; static_cast<size_t>(op) < p_columns_expected.size();
         op++) {
      ptree column_actual = p_columns_actual[op];

      // column metadata id
      boost::optional<ObjectIdType> id_actual =
          column_actual.get<ObjectIdType>(Tables::Column::ID);
      EXPECT_GT(id_actual, static_cast<ObjectIdType>(0));

      // column metadata table id
      boost::optional<ObjectIdType> table_id_actual =
          column_actual.get<ObjectIdType>(Tables::Column::TABLE_ID);
      EXPECT_EQ(column_expected->table_id, table_id_actual);

      // column name
      auto name = column_actual.get_optional<std::string>(Tables::Column::NAME);
      if (name) {
        EXPECT_EQ(column_expected->name, name.get());
      }

      // column ordinal position
      auto ordinal_position = 
          column_actual.get_optional<int64_t>(Tables::Column::ORDINAL_POSITION);
      if (ordinal_position) {
        EXPECT_EQ(column_expected->ordinal_position, ordinal_position.get());
      }

      // column data type id
      auto data_type_id = column_actual.get_optional<int64_t>(Tables::Column::DATA_TYPE_ID);
      if (data_type_id) {
        EXPECT_EQ(column_expected->data_type_id, data_type_id.get());
      }

      // column data length
      auto data_length = column_actual.get_optional<int64_t>(Tables::Column::DATA_LENGTH);
      if (data_length) {
        EXPECT_EQ(column_expected->data_length, data_length.get());
      }
      // column data lengths

      // column varying
      auto varying = column_actual.get_optional<bool>(Tables::Column::VARYING);
      if (varying) {
        EXPECT_EQ(column_expected->varying, varying.get());
      }

      // nullable
      auto nullable = column_actual.get_optional<bool>(Tables::Column::NULLABLE);
      if (nullable) {
        EXPECT_EQ(column_expected->nullable, nullable.get());
      }

      // default
      auto default_expr = column_actual.get_optional<std::string>(Tables::Column::DEFAULT);
      if (default_expr) {
        EXPECT_EQ(column_expected->default_expr, default_expr.get());
      }
    }
  } else if (columns_expected.size() == 0 && !o_columns_actual) {
    ASSERT_TRUE(true);
  } else {
    ASSERT_TRUE(false);
  }
}

void TableMetadataHelper::check_table_metadata_expected(
    const boost::property_tree::ptree& expected,
    const manager::metadata::Table& actual)
{
 // format version
  EXPECT_EQ(Tables::format_version(),
            actual.format_version);

  // generation
  EXPECT_EQ(Tables::generation(),
            actual.generation);

  // table name
  EXPECT_EQ(expected.get<std::string>(Tables::NAME),
            actual.name);

  // table id
  ObjectIdType table_id_expected = expected.get<ObjectIdType>(Tables::ID);
  EXPECT_EQ(table_id_expected, actual.id);

  // namespace
  boost::optional<std::string> o_namespace_expected =
      expected.get_optional<std::string>(Tables::NAMESPACE);
  boost::optional<std::string> o_namespace_actual =
      actual.namespace_name;

  if (o_namespace_expected && o_namespace_actual) {
    std::string& s_namespace_expected = o_namespace_expected.value();
    std::string& s_namespace_actual = o_namespace_actual.value();
    EXPECT_EQ(s_namespace_expected, s_namespace_actual);
  } else if (!o_namespace_expected && !o_namespace_actual) {
    ASSERT_TRUE(true);
  } else {
    ASSERT_TRUE(false);
  }

  // primary keys
//  check_metadata_expected(expected, actual, Tables::PRIMARY_KEY_NODE);

  // tuples
  auto o_tuples_expected = expected.get_optional<float>(Tables::TUPLES);
  auto o_tuples_actual = expected.get_optional<float>(Tables::TUPLES);
  if (o_tuples_expected && o_tuples_actual) {
    EXPECT_EQ(o_tuples_expected.value(), o_tuples_actual.value());
  } else if (!o_tuples_expected && !o_tuples_actual) {
    ASSERT_TRUE(true);
  } else {
    ASSERT_TRUE(false);
  }

  // column metadata
  auto o_columns_expected = expected.get_child_optional(Tables::COLUMNS_NODE);
  auto o_columns_actual = actual.columns;

  if (o_columns_expected) {
    std::vector<ptree> p_columns_expected;
    BOOST_FOREACH (const ptree::value_type& node, o_columns_expected.value()) {
      ptree column = node.second;
      p_columns_expected.emplace_back(column);
    }
    // Verifies that the number of column metadata is expected number.
    EXPECT_EQ(p_columns_expected.size(), o_columns_actual.size());

    auto column_actual = o_columns_actual.begin();
    for (int op = 0; static_cast<size_t>(op) < p_columns_expected.size();
         op++) {
      ptree column_expected = p_columns_expected[op];

      // column metadata id
      EXPECT_GT(column_actual->id, static_cast<ObjectIdType>(0));

      // column metadata table id
      EXPECT_EQ(table_id_expected, column_actual->table_id);

      // column name
      auto name = column_expected.get_optional<std::string>(Tables::Column::NAME);
      if (name) {
        EXPECT_EQ(name.get(), column_actual->name);
      }
      // column ordinal position
      auto ordinal_position = 
          column_expected.get_optional<int64_t>(Tables::Column::ORDINAL_POSITION);
      if (ordinal_position) {
        EXPECT_EQ(ordinal_position.get(), column_actual->ordinal_position);
      }
      // column data type id
      auto data_type_id = column_expected.get_optional<int64_t>(Tables::Column::DATA_TYPE_ID);
      if (data_type_id) {
        EXPECT_EQ(data_type_id.get(), column_actual->data_type_id);
      }
      // column data length
      auto data_length = column_expected.get_optional<int64_t>(Tables::Column::DATA_LENGTH);
      if (data_length) {
        EXPECT_EQ(data_length.get(), column_actual->data_length);
      }
      // column data lengths

      // column varying
       auto varying = column_expected.get_optional<bool>(Tables::Column::VARYING);
       if (varying) {
         EXPECT_EQ(varying.get(), column_actual->varying);
       }
      // nullable
      auto nullable = column_expected.get_optional<bool>(Tables::Column::NULLABLE);
      if (nullable) {
        EXPECT_EQ(nullable.get(), column_actual->nullable);
      }
      // default
      auto default_expr = column_expected.get_optional<std::string>(Tables::Column::DEFAULT);
      if (default_expr) {
        EXPECT_EQ(default_expr.get(), column_actual->default_expr);
      }
      column_actual++;
    }
  } else if (!o_columns_expected) {
    ASSERT_TRUE(true);
  } else {
    ASSERT_TRUE(false);
  }
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
            actual.get<FormatVersionType>(Tables::FORMAT_VERSION));

  // generation
  EXPECT_EQ(Tables::generation(),
            actual.get<GenerationType>(Tables::GENERATION));

  // table name
  EXPECT_EQ(expected.get<std::string>(Tables::NAME),
            actual.get<std::string>(Tables::NAME));

  // table id
  ObjectIdType table_id_expected = expected.get<ObjectIdType>(Tables::ID);
  EXPECT_EQ(table_id_expected, actual.get<ObjectIdType>(Tables::ID));

  // namespace
  boost::optional<std::string> o_namespace_expected =
      expected.get_optional<std::string>(Tables::NAMESPACE);
  boost::optional<std::string> o_namespace_actual =
      actual.get_optional<std::string>(Tables::NAMESPACE);

  if (o_namespace_expected && o_namespace_actual) {
    std::string& s_namespace_expected = o_namespace_expected.value();
    std::string& s_namespace_actual = o_namespace_actual.value();
    EXPECT_EQ(s_namespace_expected, s_namespace_actual);
  } else if (!o_namespace_expected && !o_namespace_actual) {
    ASSERT_TRUE(true);
  } else {
    ASSERT_TRUE(false);
  }

  // primary keys
  check_metadata_expected(expected, actual, Tables::PRIMARY_KEY_NODE);

  // tuples
  auto o_tuples_expected = expected.get_optional<float>(Tables::TUPLES);
  auto o_tuples_actual = actual.get_optional<float>(Tables::TUPLES);
  if (o_tuples_expected && o_tuples_actual) {
    EXPECT_EQ(o_tuples_expected.value(), o_tuples_actual.value());
  } else if (!o_tuples_expected && !o_tuples_actual) {
    ASSERT_TRUE(true);
  } else {
    ASSERT_TRUE(false);
  }

  // column metadata
  auto o_columns_expected = expected.get_child_optional(Tables::COLUMNS_NODE);
  auto o_columns_actual = actual.get_child_optional(Tables::COLUMNS_NODE);

  if (o_columns_expected && o_columns_actual) {
    std::vector<ptree> p_columns_expected;
    std::vector<ptree> p_columns_actual;
    BOOST_FOREACH (const ptree::value_type& node, o_columns_expected.value()) {
      ptree column = node.second;
      p_columns_expected.emplace_back(column);
    }
    BOOST_FOREACH (const ptree::value_type& node, o_columns_actual.value()) {
      ptree column = node.second;
      p_columns_actual.emplace_back(column);
    }

    // Verifies that the number of column metadata is expected number.
    EXPECT_EQ(p_columns_expected.size(), p_columns_actual.size());

    for (int op = 0; static_cast<size_t>(op) < p_columns_expected.size();
         op++) {
      ptree column_expected = p_columns_expected[op];
      ptree column_actual = p_columns_actual[op];

      // column metadata id
      boost::optional<ObjectIdType> id_actual =
          column_actual.get<ObjectIdType>(Tables::Column::ID);
      EXPECT_GT(id_actual, static_cast<ObjectIdType>(0));

      // column metadata table id
      boost::optional<ObjectIdType> table_id_actual =
          column_actual.get<ObjectIdType>(Tables::Column::TABLE_ID);
      EXPECT_EQ(table_id_expected, table_id_actual);

      // column name
      check_column_metadata_expected<std::string>(
          column_expected, column_actual, Tables::Column::NAME);
      // column ordinal position
      check_column_metadata_expected<ObjectIdType>(
          column_expected, column_actual, Tables::Column::ORDINAL_POSITION);
      // column data type id
      check_column_metadata_expected<ObjectIdType>(
          column_expected, column_actual, Tables::Column::DATA_TYPE_ID);
      // column data length
      check_metadata_expected(column_expected, column_actual,
                              Tables::Column::DATA_LENGTH);
      // column data lengths
      // column varying
      check_column_metadata_expected<bool>(column_expected, column_actual,
                                           Tables::Column::VARYING);
      // nullable
      check_column_metadata_expected<bool>(column_expected, column_actual,
                                           Tables::Column::NULLABLE);
      // default
      check_column_metadata_expected<std::string>(
          column_expected, column_actual, Tables::Column::DEFAULT);
    }
  } else if (!o_columns_expected && !o_columns_actual) {
    ASSERT_TRUE(true);
  } else {
    ASSERT_TRUE(false);
  }
}

/**
 * @brief Verifies that the actual metadata equals expected one.
 * @param (expected)   [in]  expected metadata.
 * @param (actual)     [in]  actual metadata.
 * @param (meta_name)  [in]  column name of column metadata table.
 * @return none.
 */
void TableMetadataHelper::check_metadata_expected(
    const boost::property_tree::ptree& expected,
    const boost::property_tree::ptree& actual, const char* meta_name) {
  auto o_expected = expected.get_child_optional(meta_name);
  auto o_actual = actual.get_child_optional(meta_name);

  if (o_expected && o_actual) {
    auto& p_expected = o_expected.value();
    auto& p_actual = o_actual.value();
    EXPECT_EQ(UTUtils::get_tree_string(p_expected),
              UTUtils::get_tree_string(p_actual));
  } else if ((!o_expected && !o_actual) ||
             (o_expected && o_expected.value().empty() && !o_actual) ||
             (o_actual && o_actual.value().empty() && !o_expected)) {
    ASSERT_TRUE(true);
  } else {
    ASSERT_TRUE(false);
  }
}

/**
 * @brief Verifies that the actual column metadata equals expected one.
 * @param (expected)   [in]  expected column metadata.
 * @param (actual)     [in]  actual column metadata.
 * @param (meta_name)  [in]  column name of column metadata table.
 * @return none.
 */
template <typename T>
void TableMetadataHelper::check_column_metadata_expected(
    const boost::property_tree::ptree& expected,
    const boost::property_tree::ptree& actual, const char* meta_name) {
  auto value_expected = expected.get_optional<T>(meta_name);
  auto value_actual = actual.get_optional<T>(meta_name);

  if (value_expected && value_actual) {
    EXPECT_EQ(value_expected.value(), value_actual.value());
  } else if (!value_expected && !value_actual) {
    ASSERT_TRUE(true);
  } else {
    ASSERT_TRUE(false);
  }
}

}  // namespace manager::metadata::testing
