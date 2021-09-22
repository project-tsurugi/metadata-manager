/*
 * Copyright 2020 tsurugi project.
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
#include "test/api_test_table_metadata.h"

#include <gtest/gtest.h>
#include <boost/foreach.hpp>
#include <boost/property_tree/json_parser.hpp>
#include <iostream>
#include <memory>
#include <string>

#include "manager/metadata/error_code.h"
#include "manager/metadata/tables.h"
#include "test/api_test_table_metadata_extra.h"
#include "test/utility/ut_table_metadata.h"
#include "test/utility/ut_utils.h"

using namespace manager::metadata;
using namespace boost::property_tree;

namespace manager::metadata::testing {

void ApiTestTableMetadata::SetUp() {
  if (!global->is_open()) {
    GTEST_SKIP_("metadata repository is not started.");
  }
}

/**
 * @brief Verifies that the actual metadata equals expected one.
 * @param (expected)   [in]  expected metadata.
 * @param (actual)     [in]  actual metadata.
 * @param (meta_name)  [in]  column name of column metadata table.
 * @return none.
 */
void ApiTestTableMetadata::check_metadata_expected(ptree& expected,
                                                   ptree& actual,
                                                   const char* meta_name) {
  boost::optional<ptree&> o_expected = expected.get_child_optional(meta_name);
  boost::optional<ptree&> o_actual = actual.get_child_optional(meta_name);

  if (o_expected && o_actual) {
    ptree& p_expected = o_expected.value();
    ptree& p_actual = o_actual.value();
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
void ApiTestTableMetadata::check_column_metadata_expecetd(
    ptree& expected, ptree& actual, const char* meta_name) {
  boost::optional<T> value_expected = expected.get_optional<T>(meta_name);
  boost::optional<T> value_actual = actual.get_optional<T>(meta_name);
  if (value_expected && value_actual) {
    EXPECT_EQ(value_expected.value(), value_actual.value());
  } else if (!value_expected && !value_actual) {
    ASSERT_TRUE(true);
  } else {
    ASSERT_TRUE(false);
  }
};

/**
 * @brief Verifies that the actual table metadata equals expected one.
 * @param (expected)   [in]  expected table metadata.
 * @param (actual)     [in]  actual table metadata.
 * @return none.
 */
void ApiTestTableMetadata::check_table_metadata_expected(ptree& expected,
                                                         ptree& actual) {
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

  // column metadata
  boost::optional<ptree&> o_columns_expected =
      expected.get_child_optional(Tables::COLUMNS_NODE);
  boost::optional<ptree&> o_columns_actual =
      actual.get_child_optional(Tables::COLUMNS_NODE);

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
      check_column_metadata_expecetd<std::string>(
          column_expected, column_actual, Tables::Column::NAME);
      // column ordinal position
      check_column_metadata_expecetd<ObjectIdType>(
          column_expected, column_actual, Tables::Column::ORDINAL_POSITION);
      // column data type id
      check_column_metadata_expecetd<ObjectIdType>(
          column_expected, column_actual, Tables::Column::DATA_TYPE_ID);
      // column data length
      check_metadata_expected(column_expected, column_actual,
                              Tables::Column::DATA_LENGTH);
      // column varying
      check_column_metadata_expecetd<bool>(column_expected, column_actual,
                                           Tables::Column::VARYING);
      // nullable
      check_column_metadata_expecetd<bool>(column_expected, column_actual,
                                           Tables::Column::NULLABLE);
      // default
      check_column_metadata_expecetd<std::string>(
          column_expected, column_actual, Tables::Column::DEFAULT);
      // direction
      check_column_metadata_expecetd<ObjectIdType>(
          column_expected, column_actual, Tables::Column::DIRECTION);
    }
  } else if (!o_columns_expected && !o_columns_actual) {
    ASSERT_TRUE(true);
  } else {
    ASSERT_TRUE(false);
  }
}

/**
 * @brief Add one new table metadata to table metadata table.
 * @param (table_name)       [in]   table name of new table metadata.
 * @param (ret_table_id)     [out]  table id returned from the api to add new
 * table metadata.
 * @return none.
 */
void ApiTestTableMetadata::add_table(const std::string& table_name,
                                     ObjectIdType* ret_table_id) {
  assert(ret_table_id != nullptr);

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
 * @param (new_table)        [in]   new table metadata.
 * @param (ret_table_id)     [out]  table id returned from the api to add new
 * table metadata.
 * @return none.
 */
void ApiTestTableMetadata::add_table(ptree new_table,
                                     ObjectIdType* ret_table_id) {
  assert(ret_table_id != nullptr);

  auto tables = std::make_unique<Tables>(GlobalTestEnvironment::TEST_DB);

  ErrorCode error = tables->init();
  EXPECT_EQ(ErrorCode::OK, error);

  // add table metadata.
  error = tables->add(new_table, ret_table_id);
  EXPECT_EQ(ErrorCode::OK, error);
  EXPECT_GT(*ret_table_id, 0);

  UTUtils::print("-- add table metadata --");
  UTUtils::print("new table id:", *ret_table_id);
  UTUtils::print(UTUtils::get_tree_string(new_table));
}

/**
 * @brief happy test for adding one new table metadata
 *  and getting it by table name.
 */
TEST_F(ApiTestTableMetadata, add_get_table_metadata_by_table_name) {
  // prepare test data for adding table metadata.
  UTTableMetadata testdata_table_metadata =
      *(global->testdata_table_metadata.get());
  ptree new_table = testdata_table_metadata.tables;
  std::string new_table_name =
      new_table.get<std::string>(Tables::NAME) + "_ApiTestTableMetadata1";
  new_table.put(Tables::NAME, new_table_name);

  // add table metadata.
  ObjectIdType ret_table_id = -1;
  ApiTestTableMetadata::add_table(new_table, &ret_table_id);
  new_table.put(Tables::ID, ret_table_id);

  // get table metadata by table name.
  auto tables = std::make_unique<Tables>(GlobalTestEnvironment::TEST_DB);
  ErrorCode error = tables->init();
  EXPECT_EQ(ErrorCode::OK, error);

  ptree table_metadata_inserted;
  error = tables->get(new_table_name, table_metadata_inserted);
  EXPECT_EQ(ErrorCode::OK, error);

  UTUtils::print("-- get table metadata --");
  UTUtils::print(UTUtils::get_tree_string(table_metadata_inserted));

  // verifies that the returned table metadata is expected one.
  ApiTestTableMetadata::check_table_metadata_expected(new_table,
                                                      table_metadata_inserted);
}

/**
 * @brief happy test for adding one new table metadata without returned table
 * id and getting it by table name.
 */
TEST_F(ApiTestTableMetadata,
       add_without_returned_table_id_get_table_metadata_by_table_name) {
  // prepare test data for adding table metadata.
  UTTableMetadata testdata_table_metadata =
      *(global->testdata_table_metadata.get());
  ptree new_table = testdata_table_metadata.tables;
  std::string new_table_name =
      new_table.get<std::string>(Tables::NAME) + "_ApiTestTableMetadata2";
  new_table.put(Tables::NAME, new_table_name);

  // add table metadata.
  auto tables = std::make_unique<Tables>(GlobalTestEnvironment::TEST_DB);

  ErrorCode error = tables->init();
  EXPECT_EQ(ErrorCode::OK, error);

  error = tables->add(new_table);
  EXPECT_EQ(ErrorCode::OK, error);

  UTUtils::print("-- add table metadata --");
  UTUtils::print(UTUtils::get_tree_string(new_table));

  // get table metadata by table name.
  ptree table_metadata_inserted;
  error = tables->get(new_table_name, table_metadata_inserted);
  EXPECT_EQ(ErrorCode::OK, error);

  UTUtils::print("-- get table metadata --");
  UTUtils::print(UTUtils::get_tree_string(table_metadata_inserted));

  // verifies that the returned table metadata is expected one.
  new_table.put(Tables::ID,
                table_metadata_inserted.get<ObjectIdType>(Tables::ID));
  ApiTestTableMetadata::check_table_metadata_expected(new_table,
                                                      table_metadata_inserted);
}

/**
 * @brief happy test for adding two same table metadata
 *  and getting them by table name.
 */
TEST_F(ApiTestTableMetadata, get_two_table_metadata_by_table_name) {
  // prepare test data for adding table metadata.
  UTTableMetadata testdata_table_metadata =
      *(global->testdata_table_metadata.get());
  ptree new_table = testdata_table_metadata.tables;
  std::string new_table_name =
      new_table.get<std::string>(Tables::NAME) + "_ApiTestTableMetadata3";
  new_table.put(Tables::NAME, new_table_name);

  auto tables = std::make_unique<Tables>(GlobalTestEnvironment::TEST_DB);

  ErrorCode error = tables->init();
  EXPECT_EQ(ErrorCode::OK, error);

  // prepare variables for returned value from add api.
  std::vector<ObjectIdType> ret_table_id;
  ret_table_id.emplace_back(-1);
  ret_table_id.emplace_back(-1);

  // add first table metadata.
  error = tables->add(new_table, &ret_table_id[0]);
  EXPECT_EQ(ErrorCode::OK, error);
  EXPECT_GT(ret_table_id[0], 0);

  // add second table metadata.
  error = tables->add(new_table, &ret_table_id[1]);
  EXPECT_EQ(ErrorCode::TABLE_NAME_ALREADY_EXISTS, error);
  EXPECT_EQ(ret_table_id[1], -1);

  UTUtils::print("-- add table metadata --");
  UTUtils::print(UTUtils::get_tree_string(new_table));

  // remove table metadata by table id.
  ptree table_metadata_inserted;
  error = tables->remove(ret_table_id[0]);
  EXPECT_EQ(ErrorCode::OK, error);

  UTUtils::print("-- remove table metadata --");
}

/**
 * @brief happy test for adding one new table metadata
 *  and getting it by table id.
 */
TEST_F(ApiTestTableMetadata, add_get_table_metadata_by_table_id) {
  // prepare test data for adding table metadata.
  UTTableMetadata testdata_table_metadata =
      *(global->testdata_table_metadata.get());
  ptree new_table = testdata_table_metadata.tables;
  std::string new_table_name =
      new_table.get<std::string>(Tables::NAME) + "_ApiTestTableMetadata4";
  new_table.put(Tables::NAME, new_table_name);

  // add table metadata.
  ObjectIdType ret_table_id = -1;
  ApiTestTableMetadata::add_table(new_table, &ret_table_id);
  new_table.put(Tables::ID, ret_table_id);

  // get table metadata by table id.
  auto tables = std::make_unique<Tables>(GlobalTestEnvironment::TEST_DB);
  ErrorCode error = tables->init();
  EXPECT_EQ(ErrorCode::OK, error);

  ptree table_metadata_inserted;
  error = tables->get(ret_table_id, table_metadata_inserted);
  EXPECT_EQ(ErrorCode::OK, error);

  UTUtils::print("-- get table metadata --");
  UTUtils::print(UTUtils::get_tree_string(table_metadata_inserted));

  // verifies that the returned table metadata is expected one.
  ApiTestTableMetadata::check_table_metadata_expected(new_table,
                                                      table_metadata_inserted);
}

/**
 * @brief happy test for removing one new table metadata by table name.
 */
TEST_F(ApiTestTableMetadata, remove_table_metadata_by_table_name) {
  // prepare test data for adding table metadata.
  UTTableMetadata testdata_table_metadata =
      *(global->testdata_table_metadata.get());
  ptree new_table = testdata_table_metadata.tables;
  std::string new_table_name =
      new_table.get<std::string>(Tables::NAME) + "_ApiTestTableMetadata5";
  new_table.put(Tables::NAME, new_table_name);

  // add table metadata.
  ObjectIdType ret_table_id = -1;
  ApiTestTableMetadata::add_table(new_table, &ret_table_id);

  // remove table metadata by table name.
  auto tables = std::make_unique<Tables>(GlobalTestEnvironment::TEST_DB);
  ErrorCode error = tables->init();
  EXPECT_EQ(ErrorCode::OK, error);

  ObjectIdType table_id_to_remove = -1;
  error = tables->remove(new_table_name.c_str(), &table_id_to_remove);
  EXPECT_EQ(ErrorCode::OK, error);
  EXPECT_EQ(ret_table_id, table_id_to_remove);

  // verifies that table metadata does not exist.
  ptree table_metadata_got;
  error = tables->get(table_id_to_remove, table_metadata_got);
  EXPECT_EQ(ErrorCode::ID_NOT_FOUND, error);

  UTUtils::print("-- get table metadata --");
  UTUtils::print(UTUtils::get_tree_string(table_metadata_got));
}

/**
 * @brief happy test for removing one new table metadata by table id.
 */
TEST_F(ApiTestTableMetadata, remove_table_metadata_by_table_id) {
  // prepare test data for adding table metadata.
  UTTableMetadata testdata_table_metadata =
      *(global->testdata_table_metadata.get());
  ptree new_table = testdata_table_metadata.tables;
  std::string new_table_name =
      new_table.get<std::string>(Tables::NAME) + "_ApiTestTableMetadata6";
  new_table.put(Tables::NAME, new_table_name);

  // add table metadata.
  ObjectIdType ret_table_id = -1;
  ApiTestTableMetadata::add_table(new_table, &ret_table_id);

  // remove table metadata by table id.
  auto tables = std::make_unique<Tables>(GlobalTestEnvironment::TEST_DB);
  ErrorCode error = tables->init();
  EXPECT_EQ(ErrorCode::OK, error);

  error = tables->remove(ret_table_id);
  EXPECT_EQ(ErrorCode::OK, error);

  // verifies that table metadata does not exist.
  ptree table_metadata_got;
  error = tables->get(ret_table_id, table_metadata_got);
  EXPECT_EQ(ErrorCode::ID_NOT_FOUND, error);

  UTUtils::print("-- get table metadata --");
  UTUtils::print(UTUtils::get_tree_string(table_metadata_got));
}

/**
 * @brief happy test for adding, getting and removing
 *  one new table metadata without initialization of all api.
 */
TEST_F(ApiTestTableMetadata,
       add_get_remove_table_metadata_without_initialized) {
  // prepare test data for adding table metadata.
  UTTableMetadata testdata_table_metadata =
      *(global->testdata_table_metadata.get());
  ptree new_table = testdata_table_metadata.tables;
  std::string new_table_name =
      new_table.get<std::string>(Tables::NAME) + "_ApiTestTableMetadata7";
  new_table.put(Tables::NAME, new_table_name);

  // add table metadata without initialized.
  auto tables_add = std::make_unique<Tables>(GlobalTestEnvironment::TEST_DB);

  ObjectIdType ret_table_id = -1;
  ErrorCode error = tables_add->add(new_table, &ret_table_id);
  new_table.put(Tables::ID, ret_table_id);

  EXPECT_EQ(ErrorCode::OK, error);
  EXPECT_GT(ret_table_id, 0);

  // get table metadata by table id without initialized.
  auto tables_get_by_id =
      std::make_unique<Tables>(GlobalTestEnvironment::TEST_DB);

  ptree table_metadata_inserted_by_id;
  error = tables_get_by_id->get(ret_table_id, table_metadata_inserted_by_id);
  EXPECT_EQ(ErrorCode::OK, error);

  UTUtils::print("-- get table metadata --");
  UTUtils::print(UTUtils::get_tree_string(table_metadata_inserted_by_id));

  // verifies that the returned table metadata is expected one.
  ApiTestTableMetadata::check_table_metadata_expected(
      new_table, table_metadata_inserted_by_id);

  // get table metadata by table name without initialized.
  auto tables_get_by_name =
      std::make_unique<Tables>(GlobalTestEnvironment::TEST_DB);

  ptree table_metadata_inserted_by_name;
  error =
      tables_get_by_name->get(new_table_name, table_metadata_inserted_by_name);
  EXPECT_EQ(ErrorCode::OK, error);

  UTUtils::print("-- get table metadata --");
  UTUtils::print(UTUtils::get_tree_string(table_metadata_inserted_by_name));

  // verifies that the returned table metadata is expected one.
  ApiTestTableMetadata::check_table_metadata_expected(
      new_table, table_metadata_inserted_by_name);

  // remove table metadata by table name without initialized.
  ObjectIdType table_id_to_remove = -1;
  auto tables_remove_by_name =
      std::make_unique<Tables>(GlobalTestEnvironment::TEST_DB);
  error = tables_remove_by_name->remove(new_table_name.c_str(),
                                        &table_id_to_remove);
  EXPECT_EQ(ErrorCode::OK, error);
  EXPECT_EQ(ret_table_id, table_id_to_remove);

  // add table metadata again.
  error = tables_add->add(new_table, &ret_table_id);
  new_table.put(Tables::ID, ret_table_id);

  // remove table metadata by table id without initialized.
  auto tables_remove_by_id =
      std::make_unique<Tables>(GlobalTestEnvironment::TEST_DB);
  error = tables_remove_by_id->remove(ret_table_id);
  EXPECT_EQ(ErrorCode::OK, error);
}

}  // namespace manager::metadata::testing
