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
#include <gtest/gtest.h>

#include <boost/foreach.hpp>
#include <boost/property_tree/json_parser.hpp>
#include <memory>
#include <string>

#include "manager/metadata/error_code.h"
#include "manager/metadata/roles.h"
#include "manager/metadata/tables.h"
#include "test/global_test_environment.h"
#include "test/helper/table_metadata_helper.h"
#include "test/utility/ut_table_metadata.h"
#include "test/utility/ut_utils.h"

namespace manager::metadata::testing {

using boost::property_tree::ptree;

class ApiTestTableMetadata : public ::testing::Test {
  void SetUp() override {
    if (!global->is_open()) {
      GTEST_SKIP_("metadata repository is not started.");
    }
  }
};

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
  TableMetadataHelper::add_table(new_table, &ret_table_id);
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
  TableMetadataHelper::check_table_metadata_expected(new_table,
                                                     table_metadata_inserted);

  // remove table metadata.
  TableMetadataHelper::remove_table(ret_table_id);
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
  TableMetadataHelper::check_table_metadata_expected(new_table,
                                                     table_metadata_inserted);

  // remove table metadata.
  TableMetadataHelper::remove_table(new_table_name);
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
  TableMetadataHelper::remove_table(ret_table_id[0]);
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
  TableMetadataHelper::add_table(new_table, &ret_table_id);
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
  TableMetadataHelper::check_table_metadata_expected(new_table,
                                                     table_metadata_inserted);

  // remove table metadata.
  TableMetadataHelper::remove_table(ret_table_id);
}

/**
 * @brief happy test for all table metadata getting.
 */
TEST_F(ApiTestTableMetadata, get_all_table_metadata) {
  constexpr int test_table_count = 5;
  std::string table_name_prefix =
      "ApiTestTableMetadata-GetAll-" + std::to_string(time(NULL));
  std::vector<ObjectIdType> table_ids = {};

  // get base count
  std::int64_t base_table_count = TableMetadataHelper::get_record_count();

  // gets all table metadata.
  auto tables = std::make_unique<Tables>(GlobalTestEnvironment::TEST_DB);
  ErrorCode error = tables->init();
  EXPECT_EQ(ErrorCode::OK, error);

  // prepare test data for adding table metadata.
  UTTableMetadata testdata_table_metadata =
      *(global->testdata_table_metadata.get());
  ptree expected_table = testdata_table_metadata.tables;

  // add table metadata.
  for (int count = 1; count <= test_table_count; count++) {
    std::string table_name = table_name_prefix + std::to_string(count);
    ObjectIdType table_id;
    TableMetadataHelper::add_table(table_name, &table_id);
    table_ids.emplace_back(table_id);
  }

  std::vector<boost::property_tree::ptree> container = {};

  error = tables->get_all(container);
  EXPECT_EQ(ErrorCode::OK, error);
  EXPECT_EQ(test_table_count + base_table_count, container.size());

  UTUtils::print("-- get all table metadata --");
  for (int index = 0; index < test_table_count; index++) {
    ptree table_metadata = container[index + base_table_count];
    UTUtils::print(UTUtils::get_tree_string(table_metadata));

    std::string table_name = table_name_prefix + std::to_string(index + 1);
    expected_table.put(Tables::ID, table_ids[index]);
    expected_table.put(Tables::NAME, table_name);

    // verifies that the returned table metadata is expected one.
    TableMetadataHelper::check_table_metadata_expected(expected_table,
                                                       table_metadata);
  }

  // cleanup
  for (ObjectIdType table_id : table_ids) {
    error = tables->remove(table_id);
    EXPECT_EQ(ErrorCode::OK, error);
  }
}

/**
 * @brief happy test for all table metadata getting.
 */
TEST_F(ApiTestTableMetadata, get_all_table_metadata_empty) {
  // get base count
  std::int64_t base_table_count = TableMetadataHelper::get_record_count();

  // gets all table metadata.
  auto tables = std::make_unique<Tables>(GlobalTestEnvironment::TEST_DB);
  ErrorCode error = tables->init();
  EXPECT_EQ(ErrorCode::OK, error);

  std::vector<boost::property_tree::ptree> container = {};
  error = tables->get_all(container);
  EXPECT_EQ(ErrorCode::OK, error);
  EXPECT_EQ(base_table_count, container.size());
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
  TableMetadataHelper::add_table(new_table, &ret_table_id);

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
  TableMetadataHelper::add_table(new_table, &ret_table_id);

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
  TableMetadataHelper::check_table_metadata_expected(
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
  TableMetadataHelper::check_table_metadata_expected(
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
