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
#include <gtest/gtest.h>

#include "manager/metadata/metadata_factory.h"
#include "test/common/global_test_environment.h"
#include "test/common/ut_utils.h"
#include "test/helper/api_test_helper.h"
#include "test/helper/table_metadata_helper.h"

namespace manager::metadata::testing {

namespace {

static std::vector<UtTableMetadata> valid_table_metadata;

}  // namespace

using boost::property_tree::ptree;

class ApiTestTableMetadata : public ::testing::Test {
 public:
  void SetUp() override { UTUtils::skip_if_connection_not_opened(); }
};

class ApiTestTableMetadataEx
    : public ::testing::TestWithParam<std::vector<UtTableMetadata>> {
 public:
  static void SetUpTestCase() {
    if (g_environment_->is_open()) {
      UTUtils::print(">> gtest::SetUpTestCase()");

      // If metadata repository is opened,
      // make valid table metadata used as test data.
      valid_table_metadata = TableMetadataHelper::make_valid_table_metadata();

      UTUtils::print("<< gtest::SetUpTestCase()");
    }
  }

  void SetUp() override {
    UTUtils::skip_if_connection_not_opened();

    if (g_environment_->is_open()) {
      // If valid test data could not be made, skip this test.
      if (valid_table_metadata.empty()) {
        GTEST_SKIP_("  Skipped: Could not read a json file with table metadata.");
      }
    }
  }
};

/**
 * @brief This is a test for duplicate table names.
 */
TEST_F(ApiTestTableMetadata, test_duplicate_table_name) {
  CALL_TRACE;

  // Generate tables metadata manager.
  auto managers = get_tables_ptr(GlobalTestEnvironment::TEST_DB);

  // Generate test metadata.
  UtTableMetadata ut_metadata;

  auto inserted_metadata = ut_metadata.get_metadata_ptree();

  // Test initialization.
  ApiTestHelper::test_init(managers.get(), ErrorCode::OK);

  // add first table metadata.
  ObjectId object_id_1st =
      ApiTestHelper::test_add(managers.get(), inserted_metadata, ErrorCode::OK);
  EXPECT_GT(object_id_1st, INVALID_OBJECT_ID);

  // add second table metadata.
  ObjectId object_id_2nd = ApiTestHelper::test_add(
      managers.get(), inserted_metadata, ErrorCode::ALREADY_EXISTS);
  EXPECT_EQ(object_id_2nd, INVALID_OBJECT_ID);

  // remove table metadata.
  ApiTestHelper::test_remove(managers.get(), object_id_1st, ErrorCode::OK);
}

/**
 * @brief This test executes all APIs without initialization.
 */
TEST_F(ApiTestTableMetadata, test_without_initialized) {
  CALL_TRACE;

  // Generate test metadata.
  UtTableMetadata ut_metadata;

  auto inserted_metadata  = ut_metadata.get_metadata_ptree();
  std::string object_name = ut_metadata.get_metadata_struct()->name;
  ObjectId object_id      = -1;

  // Add table metadata.
  {
    // Generate tables metadata manager.
    auto managers = get_tables_ptr(GlobalTestEnvironment::TEST_DB);

    object_id = ApiTestHelper::test_add(managers.get(), inserted_metadata,
                                        ErrorCode::OK);
  }

  // Get table metadata by table id with ptree.
  {
    // Generate tables metadata manager.
    auto managers = get_tables_ptr(GlobalTestEnvironment::TEST_DB);

    ptree retrieved_metadata;
    ApiTestHelper::test_get(managers.get(), object_id, ErrorCode::OK,
                            retrieved_metadata);
  }

  // Get table metadata by table name with ptree.
  {
    // Generate tables metadata manager.
    auto managers = get_tables_ptr(GlobalTestEnvironment::TEST_DB);

    ptree retrieved_metadata;
    ApiTestHelper::test_get(managers.get(), object_name, ErrorCode::OK,
                            retrieved_metadata);
  }

  // Get table metadata by table id with structure.
  {
    // Generate tables metadata manager.
    auto managers = get_tables_ptr(GlobalTestEnvironment::TEST_DB);

    Table retrieved_metadata;
    ApiTestHelper::test_get(managers.get(), object_id, ErrorCode::OK,
                            retrieved_metadata);
  }

  // Get table metadata by table name with structure.
  {
    // Generate tables metadata manager.
    auto managers = get_tables_ptr(GlobalTestEnvironment::TEST_DB);

    Table retrieved_metadata;
    ApiTestHelper::test_get(managers.get(), object_name, ErrorCode::OK,
                            retrieved_metadata);
  }

  // Get all table metadata with ptree.
  {
    // Generate tables metadata manager.
    auto managers = get_tables_ptr(GlobalTestEnvironment::TEST_DB);

    std::vector<ptree> container = {};
    // Get all table metadata.
    ApiTestHelper::test_getall(managers.get(), ErrorCode::OK, container);
  }

  // Update table metadata.
  {
    // Generate tables metadata manager.
    auto managers = get_tables_ptr(GlobalTestEnvironment::TEST_DB);

    // Execute the test.
    ApiTestHelper::test_update(managers.get(), object_id, inserted_metadata,
                               ErrorCode::OK);
  }

  // Remove table metadata by table id.
  {
    // Generate tables metadata manager.
    auto managers = get_tables_ptr(GlobalTestEnvironment::TEST_DB);

    // Remove table metadata by table id.
    ApiTestHelper::test_remove(managers.get(), object_id, ErrorCode::OK);
  }

  // Add table metadata.
  {
    // Generate tables metadata manager.
    auto managers = get_tables_ptr(GlobalTestEnvironment::TEST_DB);

    object_id = ApiTestHelper::test_add(managers.get(), inserted_metadata,
                                        ErrorCode::OK);
  }

  // Remove table metadata by table name.
  {
    // Generate tables metadata manager.
    auto managers = get_tables_ptr(GlobalTestEnvironment::TEST_DB);

    // Remove table metadata by table id.
    ApiTestHelper::test_remove(managers.get(), object_name, ErrorCode::OK);
  }
}

/**
 * @brief Add, get, remove valid table metadata based on table name.
 */
TEST_F(ApiTestTableMetadataEx, add_get_remove_table_metadata_by_table_name) {
  int32_t count = 0;

  // variable "table_metadata" is test data set.
  for (auto& table_metadata : valid_table_metadata) {
    auto table_expected = table_metadata.get_metadata_ptree();

    UTUtils::print(">> Test Pattern: ", ++count);

    ObjectIdType ret_table_id = -1;
    // Add valid table metadata.
    TableMetadataHelper::add_table(table_expected, &ret_table_id);

    // Generate tables metadata manager.
    auto tables = get_tables_ptr(GlobalTestEnvironment::TEST_DB);

    // Test initialization.
    ApiTestHelper::test_init(tables.get(), ErrorCode::OK);

    ptree table_metadata_inserted;
    std::string table_name = table_expected.get<std::string>(Table::NAME);

    // Get table metadata by table name with ptree.
    ApiTestHelper::test_get(tables.get(), table_name, ErrorCode::OK,
                            table_metadata_inserted);

    // Verifies that the returned table metadata is expected one.
    table_expected.put(Table::ID, ret_table_id);
    table_metadata.CHECK_METADATA_EXPECTED(table_expected,
                                           table_metadata_inserted);

    // Remove table metadata by table name.
    ApiTestHelper::test_remove(tables.get(), table_name, ErrorCode::OK);

    // Verifies that table metadata does not exist.
    ptree table_metadata_got;
    ApiTestHelper::test_get(tables.get(), table_name, ErrorCode::NAME_NOT_FOUND,
                            table_metadata_got);
  }
}

/**
 * @brief Add, get, update, remove valid table metadata based on table id.
 */
TEST_F(ApiTestTableMetadataEx,
       add_get_update_remove_table_metadata_by_table_id) {
  int32_t count = 0;

  // variable "table_metadata" is test data set.
  for (auto& table_metadata : valid_table_metadata) {
    auto table_expected = table_metadata.get_metadata_ptree();

    UTUtils::print(">> Test Pattern: ", ++count);

    ObjectIdType ret_table_id = -1;
    // Add valid table metadata.
    TableMetadataHelper::add_table(table_expected, &ret_table_id);

    // get valid table metadata by table id.
    auto tables = get_tables_ptr(GlobalTestEnvironment::TEST_DB);

    // Test initialization.
    ApiTestHelper::test_init(tables.get(), ErrorCode::OK);

    ptree table_metadata_inserted;
    // Get table metadata by table id with ptree.
    ApiTestHelper::test_get(tables.get(), ret_table_id, ErrorCode::OK,
                            table_metadata_inserted);

    // Verifies that the returned table metadata is expected one.
    table_expected.put(Table::ID, ret_table_id);
    table_metadata.CHECK_METADATA_EXPECTED(table_expected,
                                           table_metadata_inserted);

    // Update valid table metadata.
    table_expected = table_metadata_inserted;
    std::string table_name =
        table_metadata_inserted.get_optional<std::string>(Table::NAME).value() +
        "-update";
    table_expected.put(Table::NAME, table_name);

    // Update table metadata by table id with ptree.
    ApiTestHelper::test_update(tables.get(), ret_table_id, table_expected,
                               ErrorCode::OK);

    ptree table_metadata_updated;
    // Get table metadata by table id with ptree.
    ApiTestHelper::test_get(tables.get(), ret_table_id, ErrorCode::OK,
                            table_metadata_updated);

    // verifies that the returned table metadata is expected one.
    table_metadata.CHECK_METADATA_EXPECTED(table_expected,
                                           table_metadata_updated);

    // Remove table metadata by table name.
    ApiTestHelper::test_remove(tables.get(), ret_table_id, ErrorCode::OK);

    // Verifies that table metadata does not exist.
    ptree table_metadata_got;
    ApiTestHelper::test_get(tables.get(), ret_table_id, ErrorCode::ID_NOT_FOUND,
                            table_metadata_got);
  }
}

}  // namespace manager::metadata::testing
