/*
 * Copyright 2022 tsurugi project.
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
#include "test/helper/index_metadata_helper.h"
#include "test/helper/table_metadata_helper.h"
#include "test/metadata/ut_index_metadata.h"

namespace manager::metadata::testing {

using boost::property_tree::ptree;

class ApiTestIndexMetadata : public ::testing::Test {
 public:
  manager::metadata::ObjectId table_id_;

  ApiTestIndexMetadata() : table_id_(0) {}

  void SetUp() override {
    UTUtils::skip_if_connection_not_opened();

    if (global->is_open()) {
      UTUtils::print(">> gtest::SetUp()");

      // Change to a unique table name.
      std::string table_name =
          "ApiTestIndexMetadata_" + UTUtils::generate_narrow_uid();

      // Add table metadata.
      TableMetadataHelper::add_table(table_name, &table_id_);
    }
  }

  void TearDown() override {
    if (global->is_open()) {
      UTUtils::print(">> gtest::TearDown()");

      // Remove table metadata.
      TableMetadataHelper::remove_table(table_id_);
    }
  }
};

/**
 * @brief This is a test for duplicate table names.
 */
TEST_F(ApiTestIndexMetadata, test_duplicate_index_name) {
  CALL_TRACE;

  // Generate indexes metadata manager.
  auto managers = get_index_metadata(GlobalTestEnvironment::TEST_DB);

  // Generate test metadata.
  UtIndexMetadata ut_metadata(table_id_);

  ptree inserted_metadata = ut_metadata.get_metadata_ptree();

  // Test initialization.
  ApiTestHelper::test_init(managers.get(), ErrorCode::OK);

  // Add first index metadata.
  ObjectId object_id_1st =
      ApiTestHelper::test_add(managers.get(), inserted_metadata, ErrorCode::OK);
  EXPECT_GT(object_id_1st, INVALID_OBJECT_ID);

  // Add second index metadata.
  ObjectId object_id_2nd = ApiTestHelper::test_add(
      managers.get(), inserted_metadata, ErrorCode::ALREADY_EXISTS);
  EXPECT_EQ(object_id_2nd, INVALID_OBJECT_ID);

  // Remove index metadata.
  ApiTestHelper::test_remove(managers.get(), object_id_1st, ErrorCode::OK);
}

#if 0
/**
 * @brief Test for incorrect index IDs.
 */
TEST_F(ApiTestIndexMetadata, test_invalid_parameter) {
  CALL_TRACE;

  // Generate indexes metadata manager.
  auto managers = get_index_metadata(GlobalTestEnvironment::TEST_DB);

  // Test to initialize the manager.
  ApiTestHelper::test_init(managers.get(), ErrorCode::OK);

  // Generate test metadata.
  UtIndexMetadata ut_metadata(table_id_);

  ObjectId invalid_id      = INVALID_OBJECT_ID;
  std::string invalid_name = "";

  // Add index metadata by index id.
  {
    ptree index_metadata;
    ApiTestHelper::test_add(managers.get(), index_metadata,
                   ErrorCode::INVALID_PARAMETER);

    index_metadata.put(Index::TABLE_ID, invalid_id);
    ApiTestHelper::test_add(managers.get(), index_metadata,
                   ErrorCode::INVALID_PARAMETER);
  }
}
#endif

/**
 * @brief This test executes all APIs without initialization.
 */
TEST_F(ApiTestIndexMetadata, test_without_initialized) {
  CALL_TRACE;

  // Generate test metadata.
  UtIndexMetadata ut_metadata(table_id_);

  auto inserted_metadata  = ut_metadata.get_metadata_ptree();
  std::string object_name = ut_metadata.get_metadata_struct()->name;
  ObjectId object_id      = -1;

  // Add index metadata.
  {
    // Generate indexes metadata manager.
    auto managers = get_index_metadata(GlobalTestEnvironment::TEST_DB);

    object_id = ApiTestHelper::test_add(managers.get(), inserted_metadata,
                                        ErrorCode::OK);
  }

  // Get index metadata by index id with ptree.
  {
    // Generate indexes metadata manager.
    auto managers = get_index_metadata(GlobalTestEnvironment::TEST_DB);

    ptree retrieved_metadata;
    ApiTestHelper::test_get(managers.get(), object_id, ErrorCode::OK,
                            retrieved_metadata);
  }

  // Get index metadata by index name with ptree.
  {
    // Generate indexes metadata manager.
    auto managers = get_index_metadata(GlobalTestEnvironment::TEST_DB);

    ptree retrieved_metadata;
    ApiTestHelper::test_get(managers.get(), object_name, ErrorCode::OK,
                            retrieved_metadata);
  }

  // Get index metadata by index id with structure.
  {
    // Generate indexes metadata manager.
    auto managers = get_index_metadata(GlobalTestEnvironment::TEST_DB);

    Index retrieved_metadata;
    ApiTestHelper::test_get(managers.get(), object_id, ErrorCode::OK,
                            retrieved_metadata);
  }

  // Get index metadata by index name with structure.
  {
    // Generate indexes metadata manager.
    auto managers = get_index_metadata(GlobalTestEnvironment::TEST_DB);

    Index retrieved_metadata;
    ApiTestHelper::test_get(managers.get(), object_name, ErrorCode::OK,
                            retrieved_metadata);
  }

  // Get all index metadata with ptree.
  {
    // Generate indexes metadata manager.
    auto managers = get_index_metadata(GlobalTestEnvironment::TEST_DB);

    std::vector<ptree> container = {};
    // Get all index metadata.
    ApiTestHelper::test_getall(managers.get(), ErrorCode::OK, container);
  }

  // Update index metadata.
  {
    // Generate indexes metadata manager.
    auto managers = get_index_metadata(GlobalTestEnvironment::TEST_DB);

    // Execute the test.
    ApiTestHelper::test_update(managers.get(), object_id, inserted_metadata,
                               ErrorCode::OK);
  }

  // Remove index metadata by index id.
  {
    // Generate indexes metadata manager.
    auto managers = get_index_metadata(GlobalTestEnvironment::TEST_DB);

    // Remove index metadata by index id.
    ApiTestHelper::test_remove(managers.get(), object_id, ErrorCode::OK);
  }

  // Add index metadata.
  {
    // Generate indexes metadata manager.
    auto managers = get_index_metadata(GlobalTestEnvironment::TEST_DB);

    object_id = ApiTestHelper::test_add(managers.get(), inserted_metadata,
                                        ErrorCode::OK);
  }

  // Remove index metadata by index name.
  {
    // Generate indexes metadata manager.
    auto managers = get_index_metadata(GlobalTestEnvironment::TEST_DB);

    // Remove index metadata by index id.
    ApiTestHelper::test_remove(managers.get(), object_name, ErrorCode::OK);
  }
}

}  // namespace manager::metadata::testing
