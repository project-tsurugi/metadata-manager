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
#include "test/test/api_test.h"

#include <gtest/gtest.h>

#include "manager/metadata/metadata_factory.h"
#include "test/common/global_test_environment.h"
#include "test/common/ut_utils.h"
#include "test/helper/index_metadata_helper.h"
#include "test/helper/table_metadata_helper.h"
#include "test/metadata/ut_index_metadata.h"

namespace {

manager::metadata::ObjectId table_id = 0;

}  // namespace

namespace manager::metadata::testing {

using boost::property_tree::ptree;

class ApiTestIndexMetadata : public ApiTest {
 public:
  ApiTestIndexMetadata()
      : ApiTest(get_index_metadata(GlobalTestEnvironment::TEST_DB)),
        metadata_struct_(std::make_unique<::manager::metadata::Index>()) {}

  int64_t get_record_count() const override {
    return IndexMetadataHelper::get_record_count();
  }

  ::manager::metadata::Index* get_structure() const override {
    return metadata_struct_.get();
  }

  void SetUp() override {
    UTUtils::skip_if_connection_not_opened();

    UTUtils::print(">> gtest::SetUp()");

    // Change to a unique table name.
    std::string table_name =
        "ApiTestIndexMetadata_" + UTUtils::generate_narrow_uid();

    // Add table metadata.
    TableMetadataHelper::add_table(table_name, &table_id);
  }

  void TearDown() override {
    if (global->is_open()) {
      UTUtils::print(">> gtest::TearDown()");

      // Remove table metadata.
      TableMetadataHelper::remove_table(table_id);
    }
  }

 private:
  std::unique_ptr<::manager::metadata::Index> metadata_struct_;
};

/**
 * @brief Test to add new metadata and get it in ptree type
 *   with object ID as key.
 */
TEST_F(ApiTestIndexMetadata, test_get_by_id_with_ptree) {
  SCOPED_TRACE("");

  // Generate test metadata.
  UTIndexMetadata ut_metadata(table_id);
  ut_metadata.generate_test_metadata();

  // Execute the test.
  this->test_flow_get_by_id(&ut_metadata);
}

/**
 * @brief Test to add new metadata and get it in ptree type
 *   with object ID as key.
 */
TEST_F(ApiTestIndexMetadata, test_get_by_id_with_struct) {
  SCOPED_TRACE("");

  // Generate test metadata.
  UTIndexMetadata ut_metadata(table_id);
  ut_metadata.generate_test_metadata();

  // Execute the test.
  this->test_flow_get_by_id_with_struct(&ut_metadata);
}

/**
 * @brief Test to add new metadata and get it in ptree type
 *   with object name as key.
 */
TEST_F(ApiTestIndexMetadata, test_get_by_name_with_ptree) {
  SCOPED_TRACE("");

  // Generate test metadata.
  UTIndexMetadata ut_metadata(table_id);
  ut_metadata.generate_test_metadata();

  // Execute the test.
  this->test_flow_get_by_name(&ut_metadata);
}

/**
 * @brief Test to add new metadata and get it in ptree type
 *   with object name as key.
 */
TEST_F(ApiTestIndexMetadata, test_get_by_name_with_struct) {
  SCOPED_TRACE("");

  // Generate test metadata.
  UTIndexMetadata ut_metadata(table_id);
  ut_metadata.generate_test_metadata();

  // Execute the test.
  this->test_flow_get_by_name_with_struct(&ut_metadata);
}

/**
 * @brief Test to add new metadata and get_all it in ptree type.
 */
TEST_F(ApiTestIndexMetadata, test_getall_with_ptree) {
  SCOPED_TRACE("");

  // Generate test metadata.
  UTIndexMetadata ut_metadata(table_id);
  ut_metadata.generate_test_metadata();

  // Execute the test.
  this->test_flow_getall(&ut_metadata);
}

/**
 * @brief Test to add new metadata and update it in ptree type
 *   with object ID as key.
 */
TEST_F(ApiTestIndexMetadata, test_update) {
  SCOPED_TRACE("");

  // Generate test metadata.
  UTIndexMetadata ut_metadata(table_id);
  ut_metadata.generate_test_metadata();

  auto metadata_base = ut_metadata.get_metadata_struct();
  // Copy
  manager::metadata::Index metadata_update = *metadata_base;

  // name
  metadata_update.name += "-update";
  // namespace
  metadata_update.namespace_name += "-update";
  // access_method
  metadata_update.access_method =
      static_cast<int64_t>(Index::AccessMethod::MASS_TREE_METHOD);
  // is_primary
  metadata_update.is_primary = true;
  // columns
  metadata_update.keys = {11, 12};
  // columns id.
  metadata_update.keys_id = {2011, 2012};

  // Generate update test metadata.
  UTIndexMetadata ut_metadata_update(metadata_update);

  // Execute the test.
  this->test_flow_update(&ut_metadata, &ut_metadata_update);
}

/**
 * @brief Test to add new metadata and update it in ptree type
 *   with object ID as key.
 */
TEST_F(ApiTestIndexMetadata, test_name_duplicate) {
  SCOPED_TRACE("");

  // Generate indexes metadata manager.
  auto managers = get_index_metadata(GlobalTestEnvironment::TEST_DB);

  // Generate test metadata.
  UTIndexMetadata ut_metadata(table_id);
  ut_metadata.generate_test_metadata();
  ptree inserted_metadata = ut_metadata.get_metadata_ptree();

  // Add first index metadata.
  ObjectId inserted_id = this->test_add(managers.get(), inserted_metadata);

  // Add second index metadata.
  this->test_add(managers.get(), inserted_metadata, ErrorCode::ALREADY_EXISTS);

  // Remove index metadata.
  this->test_remove(managers.get(), inserted_id);
}

/**
 * @brief Test for incorrect index IDs.
 */
TEST_F(ApiTestIndexMetadata, test_not_found) {
  SCOPED_TRACE("");

  // Generate indexes metadata manager.
  auto managers = get_index_metadata(GlobalTestEnvironment::TEST_DB);

  // Test to initialize the manager.
  this->test_init(managers.get());

  // Generate test metadata.
  UTIndexMetadata ut_metadata(table_id);
  ut_metadata.generate_test_metadata();

  ObjectId object_id      = INT64_MAX;
  std::string object_name = "unregistered_dummy_name";

  // Get index metadata by index id/name with ptree.
  {
    ptree retrieved_metadata;

    // Test of get by ID with ptree.
    this->test_get(managers.get(), object_id, retrieved_metadata,
                   ErrorCode::ID_NOT_FOUND);
    EXPECT_TRUE(retrieved_metadata.empty());

    // Test of get by name with ptree.
    this->test_get(managers.get(), object_name, retrieved_metadata,
                   ErrorCode::NAME_NOT_FOUND);
    EXPECT_TRUE(retrieved_metadata.empty());
  }

  // Get index metadata by index id/name with structure.
  {
    Index retrieved_metadata_struct;
    // Test of get by ID with structure.
    this->test_get(managers.get(), object_id, retrieved_metadata_struct,
                   ErrorCode::ID_NOT_FOUND);
    // Test of get by name with structure.
    this->test_get(managers.get(), object_name, retrieved_metadata_struct,
                   ErrorCode::NAME_NOT_FOUND);
  }

  // Remove index metadata by index id/name.
  {
    // Test of remove by ID.
    this->test_remove(managers.get(), object_id, ErrorCode::ID_NOT_FOUND);
    // Test of remove by name.
    this->test_remove(managers.get(), object_name, ErrorCode::NAME_NOT_FOUND);
  }
}

/**
 * @brief Test for incorrect index IDs.
 */
TEST_F(ApiTestIndexMetadata, test_invalid_parameter) {
  SCOPED_TRACE("");

  // Generate indexes metadata manager.
  auto managers = get_index_metadata(GlobalTestEnvironment::TEST_DB);

  // Test to initialize the manager.
  this->test_init(managers.get());

  // Generate test metadata.
  UTIndexMetadata ut_metadata(table_id);
  ut_metadata.generate_test_metadata();

  ObjectId invalid_id      = INVALID_OBJECT_ID;
  std::string invalid_name = "";

#if 0
  // Add index metadata by index id.
  {
    ptree index_metadata;
    this->test_add(managers.get(), index_metadata,
                   ErrorCode::INVALID_PARAMETER);

    index_metadata.put(Index::TABLE_ID, invalid_id);
    this->test_add(managers.get(), index_metadata,
                   ErrorCode::INVALID_PARAMETER);
  }
#endif

  // Get index metadata by index id/name with ptree.
  {
    ptree retrieved_metadata;

    // Test of get by ID with ptree.
    this->test_get(managers.get(), invalid_id, retrieved_metadata,
                   ErrorCode::INVALID_PARAMETER);
    EXPECT_TRUE(retrieved_metadata.empty());

    // Test of get by name with ptree.
    this->test_get(managers.get(), invalid_name, retrieved_metadata,
                   ErrorCode::INVALID_PARAMETER);
    EXPECT_TRUE(retrieved_metadata.empty());
  }

  // Get index metadata by index id/name with structure.
  {
    Index retrieved_metadata_struct;
    // Test of get by ID with structure.
    this->test_get(managers.get(), invalid_id, retrieved_metadata_struct,
                   ErrorCode::INVALID_PARAMETER);
    // Test of get by name with structure.
    this->test_get(managers.get(), invalid_name, retrieved_metadata_struct,
                   ErrorCode::INVALID_PARAMETER);
  }

  // Remove index metadata by index id/name.
  {
    // Test of remove by ID.
    this->test_remove(managers.get(), invalid_id, ErrorCode::INVALID_PARAMETER);
    // Test of remove by name.
    this->test_remove(managers.get(), invalid_name, ErrorCode::INVALID_PARAMETER);
  }
}

/**
 * @brief happy test for adding, getting and removing
 *   one new table metadata without initialization of all api.
 */
TEST_F(ApiTestIndexMetadata, test_without_initialized) {
  SCOPED_TRACE("");

  // Generate test metadata.
  UTIndexMetadata ut_metadata(table_id);
  ut_metadata.generate_test_metadata();

  auto inserted_metadata  = ut_metadata.get_metadata_ptree();
  std::string object_name = ut_metadata.get_metadata_struct()->name;
  ObjectId object_id      = -1;

  // Add index metadata.
  {
    // Generate indexes metadata manager.
    auto managers = get_index_metadata(GlobalTestEnvironment::TEST_DB);

    object_id = this->test_add(managers_.get(), inserted_metadata);
  }

  // Get index metadata by index id with ptree.
  {
    // Generate indexes metadata manager.
    auto managers = get_index_metadata(GlobalTestEnvironment::TEST_DB);

    ptree retrieved_metadata;
    this->test_get(managers_.get(), object_id, retrieved_metadata);
  }

  // Get index metadata by index name with ptree.
  {
    // Generate indexes metadata manager.
    auto managers = get_index_metadata(GlobalTestEnvironment::TEST_DB);

    ptree retrieved_metadata;
    this->test_get(managers_.get(), object_name, retrieved_metadata);
  }

  // Get index metadata by index id with structure.
  {
    // Generate indexes metadata manager.
    auto managers = get_index_metadata(GlobalTestEnvironment::TEST_DB);

    Constraint retrieved_metadata;
    this->test_get(managers_.get(), object_id, retrieved_metadata);
  }

  // Get index metadata by index name with structure.
  {
    // Generate indexes metadata manager.
    auto managers = get_index_metadata(GlobalTestEnvironment::TEST_DB);

    Constraint retrieved_metadata;
    this->test_get(managers_.get(), object_name, retrieved_metadata);
  }

  // Get all index metadata with ptree.
  {
    // Generate indexes metadata manager.
    auto managers = get_index_metadata(GlobalTestEnvironment::TEST_DB);

    std::vector<ptree> container = {};
    // Get all index metadata.
    container = this->test_getall(managers_.get());
  }

  // Update index metadata.
  {
    // Generate indexes metadata manager.
    auto managers = get_index_metadata(GlobalTestEnvironment::TEST_DB);

    // Execute the test.
    this->test_update(managers_.get(), object_id, inserted_metadata);
  }

  // Remove index metadata by index id.
  {
    // Generate indexes metadata manager.
    auto managers = get_index_metadata(GlobalTestEnvironment::TEST_DB);

    // Remove index metadata by index id.
    this->test_remove(managers_.get(), object_id);
  }

  // Add index metadata.
  {
    // Generate indexes metadata manager.
    auto managers = get_index_metadata(GlobalTestEnvironment::TEST_DB);

    object_id = this->test_add(managers_.get(), inserted_metadata);
  }

  // Remove index metadata by index name.
  {
    // Generate indexes metadata manager.
    auto managers = get_index_metadata(GlobalTestEnvironment::TEST_DB);

    // Remove index metadata by index id.
    this->test_remove(managers_.get(), object_name);
  }
}

}  // namespace manager::metadata::testing
