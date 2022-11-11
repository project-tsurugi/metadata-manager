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
#include "test/helper/index_metadata_helper.h"
#include "test/helper/table_metadata_helper.h"
#include "test/metadata/ut_index_metadata.h"
#include "test/test/api_test_facade.h"

namespace manager::metadata::testing {

using boost::property_tree::ptree;

class ApiTestIndexMetadata
    : public ApiTestFacade<::manager::metadata::Index, IndexMetadataHelper> {
 public:
  manager::metadata::ObjectId table_id_;

  ApiTestIndexMetadata()
      : ApiTestFacade(get_index_metadata(GlobalTestEnvironment::TEST_DB)),
        table_id_(0) {}

  void SetUp() override {
    UTUtils::skip_if_connection_not_opened();

    UTUtils::print(">> gtest::SetUp()");

    // Change to a unique table name.
    std::string table_name =
        "ApiTestIndexMetadata_" + UTUtils::generate_narrow_uid();

    // Add table metadata.
    TableMetadataHelper::add_table(table_name, &table_id_);
  }

  void TearDown() override {
    if (global->is_open()) {
      UTUtils::print(">> gtest::TearDown()");

      // Remove table metadata.
      TableMetadataHelper::remove_table(table_id_);
    }
  }

  static std::unique_ptr<UtMetadataInterface> generate_update_metadata(
      const boost::property_tree::ptree& metadata) {
    // Base metadata.
    auto metadata_base = *(UtIndexMetadata(metadata).get_metadata_struct());

    // Copy
    manager::metadata::Index metadata_update;
    metadata_update.convert_from_ptree(metadata);

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

    return std::make_unique<UtIndexMetadata>(metadata_update);
  }
};

/**
 * @brief Test to add metadata with ptree type and
 *   get it with object ID as key.
 */
TEST_F(ApiTestIndexMetadata, test_get_by_id_with_ptree) {
  CALL_TRACE;

  // Execute the test.
  this->test_flow_get_by_id(UtIndexMetadata(table_id_));
}

/**
 * @brief Test to add metadata with structure type and
 *   get it with object ID as key.
 */
TEST_F(ApiTestIndexMetadata, test_get_by_id_with_struct) {
  CALL_TRACE;

  // Execute the test.
  this->test_flow_get_by_id_with_struct(UtIndexMetadata(table_id_));
}

/**
 * @brief Test to add metadata with ptree type and
 *   get it with object name as key.
 */
TEST_F(ApiTestIndexMetadata, test_get_by_name_with_ptree) {
  CALL_TRACE;

  // Execute the test.
  this->test_flow_get_by_name(UtIndexMetadata(table_id_));
}

/**
 * @brief Test to add metadata with structure type and
 *   get it with object name as key.
 */
TEST_F(ApiTestIndexMetadata, test_get_by_name_with_struct) {
  CALL_TRACE;

  // Execute the test.
  this->test_flow_get_by_name_with_struct(UtIndexMetadata(table_id_));
}

/**
 * @brief Test to add new metadata and get_all it in ptree type.
 */
TEST_F(ApiTestIndexMetadata, test_getall_with_ptree) {
  CALL_TRACE;

  // Execute the test.
  this->test_flow_getall(UtIndexMetadata(table_id_));
}

/**
 * @brief Test to add new metadata and update.
 */
TEST_F(ApiTestIndexMetadata, test_update) {
  CALL_TRACE;

  // Generate test metadata.
  UtIndexMetadata ut_metadata(table_id_);

  // Execute the test.
  this->test_flow_update(ut_metadata, generate_update_metadata);
}

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
  this->test_init(managers.get(), ErrorCode::OK);

  // Add first index metadata.
  ObjectId object_id_1st =
      this->test_add(managers.get(), inserted_metadata, ErrorCode::OK);
  EXPECT_GT(object_id_1st, INVALID_OBJECT_ID);

  // Add second index metadata.
  ObjectId object_id_2nd = this->test_add(managers.get(), inserted_metadata,
                                          ErrorCode::ALREADY_EXISTS);
  EXPECT_EQ(object_id_2nd, INVALID_OBJECT_ID);

  // Remove index metadata.
  this->test_remove(managers.get(), object_id_1st, ErrorCode::OK);
}

/**
 * @brief Test for incorrect index IDs.
 */
TEST_F(ApiTestIndexMetadata, test_not_found) {
  CALL_TRACE;

  // Generate indexes metadata manager.
  auto managers = get_index_metadata(GlobalTestEnvironment::TEST_DB);

  // Test to initialize the manager.
  this->test_init(managers.get(), ErrorCode::OK);

  // Generate test metadata.
  UtIndexMetadata ut_metadata(table_id_);

  ObjectId object_id      = INT64_MAX;
  std::string object_name = "unregistered_dummy_name";

  // Get index metadata by index id/name with ptree.
  {
    ptree retrieved_metadata;

    // Test of get by ID with ptree.
    this->test_get(managers.get(), object_id, ErrorCode::ID_NOT_FOUND,
                   retrieved_metadata);
    EXPECT_TRUE(retrieved_metadata.empty());

    // Test of get by name with ptree.
    this->test_get(managers.get(), object_name, ErrorCode::NAME_NOT_FOUND,
                   retrieved_metadata);
    EXPECT_TRUE(retrieved_metadata.empty());
  }

  // Get index metadata by index id/name with structure.
  {
    Index retrieved_metadata_struct;
    // Test of get by ID with structure.
    this->test_get(managers.get(), object_id, ErrorCode::ID_NOT_FOUND,
                   retrieved_metadata_struct);
    // Test of get by name with structure.
    this->test_get(managers.get(), object_name, ErrorCode::NAME_NOT_FOUND,
                   retrieved_metadata_struct);
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
  CALL_TRACE;

  // Generate indexes metadata manager.
  auto managers = get_index_metadata(GlobalTestEnvironment::TEST_DB);

  // Test to initialize the manager.
  this->test_init(managers.get(), ErrorCode::OK);

  // Generate test metadata.
  UtIndexMetadata ut_metadata(table_id_);

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
    this->test_get(managers.get(), invalid_id, ErrorCode::INVALID_PARAMETER,
                   retrieved_metadata);
    EXPECT_TRUE(retrieved_metadata.empty());

    // Test of get by name with ptree.
    this->test_get(managers.get(), invalid_name, ErrorCode::INVALID_PARAMETER,
                   retrieved_metadata);
    EXPECT_TRUE(retrieved_metadata.empty());
  }

  // Get index metadata by index id/name with structure.
  {
    Index retrieved_metadata_struct;
    // Test of get by ID with structure.
    this->test_get(managers.get(), invalid_id, ErrorCode::INVALID_PARAMETER,
                   retrieved_metadata_struct);
    // Test of get by name with structure.
    this->test_get(managers.get(), invalid_name, ErrorCode::INVALID_PARAMETER,
                   retrieved_metadata_struct);
  }

  // Remove index metadata by index id/name.
  {
    // Test of remove by ID.
    this->test_remove(managers.get(), invalid_id, ErrorCode::INVALID_PARAMETER);
    // Test of remove by name.
    this->test_remove(managers.get(), invalid_name,
                      ErrorCode::INVALID_PARAMETER);
  }
}

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

    object_id =
        this->test_add(managers.get(), inserted_metadata, ErrorCode::OK);
  }

  // Get index metadata by index id with ptree.
  {
    // Generate indexes metadata manager.
    auto managers = get_index_metadata(GlobalTestEnvironment::TEST_DB);

    ptree retrieved_metadata;
    this->test_get(managers.get(), object_id, ErrorCode::OK,
                   retrieved_metadata);
  }

  // Get index metadata by index name with ptree.
  {
    // Generate indexes metadata manager.
    auto managers = get_index_metadata(GlobalTestEnvironment::TEST_DB);

    ptree retrieved_metadata;
    this->test_get(managers.get(), object_name, ErrorCode::OK,
                   retrieved_metadata);
  }

  // Get index metadata by index id with structure.
  {
    // Generate indexes metadata manager.
    auto managers = get_index_metadata(GlobalTestEnvironment::TEST_DB);

    Index retrieved_metadata;
    this->test_get(managers.get(), object_id, ErrorCode::OK,
                   retrieved_metadata);
  }

  // Get index metadata by index name with structure.
  {
    // Generate indexes metadata manager.
    auto managers = get_index_metadata(GlobalTestEnvironment::TEST_DB);

    Index retrieved_metadata;
    this->test_get(managers.get(), object_name, ErrorCode::OK,
                   retrieved_metadata);
  }

  // Get all index metadata with ptree.
  {
    // Generate indexes metadata manager.
    auto managers = get_index_metadata(GlobalTestEnvironment::TEST_DB);

    std::vector<ptree> container = {};
    // Get all index metadata.
    this->test_getall(managers.get(), ErrorCode::OK, container);
  }

  // Update index metadata.
  {
    // Generate indexes metadata manager.
    auto managers = get_index_metadata(GlobalTestEnvironment::TEST_DB);

    // Execute the test.
    this->test_update(managers.get(), object_id, inserted_metadata,
                      ErrorCode::OK);
  }

  // Remove index metadata by index id.
  {
    // Generate indexes metadata manager.
    auto managers = get_index_metadata(GlobalTestEnvironment::TEST_DB);

    // Remove index metadata by index id.
    this->test_remove(managers.get(), object_id, ErrorCode::OK);
  }

  // Add index metadata.
  {
    // Generate indexes metadata manager.
    auto managers = get_index_metadata(GlobalTestEnvironment::TEST_DB);

    object_id =
        this->test_add(managers.get(), inserted_metadata, ErrorCode::OK);
  }

  // Remove index metadata by index name.
  {
    // Generate indexes metadata manager.
    auto managers = get_index_metadata(GlobalTestEnvironment::TEST_DB);

    // Remove index metadata by index id.
    this->test_remove(managers.get(), object_name, ErrorCode::OK);
  }
}

}  // namespace manager::metadata::testing
