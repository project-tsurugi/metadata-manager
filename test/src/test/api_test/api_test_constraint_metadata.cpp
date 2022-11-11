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
#include "test/helper/constraint_metadata_helper.h"
#include "test/helper/table_metadata_helper.h"
#include "test/metadata/ut_constraint_metadata.h"
#include "test/test/api_test_facade.h"

namespace manager::metadata::testing {

using boost::property_tree::ptree;

class ApiTestConstraintMetadata
    : public ApiTestFacade<::manager::metadata::Constraint,
                           ConstraintMetadataHelper> {
 public:
  manager::metadata::ObjectId table_id_;

  ApiTestConstraintMetadata()
      : ApiTestFacade(get_constraint_metadata(GlobalTestEnvironment::TEST_DB)),
        table_id_(0) {}

  void SetUp() override {
    UTUtils::skip_if_connection_not_opened();

    UTUtils::print(">> gtest::SetUp()");

    // Change to a unique table name.
    std::string table_name =
        "ApiTestConstraintMetadata_" + UTUtils::generate_narrow_uid();

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
};

/**
 * @brief Test to add metadata with ptree type and
 *   get it with object ID as key.
 */
TEST_F(ApiTestConstraintMetadata, test_get_by_id_with_ptree) {
  CALL_TRACE;

  // Execute the test.
  this->test_flow_get_by_id(UtConstraintMetadata(table_id_));
}

/**
 * @brief Test to add metadata with structure type and
 *   get it with object ID as key.
 */
TEST_F(ApiTestConstraintMetadata, test_get_by_id_with_struct) {
  CALL_TRACE;

  // Execute the test.
  this->test_flow_get_by_id_with_struct(UtConstraintMetadata(table_id_));
}

/**
 * @brief Test to add metadata with ptree type and
 *   get it with object name as key.
 */
TEST_F(ApiTestConstraintMetadata, test_get_by_name_with_ptree) {
  CALL_TRACE;

  // Generate test metadata.
  auto managers = get_constraint_metadata(GlobalTestEnvironment::TEST_DB);

  auto object_name = "dummy_name";
  ptree retrieved_metadata;

  // Execute the test.
  this->test_get(managers.get(), object_name, ErrorCode::UNKNOWN,
                 retrieved_metadata);
  this->test_remove(managers.get(), object_name, ErrorCode::UNKNOWN);
}

/**
 * @brief Test to add metadata with structure type and
 *   get it with object name as key.
 */
TEST_F(ApiTestConstraintMetadata, test_get_by_name_with_struct) {
  CALL_TRACE;

  // Generate test metadata.
  auto managers = get_constraint_metadata(GlobalTestEnvironment::TEST_DB);

  auto object_name = "dummy_name";
  Constraint retrieved_metadata;

  // Execute the test.
  this->test_get(managers.get(), object_name, ErrorCode::UNKNOWN,
                 retrieved_metadata);
  this->test_remove(managers.get(), object_name, ErrorCode::UNKNOWN);
}

/**
 * @brief Test to add new metadata and get_all it in ptree type.
 */
TEST_F(ApiTestConstraintMetadata, test_getall_with_ptree) {
  CALL_TRACE;

  // Execute the test.
  this->test_flow_getall(UtConstraintMetadata(table_id_));
}

/**
 * @brief Test to add new metadata and update it in ptree type
 *   with object ID as key.
 */
TEST_F(ApiTestConstraintMetadata, test_update) {
  CALL_TRACE;

  // Generate test metadata.
  auto managers = get_constraint_metadata(GlobalTestEnvironment::TEST_DB);

  // Generate test metadata.
  UtConstraintMetadata ut_metadata(table_id_);

  auto updated_metadata   = ut_metadata.get_metadata_ptree();
  std::string object_name = ut_metadata.get_metadata_struct()->name;
  ObjectId object_id      = INT64_MAX;

  // Execute the test.
  this->test_update(managers.get(), object_id, updated_metadata,
                    ErrorCode::UNKNOWN);
}

/**
 * @brief Test for incorrect constraint IDs.
 */
TEST_F(ApiTestConstraintMetadata, test_not_found) {
  CALL_TRACE;

  // Generate constraints metadata manager.
  auto managers = get_constraint_metadata(GlobalTestEnvironment::TEST_DB);

  // Test to initialize the manager.
  this->test_init(managers.get(), ErrorCode::OK);

  // Generate test metadata.
  UtConstraintMetadata ut_metadata(table_id_);

  ObjectId object_id      = INT64_MAX;
  std::string object_name = "unregistered_dummy_name";

  // Get constraint metadata by constraint id/name with ptree.
  {
    ptree retrieved_metadata;

    // Test of get by ID with ptree.
    this->test_get(managers.get(), object_id, ErrorCode::ID_NOT_FOUND,
                   retrieved_metadata);
    EXPECT_TRUE(retrieved_metadata.empty());

    // Test of get by name with ptree.
    this->test_get(managers.get(), object_name, ErrorCode::UNKNOWN,
                   retrieved_metadata);
    EXPECT_TRUE(retrieved_metadata.empty());
  }

  // Get constraint metadata by constraint id/name with structure.
  {
    Index retrieved_metadata_struct;
    // Test of get by ID with structure.
    this->test_get(managers.get(), object_id, ErrorCode::ID_NOT_FOUND,
                   retrieved_metadata_struct);
    // Test of get by name with structure.
    this->test_get(managers.get(), object_name, ErrorCode::UNKNOWN,
                   retrieved_metadata_struct);
  }

  // Remove constraint metadata by constraint id/name.
  {
    // Test of remove by ID.
    this->test_remove(managers.get(), object_id, ErrorCode::ID_NOT_FOUND);
    // Test of remove by name.
    this->test_remove(managers.get(), object_name, ErrorCode::UNKNOWN);
  }
}

/**
 * @brief Test for incorrect constraint IDs.
 */
TEST_F(ApiTestConstraintMetadata, test_invalid_parameter) {
  CALL_TRACE;

  // Generate constraints metadata manager.
  auto managers = get_constraint_metadata(GlobalTestEnvironment::TEST_DB);

  // Test to initialize the manager.
  this->test_init(managers.get(), ErrorCode::OK);

  // Generate test metadata.
  UtConstraintMetadata ut_metadata(table_id_);

  ObjectId invalid_id      = INVALID_OBJECT_ID;
  std::string invalid_name = "";

  // Add constraint metadata by constraint id.
  {
    ptree constraint_metadata;
    this->test_add(managers.get(), constraint_metadata,
                   ErrorCode::INVALID_PARAMETER);

    constraint_metadata.put(Constraint::TABLE_ID, invalid_id);
    this->test_add(managers.get(), constraint_metadata,
                   ErrorCode::INVALID_PARAMETER);
  }

  // Get constraint metadata by constraint id/name with ptree.
  {
    ptree retrieved_metadata;

    // Test of get by ID with ptree.
    this->test_get(managers.get(), invalid_id, ErrorCode::ID_NOT_FOUND,
                   retrieved_metadata);
    EXPECT_TRUE(retrieved_metadata.empty());

    // Test of get by name with ptree.
    this->test_get(managers.get(), invalid_name, ErrorCode::UNKNOWN,
                   retrieved_metadata);
    EXPECT_TRUE(retrieved_metadata.empty());
  }

  // Get constraint metadata by constraint id/name with structure.
  {
    Index retrieved_metadata_struct;
    // Test of get by ID with structure.
    this->test_get(managers.get(), invalid_id, ErrorCode::ID_NOT_FOUND,
                   retrieved_metadata_struct);
    // Test of get by name with structure.
    this->test_get(managers.get(), invalid_name, ErrorCode::UNKNOWN,
                   retrieved_metadata_struct);
  }

  // Remove constraint metadata by constraint id/name.
  {
    // Test of remove by ID.
    this->test_remove(managers.get(), invalid_id, ErrorCode::ID_NOT_FOUND);
    // Test of remove by name.
    this->test_remove(managers.get(), invalid_name, ErrorCode::UNKNOWN);
  }
}

/**
 * @brief This test executes all APIs without initialization.
 */
TEST_F(ApiTestConstraintMetadata, test_without_initialized) {
  CALL_TRACE;

  // Generate test metadata.
  UtConstraintMetadata ut_metadata(table_id_);

  auto inserted_metadata  = ut_metadata.get_metadata_ptree();
  std::string object_name = ut_metadata.get_metadata_struct()->name;
  ObjectId object_id      = -1;

  // Add constraint metadata.
  {
    // Generate constraints metadata manager.
    auto managers = get_constraint_metadata(GlobalTestEnvironment::TEST_DB);

    object_id =
        this->test_add(managers.get(), inserted_metadata, ErrorCode::OK);
  }

  // Get constraint metadata by constraint id with ptree.
  {
    // Generate constraints metadata manager.
    auto managers = get_constraint_metadata(GlobalTestEnvironment::TEST_DB);

    ptree retrieved_metadata;
    this->test_get(managers.get(), object_id, ErrorCode::OK,
                   retrieved_metadata);
  }

  // Get constraint metadata by constraint name with ptree.
  {
    // Generate constraints metadata manager.
    auto managers = get_constraint_metadata(GlobalTestEnvironment::TEST_DB);

    ptree retrieved_metadata;
    this->test_get(managers.get(), object_name, ErrorCode::UNKNOWN,
                   retrieved_metadata);
  }

  // Get constraint metadata by constraint id with structure.
  {
    // Generate constraints metadata manager.
    auto managers = get_constraint_metadata(GlobalTestEnvironment::TEST_DB);

    Constraint retrieved_metadata;
    this->test_get(managers.get(), object_id, ErrorCode::OK,
                   retrieved_metadata);
  }

  // Get constraint metadata by constraint name with structure.
  {
    // Generate constraints metadata manager.
    auto managers = get_constraint_metadata(GlobalTestEnvironment::TEST_DB);

    Constraint retrieved_metadata;
    this->test_get(managers.get(), object_name, ErrorCode::UNKNOWN,
                   retrieved_metadata);
  }

  // Get all constraint metadata with ptree.
  {
    // Generate constraints metadata manager.
    auto managers = get_constraint_metadata(GlobalTestEnvironment::TEST_DB);

    std::vector<ptree> container = {};
    // Get all constraints metadata.
    this->test_getall(managers.get(), ErrorCode::OK, container);
  }

  // Update constraint metadata.
  {
    // Generate constraints metadata manager.
    auto managers = get_constraint_metadata(GlobalTestEnvironment::TEST_DB);

    // Execute the test.
    this->test_update(managers.get(), object_id, inserted_metadata,
                      ErrorCode::UNKNOWN);
  }

  // Remove constraint metadata by constraint id.
  {
    // Generate constraints metadata manager.
    auto managers = get_constraint_metadata(GlobalTestEnvironment::TEST_DB);

    // Remove constraint metadata by constraint id.
    this->test_remove(managers.get(), object_id, ErrorCode::OK);
  }

  // Remove constraint metadata by constraint name.
  {
    // Generate constraints metadata manager.
    auto managers = get_constraint_metadata(GlobalTestEnvironment::TEST_DB);

    // Remove constraint metadata by constraint name.
    this->test_remove(managers.get(), object_name, ErrorCode::UNKNOWN);
  }
}

}  // namespace manager::metadata::testing
