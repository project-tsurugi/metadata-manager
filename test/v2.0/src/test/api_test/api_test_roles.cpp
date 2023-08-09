/*
 * Copyright 2021-2023 tsurugi project.
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
#include "test/helper/role_metadata_helper.h"
#include "test/metadata/ut_role_metadata.h"

namespace manager::metadata::testing {

using boost::property_tree::ptree;

class ApiTestRolesMetadataPg : public ::testing::Test {
 public:
  manager::metadata::ObjectId role_id_;

  ApiTestRolesMetadataPg() : role_id_(0) {}

  void SetUp() override {
    UTUtils::skip_if_json();
    UTUtils::skip_if_connection_not_opened();

    if (UTUtils::is_postgresql() && g_environment_->is_open()) {
      UTUtils::print(">> gtest::SetUp()");

      // Create dummy data for ROLE.
      role_id_ = RoleMetadataHelper::create_role(
          UtRoleMetadata::kRoleName,
          "NOINHERIT CREATEROLE CREATEDB REPLICATION CONNECTION LIMIT 10");
    }
  }

  void TearDown() override {
    if (UTUtils::is_postgresql() && g_environment_->is_open()) {
      UTUtils::print(">> gtest::TearDown()");

      // Remove dummy data for ROLE.
      RoleMetadataHelper::drop_role(UtRoleMetadata::kRoleName);
    }
  }
};

class ApiTestRolesMetadataJson : public ::testing::Test {
 public:
  void SetUp() override { UTUtils::skip_if_postgresql(); }
  void TearDown() override { UTUtils::skip_if_postgresql(); }
};

/**
 * @brief api test for add role metadata.
 */
TEST_F(ApiTestRolesMetadataPg, test_add) {
  CALL_TRACE;

  // Generate roles metadata manager.
  auto managers = get_roles_ptr(GlobalTestEnvironment::TEST_DB);

  ptree inserted_metadata;

  // Test to initialize the manager.
  ApiTestHelper::test_init(managers.get(), ErrorCode::OK);

  // Execute the test.
  ApiTestHelper::test_add(managers.get(), inserted_metadata,
                          ErrorCode::UNKNOWN);
}

/**
 * @brief Test to get it in ptree type with object ID as key.
 */
TEST_F(ApiTestRolesMetadataPg, test_get_by_id) {
  CALL_TRACE;

  // Generate roles metadata manager.
  auto managers = get_roles_ptr(GlobalTestEnvironment::TEST_DB);

  // Test to initialize the manager.
  ApiTestHelper::test_init(managers.get(), ErrorCode::OK);

  ptree retrieve_metadata;

  // Test getting by role id.
  ApiTestHelper::test_get(managers.get(), this->role_id_, ErrorCode::OK,
                          retrieve_metadata);

  // Generate test metadata.
  UtRoleMetadata ut_metadata(this->role_id_);

  // verifies that returned role metadata equals expected one.
  ut_metadata.CHECK_METADATA_EXPECTED_OBJ(retrieve_metadata);
}

/**
 * @brief Test to get it in ptree type with object name as key.
 */
TEST_F(ApiTestRolesMetadataPg, test_get_by_name) {
  CALL_TRACE;

  // Generate roles metadata manager.
  auto managers = get_roles_ptr(GlobalTestEnvironment::TEST_DB);

  // Test to initialize the manager.
  ApiTestHelper::test_init(managers.get(), ErrorCode::OK);

  ptree retrieve_metadata;

  // Test getting by role name.
  ApiTestHelper::test_get(managers.get(), UtRoleMetadata::kRoleName,
                          ErrorCode::OK, retrieve_metadata);

  // Generate test metadata.
  UtRoleMetadata ut_metadata(this->role_id_);

  // verifies that returned role metadata equals expected one.
  ut_metadata.CHECK_METADATA_EXPECTED_OBJ(retrieve_metadata);
}

/**
 * @brief Test to get all it in ptree type.
 */
TEST_F(ApiTestRolesMetadataPg, test_getall) {
  CALL_TRACE;

  // Generate roles metadata manager.
  auto managers = get_roles_ptr(GlobalTestEnvironment::TEST_DB);

  // Test to initialize the manager.
  ApiTestHelper::test_init(managers.get(), ErrorCode::OK);

  std::vector<ptree> container = {};

  // Execute the test.
  ApiTestHelper::test_getall(managers.get(), ErrorCode::UNKNOWN, container);
  EXPECT_TRUE(container.empty());
}

/**
 * @brief Test to remove it with object ID as key.
 */
TEST_F(ApiTestRolesMetadataPg, test_remove_by_id) {
  CALL_TRACE;

  // Generate roles metadata manager.
  auto managers = get_roles_ptr(GlobalTestEnvironment::TEST_DB);

  // Test to initialize the manager.
  ApiTestHelper::test_init(managers.get(), ErrorCode::OK);

  // Execute the test.
  ApiTestHelper::test_remove(managers.get(), INT64_MAX, ErrorCode::UNKNOWN);
}

/**
 * @brief Test to remove it with object name as key.
 */
TEST_F(ApiTestRolesMetadataPg, test_remove_by_name) {
  CALL_TRACE;

  // Generate roles metadata manager.
  auto managers = get_roles_ptr(GlobalTestEnvironment::TEST_DB);

  // Test to initialize the manager.
  ApiTestHelper::test_init(managers.get(), ErrorCode::OK);

  // Execute the test.
  ApiTestHelper::test_remove(managers.get(), UtRoleMetadata::kRoleName,
                             ErrorCode::UNKNOWN);
}

/**
 * @brief Test for getting role metadata based on unknown role id and role name.
 */
TEST_F(ApiTestRolesMetadataPg, test_not_found) {
  CALL_TRACE;

  // Generate roles metadata manager.
  auto managers = get_roles_ptr(GlobalTestEnvironment::TEST_DB);

  // Test to initialize the manager.
  ApiTestHelper::test_init(managers.get(), ErrorCode::OK);

  // The role id (0) does not exist.
  {
    ptree retrieved_metadata;
    ObjectId object_id = 0;

    // Test of get by ID with ptree.
    ApiTestHelper::test_get(managers.get(), object_id, ErrorCode::ID_NOT_FOUND,
                            retrieved_metadata);
    EXPECT_TRUE(retrieved_metadata.empty());
  }

  // The role id (INT64_MAX) does not exist.
  {
    ptree retrieved_metadata;
    ObjectId object_id = INT32_MAX;

    // Test of get by ID with ptree.
    ApiTestHelper::test_get(managers.get(), object_id, ErrorCode::ID_NOT_FOUND,
                            retrieved_metadata);
    EXPECT_TRUE(retrieved_metadata.empty());
  }

  // The role name (empty) does not exist.
  {
    ptree retrieved_metadata;
    std::string object_name = "";

    // Test of get by ID with ptree.
    ApiTestHelper::test_get(managers.get(), object_name,
                            ErrorCode::NAME_NOT_FOUND, retrieved_metadata);
    EXPECT_TRUE(retrieved_metadata.empty());
  }

  // The role name (unregistered) does not exist.
  {
    ptree retrieved_metadata;
    std::string object_name = "unregistered_dummy_name";

    // Test of get by ID with ptree.
    ApiTestHelper::test_get(managers.get(), object_name,
                            ErrorCode::NAME_NOT_FOUND, retrieved_metadata);
    EXPECT_TRUE(retrieved_metadata.empty());
  }
}

/**
 * @brief Test for incorrect constraint IDs.
 */
TEST_F(ApiTestRolesMetadataPg, test_invalid_parameter) {
  CALL_TRACE;

  // Generate roles metadata manager.
  auto managers = get_roles_ptr(GlobalTestEnvironment::TEST_DB);

  // Test to initialize the manager.
  ApiTestHelper::test_init(managers.get(), ErrorCode::OK);

  ObjectId invalid_id = INVALID_OBJECT_ID;

  // Get role metadata by role id.
  {
    ptree retrieved_metadata;

    // Test of get by ID with ptree.
    ApiTestHelper::test_get(managers.get(), invalid_id, ErrorCode::ID_NOT_FOUND,
                            retrieved_metadata);
    EXPECT_TRUE(retrieved_metadata.empty());
  }
}

/**
 * @brief api test for add role metadata.
 */
TEST_F(ApiTestRolesMetadataJson, test_add) {
  CALL_TRACE;

  // Generate roles metadata manager.
  auto managers = get_roles_ptr(GlobalTestEnvironment::TEST_DB);

  // Test to initialize the manager.
  ApiTestHelper::test_init(managers.get(), ErrorCode::OK);

  ptree inserted_metadata;

  // Test to add the manager.
  ApiTestHelper::test_add(managers.get(), inserted_metadata,
                          ErrorCode::UNKNOWN);
}

/**
 * @brief Unsupported test in JSON version.
 */
TEST_F(ApiTestRolesMetadataJson, test_get) {
  CALL_TRACE;

  // Generate roles metadata manager.
  auto managers = get_roles_ptr(GlobalTestEnvironment::TEST_DB);

  // Test to initialize the manager.
  ApiTestHelper::test_init(managers.get(), ErrorCode::OK);

  ptree retrieve_metadata;

  // Test to get the manager by role id.
  ApiTestHelper::test_get(managers.get(), INT32_MAX, ErrorCode::NOT_SUPPORTED,
                          retrieve_metadata);

  // Test to get the manager by role name.
  ApiTestHelper::test_get(managers.get(), "role_name", ErrorCode::NOT_SUPPORTED,
                          retrieve_metadata);
}

/**
 * @brief api test for get_all role metadata.
 */
TEST_F(ApiTestRolesMetadataJson, test_getall) {
  CALL_TRACE;

  // Generate roles metadata manager.
  auto managers = get_roles_ptr(GlobalTestEnvironment::TEST_DB);

  // Test to initialize the manager.
  ApiTestHelper::test_init(managers.get(), ErrorCode::OK);

  std::vector<boost::property_tree::ptree> container = {};

  // Test to gte all the manager.
  ApiTestHelper::test_getall(managers.get(), ErrorCode::UNKNOWN, container);
  EXPECT_TRUE(container.empty());
}

/**
 * @brief api test for remove role metadata.
 */
TEST_F(ApiTestRolesMetadataJson, remove_role_metadata) {
  CALL_TRACE;

  // Generate roles metadata manager.
  auto managers = get_roles_ptr(GlobalTestEnvironment::TEST_DB);

  // Test to initialize the manager.
  ApiTestHelper::test_init(managers.get(), ErrorCode::OK);

  std::vector<boost::property_tree::ptree> container = {};

  // Test to remove the manager by role id.
  ApiTestHelper::test_remove(managers.get(), INT32_MAX, ErrorCode::UNKNOWN);

  // Test to remove the manager by role name.
  ApiTestHelper::test_remove(managers.get(), "role_name", ErrorCode::UNKNOWN);
}

}  // namespace manager::metadata::testing
