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

#include <memory>
#include <string>
#include <string_view>

#include "manager/metadata/dao/postgresql/db_session_manager_pg.h"
#include "manager/metadata/dao/postgresql/roles_dao_pg.h"
#include "test/common/global_test_environment.h"
#include "test/common/ut_utils.h"
#include "test/helper/role_metadata_helper.h"
#include "test/metadata/ut_role_metadata.h"

namespace manager::metadata::testing {

using boost::property_tree::ptree;

class DaoTestRolesMetadata : public ::testing::Test {
 public:
  manager::metadata::ObjectId role_id_;

  void SetUp() override {
    UTUtils::skip_if_connection_not_opened();

    if (g_environment_->is_open()) {
      UTUtils::print(">> gtest::SetUp()");

      // Create dummy data for ROLE.
      role_id_ = RoleMetadataHelper::create_role(
          UtRoleMetadata::kRoleName,
          "NOINHERIT CREATEROLE CREATEDB REPLICATION CONNECTION LIMIT 10");
    }
  }

  void TearDown() override {
    if (g_environment_->is_open()) {
      UTUtils::print(">> gtest::TearDown()");

      // Remove dummy data for ROLE.
      RoleMetadataHelper::drop_role(UtRoleMetadata::kRoleName);
    }
  }
};  // class DaoTestRolesMetadata

/**
 * @brief Happy test for getting all data type metadata based on data type
 * name.
 */
TEST_F(DaoTestRolesMetadata, select_role_metadata) {
  CALL_TRACE;

  ErrorCode error = ErrorCode::UNKNOWN;

  db::DbSessionManagerPg db_session_manager;

  error = db_session_manager.connect();
  ASSERT_EQ(ErrorCode::OK, error);

  std::shared_ptr<db::Dao> roles_dao;
  error = db_session_manager.get_roles_dao(roles_dao);
  ASSERT_NE(nullptr, roles_dao);
  ASSERT_EQ(ErrorCode::OK, error);

  // Set condition keys for testing.
  std::map<std::string_view, std::string_view> keys_name = {
      {Roles::ROLE_ROLNAME, UtRoleMetadata::kRoleName}
  };

  ptree role_metadata;
  // Test getting by role name.
  error = roles_dao->select(keys_name, role_metadata);
  EXPECT_EQ(ErrorCode::OK, error);

  UTUtils::print("-- get role metadata by role name --");
  UTUtils::print(UTUtils::get_tree_string(role_metadata));

  // Generate test metadata.
  UtRoleMetadata ut_metadata(this->role_id_);

  // verifies that returned role metadata equals expected one.
  ASSERT_EQ(1, role_metadata.size());
  ut_metadata.CHECK_METADATA_EXPECTED_OBJ(role_metadata.front().second);

  role_metadata.clear();

  // Set condition keys for testing.
  auto s_table_id = std::to_string(this->role_id_);
  std::map<std::string_view, std::string_view> keys_id = {
      {Roles::ROLE_OID, s_table_id}
  };

  // Test getting by role id.
  error = roles_dao->select(keys_id, role_metadata);
  EXPECT_EQ(ErrorCode::OK, error);

  UTUtils::print("-- get role metadata by role id --");
  UTUtils::print(UTUtils::get_tree_string(role_metadata));

  // verifies that returned role metadata equals expected one.
  ASSERT_EQ(1, role_metadata.size());
  ut_metadata.CHECK_METADATA_EXPECTED_OBJ(role_metadata.front().second);

  // Testing for invalid parameters.
  {
    // Set condition keys for testing.
    std::map<std::string_view, std::string_view> keys = {
        {Roles::ROLE_ROLCANLOGIN, ""}
    };
    error = roles_dao->select(keys, role_metadata);
    EXPECT_EQ(ErrorCode::INVALID_PARAMETER, error);
  }

  {
    // Set condition keys for testing.
    std::map<std::string_view, std::string_view> keys = {
        {Roles::ROLE_OID, "0"}
    };
    error = roles_dao->select(keys, role_metadata);
    EXPECT_EQ(ErrorCode::OK, error);
    EXPECT_EQ(0, role_metadata.size());
  }

  {
    // Set condition keys for testing.
    std::map<std::string_view, std::string_view> keys = {
        {Roles::ROLE_OID, ""}
    };
    error = roles_dao->select(keys, role_metadata);
    EXPECT_EQ(ErrorCode::INVALID_PARAMETER, error);
  }

  {
    // Set condition keys for testing.
    std::map<std::string_view, std::string_view> keys = {
        {Roles::ROLE_ROLNAME, "invalid_role_name"}
    };
    error = roles_dao->select(keys, role_metadata);
    EXPECT_EQ(ErrorCode::OK, error);
    EXPECT_EQ(0, role_metadata.size());
  }

  {
    // Set condition keys for testing.
    std::map<std::string_view, std::string_view> keys = {
        {Roles::ROLE_ROLNAME, ""}
    };
    error = roles_dao->select(keys, role_metadata);
    EXPECT_EQ(ErrorCode::OK, error);
    EXPECT_EQ(0, role_metadata.size());
  }

  {
    // Set condition keys for testing.
    std::map<std::string_view, std::string_view> keys = {
        {"", ""}
    };
    error = roles_dao->select(keys, role_metadata);
    EXPECT_EQ(ErrorCode::INVALID_PARAMETER, error);
  }
}

}  // namespace manager::metadata::testing
