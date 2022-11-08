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
#include <gtest/gtest.h>

#include <memory>
#include <string>
#include <string_view>

#include "manager/metadata/dao/postgresql/db_session_manager_pg.h"
#include "manager/metadata/dao/roles_dao.h"
#include "test/common/global_test_environment.h"
#include "test/common/ut_utils.h"
#include "test/helper/role_metadata_helper.h"
#include "test/metadata/ut_role_metadata.h"

namespace manager::metadata::testing {

using boost::property_tree::ptree;
using db::postgresql::DBSessionManager;

class DaoTestRolesMetadata : public ::testing::Test {
 public:
  manager::metadata::ObjectId role_id_;

  void SetUp() override {
    UTUtils::skip_if_json();
    UTUtils::skip_if_connection_not_opened();

    UTUtils::print(">> gtest::SetUp()");

    // Create dummy data for ROLE.
    role_id_ = RoleMetadataHelper::create_role(
        UtRoleMetadata::kRoleName,
        "NOINHERIT CREATEROLE CREATEDB REPLICATION CONNECTION LIMIT 10");
  }

  void TearDown() override {
    UTUtils::skip_if_json();

    if (global->is_open()) {
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
  SCOPED_TRACE("");

  ErrorCode error = ErrorCode::UNKNOWN;

  std::shared_ptr<db::GenericDAO> gdao = nullptr;

  DBSessionManager db_session_manager;

  error = db_session_manager.get_dao(db::GenericDAO::TableName::ROLES, gdao);
  EXPECT_EQ(ErrorCode::OK, error);

  std::shared_ptr<db::RolesDAO> rdao =
      std::static_pointer_cast<db::RolesDAO>(gdao);

  ptree role_metadata;
  // Test getting by role name.
  error = rdao->select_role_metadata(Roles::ROLE_ROLNAME,
                                     UtRoleMetadata::kRoleName, role_metadata);
  EXPECT_EQ(ErrorCode::OK, error);

  UTUtils::print("-- get role metadata by role name --");
  UTUtils::print(UTUtils::get_tree_string(role_metadata));

  // Generate test metadata.
  UtRoleMetadata ut_metadata(this->role_id_);

  // verifies that returned role metadata equals expected one.
  ut_metadata.check_metadata_expected(role_metadata, __FILE__, __LINE__);

  role_metadata.clear();

  // Test getting by role id.
  error = rdao->select_role_metadata(
      Roles::ROLE_OID, std::to_string(this->role_id_), role_metadata);
  EXPECT_EQ(ErrorCode::OK, error);

  UTUtils::print("-- get role metadata by role id --");
  UTUtils::print(UTUtils::get_tree_string(role_metadata));

  // verifies that returned role metadata equals expected one.
  ut_metadata.check_metadata_expected(role_metadata, __FILE__, __LINE__);

  // Testing for invalid parameters.
  error =
      rdao->select_role_metadata(Roles::ROLE_ROLCANLOGIN, "", role_metadata);
  EXPECT_EQ(ErrorCode::INVALID_PARAMETER, error);

  error = rdao->select_role_metadata(Roles::ROLE_OID, "0", role_metadata);
  EXPECT_EQ(ErrorCode::ID_NOT_FOUND, error);

  error = rdao->select_role_metadata(Roles::ROLE_OID, "", role_metadata);
  EXPECT_EQ(ErrorCode::INVALID_PARAMETER, error);

  error = rdao->select_role_metadata(Roles::ROLE_ROLNAME, "invalid_role_name",
                                     role_metadata);
  EXPECT_EQ(ErrorCode::NAME_NOT_FOUND, error);

  error = rdao->select_role_metadata(Roles::ROLE_ROLNAME, "", role_metadata);
  EXPECT_EQ(ErrorCode::NAME_NOT_FOUND, error);

  error = rdao->select_role_metadata("", "", role_metadata);
  EXPECT_EQ(ErrorCode::INVALID_PARAMETER, error);
}

}  // namespace manager::metadata::testing
