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
#include "test/global_test_environment.h"
#include "test/helper/role_metadata_helper.h"
#include "test/utility/ut_utils.h"

namespace {

constexpr std::string_view role_name = "tsurugi_dao_ut_role_user_1";

}  // namespace

namespace manager::metadata::testing {

using boost::property_tree::ptree;
using db::postgresql::DBSessionManager;

class DaoTestRolesMetadata : public ::testing::Test {
 public:
  void SetUp() override { UTUtils::skip_if_connection_not_opened(); }
};  // class DaoTestRolesMetadata

/**
 * @brief Happy test for getting all data type metadata based on data type
 * name.
 */
TEST_F(DaoTestRolesMetadata, select_role_metadata) {
  ErrorCode error = ErrorCode::UNKNOWN;

  // create dummy data for ROLE.
  ObjectIdType role_id = RoleMetadataHelper::create_role(
      role_name, "NOINHERIT SUPERUSER LOGIN BYPASSRLS");

  std::shared_ptr<db::GenericDAO> gdao = nullptr;

  DBSessionManager db_session_manager;

  error = db_session_manager.get_dao(db::GenericDAO::TableName::ROLES, gdao);
  EXPECT_EQ(ErrorCode::OK, error);

  std::shared_ptr<db::RolesDAO> rdao =
      std::static_pointer_cast<db::RolesDAO>(gdao);

  ptree role_metadata;
  ptree expect_metadata;
  expect_metadata.put(Roles::FORMAT_VERSION, Roles::format_version());
  expect_metadata.put(Roles::GENERATION, Roles::generation());
  expect_metadata.put(Roles::ROLE_ROLNAME, role_name);
  expect_metadata.put(Roles::ROLE_ROLSUPER, "true");        // true
  expect_metadata.put(Roles::ROLE_ROLINHERIT, "false");      // false
  expect_metadata.put(Roles::ROLE_ROLCREATEROLE, "false");   // false
  expect_metadata.put(Roles::ROLE_ROLCREATEDB, "false");     // false
  expect_metadata.put(Roles::ROLE_ROLCANLOGIN, "true");     // true
  expect_metadata.put(Roles::ROLE_ROLREPLICATION, "false");  // false
  expect_metadata.put(Roles::ROLE_ROLBYPASSRLS, "true");    // true
  expect_metadata.put(Roles::ROLE_ROLCONNLIMIT, "-1");   // -1
  expect_metadata.put(Roles::ROLE_ROLPASSWORD, "");      // empty
  expect_metadata.put(Roles::ROLE_ROLVALIDUNTIL, "");    // empty

  // Test getting by role name.
  error =
      rdao->select_role_metadata(Roles::ROLE_ROLNAME, role_name, role_metadata);
  EXPECT_EQ(ErrorCode::OK, error);

  UTUtils::print("-- get role metadata by role name --");
  UTUtils::print(UTUtils::get_tree_string(role_metadata));

  // Verifies that returned role metadata equals expected one.
  RoleMetadataHelper::check_roles_expected(role_metadata, expect_metadata);

  role_metadata.clear();

  // Test getting by role id.
  error = rdao->select_role_metadata(Roles::ROLE_OID, std::to_string(role_id),
                                     role_metadata);
  EXPECT_EQ(ErrorCode::OK, error);

  UTUtils::print("-- get role metadata by role id --");
  UTUtils::print(UTUtils::get_tree_string(role_metadata));

  // Verifies that returned role metadata equals expected one.
  RoleMetadataHelper::check_roles_expected(role_metadata, expect_metadata);

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

  // remove dummy data for ROLE.
  RoleMetadataHelper::drop_role(role_name);
}

}  // namespace manager::metadata::testing
