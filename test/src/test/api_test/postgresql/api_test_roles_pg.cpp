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

#include <boost/format.hpp>

#include "manager/metadata/common/config.h"
#include "manager/metadata/dao/postgresql/dbc_utils_pg.h"
#include "manager/metadata/roles.h"
#include "test/common/global_test_environment.h"
#include "test/common/ut_utils.h"
#include "test/helper/postgresql/role_metadata_helper_pg.h"

namespace {

constexpr std::string_view role_name = "tsurugi_api_ut_role_user_1";

}  // namespace

namespace manager::metadata::testing {

using boost::property_tree::ptree;

class ApiTestRolesMetadata : public ::testing::Test {
 public:
  void SetUp() override { UTUtils::skip_if_connection_not_opened(); }
};

/**
 * @brief Happy test for getting role metadata.
 */
TEST_F(ApiTestRolesMetadata, get_role) {
  ErrorCode error = ErrorCode::UNKNOWN;

  // create dummy data for ROLE.
  ObjectIdType role_id = RoleMetadataHelper::create_role(
      role_name,
      "NOINHERIT CREATEROLE CREATEDB REPLICATION CONNECTION LIMIT 10");

  auto roles = std::make_unique<Roles>(GlobalTestEnvironment::TEST_DB);
  error      = roles->init();
  EXPECT_EQ(ErrorCode::OK, error);

  ptree role_metadata;
  ptree expect_metadata;
  expect_metadata.put(Roles::FORMAT_VERSION, Roles::format_version());
  expect_metadata.put(Roles::GENERATION, Roles::generation());
  expect_metadata.put(Roles::ROLE_ROLNAME, role_name);
  expect_metadata.put(Roles::ROLE_ROLSUPER, "false");       // false
  expect_metadata.put(Roles::ROLE_ROLINHERIT, "false");     // false
  expect_metadata.put(Roles::ROLE_ROLCREATEROLE, "true");   // true
  expect_metadata.put(Roles::ROLE_ROLCREATEDB, "true");     // true
  expect_metadata.put(Roles::ROLE_ROLCANLOGIN, "false");    // false
  expect_metadata.put(Roles::ROLE_ROLREPLICATION, "true");  // true
  expect_metadata.put(Roles::ROLE_ROLBYPASSRLS, "false");   // false
  expect_metadata.put(Roles::ROLE_ROLCONNLIMIT, "10");      // 10
  expect_metadata.put(Roles::ROLE_ROLPASSWORD, "");         // empty
  expect_metadata.put(Roles::ROLE_ROLVALIDUNTIL, "");       // empty

  // test getting by role id.
  error = roles->get(role_id, role_metadata);
  EXPECT_EQ(ErrorCode::OK, error);

  UTUtils::print("-- get role metadata by role id --");
  UTUtils::print(UTUtils::get_tree_string(role_metadata));

  // verifies that returned role metadata equals expected one.
  RoleMetadataHelper::check_roles_expected(role_metadata, expect_metadata);

  // clear property_tree.
  role_metadata.clear();

  // test getting by role name.
  error = roles->get(role_name, role_metadata);
  EXPECT_EQ(ErrorCode::OK, error);

  UTUtils::print("-- get role metadata by role name --");
  UTUtils::print(UTUtils::get_tree_string(role_metadata));

  // verifies that returned role metadata equals expected one.
  RoleMetadataHelper::check_roles_expected(role_metadata, expect_metadata);

  // remove dummy data for ROLE.
  RoleMetadataHelper::drop_role(role_name);
}

/**
 * @brief test for getting role metadata based on unknown role id and role name.
 */
TEST_F(ApiTestRolesMetadata, role_does_not_exist) {
  ErrorCode error = ErrorCode::UNKNOWN;

  auto roles = std::make_unique<Roles>(GlobalTestEnvironment::TEST_DB);
  error      = roles->init();
  EXPECT_EQ(ErrorCode::OK, error);

  ptree role_metadata;
  ObjectIdType role_id;
  std::string role_name;

  // the role id (0) does not exist.
  role_id = 0;
  UTUtils::print("  Test pattern: [", role_id, "]");
  error = roles->get(0, role_metadata);
  EXPECT_EQ(ErrorCode::ID_NOT_FOUND, error);

  // the role id (9999999) does not exist.
  role_id = 9999999L;
  UTUtils::print("  Test pattern: [", role_id, "]");
  error = roles->get(role_id, role_metadata);
  EXPECT_EQ(ErrorCode::ID_NOT_FOUND, error);

  // the role name is empty.
  role_name = "";
  UTUtils::print("  Test pattern: [", role_name, "]");
  error = roles->get(role_name, role_metadata);
  EXPECT_EQ(ErrorCode::NAME_NOT_FOUND, error);

  // the role name does not exist.
  role_name = "undefined-name";
  UTUtils::print("  Test pattern: [", role_name, "]");
  error = roles->get(role_name, role_metadata);
  EXPECT_EQ(ErrorCode::NAME_NOT_FOUND, error);
}

/**
 * @brief api test for add role metadata.
 */
TEST_F(ApiTestRolesMetadata, add_role_metadata) {
  ErrorCode error = ErrorCode::UNKNOWN;

  auto roles = std::make_unique<Roles>(GlobalTestEnvironment::TEST_DB);
  error      = roles->init();
  EXPECT_EQ(ErrorCode::OK, error);

  ptree role_metadata;

  error = roles->add(role_metadata);
  EXPECT_EQ(ErrorCode::UNKNOWN, error);

  ObjectIdType retval_role_id = -1;
  error                       = roles->add(role_metadata, &retval_role_id);
  EXPECT_EQ(ErrorCode::UNKNOWN, error);
  EXPECT_EQ(-1, retval_role_id);
}

/**
 * @brief api test for get_all role metadata.
 */
TEST_F(ApiTestRolesMetadata, get_all_role_metadata) {
  ErrorCode error = ErrorCode::UNKNOWN;

  auto roles = std::make_unique<Roles>(GlobalTestEnvironment::TEST_DB);
  error      = roles->init();
  EXPECT_EQ(ErrorCode::OK, error);

  std::vector<boost::property_tree::ptree> container = {};
  error = roles->get_all(container);
  EXPECT_EQ(ErrorCode::UNKNOWN, error);
  EXPECT_TRUE(container.empty());
}

/**
 * @brief api test for remove role metadata.
 */
TEST_F(ApiTestRolesMetadata, remove_role_metadata) {
  ErrorCode error = ErrorCode::UNKNOWN;

  auto roles = std::make_unique<Roles>(GlobalTestEnvironment::TEST_DB);
  error      = roles->init();
  EXPECT_EQ(ErrorCode::OK, error);

  error = roles->remove(99999);
  EXPECT_EQ(ErrorCode::UNKNOWN, error);

  ObjectIdType retval_role_id = -1;
  error                       = roles->remove("role_name", &retval_role_id);
  EXPECT_EQ(ErrorCode::UNKNOWN, error);
  EXPECT_EQ(-1, retval_role_id);
}

}  // namespace manager::metadata::testing
