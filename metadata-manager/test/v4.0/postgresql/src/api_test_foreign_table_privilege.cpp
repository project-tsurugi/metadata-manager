/*
 * Copyright 2020-2021 tsurugi project.
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
// #include "test/api_test_foreign_table_privilege.h"

#include <gtest/gtest.h>

#include <boost/format.hpp>
#include <memory>
#include <string>

#include "manager/metadata/dao/common/config.h"
#include "manager/metadata/dao/postgresql/dbc_utils.h"
#include "manager/metadata/tables.h"
#include "test/global_test_environment.h"
#include "test/helper/foreign_table_helper.h"
#include "test/helper/role_metadata_helper.h"
#include "test/helper/table_metadata_helper.h"
#include "test/utility/ut_utils.h"

namespace {

constexpr std::string_view role_name = "tsurugi_api_ut_privileges_user_1";
constexpr std::string_view foreign_table_name_ro =
    "tsurugi_api_ut_foreign_table_ro";
constexpr std::string_view foreign_table_name_rw =
    "tsurugi_api_ut_foreign_table_rw";
constexpr std::string_view foreign_table_name_none =
    "tsurugi_api_ut_foreign_table_none";

std::shared_ptr<PGconn> connection;
std::unique_ptr<manager::metadata::Tables> tables;
Oid role_id = 0;
Oid table_id_ro = 0;
Oid table_id_rw = 0;
Oid foreign_table_id_ro = 0;
Oid foreign_table_id_rw = 0;

}  // namespace

namespace manager::metadata::testing {

namespace storage = manager::metadata::db::postgresql;

class ApiTestForeignTablePrivileges {
 public:
  /**
   * @brief setup the data for testing.
   */
  static void test_setup() {
    boost::format statement;

    if (global->is_open()) {
      // create an instance of the Tables class.
      tables = std::make_unique<Tables>(GlobalTestEnvironment::TEST_DB);
      tables->init();

      // create dummy data for ROLE.
      role_id = RoleMetadataHelper::create_role(role_name, "");

      // create dummy data for TABLE.
      table_id_ro = ForeignTableHelper::create_table(foreign_table_name_ro,
                                                     role_name, "SELECT");
      table_id_rw = ForeignTableHelper::create_table(
          foreign_table_name_rw, role_name, "SELECT,INSERT,UPDATE,DELETE");

      // create dummy data for pg_foreign_table.
      foreign_table_id_ro =
          ForeignTableHelper::insert_foreign_table(foreign_table_name_ro);
      foreign_table_id_rw =
          ForeignTableHelper::insert_foreign_table(foreign_table_name_rw);
    } else {
      GTEST_SKIP_("metadata repository is not started.");
    }
  }

  /**
   * @brief discard the data for testing.
   */
  static void test_teardown() {
    if (global->is_open()) {
      // remove dummy data for pg_foreign_table.
      ForeignTableHelper::delete_foreign_table(foreign_table_id_ro);
      ForeignTableHelper::delete_foreign_table(foreign_table_id_rw);

      // remove dummy data for TABLE.
      ForeignTableHelper::drop_table(foreign_table_name_ro);
      ForeignTableHelper::drop_table(foreign_table_name_rw);

      // remove dummy data for ROLE.
      RoleMetadataHelper::drop_role(role_name);
    }
  }
};

/**
 * @brief ApiTestTablePrivilegesSingle
 */
class ApiTestTablePrivilegesSingle
    : public ::testing::TestWithParam<
          std::vector<std::tuple<const char*, bool>>> {
 public:
  void SetUp() override {
    ApiTestForeignTablePrivileges::test_setup();
    if (global->is_open()) {
      // add read-write table metadata.
      TableMetadataHelper::add_table(foreign_table_name_rw);
    }
  }
  void TearDown() override {
    if (global->is_open()) {
      // remove table metadata.
      TableMetadataHelper::remove_table(foreign_table_name_rw);
    }
    ApiTestForeignTablePrivileges::test_teardown();
  }
};

/**
 * @brief ApiTestTablePrivilegesMultiple
 */
class ApiTestTablePrivilegesMultiple
    : public ::testing::TestWithParam<
          std::vector<std::tuple<const char*, bool>>> {
 public:
  void SetUp() override {
    ApiTestForeignTablePrivileges::test_setup();

    if (global->is_open()) {
      // add read-write table metadata.
      TableMetadataHelper::add_table(foreign_table_name_rw);
      // add read-only table metadata.
      TableMetadataHelper::add_table(foreign_table_name_ro);
    }
  }
  void TearDown() override {
    if (global->is_open()) {
      // remove table metadata.
      TableMetadataHelper::remove_table(foreign_table_name_rw);
      TableMetadataHelper::remove_table(foreign_table_name_ro);
    }
    ApiTestForeignTablePrivileges::test_teardown();
  }
};

/**
 * @brief ApiTestForeignTable
 */
class ApiTestForeignTable : public ::testing::Test {
 public:
  void SetUp() override {
    ApiTestForeignTablePrivileges::test_setup();
    if (global->is_open()) {
      // add read-write table metadata.
      TableMetadataHelper::add_table(foreign_table_name_rw);
    }
  }
  void TearDown() override {
    if (global->is_open()) {
      // remove table metadata.
      TableMetadataHelper::remove_table(foreign_table_name_rw);
    }
    ApiTestForeignTablePrivileges::test_teardown();
  }
};

/**
 * @brief ApiTestForeignTableNotExists
 */
class ApiTestForeignTableNotExists : public ::testing::Test {
 public:
  void SetUp() override { ApiTestForeignTablePrivileges::test_setup(); }
  void TearDown() override { ApiTestForeignTablePrivileges::test_teardown(); }
};

/**
 * @brief ApiTestTablePrivilegesInvalid
 */
class ApiTestTablePrivilegesInvalid
    : public ::testing::TestWithParam<std::vector<const char*>> {
 public:
  void SetUp() override {
    ApiTestForeignTablePrivileges::test_setup();

    if (global->is_open()) {
      // add read-write table metadata.
      TableMetadataHelper::add_table(foreign_table_name_rw);
    }
  }
  void TearDown() override {
    if (global->is_open()) {
      // remove table metadata.
      TableMetadataHelper::remove_table(foreign_table_name_rw);
    }
    ApiTestForeignTablePrivileges::test_teardown();
  }
};

std::vector<std::tuple<const char*, bool>> test_pattern_list_single = {
    std::make_tuple("r", true),    std::make_tuple("a", true),
    std::make_tuple("w", true),    std::make_tuple("d", true),
    std::make_tuple("D", false),   std::make_tuple("x", false),
    std::make_tuple("t", false),   std::make_tuple("rwa", true),
    std::make_tuple("rwad", true), std::make_tuple("arwdDxt", false)};
std::vector<std::tuple<const char*, bool>> test_pattern_list_multiple = {
    std::make_tuple("r", true),     std::make_tuple("a", false),
    std::make_tuple("w", false),    std::make_tuple("d", false),
    std::make_tuple("D", false),    std::make_tuple("x", false),
    std::make_tuple("t", false),    std::make_tuple("rwa", false),
    std::make_tuple("rwad", false), std::make_tuple("arwdDxt", false)};
std::vector<const char*> test_pattern_list_invalid = {
    "X", "U", "C", "c", "T", "*", "arwdDxtXUCcT"};

INSTANTIATE_TEST_CASE_P(ParamtererizedTest, ApiTestTablePrivilegesSingle,
                        ::testing::Values(test_pattern_list_single));

INSTANTIATE_TEST_CASE_P(ParamtererizedTest, ApiTestTablePrivilegesMultiple,
                        ::testing::Values(test_pattern_list_multiple));

INSTANTIATE_TEST_CASE_P(ParamtererizedTest, ApiTestTablePrivilegesInvalid,
                        ::testing::Values(test_pattern_list_invalid));

/**
 * @brief test for confirm permissions.
 */
TEST_P(ApiTestTablePrivilegesSingle, confirm_tables_permission) {
  auto params = GetParam();

  for (auto param : params) {
    const char* permission = std::get<0>(param);
    bool expected = std::get<1>(param);

    ErrorCode error = ErrorCode::UNKNOWN;
    bool actual = false;

    UTUtils::print("  Test pattern: [", permission, "]");

    // check the table permissions by role id.
    error = tables->confirm_permission_in_acls(role_id, permission, actual);
    EXPECT_EQ(ErrorCode::OK, error);
    EXPECT_EQ(expected, actual);

    // check the table permissions by role name.
    error = tables->confirm_permission_in_acls(role_name, permission, actual);
    EXPECT_EQ(ErrorCode::OK, error);
    EXPECT_EQ(expected, actual);
  }
}

/**
 * @brief test for confirm permissions.
 */
TEST_P(ApiTestTablePrivilegesMultiple, confirm_tables_permission) {
  auto params = GetParam();

  for (auto param : params) {
    const char* permission = std::get<0>(param);
    bool expected = std::get<1>(param);

    ErrorCode error = ErrorCode::UNKNOWN;
    bool actual = false;

    UTUtils::print("  Test pattern: [", permission, "]");

    // check the table permissions by role id.
    error = tables->confirm_permission_in_acls(role_id, permission, actual);
    EXPECT_EQ(ErrorCode::OK, error);
    EXPECT_EQ(expected, actual);

    // check the table permissions by role name.
    error = tables->confirm_permission_in_acls(role_name, permission, actual);
    EXPECT_EQ(ErrorCode::OK, error);
    EXPECT_EQ(expected, actual);
  }
}

/**
 * @brief test retrieving table metadata for an foreign table.
 */
TEST_F(ApiTestForeignTable, get_table_metadata) {
  ErrorCode error = ErrorCode::UNKNOWN;

  boost::property_tree::ptree object;
  error = tables->get(foreign_table_name_rw, object);
  EXPECT_EQ(ErrorCode::OK, error);

  UTUtils::print("-- get an foreign table metadata --");
  UTUtils::print(UTUtils::get_tree_string(object));

  auto res_role_id = object.get_optional<Oid>(Tables::OWNER_ROLE_ID);
  auto res_acl = object.get_child(Tables::ACL);
  EXPECT_GT(res_role_id.value(), 0);
  EXPECT_GT(res_acl.size(), 0);
}

/**
 * @brief test for the case where table metadata does not exists.
 */
TEST_F(ApiTestForeignTableNotExists, table_metadata_does_not_exist) {
  ErrorCode error = ErrorCode::UNKNOWN;
  bool res_permission = false;

  UTUtils::print("-- confirm permission by role id --");
  error = tables->confirm_permission_in_acls(role_id, "r", res_permission);
  EXPECT_EQ(ErrorCode::NOT_FOUND, error);

  UTUtils::print("-- confirm permission by role name --");
  error = tables->confirm_permission_in_acls(role_name, "r", res_permission);
  EXPECT_EQ(ErrorCode::NOT_FOUND, error);
}

/**
 * @brief test for the case where foreign table does not exists.
 */
TEST_F(ApiTestForeignTableNotExists, foreign_table_does_not_exist) {
  ErrorCode error = ErrorCode::UNKNOWN;
  bool res_permission = false;

  // add read-write table metadata.
  TableMetadataHelper::add_table(foreign_table_name_none);

  UTUtils::print("-- confirm permission by role id --");
  error = tables->confirm_permission_in_acls(role_id, "r", res_permission);
  EXPECT_EQ(ErrorCode::NOT_FOUND, error);

  UTUtils::print("-- confirm permission by role name --");
  error = tables->confirm_permission_in_acls(role_name, "r", res_permission);
  EXPECT_EQ(ErrorCode::NOT_FOUND, error);

  // remove table metadata.
  TableMetadataHelper::remove_table(foreign_table_name_none);
}

/**
 * @brief test for the case where role id or name does not exists.
 */
TEST_F(ApiTestForeignTableNotExists, role_does_not_exist) {
  ErrorCode error = ErrorCode::UNKNOWN;
  bool res_permission = false;

  // add read-write table metadata.
  TableMetadataHelper::add_table(foreign_table_name_rw);

  ObjectIdType role_id;
  std::string role_name;

  // the role id (0) does not exist.
  role_id = 0;
  UTUtils::print("  Test pattern: [", role_id, "]");
  error = tables->confirm_permission_in_acls(0, "r", res_permission);
  EXPECT_EQ(ErrorCode::ID_NOT_FOUND, error);

  // the role id (9999999) does not exist.
  role_id = 9999999L;
  UTUtils::print("  Test pattern: [", role_id, "]");
  error = tables->confirm_permission_in_acls(role_id, "r", res_permission);
  EXPECT_EQ(ErrorCode::ID_NOT_FOUND, error);

  // the role name is empty.
  role_name = "";
  UTUtils::print("  Test pattern: [", role_name, "]");
  error = tables->confirm_permission_in_acls(role_name, "r", res_permission);
  EXPECT_EQ(ErrorCode::NAME_NOT_FOUND, error);

  // the role name does not exist.
  role_name = "undefined-name";
  UTUtils::print("  Test pattern: [", role_name, "]");
  error = tables->confirm_permission_in_acls(role_name, "r", res_permission);
  EXPECT_EQ(ErrorCode::NAME_NOT_FOUND, error);

  // remove table metadata.
  TableMetadataHelper::remove_table(foreign_table_name_rw);
}

/**
 * @brief test for confirm permissions with invalid parameter.
 */
TEST_P(ApiTestTablePrivilegesInvalid, confirm_tables_permission) {
  auto params = GetParam();

  for (auto permission : params) {
    ErrorCode error = ErrorCode::UNKNOWN;
    bool actual = false;

    UTUtils::print("  Test pattern: [", permission, "]");

    // check the table permissions by role id.
    error = tables->confirm_permission_in_acls(role_id, permission, actual);
    EXPECT_EQ(ErrorCode::INVALID_PARAMETER, error);

    // check the table permissions by role name.
    error = tables->confirm_permission_in_acls(role_name, permission, actual);
    EXPECT_EQ(ErrorCode::INVALID_PARAMETER, error);
  }
}

}  // namespace manager::metadata::testing
