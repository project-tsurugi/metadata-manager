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
#include <gtest/gtest.h>

#include <memory>
#include <string>

#include <boost/format.hpp>

#include "manager/metadata/metadata_factory.h"
#include "test/common/global_test_environment.h"
#include "test/common/ut_utils.h"
#include "test/helper/foreign_table_helper.h"
#include "test/helper/role_metadata_helper.h"
#include "test/helper/table_metadata_helper.h"

namespace {

static constexpr const char* const kRoleName =
    "tsurugi_api_ut_privileges_user_1";
static constexpr const char* const kForeignTableNameRo =
    "api_ut_foreign_table_ro";
static constexpr const char* const kForeignTableNameRw =
    "api_ut_foreign_table_rw";
static constexpr const char* const kForeignTableNameNone =
    "api_ut_foreign_table_none";

int64_t role_id;
int64_t table_id_ro;
int64_t table_id_rw;
int64_t foreign_table_id_ro;
int64_t foreign_table_id_rw;

}  // namespace

namespace manager::metadata::testing {

class TablePrivilegesPg {
 public:
  /**
   * @brief setup the data for testing.
   */
  static void test_setup() {
    UTUtils::skip_if_json();
    UTUtils::skip_if_connection_not_opened();

    if (UTUtils::is_postgresql() && global->is_open()) {
      boost::format statement;

      // create an instance of the Tables class.
      auto managers = get_table_metadata(GlobalTestEnvironment::TEST_DB);
      managers->init();

      // create dummy data for ROLE.
      role_id = RoleMetadataHelper::create_role(kRoleName, "");

      // create dummy data for TABLE.
      table_id_ro = ForeignTableHelper::create_table(kForeignTableNameRo,
                                                     kRoleName, "SELECT");
      table_id_rw = ForeignTableHelper::create_table(
          kForeignTableNameRw, kRoleName, "SELECT,INSERT,UPDATE,DELETE");

      // create dummy data for pg_foreign_table.
      foreign_table_id_ro =
          ForeignTableHelper::insert_foreign_table(kForeignTableNameRo);
      foreign_table_id_rw =
          ForeignTableHelper::insert_foreign_table(kForeignTableNameRw);
    }
  }

  /**
   * @brief discard the data for testing.
   */
  static void test_teardown() {
    UTUtils::skip_if_json();

    if (UTUtils::is_postgresql() && global->is_open()) {
      // remove dummy data for pg_foreign_table.
      ForeignTableHelper::delete_foreign_table(foreign_table_id_ro);
      ForeignTableHelper::delete_foreign_table(foreign_table_id_rw);

      // remove dummy data for TABLE.
      ForeignTableHelper::drop_table(kForeignTableNameRo);
      ForeignTableHelper::drop_table(kForeignTableNameRw);

      // remove dummy data for ROLE.
      RoleMetadataHelper::drop_role(kRoleName);
    }
  }
};

/**
 * @brief ApiTestTablePrivilegesSingle
 */
class ApiTestTablePrivilegesSinglePg
    : public ::testing::TestWithParam<
          std::vector<std::tuple<const char*, bool>>> {
 public:
  void SetUp() override {
    TablePrivilegesPg::test_setup();

    if (UTUtils::is_postgresql() && global->is_open()) {
      // add read-write table metadata.
      TableMetadataHelper::add_table(kForeignTableNameRw);
    }
  }
  void TearDown() override {
    if (UTUtils::is_postgresql() && global->is_open()) {
      // remove table metadata.
      TableMetadataHelper::remove_table(kForeignTableNameRw);
    }
    TablePrivilegesPg::test_teardown();
  }
};

/**
 * @brief ApiTestTablePrivilegesMultiplePg
 */
class ApiTestTablePrivilegesMultiplePg
    : public ::testing::TestWithParam<
          std::vector<std::tuple<const char*, bool>>> {
 public:
  void SetUp() override {
    TablePrivilegesPg::test_setup();

    if (UTUtils::is_postgresql() && global->is_open()) {
      // add read-write table metadata.
      TableMetadataHelper::add_table(kForeignTableNameRw);
      // add read-only table metadata.
      TableMetadataHelper::add_table(kForeignTableNameRo);
    }
  }
  void TearDown() override {
    UTUtils::skip_if_json();

    if (UTUtils::is_postgresql() && global->is_open()) {
      // remove table metadata.
      TableMetadataHelper::remove_table(kForeignTableNameRw);
      TableMetadataHelper::remove_table(kForeignTableNameRo);
    }
    TablePrivilegesPg::test_teardown();
  }
};

/**
 * @brief ApiTestForeignTablePg
 */
class ApiTestForeignTablePg : public ::testing::Test {
 public:
  void SetUp() override {
    UTUtils::skip_if_json();

    TablePrivilegesPg::test_setup();

    if (UTUtils::is_postgresql() && global->is_open()) {
      // add read-write table metadata.
      TableMetadataHelper::add_table(kForeignTableNameRw);
    }
  }
  void TearDown() override {
    UTUtils::skip_if_json();

    if (UTUtils::is_postgresql() && global->is_open()) {
      // remove table metadata.
      TableMetadataHelper::remove_table(kForeignTableNameRw);
    }
    TablePrivilegesPg::test_teardown();
  }
};

/**
 * @brief ApiTestForeignTableNotExistsPg
 */
class ApiTestForeignTableNotExistsPg : public ::testing::Test {
 public:
  void SetUp() override { TablePrivilegesPg::test_setup(); }
  void TearDown() override { TablePrivilegesPg::test_teardown(); }
};

/**
 * @brief ApiTestTablePrivilegesInvalid
 */
class ApiTestTablePrivilegesInvalidPg
    : public ::testing::TestWithParam<std::vector<const char*>> {
 public:
  void SetUp() override {
    TablePrivilegesPg::test_setup();

    if (UTUtils::is_postgresql() && global->is_open()) {
      // add read-write table metadata.
      TableMetadataHelper::add_table(kForeignTableNameRw);
    }
  }
  void TearDown() override {
    UTUtils::skip_if_json();

    if (UTUtils::is_postgresql() && global->is_open()) {
      // remove table metadata.
      TableMetadataHelper::remove_table(kForeignTableNameRw);
    }
    TablePrivilegesPg::test_teardown();
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

INSTANTIATE_TEST_CASE_P(ParameterizedTest, ApiTestTablePrivilegesSinglePg,
                        ::testing::Values(test_pattern_list_single));

INSTANTIATE_TEST_CASE_P(ParameterizedTest, ApiTestTablePrivilegesMultiplePg,
                        ::testing::Values(test_pattern_list_multiple));

INSTANTIATE_TEST_CASE_P(ParameterizedTest, ApiTestTablePrivilegesInvalidPg,
                        ::testing::Values(test_pattern_list_invalid));

/**
 * @brief test for confirm permissions.
 */
TEST_P(ApiTestTablePrivilegesSinglePg, confirm_tables_permission) {
  ErrorCode error = ErrorCode::UNKNOWN;
  auto params     = GetParam();

  // create an instance of the Tables class.
  auto managers = get_table_metadata(GlobalTestEnvironment::TEST_DB);
  auto tables   = static_cast<Tables*>(managers.get());

  error = tables->init();
  EXPECT_EQ(ErrorCode::OK, error);

  for (auto param : params) {
    const char* permission = std::get<0>(param);
    bool expected          = std::get<1>(param);
    bool actual            = false;

    UTUtils::print("  Test pattern: [", permission, "]");

    // check the table permissions by role id.
    error = tables->confirm_permission_in_acls(role_id, permission, actual);
    EXPECT_EQ(ErrorCode::OK, error);
    EXPECT_EQ(expected, actual);

    // check the table permissions by role name.
    error = tables->confirm_permission_in_acls(kRoleName, permission, actual);
    EXPECT_EQ(ErrorCode::OK, error);
    EXPECT_EQ(expected, actual);
  }
}

/**
 * @brief test for confirm permissions.
 */
TEST_P(ApiTestTablePrivilegesMultiplePg, confirm_tables_permission) {
  ErrorCode error = ErrorCode::UNKNOWN;
  auto params     = GetParam();

  // create an instance of the Tables class.
  auto managers = get_table_metadata(GlobalTestEnvironment::TEST_DB);
  auto tables   = static_cast<Tables*>(managers.get());

  error = tables->init();
  EXPECT_EQ(ErrorCode::OK, error);

  for (auto param : params) {
    const char* permission = std::get<0>(param);
    bool expected          = std::get<1>(param);
    bool actual            = false;

    UTUtils::print("  Test pattern: [", permission, "]");

    // check the table permissions by role id.
    error = tables->confirm_permission_in_acls(role_id, permission, actual);
    EXPECT_EQ(ErrorCode::OK, error);
    EXPECT_EQ(expected, actual);

    // check the table permissions by role name.
    error = tables->confirm_permission_in_acls(kRoleName, permission, actual);
    EXPECT_EQ(ErrorCode::OK, error);
    EXPECT_EQ(expected, actual);
  }
}

/**
 * @brief test retrieving table metadata for an foreign table.
 */
TEST_F(ApiTestForeignTablePg, get_table_metadata) {
  ErrorCode error = ErrorCode::UNKNOWN;

  // create an instance of the Tables class.
  auto managers = get_table_metadata(GlobalTestEnvironment::TEST_DB);
  auto tables   = static_cast<Tables*>(managers.get());

  error = tables->init();
  EXPECT_EQ(ErrorCode::OK, error);

  boost::property_tree::ptree object;
  error = tables->get(kForeignTableNameRw, object);

  UTUtils::print("-- get an foreign table metadata --");
  UTUtils::print(UTUtils::get_tree_string(object));

  auto res_role_id = object.get_optional<int64_t>(Table::OWNER_ROLE_ID);
  auto res_acl     = object.get_child(Table::ACL);
  EXPECT_GT(res_role_id.value(), 0);
  EXPECT_GT(res_acl.size(), 0);
}

/**
 * @brief test for the case where table metadata does not exists.
 */
TEST_F(ApiTestForeignTableNotExistsPg, table_metadata_does_not_exist) {
  ErrorCode error     = ErrorCode::UNKNOWN;
  bool res_permission = false;

  // create an instance of the Tables class.
  auto managers = get_table_metadata(GlobalTestEnvironment::TEST_DB);
  auto tables   = static_cast<Tables*>(managers.get());

  error = tables->init();
  EXPECT_EQ(ErrorCode::OK, error);

  UTUtils::print("-- confirm permission by role id --");
  error = tables->confirm_permission_in_acls(role_id, "r", res_permission);
  EXPECT_EQ(ErrorCode::NOT_FOUND, error);

  UTUtils::print("-- confirm permission by role name --");
  error = tables->confirm_permission_in_acls(kRoleName, "r", res_permission);
  EXPECT_EQ(ErrorCode::NOT_FOUND, error);
}

/**
 * @brief test for the case where foreign table does not exists.
 */
TEST_F(ApiTestForeignTableNotExistsPg, foreign_table_does_not_exist) {
  ErrorCode error     = ErrorCode::UNKNOWN;
  bool res_permission = false;

  // create an instance of the Tables class.
  auto managers = get_table_metadata(GlobalTestEnvironment::TEST_DB);
  auto tables   = static_cast<Tables*>(managers.get());

  error = tables->init();
  EXPECT_EQ(ErrorCode::OK, error);

  // add read-write table metadata.
  TableMetadataHelper::add_table(kForeignTableNameNone);

  UTUtils::print("-- confirm permission by role id --");
  error = tables->confirm_permission_in_acls(role_id, "r", res_permission);
  EXPECT_EQ(ErrorCode::NOT_FOUND, error);

  UTUtils::print("-- confirm permission by role name --");
  error = tables->confirm_permission_in_acls(kRoleName, "r", res_permission);
  EXPECT_EQ(ErrorCode::NOT_FOUND, error);

  // remove table metadata.
  TableMetadataHelper::remove_table(kForeignTableNameNone);
}

/**
 * @brief test for the case where role id or name does not exists.
 */
TEST_F(ApiTestForeignTableNotExistsPg, role_does_not_exist) {
  ErrorCode error     = ErrorCode::UNKNOWN;
  bool res_permission = false;

  // create an instance of the Tables class.
  auto managers = get_table_metadata(GlobalTestEnvironment::TEST_DB);
  auto tables   = static_cast<Tables*>(managers.get());

  error = tables->init();
  EXPECT_EQ(ErrorCode::OK, error);

  // add read-write table metadata.
  TableMetadataHelper::add_table(kForeignTableNameRw);

  ObjectIdType invalid_role_id;
  std::string role_name;

  // the role id (0) does not exist.
  invalid_role_id = 0;
  UTUtils::print("  Test pattern: [", invalid_role_id, "]");
  error = tables->confirm_permission_in_acls(0, "r", res_permission);
  EXPECT_EQ(ErrorCode::ID_NOT_FOUND, error);

  // the role id (9999999) does not exist.
  invalid_role_id = 9999999L;
  UTUtils::print("  Test pattern: [", invalid_role_id, "]");
  error = tables->confirm_permission_in_acls(invalid_role_id, "r", res_permission);
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
  TableMetadataHelper::remove_table(kForeignTableNameRw);
}

/**
 * @brief test for confirm permissions with invalid parameter.
 */
TEST_P(ApiTestTablePrivilegesInvalidPg, confirm_tables_permission) {
  ErrorCode error = ErrorCode::UNKNOWN;
  auto params     = GetParam();

  // create an instance of the Tables class.
  auto managers = get_table_metadata(GlobalTestEnvironment::TEST_DB);
  auto tables   = static_cast<Tables*>(managers.get());

  error = tables->init();
  EXPECT_EQ(ErrorCode::OK, error);

  for (auto permission : params) {
    bool actual = false;

    UTUtils::print("  Test pattern: [", permission, "]");

    // check the table permissions by role id.
    error = tables->confirm_permission_in_acls(kRoleName, permission, actual);
    EXPECT_EQ(ErrorCode::INVALID_PARAMETER, error);

    // check the table permissions by role name.
    error = tables->confirm_permission_in_acls(kRoleName, permission, actual);
    EXPECT_EQ(ErrorCode::INVALID_PARAMETER, error);
  }
}

class ApiTestForeignTableJson : public ::testing::Test {
 public:
  void SetUp() override { UTUtils::skip_if_postgresql(); }
  void TearDown() override { UTUtils::skip_if_postgresql(); }
};

/**
 * @brief Unsupported test in JSON version.
 */
TEST_F(ApiTestForeignTableJson, confirm_permission_in_acls) {
  ErrorCode error = ErrorCode::UNKNOWN;

  // create an instance of the Tables class.
  auto managers = get_table_metadata(GlobalTestEnvironment::TEST_DB);
  auto tables   = static_cast<Tables*>(managers.get());

  error = tables->init();
  EXPECT_EQ(ErrorCode::OK, error);

  UTUtils::print("-- confirm permission by role id --");
  bool res_permission = false;
  // test by role id.
  error = tables->confirm_permission_in_acls(9999, "r", res_permission);
  EXPECT_EQ(ErrorCode::NOT_SUPPORTED, error);

  UTUtils::print("-- confirm permission by role name --");
  // test by role name.
  error = tables->confirm_permission_in_acls("role_name", "r", res_permission);
  EXPECT_EQ(ErrorCode::NOT_SUPPORTED, error);
}

}  // namespace manager::metadata::testing
