/*
 * Copyright 2020-2022 tsurugi project.
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

#include "test/common/global_test_environment.h"
#include "test/common/ut_utils.h"
#include "test/helper/postgresql/foreign_table_helper_pg.h"
#include "test/helper/postgresql/role_metadata_helper_pg.h"
#include "test/helper/table_metadata_helper.h"
#include "test/helper/token_helper.h"

namespace {

namespace foreign_table_1 {

constexpr std::string_view table_name = "tsurugi_api_ut_foreign_table_1";
std::int32_t table_id                 = 0;
std::int32_t foreign_table_id         = 0;

}  // namespace foreign_table_1

namespace foreign_table_2 {

constexpr std::string_view table_name = "tsurugi_api_ut_foreign_table_2";
std::int32_t table_id                 = 0;
std::int32_t foreign_table_id         = 0;

}  // namespace foreign_table_2

namespace foreign_table_3 {

constexpr std::string_view table_name = "tsurugi_api_ut_foreign_table_3";
std::int32_t table_id                 = 0;
std::int32_t foreign_table_id         = 0;

}  // namespace foreign_table_3

namespace role_1 {

constexpr std::string_view role_name = "tsurugi_api_ut_tables_user_1";
std::int32_t role_id                 = 0;

}  // namespace role_1

namespace role_2 {

constexpr std::string_view role_name = "tsurugi_api_ut_tables_user_2";
std::int32_t role_id                 = 0;

}  // namespace role_2

namespace role_3 {

constexpr std::string_view role_name = "tsurugi_api_ut_tables_user_3";
std::int32_t role_id                 = 0;

}  // namespace role_3

}  // namespace

namespace manager::metadata::testing {

using boost::property_tree::ptree;

class ApiTestTableAcls : public ::testing::Test {
 public:
  void SetUp() override {
    UTUtils::skip_if_connection_not_opened();

    test_setup();
  }

  void TearDown() override {
    if (global->is_open()) {
      test_teardown();
    }
  }

 private:
  /**
   * @brief setup the data for testing.
   */
  static void test_setup() {
    // create dummy data for ROLE.
    role_1::role_id = RoleMetadataHelper::create_role(role_1::role_name, "");
    role_2::role_id = RoleMetadataHelper::create_role(role_2::role_name, "");
    role_3::role_id = RoleMetadataHelper::create_role(role_3::role_name, "");

    // (role-1) create dummy data for TABLE.
    foreign_table_1::table_id = ForeignTableHelper::create_table(
        foreign_table_1::table_name, role_1::role_name, "SELECT");
    foreign_table_2::table_id = ForeignTableHelper::create_table(
        foreign_table_2::table_name, role_1::role_name,
        "SELECT,INSERT,UPDATE,DELETE");
    foreign_table_3::table_id = ForeignTableHelper::create_table(
        foreign_table_3::table_name, role_1::role_name, "");

    // (role-2) grant dummy data for TABLE.
    ForeignTableHelper::grant_table(foreign_table_1::table_name,
                                    role_2::role_name,
                                    "SELECT,INSERT,UPDATE,DELETE");
    ForeignTableHelper::grant_table(foreign_table_2::table_name,
                                    role_2::role_name, "SELECT");
    ForeignTableHelper::grant_table(foreign_table_3::table_name,
                                    role_2::role_name, "SELECT,UPDATE");

    // create dummy data for pg_foreign_table.
    foreign_table_1::foreign_table_id =
        ForeignTableHelper::insert_foreign_table(foreign_table_1::table_name);
    foreign_table_2::foreign_table_id =
        ForeignTableHelper::insert_foreign_table(foreign_table_2::table_name);
    foreign_table_3::foreign_table_id =
        ForeignTableHelper::insert_foreign_table(foreign_table_3::table_name);

    // create dummy data for table metadata.
    TableMetadataHelper::add_table(foreign_table_1::table_name);
    TableMetadataHelper::add_table(foreign_table_2::table_name);
    TableMetadataHelper::add_table(foreign_table_3::table_name);
  }

  /**
   * @brief discard the data for testing.
   */
  static void test_teardown() {
    // remove dummy data for table metadata.
    TableMetadataHelper::remove_table(foreign_table_1::table_name);
    TableMetadataHelper::remove_table(foreign_table_2::table_name);
    TableMetadataHelper::remove_table(foreign_table_3::table_name);

    // remove dummy data for pg_foreign_table.
    ForeignTableHelper::delete_foreign_table(foreign_table_1::foreign_table_id);
    ForeignTableHelper::delete_foreign_table(foreign_table_2::foreign_table_id);
    ForeignTableHelper::delete_foreign_table(foreign_table_3::foreign_table_id);

    // remove dummy data for TABLE.
    ForeignTableHelper::drop_table(foreign_table_1::table_name);
    ForeignTableHelper::drop_table(foreign_table_2::table_name);
    ForeignTableHelper::drop_table(foreign_table_3::table_name);

    // remove dummy data for ROLE.
    RoleMetadataHelper::drop_role(role_1::role_name);
    RoleMetadataHelper::drop_role(role_2::role_name);
    RoleMetadataHelper::drop_role(role_3::role_name);
  }
};

/**
 * @brief This test is to retrieve pre-defined role names and privileges.
 */
TEST_F(ApiTestTableAcls, get_acl) {
  // Generation of table metadata management.
  auto tables = std::make_unique<Tables>(GlobalTestEnvironment::TEST_DB);
  // initialize.
  ErrorCode error = tables->init();
  EXPECT_EQ(ErrorCode::OK, error);

  {
    ptree table_metadata;
    std::string token_string = "";

    UTUtils::print("-- get acls -- [", role_1::role_name, "]");
    token_string = TokenHelper::generate_token(role_1::role_name, 300);
    error        = tables->get_acls(token_string, table_metadata);
    EXPECT_EQ(ErrorCode::OK, error);

    if (error == ErrorCode::OK) {
      UTUtils::print(" ", UTUtils::get_tree_string(table_metadata));

      std::map<std::string_view, std::string_view> acls_expected;
      acls_expected[foreign_table_1::table_name] = "r";
      acls_expected[foreign_table_2::table_name] = "arwd";
      acls_expected[foreign_table_3::table_name] = "";

      TableMetadataHelper::check_table_acls_expected(acls_expected,
                                                     table_metadata);
    }
  }

  {
    ptree table_metadata;
    std::string token_string = "";

    UTUtils::print("-- get acls -- [", role_2::role_name, "]");
    token_string = TokenHelper::generate_token(role_2::role_name, 300);
    error        = tables->get_acls(token_string, table_metadata);
    EXPECT_EQ(ErrorCode::OK, error);

    if (error == ErrorCode::OK) {
      UTUtils::print(" ", UTUtils::get_tree_string(table_metadata));

      std::map<std::string_view, std::string_view> acls_expected;
      acls_expected[foreign_table_1::table_name] = "arwd";
      acls_expected[foreign_table_2::table_name] = "r";
      acls_expected[foreign_table_3::table_name] = "rw";

      TableMetadataHelper::check_table_acls_expected(acls_expected,
                                                     table_metadata);
    }
  }

  {
    ptree table_metadata;
    std::string token_string = "";

    UTUtils::print("-- get acls -- [", role_3::role_name, "]");
    token_string = TokenHelper::generate_token(role_3::role_name, 300);
    error        = tables->get_acls(token_string, table_metadata);
    EXPECT_EQ(ErrorCode::OK, error);

    if (error == ErrorCode::OK) {
      UTUtils::print(" ", UTUtils::get_tree_string(table_metadata));

      std::map<std::string_view, std::string_view> acls_expected;
      acls_expected[foreign_table_1::table_name] = "";
      acls_expected[foreign_table_2::table_name] = "";
      acls_expected[foreign_table_3::table_name] = "";

      TableMetadataHelper::check_table_acls_expected(acls_expected,
                                                     table_metadata);
    }
  }
}

/**
 * @brief This is a test of obtaining privileges for unregistered users.
 */
TEST_F(ApiTestTableAcls, get_acl_unknown_user) {
  // Generation of table metadata management.
  auto tables = std::make_unique<Tables>(GlobalTestEnvironment::TEST_DB);
  // initialize.
  ErrorCode error = tables->init();
  EXPECT_EQ(ErrorCode::OK, error);

  {
    ptree table_metadata;
    std::string token_string = "";

    UTUtils::print("-- get acls -- [unknown_user]");
    token_string = TokenHelper::generate_token("unknown_user", 300);
    error        = tables->get_acls(token_string, table_metadata);
    EXPECT_EQ(ErrorCode::NAME_NOT_FOUND, error);
  }
}

/**
 * @brief This is a test for obtaining privileges when the
 *   access token is invalid.
 */
TEST_F(ApiTestTableAcls, get_acl_token_invalid) {
  // Generation of table metadata management.
  auto tables = std::make_unique<Tables>(GlobalTestEnvironment::TEST_DB);
  // initialize.
  ErrorCode error = tables->init();
  EXPECT_EQ(ErrorCode::OK, error);

  {
    ptree table_metadata;
    std::string token_string = "";

    UTUtils::print("-- get acls -- [", role_1::role_name, "]");
    token_string =
        TokenHelper::generate_token(role_1::role_name, 300) + "invalid";
    error = tables->get_acls(token_string, table_metadata);
    EXPECT_EQ(ErrorCode::INVALID_PARAMETER, error);
  }
}

/**
 * @brief This is a test for obtaining privileges when the
 *   access token is expired.
 */
TEST_F(ApiTestTableAcls, get_acl_expired) {
  // Generation of table metadata management.
  auto tables = std::make_unique<Tables>(GlobalTestEnvironment::TEST_DB);
  // initialize.
  ErrorCode error = tables->init();
  EXPECT_EQ(ErrorCode::OK, error);

  {
    ptree table_metadata;
    std::string token_string = "";

    UTUtils::print("-- get acls -- [", role_1::role_name, "]");
    token_string = TokenHelper::generate_token(role_1::role_name, -60);
    error        = tables->get_acls(token_string, table_metadata);
    EXPECT_EQ(ErrorCode::INVALID_PARAMETER, error);
  }
}

}  // namespace manager::metadata::testing
