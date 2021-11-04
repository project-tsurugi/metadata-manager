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

#include "manager/authentication/authentication.h"
#include "test/helper/role_metadata_helper.h"
#include "test/utility/ut_utils.h"

namespace {

constexpr std::string_view role_name = "tsurugi_api_ut_role_user_1";

}  // namespace

namespace manager::authentication::testing {

/**
 * @brief ApiTestAuthentication
 */
class ApiTestAuthentication : public ::testing::Test {
 public:
  void SetUp() override {}
  void TearDown() override {}

  /**
   * @brief Test for patterns of connection success.
   */
  static void test_authentication(const boost::property_tree::ptree& params,
                                  ErrorCode expected) {
    ErrorCode result = ErrorCode::UNKNOWN;

    UTUtils::print("  test by property tree");

    // test connect by property tree.
    result = Authentication::auth_user(params);
    EXPECT_EQ(expected, result);

    // create test data for connection string.
    std::string conn_string = "";
    for (auto pos = params.begin(); pos != params.end(); pos++) {
      boost::format key_value = boost::format("%s %s=%s") % conn_string %
                                pos->first % pos->second.data();
      conn_string = key_value.str();
    }

    UTUtils::print("  test by connection string");

    // test connect by connection string.
    result = Authentication::auth_user(conn_string);
    EXPECT_EQ(expected, result);
  }
};  // class ApiTestAuthentication

/**
 * @brief Test for patterns of connection success.
 */
TEST_F(ApiTestAuthentication, authentication_success) {
  boost::property_tree::ptree params;

  // create test data for property tree.
  params.put("host", "localhost");
  params.put("port", "5432");
  params.put("dbname", "tsurugi");
  params.put("user", role_name);
  params.put("password", "1234");
  params.put("connect_timeout", "2");

  // create dummy data for ROLE.
  boost::format role_options = boost::format("LOGIN PASSWORD '%s'") %
                               params.get<std::string>("password");
  RoleMetadataHelper::create_role(params.get<std::string>("user"),
                                  role_options.str());

  // test of host name.
  UTUtils::print("-- test of host name --");
  ApiTestAuthentication::test_authentication(params, ErrorCode::OK);

  // test of invalid host.
  params.erase("host");
  params.put("hostaddr", "127.0.0.1");
  UTUtils::print("-- test of hostaddr --");
  ApiTestAuthentication::test_authentication(params, ErrorCode::OK);

  // remove dummy data for ROLE.
  RoleMetadataHelper::drop_role(params.get<std::string>("user"));
}

/**
 * @brief Test for patterns of connection failures on invalid host.
 */
TEST_F(ApiTestAuthentication, authentication_failures_host) {
  boost::property_tree::ptree params;

  // create test data for property tree.
  params.put("host", "api_ut_dummy_host");
  params.put("port", "5432");
  params.put("dbname", "tsurugi");
  params.put("user", role_name);
  params.put("password", "1234");
  params.put("connect_timeout", "2");

  // test of invalid host.
  UTUtils::print("-- test of invalid host name --");
  ApiTestAuthentication::test_authentication(params,
                                             ErrorCode::CONNECTION_FAILURE);

  // test of invalid host.
  params.erase("host");
  params.put("hostaddr", "192.168.10.255");
  UTUtils::print("-- test of invalid hostaddr --");
  ApiTestAuthentication::test_authentication(params,
                                             ErrorCode::CONNECTION_FAILURE);
}

/**
 * @brief Test for patterns of connection failures on invalid port.
 */
TEST_F(ApiTestAuthentication, authentication_failures_port) {
  boost::property_tree::ptree params;

  // create test data for property tree.
  params.put("host", "localhost");
  params.put("port", "9999");
  params.put("dbname", "");
  params.put("user", role_name);
  params.put("password", "1234");

  // test.
  ApiTestAuthentication::test_authentication(params,
                                             ErrorCode::CONNECTION_FAILURE);
}

/**
 * @brief Test for patterns of connection failures on invalid dbname.
 */
TEST_F(ApiTestAuthentication, authentication_failures_dbname) {
  boost::property_tree::ptree params;

  // create test data for property tree.
  params.put("host", "localhost");
  params.put("port", "5432");
  params.put("dbname", "api_ut_dummy_db_name");
  params.put("user", role_name);
  params.put("password", "1234");

  // create dummy data for ROLE.
  boost::format role_options = boost::format("LOGIN PASSWORD '%s'") %
                               params.get<std::string>("password");
  RoleMetadataHelper::create_role(params.get<std::string>("user"),
                                  role_options.str());

  // test.
  ApiTestAuthentication::test_authentication(params,
                                             ErrorCode::AUTHENTICATION_FAILURE);

  // remove dummy data for ROLE.
  RoleMetadataHelper::drop_role(params.get<std::string>("user"));
}

/**
 * @brief Test for patterns of connection failures on invalid user.
 */
TEST_F(ApiTestAuthentication, authentication_failures_user) {
  boost::property_tree::ptree params;

  // create test data for property tree.
  params.put("host", "localhost");
  params.put("port", "5432");
  params.put("dbname", "tsurugi");
  params.put("user", role_name);
  params.put("password", "1234");

  // test.
  ApiTestAuthentication::test_authentication(params,
                                             ErrorCode::AUTHENTICATION_FAILURE);
}

/**
 * @brief Test for patterns of connection failures on invalid user.
 */
TEST_F(ApiTestAuthentication, authentication_failures_user_nologin) {
  boost::property_tree::ptree params;

  // create test data for property tree.
  params.put("host", "localhost");
  params.put("port", "5432");
  params.put("dbname", "tsurugi");
  params.put("user", role_name);
  params.put("password", "1234");

  // create dummy data for ROLE.
  boost::format role_options = boost::format("NOLOGIN PASSWORD '%s'") %
                               params.get<std::string>("password");
  RoleMetadataHelper::create_role(params.get<std::string>("user"),
                                  role_options.str());

  // test.
  ApiTestAuthentication::test_authentication(params,
                                             ErrorCode::AUTHENTICATION_FAILURE);

  // remove dummy data for ROLE.
  RoleMetadataHelper::drop_role(params.get<std::string>("user"));
}

/**
 * @brief Test for patterns of connection failures on invalid password.
 */
TEST_F(ApiTestAuthentication, authentication_failures_password) {
  boost::property_tree::ptree params;

  // create test data for property tree.
  params.put("host", "localhost");
  params.put("port", "5432");
  params.put("dbname", "tsurugi");
  params.put("user", role_name);
  params.put("password", "api_ut_dummy_password");

  // create dummy data for ROLE.
  boost::format role_options =
      boost::format("LOGIN PASSWORD '%s'") % "password";
  RoleMetadataHelper::create_role(params.get<std::string>("user"),
                                  role_options.str());

  // test of invalid password.
  UTUtils::print("-- test of invalid password --");
  ApiTestAuthentication::test_authentication(params,
                                             ErrorCode::AUTHENTICATION_FAILURE);

  // test of empty password.
  UTUtils::print("-- test of empty password --");
  params.erase("password");
  ApiTestAuthentication::test_authentication(params,
                                             ErrorCode::AUTHENTICATION_FAILURE);

  // remove dummy data for ROLE.
  RoleMetadataHelper::drop_role(params.get<std::string>("user"));
}

/**
 * @brief Test for patterns of connection failures on invalid password.
 */
TEST_F(ApiTestAuthentication, authentication_failures_password_not_set) {
  boost::property_tree::ptree params;

  // create test data for property tree.
  params.put("host", "localhost");
  params.put("port", "5432");
  params.put("dbname", "tsurugi");
  params.put("user", role_name);
  params.put("password", "1234");

  // create dummy data for ROLE.
  RoleMetadataHelper::create_role(params.get<std::string>("user"), "LOGIN");

  // test.
  ApiTestAuthentication::test_authentication(params,
                                             ErrorCode::AUTHENTICATION_FAILURE);

  // remove dummy data for ROLE.
  RoleMetadataHelper::drop_role(params.get<std::string>("user"));
}

/**
 * @brief Test for patterns of connection failures on invalid password.
 */
TEST_F(ApiTestAuthentication, authentication_failures_password_) {
  boost::property_tree::ptree params;

  // create test data for property tree.
  params.put("host", "localhost");
  params.put("port", "5432");
  params.put("dbname", "tsurugi");
  params.put("user", role_name);
  //  params.put("password", "1234");

  // create dummy data for ROLE.
  boost::format role_options = boost::format("LOGIN PASSWORD '%s'") % "1234";
  RoleMetadataHelper::create_role(params.get<std::string>("user"),
                                  role_options.str());

  // test.
  ApiTestAuthentication::test_authentication(params,
                                             ErrorCode::AUTHENTICATION_FAILURE);

  // remove dummy data for ROLE.
  RoleMetadataHelper::drop_role(params.get<std::string>("user"));
}

}  // namespace manager::authentication::testing
