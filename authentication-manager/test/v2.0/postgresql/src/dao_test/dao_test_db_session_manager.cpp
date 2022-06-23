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

#include <optional>
#include <string>
#include <string_view>

#include <boost/format.hpp>

#include "manager/authentication/dao/postgresql/db_session_manager.h"
#include "test/helper/role_metadata_helper.h"
#include "test/utility/ut_utils.h"

namespace manager::authentication::testing {

using boost::property_tree::ptree;
using manager::authentication::db::postgresql::DBSessionManager;

namespace role {

namespace standard {

constexpr const char* const name = "tsurugi_dao_ut_role_user_1";
constexpr const char* const pswd = "1234";

}  // namespace standard

namespace nologin {

constexpr const char* const name = "tsurugi_dao_ut_role_nologin_user";
constexpr const char* const pswd = "1234";

}  // namespace nologin

namespace nopswd {

constexpr const char* const name = "tsurugi_dao_ut_role_nopswd_user";
constexpr const char* const pswd = "1234";

}  // namespace nopswd

}  // namespace role

namespace pattern {

using TestPattern =
    std::vector<std::tuple<std::string, std::string, std::string, std::string,
                           std::string, ErrorCode>>;

/**
 * @brief Test pattern that succeeds authentication.
 */
static const TestPattern auth_success = {
    std::make_tuple("localhost", "5432", "tsurugi", role::standard::name,
                    role::standard::pswd, ErrorCode::OK)};

/**
 * @brief Test pattern that fails authentication.
 */
static const TestPattern auth_failed = {
    // invalid user_name
    std::make_tuple("localhost", "5432", "tsurugi", "dao_ut_unknown_user_name",
                    role::standard::pswd, ErrorCode::AUTHENTICATION_FAILURE),
    // invalid password
    std::make_tuple("localhost", "5432", "tsurugi", role::standard::name,
                    "dao_ut_invalid_password",
                    ErrorCode::AUTHENTICATION_FAILURE),
    // login is not allowed
    std::make_tuple("localhost", "5432", "tsurugi", role::nologin::name,
                    role::nologin::pswd, ErrorCode::AUTHENTICATION_FAILURE),
    // password not registered
    std::make_tuple("localhost", "5432", "tsurugi", role::nopswd::name,
                    role::nopswd::pswd, ErrorCode::AUTHENTICATION_FAILURE),
    // invalid db_name
    std::make_tuple("localhost", "5432", "dao_ut_invalid_db_name",
                    role::standard::name, role::standard::pswd,
                    ErrorCode::AUTHENTICATION_FAILURE),
};

/**
 * @brief Test pattern that fails to connect to DB.
 */
static const TestPattern conn_failed = {
    // invalid host
    std::make_tuple("dao_ut_invalid_host", "5432", "tsurugi",
                    role::standard::name, role::standard::pswd,
                    ErrorCode::CONNECTION_FAILURE),
    // invalid port
    std::make_tuple("localhost", "9999", "tsurugi", role::standard::name,
                    role::standard::pswd, ErrorCode::CONNECTION_FAILURE),
};

}  // namespace pattern

/**
 * @brief DaoTestDbSessionManager
 */
class DaoTestDbSessionManager
    : public ::testing::TestWithParam<pattern::TestPattern> {
 public:
  void SetUp() override {
    // create dummy data for ROLE.
    boost::format role_options =
        boost::format("LOGIN PASSWORD '%s'") % role::standard::pswd;
    RoleMetadataHelper::create_role(role::standard::name, role_options.str());

    // Roles for which login is not allowed.
    role_options = boost::format("NOLOGIN PASSWORD '%s'") % role::nologin::pswd;
    RoleMetadataHelper::create_role(role::nologin::name, role_options.str());

    // Roles for which passwords have not been set.
    RoleMetadataHelper::create_role(role::nopswd::name, "LOGIN");
  }

  void TearDown() override {
    // remove dummy data for ROLE.
    RoleMetadataHelper::drop_role(role::standard::name);
    RoleMetadataHelper::drop_role(role::nologin::name);
    RoleMetadataHelper::drop_role(role::nopswd::name);
  }
};
INSTANTIATE_TEST_CASE_P(SucceedsAuthenticationTest, DaoTestDbSessionManager,
                        ::testing::Values(pattern::auth_success));
INSTANTIATE_TEST_CASE_P(FailsAuthenticationTest, DaoTestDbSessionManager,
                        ::testing::Values(pattern::auth_failed));
INSTANTIATE_TEST_CASE_P(FailsConnectionTest, DaoTestDbSessionManager,
                        ::testing::Values(pattern::conn_failed));

/**
 * @brief Test of attempt_connection by property tree.
 */
TEST_P(DaoTestDbSessionManager, attempt_connection_ptree) {
  auto params = GetParam();

  for (auto param : params) {
    auto host = std::get<0>(param);
    auto port = std::get<1>(param);
    auto db_name = std::get<2>(param);
    auto role_name = std::get<3>(param);
    auto password = std::get<4>(param);
    auto expected = std::get<5>(param);

    auto fmter_pattern =
        boost::format(
            "host=%1%, port=%2%, db_name=%3%, role=%4%, password=%5%") %
        host % port % db_name % role_name % password;
    UTUtils::print(" Patterns of [", fmter_pattern.str(), "]");

    // test connect by property tree.
    {
      UTUtils::print("  Test by property tree.");

      // create test data for property tree.
      boost::property_tree::ptree params;
      params.put("host", host);
      params.put("port", port);
      params.put("dbname", db_name);
      params.put("user", role_name);
      params.put("password", password);
      params.put("connect_timeout", "1");

      // Calls the function under test.
      ErrorCode actual = DBSessionManager::attempt_connection(params);
      // Verify test results.
      EXPECT_EQ(expected, actual);
    }
  }
}

/**
 * @brief Test of attempt_connection by connection strings (URI pattern).
 */
TEST_P(DaoTestDbSessionManager, attempt_connection_uri) {
  auto params = GetParam();

  for (auto param : params) {
    auto host = std::get<0>(param);
    auto port = std::get<1>(param);
    auto db_name = std::get<2>(param);
    auto role_name = std::get<3>(param);
    auto password = std::get<4>(param);
    auto expected = std::get<5>(param);

    auto fmter_pattern =
        boost::format(
            "host=%1%, port=%2%, db_name=%3%, role=%4%, password=%5%") %
        host % port % db_name % role_name % password;
    UTUtils::print(" Patterns of [", fmter_pattern.str(), "]");

    // test connect by connection string (URI pattern).
    {
      UTUtils::print("  Test by connection string (URI).");

      // Create host information.
      std::string host_info = "";
      if (!host.empty()) {
        host_info += host;
        if (!port.empty()) {
          host_info += ":" + port;
        }
      }
      // Create authentication information.
      std::string auth_info = "";
      if (!role_name.empty()) {
        auth_info = role_name;
        if (!password.empty()) {
          auth_info += ":" + password;
        }
      }

      fmter_pattern =
          boost::format("postgresql://%1%%2%%3%%4%%5%?connect_timeout=1") %
          auth_info % (auth_info.empty() ? "" : "@") % host_info %
          (db_name.empty() ? "" : "/") % db_name;

      UTUtils::print("    ", fmter_pattern.str());

      // Calls the function under test.
      ErrorCode actual = DBSessionManager::attempt_connection(
          fmter_pattern.str(), std::nullopt, std::nullopt);
      // Verify test results.
      EXPECT_EQ(expected, actual);
    }
  }
}

/**
 * @brief Test of attempt_connection by connection strings and
 *   user-name/password (URI pattern).
 */
TEST_P(DaoTestDbSessionManager, attempt_connection_uri_authinfo) {
  auto params = GetParam();

  for (auto param : params) {
    auto host = std::get<0>(param);
    auto port = std::get<1>(param);
    auto db_name = std::get<2>(param);
    auto role_name = std::get<3>(param);
    auto password = std::get<4>(param);
    auto expected = std::get<5>(param);

    auto fmter_pattern =
        boost::format(
            "host=%1%, port=%2%, db_name=%3%, role=%4%, password=%5%") %
        host % port % db_name % role_name % password;
    UTUtils::print(" Patterns of [", fmter_pattern.str(), "]");

    // test connect by connection string and user-name/password (URI pattern).
    {
      UTUtils::print(
          "  Test by connection string and user-name/password (URI).");

      // Create host information.
      std::string host_info = "";
      if (!host.empty()) {
        host_info += host;
        if (!port.empty()) {
          host_info += ":" + port;
        }
      }

      fmter_pattern =
          boost::format("postgresql://%1%%2%%3%?connect_timeout=1") %
          host_info % (db_name.empty() ? "" : "/") % db_name;
      std::optional<std::string> param_user =
          (!role_name.empty() ? std::optional(role_name) : std::nullopt);
      std::optional<std::string> param_pswd =
          (!password.empty() ? std::optional(password) : std::nullopt);

      UTUtils::print("    ", fmter_pattern.str(), ", ",
                     param_user.value_or("<nullopt>"), ", ",
                     param_pswd.value_or("<nullopt>"));
      // Calls the function under test.
      ErrorCode actual = DBSessionManager::attempt_connection(
          fmter_pattern.str(), param_user, param_pswd);
      // Verify test results.
      EXPECT_EQ(expected, actual);
    }
  }
}

/**
 * @brief Test of attempt_connection by connection strings (key/value pattern).
 */
TEST_P(DaoTestDbSessionManager, attempt_connection_key_value) {
  auto params = GetParam();

  for (auto param : params) {
    auto host = std::get<0>(param);
    auto port = std::get<1>(param);
    auto db_name = std::get<2>(param);
    auto role_name = std::get<3>(param);
    auto password = std::get<4>(param);
    auto expected = std::get<5>(param);

    auto fmter_pattern =
        boost::format(
            "host=%1%, port=%2%, db_name=%3%, role=%4%, password=%5%") %
        host % port % db_name % role_name % password;
    UTUtils::print(" Patterns of [", fmter_pattern.str(), "]");

    // test connect by connection string (key/value pattern).
    {
      UTUtils::print("  Test by connection string (key/value).");

      fmter_pattern =
          boost::format("%1% %2% %3% %4% %5% connect_timeout=1") %
          (!host.empty() ? "host=" + std::string(host) : "") %
          (!port.empty() ? "port=" + std::string(port) : "") %
          (!db_name.empty() ? "dbname=" + std::string(db_name) : "") %
          (!role_name.empty() ? "user=" + std::string(role_name) : "") %
          (!password.empty() ? "password=" + std::string(password) : "");

      UTUtils::print("    ", fmter_pattern.str());

      // Calls the function under test.
      ErrorCode actual = DBSessionManager::attempt_connection(
          fmter_pattern.str(), std::nullopt, std::nullopt);
      // Verify test results.
      EXPECT_EQ(expected, actual);
    }
  }
}

/**
 * @brief Test of attempt_connection by connection strings and
 *   user-name/password (key/value pattern).
 */
TEST_P(DaoTestDbSessionManager, attempt_connection_key_value_authinfo) {
  auto params = GetParam();

  for (auto param : params) {
    auto host = std::get<0>(param);
    auto port = std::get<1>(param);
    auto db_name = std::get<2>(param);
    auto role_name = std::get<3>(param);
    auto password = std::get<4>(param);
    auto expected = std::get<5>(param);

    auto fmter_pattern =
        boost::format(
            "host=%1%, port=%2%, db_name=%3%, role=%4%, password=%5%") %
        host % port % db_name % role_name % password;
    UTUtils::print(" Patterns of [", fmter_pattern.str(), "]");

    // test connect by connection string and user-name/password
    // (key/value pattern).
    {
      UTUtils::print(
          "  Test by connection string and user-name/password (key/value).");
      fmter_pattern =
          boost::format("%1% %2% %3% connect_timeout=1") %
          (!host.empty() ? "host=" + std::string(host) : "") %
          (!port.empty() ? "port=" + std::string(port) : "") %
          (!db_name.empty() ? "dbname=" + std::string(db_name) : "");
      std::optional<std::string> param_user =
          (!role_name.empty() ? std::optional(role_name) : std::nullopt);
      std::optional<std::string> param_pswd =
          (!password.empty() ? std::optional(password) : std::nullopt);

      UTUtils::print("    ", fmter_pattern.str(), ", ",
                     param_user.value_or("<nullopt>"), ", ",
                     param_pswd.value_or("<nullopt>"));

      // Calls the function under test.
      ErrorCode actual = DBSessionManager::attempt_connection(
          fmter_pattern.str(), param_user, param_pswd);
      // Verify test results.
      EXPECT_EQ(expected, actual);
    }
  }
}

/**
 * @brief Test pattern with empty parameters.
 */
TEST_F(DaoTestDbSessionManager, attempt_connection_param_empty) {
  ErrorCode actual = ErrorCode::UNKNOWN;

  actual = DBSessionManager::attempt_connection("postgresql://", std::nullopt,
                                                std::nullopt);
  EXPECT_EQ(ErrorCode::OK, actual);

  // Calls the function under test.
  actual = DBSessionManager::attempt_connection(
      "postgresql://", std::optional(""), std::optional(""));
  // Verify test results.
  EXPECT_EQ(ErrorCode::OK, actual);

  // Calls the function under test.
  actual = DBSessionManager::attempt_connection("", std::nullopt, std::nullopt);
  // Verify test results.
  EXPECT_EQ(ErrorCode::OK, actual);

  // Calls the function under test.
  actual = DBSessionManager::attempt_connection("", std::optional(""),
                                                std::optional(""));
  // Verify test results.
  EXPECT_EQ(ErrorCode::OK, actual);
}

/**
 * @brief Testing hostaddr patterns.
 */
TEST_F(DaoTestDbSessionManager, patterns_hostaddr) {
  ErrorCode actual = ErrorCode::UNKNOWN;

  // create test data for property tree.
  boost::property_tree::ptree params;
  params.put("hostaddr", "127.0.0.1");
  params.put("port", "5432");
  params.put("dbname", "tsurugi");
  params.put("user", role::standard::name);
  params.put("password", role::standard::pswd);
  params.put("connect_timeout", "1");

  // test connect by property tree.
  {
    UTUtils::print("  test by property tree");

    // Calls the function under test.
    actual = DBSessionManager::attempt_connection(params);
    // Verify test results.
    EXPECT_EQ(ErrorCode::OK, actual);
  }

  // test connect by connection string.
  {
    UTUtils::print("  test by connection string");

    // create test data for connection string.
    std::string conn_string = "";
    for (auto pos = params.begin(); pos != params.end(); pos++) {
      boost::format key_value = boost::format("%s %s=%s") % conn_string %
                                pos->first % pos->second.data();
      conn_string = key_value.str();
    }
    // Calls the function under test.
    actual = DBSessionManager::attempt_connection(conn_string, std::nullopt,
                                                  std::nullopt);
    // Verify test results.
    EXPECT_EQ(ErrorCode::OK, actual);
  }
}

}  // namespace manager::authentication::testing
