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

namespace {

namespace role_1 {

constexpr const char* const name = "tsurugi_dao_ut_role_user_1";
constexpr const char* const password = "1234";

}  // namespace role_1

namespace role_2 {

constexpr const char* const name = "tsurugi_dao_ut_role_user_2";
constexpr const char* const password = "1234";

}  // namespace role_2

namespace role_3 {

constexpr const char* const name = "tsurugi_dao_ut_role_user_3";
constexpr const char* const password = "1234";

}  // namespace role_3

}  // namespace

namespace manager::authentication::testing {

using boost::property_tree::ptree;
using manager::authentication::db::postgresql::DBSessionManager;

/**
 * @brief DaoTestDbSessionManager
 */
class DaoTestDbSessionManager
    : public ::testing::TestWithParam<
          std::vector<std::tuple<const char*, const char*, const char*,
                                 const char*, const char*, ErrorCode>>> {
 public:
  void SetUp() override {
    // create dummy data for ROLE.
    boost::format role_options =
        boost::format("LOGIN PASSWORD '%s'") % role_1::password;
    RoleMetadataHelper::create_role(role_1::name, role_options.str());

    // Roles for which login is not allowed.
    role_options = boost::format("NOLOGIN PASSWORD '%s'") % role_2::password;
    RoleMetadataHelper::create_role(role_2::name, role_options.str());

    // Roles for which passwords have not been set.
    RoleMetadataHelper::create_role(role_3::name, "LOGIN");
  }

  void TearDown() override {
    // remove dummy data for ROLE.
    RoleMetadataHelper::drop_role(role_1::name);
    RoleMetadataHelper::drop_role(role_2::name);
    RoleMetadataHelper::drop_role(role_3::name);
  }
};

/**
 * @brief Test pattern that succeeds authentication.
 */
std::vector<std::tuple<const char*, const char*, const char*, const char*,
                       const char*, ErrorCode>>
    test_pattern_success = {std::make_tuple("localhost", "5432", "tsurugi",
                                            role_1::name, role_1::password,
                                            ErrorCode::OK)};

/**
 * @brief Test pattern that fails authentication.
 */
std::vector<std::tuple<const char*, const char*, const char*, const char*,
                       const char*, ErrorCode>>
    test_pattern_auth_failed = {
        std::make_tuple("localhost", "5432", "tsurugi",
                        "dao_ut_unknown_user_name", role_1::password,
                        ErrorCode::AUTHENTICATION_FAILURE),
        std::make_tuple("localhost", "5432", "tsurugi", role_1::name,
                        "dao_ut_invalid_password",
                        ErrorCode::AUTHENTICATION_FAILURE),
        std::make_tuple("localhost", "5432", "tsurugi", role_2::name,
                        role_2::password, ErrorCode::AUTHENTICATION_FAILURE),
        std::make_tuple("localhost", "5432", "tsurugi", role_3::name,
                        role_3::password, ErrorCode::AUTHENTICATION_FAILURE),
        std::make_tuple("localhost", "5432", "dao_ut_invalid_db_name",
                        role_1::name, role_1::password,
                        ErrorCode::AUTHENTICATION_FAILURE)};

/**
 * @brief Test pattern that fails to connect to DB.
 */
std::vector<std::tuple<const char*, const char*, const char*, const char*,
                       const char*, ErrorCode>>
    test_pattern_conn_failed = {
        std::make_tuple("dao_ut_invalid_host", "5432", "tsurugi", role_1::name,
                        role_1::password, ErrorCode::CONNECTION_FAILURE),
        std::make_tuple("localhost", "9999", "tsurugi", role_1::name,
                        role_1::password, ErrorCode::CONNECTION_FAILURE)};

/**
 * @brief Test pattern for undefined parameters (null).
 */
std::vector<std::tuple<const char*, const char*, const char*, const char*,
                       const char*, ErrorCode>>
    test_pattern_param_error = {
        std::make_tuple(nullptr, "5432", "tsurugi", role_1::name,
                        role_1::password, ErrorCode::OK),
        std::make_tuple("localhost", nullptr, "tsurugi", role_1::name,
                        role_1::password, ErrorCode::OK),
        std::make_tuple("localhost", "5432", nullptr, role_1::name,
                        role_1::password, ErrorCode::AUTHENTICATION_FAILURE),
        std::make_tuple("localhost", "5432", "tsurugi", nullptr,
                        role_1::password, ErrorCode::AUTHENTICATION_FAILURE),
        std::make_tuple("localhost", "5432", "tsurugi", role_1::name, nullptr,
                        ErrorCode::AUTHENTICATION_FAILURE)};

INSTANTIATE_TEST_CASE_P(SucceedsAuthenticationTest, DaoTestDbSessionManager,
                        ::testing::Values(test_pattern_success));
INSTANTIATE_TEST_CASE_P(FailsAuthenticationTest, DaoTestDbSessionManager,
                        ::testing::Values(test_pattern_auth_failed));
INSTANTIATE_TEST_CASE_P(FailsConnectionTest, DaoTestDbSessionManager,
                        ::testing::Values(test_pattern_conn_failed));
INSTANTIATE_TEST_CASE_P(UndefinedParameterTest, DaoTestDbSessionManager,
                        ::testing::Values(test_pattern_param_error));

/**
 * @brief Testing by pattern list.
 */
TEST_P(DaoTestDbSessionManager, attempt_connection_pattern) {
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
        (host == nullptr ? "<null>" : host) %
        (port == nullptr ? "<null>" : port) %
        (db_name == nullptr ? "<null>" : db_name) %
        (role_name == nullptr ? "<null>" : role_name) %
        (password == nullptr ? "<null>" : password);
    UTUtils::print(" Patterns of [", fmter_pattern.str(), "]");

    // test connect by property tree.
    {
      UTUtils::print("  Test by property tree.");

      // create test data for property tree.
      boost::property_tree::ptree params;
      if (host != nullptr) {
        params.put("host", host);
      }
      if (port != nullptr) {
        params.put("port", port);
      }
      if (db_name != nullptr) {
        params.put("dbname", db_name);
      }
      if (role_name != nullptr) {
        params.put("user", role_name);
      }
      if (password != nullptr) {
        params.put("password", password);
      }
      params.put("connect_timeout", "1");

      ErrorCode actual = DBSessionManager::attempt_connection(params);
      EXPECT_EQ(expected, actual);
    }

    // test connect by connection string (URI pattern).
    {
      UTUtils::print("  Test by connection string (URI).");

      // Create host information.
      std::string host_info = "";
      if (host != nullptr) {
        host_info += std::string(host);
        if (port != nullptr) {
          host_info += ":" + std::string(port);
        }
      }
      // Create authentication information.
      std::string auth_info = "";
      if (role_name != nullptr) {
        auth_info += std::string(role_name);
        if (password != nullptr) {
          auth_info += ":" + std::string(password);
        }
      }

      fmter_pattern =
          boost::format("postgresql://%1%%2%%3%%4%%5%?connect_timeout=1") %
          auth_info % (auth_info.empty() ? "" : "@") % host_info %
          (db_name == nullptr ? "" : "/") % db_name;

      UTUtils::print("    ", fmter_pattern.str());
      ErrorCode actual = DBSessionManager::attempt_connection(
          fmter_pattern.str(), std::nullopt, std::nullopt);
      EXPECT_EQ(expected, actual);
    }

    // test connect by connection string and user-name/password (URI pattern).
    {
      UTUtils::print(
          "  Test by connection string and user-name/password (URI).");

      // Create host information.
      std::string host_info = "";
      if (host != nullptr) {
        host_info += std::string(host);
        if (port != nullptr) {
          host_info += ":" + std::string(port);
        }
      }

      fmter_pattern =
          boost::format("postgresql://%1%%2%%3%?connect_timeout=1") %
          host_info % (db_name == nullptr ? "" : "/") % db_name;
      std::optional<std::string> param_user =
          (role_name != nullptr ? std::optional(role_name) : std::nullopt);
      std::optional<std::string> param_pswd =
          (password != nullptr ? std::optional(password) : std::nullopt);

      UTUtils::print("    ", fmter_pattern.str(), ", ",
                     param_user.value_or("<nullopt>"), ", ",
                     param_pswd.value_or("<nullopt>"));
      ErrorCode actual = DBSessionManager::attempt_connection(
          fmter_pattern.str(), param_user, param_pswd);
      EXPECT_EQ(expected, actual);
    }

    // test connect by connection string (key/value pattern).
    {
      UTUtils::print("  Test by connection string (key/value).");

      fmter_pattern =
          boost::format("%1% %2% %3% %4% %5% connect_timeout=1") %
          (host != nullptr ? "host=" + std::string(host) : "") %
          (port != nullptr ? "port=" + std::string(port) : "") %
          (db_name != nullptr ? "dbname=" + std::string(db_name) : "") %
          (role_name != nullptr ? "user=" + std::string(role_name) : "") %
          (password != nullptr ? "password=" + std::string(password) : "");

      UTUtils::print("    ", fmter_pattern.str());
      ErrorCode actual = DBSessionManager::attempt_connection(
          fmter_pattern.str(), std::nullopt, std::nullopt);
      EXPECT_EQ(expected, actual);
    }

    // test connect by connection string and user-name/password
    // (key/value pattern).
    {
      UTUtils::print(
          "  Test by connection string and user-name/password (key/value).");
      fmter_pattern =
          boost::format("%1% %2% %3% connect_timeout=1") %
          (host != nullptr ? "host=" + std::string(host) : "") %
          (port != nullptr ? "port=" + std::string(port) : "") %
          (db_name != nullptr ? "dbname=" + std::string(db_name) : "");
      std::optional<std::string> param_user =
          (role_name != nullptr ? std::optional(role_name) : std::nullopt);
      std::optional<std::string> param_pswd =
          (password != nullptr ? std::optional(password) : std::nullopt);

      UTUtils::print("    ", fmter_pattern.str(), ", ",
                     param_user.value_or("<nullopt>"), ", ",
                     param_pswd.value_or("<nullopt>"));
      ErrorCode actual = DBSessionManager::attempt_connection(
          fmter_pattern.str(), param_user, param_pswd);
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
  EXPECT_EQ(ErrorCode::AUTHENTICATION_FAILURE, actual);

  actual = DBSessionManager::attempt_connection(
      "postgresql://", std::optional(""), std::optional(""));
  EXPECT_EQ(ErrorCode::AUTHENTICATION_FAILURE, actual);

  actual = DBSessionManager::attempt_connection("", std::nullopt, std::nullopt);
  EXPECT_EQ(ErrorCode::AUTHENTICATION_FAILURE, actual);

  actual = DBSessionManager::attempt_connection("", std::optional(""),
                                                std::optional(""));
  EXPECT_EQ(ErrorCode::AUTHENTICATION_FAILURE, actual);
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
  params.put("user", role_1::name);
  params.put("password", role_1::password);
  params.put("connect_timeout", "1");

  // test connect by property tree.
  {
    UTUtils::print("  test by property tree");

    actual = DBSessionManager::attempt_connection(params);
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
    // test connect by connection string.
    actual = DBSessionManager::attempt_connection(conn_string, std::nullopt,
                                                  std::nullopt);
    EXPECT_EQ(ErrorCode::OK, actual);
  }
}

}  // namespace manager::authentication::testing
