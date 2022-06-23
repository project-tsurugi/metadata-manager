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
#include <regex>
#include <string>

#include <boost/format.hpp>
#include "jwt-cpp/jwt.h"

#include "manager/authentication/authentication.h"
#include "manager/authentication/common/config.h"
#include "manager/authentication/common/jwt_claims.h"
#include "test/helper/role_metadata_helper.h"
#include "test/helper/token_helper.h"
#include "test/utility/ut_utils.h"

namespace manager::authentication::testing {

namespace role {

namespace standard {

constexpr const char* const name = "tsurugi_api_ut_role_user";
constexpr const char* const pswd = "1234";

}  // namespace standard

namespace nologin {

constexpr const char* const name = "tsurugi_api_ut_role_nologin_user";
constexpr const char* const pswd = "1234";

}  // namespace nologin

namespace nopswd {

constexpr const char* const name = "tsurugi_api_ut_role_nopswd_user";
constexpr const char* const pswd = "1234";

}  // namespace nopswd

}  // namespace role

namespace pattern {

namespace auth {

using TestPattern =
    std::vector<std::tuple<std::string, std::string, std::string, std::string,
                           std::string, ErrorCode>>;

/**
 * @brief Test pattern that succeeds authentication.
 */
static const TestPattern auth_success = {
    // standard
    std::make_tuple("localhost", "5432", "tsurugi", role::standard::name,
                    role::standard::pswd, ErrorCode::OK),
};

/**
 * @brief Test pattern that fails authentication.
 */
static const TestPattern auth_failed = {
    // invalid user_name
    std::make_tuple("localhost", "5432", "tsurugi", "api_ut_unknown_user_name",
                    role::standard::pswd, ErrorCode::AUTHENTICATION_FAILURE),
    // invalid password
    std::make_tuple("localhost", "5432", "tsurugi", role::standard::name,
                    "api_ut_invalid_password",
                    ErrorCode::AUTHENTICATION_FAILURE),
    // login is not allowed
    std::make_tuple("localhost", "5432", "tsurugi", role::nologin::name,
                    role::nologin::pswd, ErrorCode::AUTHENTICATION_FAILURE),
    // password not registered
    std::make_tuple("localhost", "5432", "tsurugi", role::nopswd::name,
                    role::nopswd::pswd, ErrorCode::AUTHENTICATION_FAILURE),
    // invalid db_name
    std::make_tuple("localhost", "5432", "api_ut_invalid_db_name",
                    role::standard::name, role::standard::pswd,
                    ErrorCode::AUTHENTICATION_FAILURE),
};

/**
 * @brief Test pattern that fails to connect to DB.
 */
static const TestPattern conn_failed = {
    // invalid host
    std::make_tuple("api_ut_invalid_host", "5432", "tsurugi",
                    role::standard::name, role::standard::pswd,
                    ErrorCode::CONNECTION_FAILURE),
    // invalid port
    std::make_tuple("localhost", "9999", "tsurugi", role::standard::name,
                    role::standard::pswd, ErrorCode::CONNECTION_FAILURE),
};

}  // namespace auth

namespace token {

using TestPattern =
    std::vector<std::tuple<std::string, std::string, ErrorCode>>;

/**
 * @brief Test pattern that succeeds authentication.
 */
static const TestPattern auth_success = {
    // standard
    std::make_tuple(role::standard::name, role::standard::pswd, ErrorCode::OK),
};

/**
 * @brief Test pattern that fails authentication.
 */
static const TestPattern auth_failed = {
    // invalid user_name
    std::make_tuple("api_ut_unknown_user_name", role::standard::pswd,
                    ErrorCode::AUTHENTICATION_FAILURE),
    // invalid password
    std::make_tuple(role::standard::name, "api_ut_invalid_password",
                    ErrorCode::AUTHENTICATION_FAILURE),
    // login is not allowed
    std::make_tuple(role::nologin::name, role::nologin::pswd,
                    ErrorCode::AUTHENTICATION_FAILURE),
    // password not registered
    std::make_tuple(role::nopswd::name, role::nopswd::pswd,
                    ErrorCode::AUTHENTICATION_FAILURE),
    // empty user_name
    std::make_tuple("", role::standard::pswd,
                    ErrorCode::AUTHENTICATION_FAILURE),
    // empty password
    std::make_tuple(role::standard::name, "",
                    ErrorCode::AUTHENTICATION_FAILURE),
};

}  // namespace token

}  // namespace pattern

/**
 * @brief ApiTestAuthentication
 */
class ApiTestAuthentication
    : public ::testing::TestWithParam<pattern::auth::TestPattern> {
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
};  // class ApiTestAuthentication
INSTANTIATE_TEST_CASE_P(SucceedsAuthenticationTest, ApiTestAuthentication,
                        ::testing::Values(pattern::auth::auth_success));
INSTANTIATE_TEST_CASE_P(FailsAuthenticationTest, ApiTestAuthentication,
                        ::testing::Values(pattern::auth::auth_failed));
INSTANTIATE_TEST_CASE_P(FailsConnectionTest, ApiTestAuthentication,
                        ::testing::Values(pattern::auth::conn_failed));

/**
 * @brief ApiTestAuthentication
 */
class ApiTestAuthenticationToken
    : public ::testing::TestWithParam<pattern::token::TestPattern> {
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

  static void VerifyToken(std::string_view token_string) {
    // Decode the access token.
    auto decoded_token = jwt::decode(std::string(token_string));

    // Cryptographic algorithms.
    auto algorithm = jwt::algorithm::hs256{Config::get_jwt_secret_key()};
    // Setting up data for token.
    auto verifier = jwt::verify().allow_algorithm(algorithm).expires_at_leeway(
        Token::Leeway::kExpiration);
    // Verify the JWT token.
    verifier.verify(decoded_token);
  }

  static void CheckToken(std::string_view token_string,
                         std::string_view expected_user_name) {
    ASSERT_FALSE(token_string.empty());
    ASSERT_NO_THROW(VerifyToken(token_string));

    auto now_time = std::chrono::system_clock::now();
    time_t expected_now = std::chrono::system_clock::to_time_t(now_time);

    // Decode the access token.
    auto decoded_token = jwt::decode(std::string(token_string));

    // Check if algortihm is correct ("alg").
    EXPECT_EQ("HS256", decoded_token.get_algorithm());

    // Check if type is correct ("typ").
    EXPECT_EQ(Token::Header::kType, decoded_token.get_type());

    // Check if issuer is correct ("iss").
    EXPECT_EQ(Config::get_jwt_issuer(), decoded_token.get_issuer());

    // Check if audience is correct ("aud").
    for (auto audience : decoded_token.get_audience()) {
      EXPECT_EQ(Config::get_jwt_audience(), audience);
    }

    // Check if subject is correct ("sub").
    EXPECT_EQ(Config::get_jwt_subject(), decoded_token.get_subject());

    // Check if  issued date is correct ("iat").
    time_t actual_iat =
        std::chrono::system_clock::to_time_t(decoded_token.get_issued_at());
    EXPECT_NEAR(expected_now, actual_iat, 1);

    // Check if expires date is correct ("exp").
    time_t actual_exp =
        std::chrono::system_clock::to_time_t(decoded_token.get_expires_at());
    time_t expected_exp = expected_now + Config::get_jwt_expiration();
    EXPECT_NEAR(expected_exp, actual_exp, 1);

    // Check if a payload claim is correct (User Name).
    EXPECT_EQ(std::string(expected_user_name),
              decoded_token.get_payload_claim(Token::Payload::kAuthUserName)
                  .as_string());

    // Check if a payload claim is correct (Refresh Expiration).
    time_t actual_refresh = std::chrono::system_clock::to_time_t(
        decoded_token.get_payload_claim(Token::Payload::kExpirationRefresh)
            .as_date());
    time_t expected_refresh =
        expected_now + Config::get_jwt_expiration_refresh();
    EXPECT_NEAR(expected_refresh, actual_refresh, 1);

    // Check if a payload claim is correct (Token Use Expiration).
    time_t actual_available = std::chrono::system_clock::to_time_t(
        decoded_token.get_payload_claim(Token::Payload::kExpirationAvailable)
            .as_date());
    time_t expected_available =
        expected_now + Config::get_jwt_expiration_available();
    EXPECT_NEAR(expected_available, actual_available, 1);
  }
};  // class ApiTestAuthenticationToken
INSTANTIATE_TEST_CASE_P(SucceedsAuthenticationTest, ApiTestAuthenticationToken,
                        ::testing::Values(pattern::token::auth_success));
INSTANTIATE_TEST_CASE_P(FailsAuthenticationTest, ApiTestAuthenticationToken,
                        ::testing::Values(pattern::token::auth_failed));

/**
 * @brief Test of authentication by property tree.
 */
TEST_P(ApiTestAuthentication, auth_user_ptree) {
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

    // create test data for property tree.
    boost::property_tree::ptree params;
    params.put("host", host);
    params.put("port", port);
    params.put("dbname", db_name);
    params.put("user", role_name);
    params.put("password", password);
    params.put("connect_timeout", "1");

    // Calls the function under test.
    ErrorCode actual = Authentication::auth_user(params);
    // Verify test results.
    EXPECT_EQ(expected, actual);
  }
}

/**
 * @brief Test of authentication by connection strings (URI pattern).
 */
TEST_P(ApiTestAuthentication, auth_user_uri) {
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
      auth_info += role_name;
      if (!password.empty()) {
        auth_info += ":" + password;
      }
    }

    fmter_pattern =
        boost::format("postgresql://%1%%2%%3%%4%%5%?connect_timeout=1") %
        auth_info % (auth_info.empty() ? "" : "@") % host_info %
        (db_name.empty() ? "" : "/") % db_name;

    // Calls the function under test.
    ErrorCode actual = Authentication::auth_user(fmter_pattern.str());
    // Verify test results.
    EXPECT_EQ(expected, actual);
  }
}

/**
 * @brief Test of authentication by connection strings and
 *   user-name/password (URI pattern).
 */
TEST_P(ApiTestAuthentication, auth_user_uri_authinfo) {
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

    // Create host information.
    std::string host_info = "";
    if (!host.empty()) {
      host_info += host;
      if (!port.empty()) {
        host_info += ":" + port;
      }
    }

    fmter_pattern = boost::format("postgresql://%1%%2%%3%?connect_timeout=1") %
                    host_info % (db_name.empty() ? "" : "/") % db_name;

    // Calls the function under test.
    ErrorCode actual = Authentication::auth_user(fmter_pattern.str(), role_name,
                                                 password, nullptr);
    // Verify test results.
    EXPECT_EQ(expected, actual);
  }
}

/**
 * @brief Test of authentication by connection strings (Key=Value pattern).
 */
TEST_P(ApiTestAuthentication, auth_user_key_value) {
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

    fmter_pattern = boost::format("%1% %2% %3% %4% %5% connect_timeout=1") %
                    (!host.empty() ? "host=" + host : "") %
                    (!port.empty() ? "port=" + port : "") %
                    (!db_name.empty() ? "dbname=" + db_name : "") %
                    (!role_name.empty() ? "user=" + role_name : "") %
                    (!password.empty() ? "password=" + password : "");

    // Calls the function under test.
    ErrorCode actual = Authentication::auth_user(fmter_pattern.str());
    // Verify test results.
    EXPECT_EQ(expected, actual);
  }
}

/**
 * @brief Test of authentication by connection strings and
 *   user-name/password (Key=Value pattern).
 */
TEST_P(ApiTestAuthentication, auth_user_key_value_authinfo) {
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

    fmter_pattern = boost::format("%1% %2% %3% connect_timeout=1") %
                    (!host.empty() ? "host=" + host : "") %
                    (!port.empty() ? "port=" + port : "") %
                    (!db_name.empty() ? "dbname=" + db_name : "");

    // Calls the function under test.
    ErrorCode actual = Authentication::auth_user(fmter_pattern.str(), role_name,
                                                 password, nullptr);
    // Verify test results.
    EXPECT_EQ(expected, actual);
  }
}

/**
 * @brief Test of authentication by hostaddr.
 */
TEST_F(ApiTestAuthentication, patterns_hostaddr) {
  ErrorCode actual = ErrorCode::UNKNOWN;
  std::string conn_string = "";

  // create test data for property tree.
  boost::property_tree::ptree params;
  params.put("hostaddr", "127.0.0.1");
  params.put("port", "5432");
  params.put("dbname", "tsurugi");
  params.put("user", role::standard::name);
  params.put("password", role::standard::pswd);
  params.put("connect_timeout", "1");

  // Test for normal patterns in the property tree.
  {
    UTUtils::print("  Test by property tree [",
                   params.get<std::string>("hostaddr"), "]");

    // Calls the function under test.
    actual = Authentication::auth_user(params);
    // Verify test results.
    EXPECT_EQ(ErrorCode::OK, actual);
  }

  // Test for authentication by connection string.
  {
    UTUtils::print("  Test by connection string [",
                   params.get<std::string>("hostaddr"), "]");

    // create test data for connection string.
    for (auto pos = params.begin(); pos != params.end(); pos++) {
      boost::format key_value = boost::format("%s %s=%s") % conn_string %
                                pos->first % pos->second.data();
      conn_string = key_value.str();
    }
    // Calls the function under test.
    actual = Authentication::auth_user(conn_string);
    // Verify test results.
    EXPECT_EQ(ErrorCode::OK, actual);
  }

  // Test for connection failure by property tree.
  {
    params.erase("hostaddr");
    params.put("hostaddr", "192.168.10.255");

    UTUtils::print("  Test by property tree [",
                   params.get<std::string>("hostaddr"), "]");

    // Calls the function under test.
    actual = Authentication::auth_user(params);
    // Verify test results.
    EXPECT_EQ(ErrorCode::CONNECTION_FAILURE, actual);
  }

  // Test for connection failure by connection string.
  {
    UTUtils::print("  Test by connection string [192.168.10.255]");

    // create test data for connection string.
    conn_string = std::regex_replace(conn_string, std::regex("=127.0.0.1"),
                                     "=192.168.10.255");

    // Calls the function under test.
    actual = Authentication::auth_user(conn_string);
    // Verify test results.
    EXPECT_EQ(ErrorCode::CONNECTION_FAILURE, actual);
  }
}

/**
 * @brief Test of authentication by parameters empty.
 */
TEST_F(ApiTestAuthentication, patterns_parameter_empty) {
  ErrorCode actual = ErrorCode::UNKNOWN;

  // Test for authentication by property tree.
  {
    UTUtils::print("  test by property tree");

    boost::property_tree::ptree params;
    // Calls the function under test.
    actual = Authentication::auth_user(params);
    // Verify test results.
    EXPECT_EQ(ErrorCode::OK, actual);
  }

  // Test for authentication by connection string.
  {
    UTUtils::print("  test by connection string");

    std::string conn_string = "";
    // Calls the function under test.
    actual = Authentication::auth_user(conn_string);
    // Verify test results.
    EXPECT_EQ(ErrorCode::OK, actual);
  }

  // Test for authentication by connection string.
  {
    UTUtils::print("  test by user_name / password");

    std::string conn_string = "";
    std::string user_name = "";
    std::string password = "";
    // Calls the function under test.
    actual = Authentication::auth_user(user_name, password, nullptr);
    // Verify test results.
    EXPECT_EQ(ErrorCode::OK, actual);
  }

  // Test for authentication by connection string.
  {
    UTUtils::print("  test by connection string & user_name / password");

    std::string conn_string = "";
    std::string user_name = "";
    std::string password = "";
    // Calls the function under test.
    actual =
        Authentication::auth_user(conn_string, user_name, password, nullptr);
    // Verify test results.
    EXPECT_EQ(ErrorCode::OK, actual);
  }
}

/**
 * @brief Test to issue authentication tokens.
 */
TEST_P(ApiTestAuthenticationToken, auth_user_token) {
  auto params = GetParam();

  for (auto param : params) {
    auto role_name = std::get<0>(param);
    auto password = std::get<1>(param);
    auto expected = std::get<2>(param);
    std::string host = "localhost";
    std::string db_name = "tsurugi";

    auto fmter_pattern =
        boost::format("role=%1%, password=%2%") % role_name % password;
    UTUtils::print(" Patterns of [", fmter_pattern.str(), "]");

    // Test for authentication by connection string (URI pattern).
    {
      UTUtils::print("  Test by connection string (URI).");

      fmter_pattern = boost::format("postgresql://%1%/%2%") % host % db_name;

      std::string token = "";
      // Calls the function under test.
      ErrorCode actual = Authentication::auth_user(fmter_pattern.str(),
                                                   role_name, password, &token);
      // Verify test results.
      ASSERT_EQ(expected, actual);
      if (actual == ErrorCode::OK) {
        EXPECT_NO_THROW(CheckToken(token, role_name));
      } else {
        EXPECT_TRUE(token.empty());
      }
    }

    // Test for authentication by connection string (key/value pattern).
    {
      UTUtils::print("  Test by connection string (Key/Value).");

      fmter_pattern = boost::format("host=%1% dbname=%2%") % host % db_name;

      std::string token = "";
      // Calls the function under test.
      ErrorCode actual = Authentication::auth_user(fmter_pattern.str(),
                                                   role_name, password, &token);
      // Verify test results.
      ASSERT_EQ(expected, actual);
      if (actual == ErrorCode::OK) {
        ASSERT_NO_THROW(CheckToken(token, role_name));
      } else {
        EXPECT_TRUE(token.empty());
      }
    }
  }
}

/**
 * @brief Test to refresh tokens.
 */
TEST_F(ApiTestAuthenticationToken, refresh_token) {
  std::string role_name = role::standard::name;
  std::string password = role::standard::pswd;
  std::string host = "localhost";
  std::string db_name = "tsurugi";

  boost::format fmter_pattern =
      boost::format("postgresql://%1%/%2%") % host % db_name;

  std::string token = "";
  // Calls the function under test.
  ErrorCode actual_result = Authentication::auth_user(
      fmter_pattern.str(), role_name, password, &token);
  // Verify test results.
  ASSERT_EQ(ErrorCode::OK, actual_result);

  std::string new_token = token;
  auto now_time = std::chrono::system_clock::now();
  time_t expected_now = std::chrono::system_clock::to_time_t(now_time);

  // wait.
  sleep(1);

  // Calls the function under test.
  actual_result =
      Authentication::refresh_token(new_token, std::chrono::hours(24));
  // Verify test results.
  ASSERT_EQ(ErrorCode::OK, actual_result);
  EXPECT_NE(token, new_token);
  ASSERT_NO_THROW(VerifyToken(new_token));

  // Examine the details of the token.
  {
    // Decode the access token.
    auto decode_old = jwt::decode(std::string(token));
    auto decode_new = jwt::decode(std::string(new_token));

    // Check if algortihm is correct ("alg").
    EXPECT_EQ(decode_old.get_algorithm(), decode_new.get_algorithm());

    // Check if type is correct ("typ").
    EXPECT_EQ(decode_old.get_type(), decode_new.get_type());

    // Check if issuer is correct ("iss").
    EXPECT_EQ(decode_old.get_issuer(), decode_new.get_issuer());

    // Check if subject is correct ("sub").
    EXPECT_EQ(decode_old.get_subject(), decode_new.get_subject());

    // Check if  issued date is correct ("iat").
    time_t expected_iat =
        std::chrono::system_clock::to_time_t(decode_old.get_issued_at());
    time_t actual_iat =
        std::chrono::system_clock::to_time_t(decode_new.get_issued_at());
    EXPECT_EQ(expected_iat, actual_iat);

    // Check if expires date is correct ("exp").
    time_t actual_exp =
        std::chrono::system_clock::to_time_t(decode_new.get_expires_at());
    time_t expected_exp = expected_now + (3600 * 24);
    EXPECT_NEAR(expected_exp, actual_exp, 1);

    // Check if a payload claim is correct (User Name).
    std::string expected_user_name =
        decode_old.get_payload_claim(Token::Payload::kAuthUserName).as_string();
    std::string actual_user_name =
        decode_new.get_payload_claim(Token::Payload::kAuthUserName).as_string();
    EXPECT_EQ(expected_user_name, actual_user_name);

    // Check if a payload claim is correct (Refresh Expiration).
    time_t expected_refresh =
        expected_now + Config::get_jwt_expiration_refresh();
    time_t actual_refresh = std::chrono::system_clock::to_time_t(
        decode_new.get_payload_claim(Token::Payload::kExpirationRefresh)
            .as_date());
    EXPECT_NEAR(expected_refresh, actual_refresh, 1);

    // Check if a payload claim is correct (Token Use Expiration).
    time_t expected_available = std::chrono::system_clock::to_time_t(
        decode_old.get_payload_claim(Token::Payload::kExpirationAvailable)
            .as_date());
    time_t actual_available = std::chrono::system_clock::to_time_t(
        decode_new.get_payload_claim(Token::Payload::kExpirationAvailable)
            .as_date());
    EXPECT_EQ(expected_available, actual_available);
  }
}

/**
 * @brief Test to refresh tokens (expired).
 */
TEST_F(ApiTestAuthenticationToken, refresh_token_expired) {
  // Cryptographic algorithms.
  auto algorithm = jwt::algorithm::hs256{Config::get_jwt_secret_key()};

  // Set the expiration date.
  auto now_time = std::chrono::system_clock::now() - std::chrono::minutes{1};
  auto iss_time = now_time - std::chrono::minutes{1};
  auto exp_time = now_time - std::chrono::minutes{1};
  auto exp_ref_time = now_time + std::chrono::minutes{60};
  auto exp_use_time = now_time + std::chrono::minutes{60};

  // Setting up data for token.
  auto jwt_builder =
      jwt::create()
          .set_type(Token::Header::kType)
          .set_issuer(Config::get_jwt_issuer())
          .set_audience(Config::get_jwt_audience())
          .set_subject(Config::get_jwt_subject())
          .set_issued_at(iss_time)
          .set_expires_at(exp_time)
          .set_payload_claim(Token::Payload::kExpirationRefresh,
                             jwt::claim(exp_ref_time))
          .set_payload_claim(Token::Payload::kExpirationAvailable,
                             jwt::claim(exp_use_time))
          .set_payload_claim(Token::Payload::kAuthUserName,
                             jwt::claim(std::string(role::standard::name)));
  // Sign the JWT token.
  auto signed_token = jwt_builder.sign(algorithm);
  std::string token_old = signed_token.c_str();
  std::string token_new = token_old;
  // Calls the function under test.
  ErrorCode actual_result =
      Authentication::refresh_token(token_new, std::chrono::minutes{30});
  // Verify test results.
  ASSERT_EQ(ErrorCode::OK, actual_result);
  ASSERT_NE(token_old, token_new);
}

/**
 * @brief Test to refresh tokens (refresh expired).
 * @brief Test to refresh tokens.
 */
TEST_F(ApiTestAuthenticationToken, refresh_token_refresh_expired) {
  // Cryptographic algorithms.
  auto algorithm = jwt::algorithm::hs256{Config::get_jwt_secret_key()};

  // Set the expiration date.
  auto now_time = std::chrono::system_clock::now();
  auto iss_time = now_time - std::chrono::minutes{1};
  auto exp_time = now_time - std::chrono::minutes{1};
  auto exp_ref_time = now_time - std::chrono::minutes{1};
  auto exp_use_time = now_time + std::chrono::minutes{60};

  // Setting up data for token.
  auto jwt_builder =
      jwt::create()
          .set_type(Token::Header::kType)
          .set_issuer(Config::get_jwt_issuer())
          .set_audience(Config::get_jwt_audience())
          .set_subject(Config::get_jwt_subject())
          .set_issued_at(iss_time)
          .set_expires_at(exp_time)
          .set_payload_claim(Token::Payload::kExpirationRefresh,
                             jwt::claim(exp_ref_time))
          .set_payload_claim(Token::Payload::kExpirationAvailable,
                             jwt::claim(exp_use_time))
          .set_payload_claim(Token::Payload::kAuthUserName,
                             jwt::claim(std::string(role::standard::name)));
  // Sign the JWT token.
  auto signed_token = jwt_builder.sign(algorithm);
  std::string token = signed_token.c_str();

  // Calls the function under test.
  ErrorCode actual_result =
      Authentication::refresh_token(token, std::chrono::minutes{30});
  // Verify test results.
  ASSERT_EQ(ErrorCode::INVALID_PARAMETER, actual_result);
}

/**
 * @brief Test to refresh tokens (available expired).
 * @brief Test to refresh tokens.
 */
TEST_F(ApiTestAuthenticationToken, refresh_token_available_expired) {
  // Cryptographic algorithms.
  auto algorithm = jwt::algorithm::hs256{Config::get_jwt_secret_key()};

  // Set the expiration date.
  auto now_time = std::chrono::system_clock::now();
  auto iss_time = now_time;
  auto exp_time = now_time + std::chrono::minutes{60};
  auto exp_ref_time = now_time + std::chrono::minutes{60};
  auto exp_use_time = now_time - std::chrono::minutes{1};

  // Setting up data for token.
  auto jwt_builder =
      jwt::create()
          .set_type(Token::Header::kType)
          .set_issuer(Config::get_jwt_issuer())
          .set_audience(Config::get_jwt_audience())
          .set_subject(Config::get_jwt_subject())
          .set_issued_at(iss_time)
          .set_expires_at(exp_time)
          .set_payload_claim(Token::Payload::kExpirationRefresh,
                             jwt::claim(exp_ref_time))
          .set_payload_claim(Token::Payload::kExpirationAvailable,
                             jwt::claim(exp_use_time))
          .set_payload_claim(Token::Payload::kAuthUserName,
                             jwt::claim(std::string(role::standard::name)));
  // Sign the JWT token.
  auto signed_token = jwt_builder.sign(algorithm);
  std::string token = signed_token.c_str();

  // Calls the function under test.
  ErrorCode actual_result =
      Authentication::refresh_token(token, std::chrono::minutes{30});
  // Verify test results.
  ASSERT_EQ(ErrorCode::INVALID_PARAMETER, actual_result);
}

/**
 * @brief Test to refresh tokens (illegal token).
 * @brief Test to refresh tokens.
 */
TEST_F(ApiTestAuthenticationToken, refresh_token_illegal_token) {
  // Illegal
  {
    std::string token = "header.payload.signature";
    // Calls the function under test.
    ErrorCode actual_result =
        Authentication::refresh_token(token, std::chrono::minutes{30});
    // Verify test results.
    ASSERT_EQ(ErrorCode::INVALID_PARAMETER, actual_result);
  }

  // empty
  {
    std::string token = "";
    // Calls the function under test.
    ErrorCode actual_result =
        Authentication::refresh_token(token, std::chrono::minutes{30});
    // Verify test results.
    ASSERT_EQ(ErrorCode::INVALID_PARAMETER, actual_result);
  }
}

}  // namespace manager::authentication::testing
