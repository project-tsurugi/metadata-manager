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

#include "manager/authentication/access_token.h"
#include "manager/authentication/common/config.h"
#include "manager/authentication/common/jwt_claims.h"
#include "test/helper/role_metadata_helper.h"
#include "test/helper/token_helper.h"
#include "test/utility/ut_utils.h"

namespace manager::authentication::testing {

constexpr const char* const role_name = "tsurugi_api_ut_role_user";

/**
 * @brief ApiTestAccessToken
 */
class ApiTestAccessToken : public ::testing::Test {
 public:
  void SetUp() override {}
  void TearDown() override {}

  std::string generate_token(jwt::date iss, jwt::date exp, jwt::date ref,
                             jwt::date use) {
    // Cryptographic algorithms.
    auto algorithm = jwt::algorithm::hs256{Config::get_jwt_secret_key()};

    // Setting up data for token.
    auto jwt_builder =
        jwt::create()
            .set_type(Token::Header::kType)
            .set_issuer(Config::get_jwt_issuer())
            .set_audience(Config::get_jwt_audience())
            .set_subject(Config::get_jwt_subject())
            .set_issued_at(iss)
            .set_expires_at(exp)
            .set_payload_claim(Token::Payload::kExpirationRefresh,
                               jwt::claim(ref))
            .set_payload_claim(Token::Payload::kExpirationAvailable,
                               jwt::claim(use))
            .set_payload_claim(Token::Payload::kAuthUserName,
                               jwt::claim(std::string(role_name)));
    // Sign the JWT token.
    auto signed_token = jwt_builder.sign(algorithm);
    return signed_token.c_str();
  }
};  // class ApiTestAccessToken

/**
 * @brief Base test of access tokens.
 */
TEST_F(ApiTestAccessToken, access_token_base) {
  // Set the expiration date.
  auto now_time = std::chrono::system_clock::now();
  auto iss_time = now_time;
  auto exp_time = now_time + std::chrono::minutes{5};
  auto exp_ref_time = now_time + std::chrono::hours{24};
  auto exp_use_time = now_time + std::chrono::hours{72};

  // Generate an access token.
  std::string token_string =
      generate_token(iss_time, exp_time, exp_ref_time, exp_use_time);

  std::time_t expected_iss_time =
      std::chrono::system_clock::to_time_t(iss_time);
  std::time_t expected_exp_time =
      std::chrono::system_clock::to_time_t(exp_time);
  std::time_t expected_ref_time =
      std::chrono::system_clock::to_time_t(exp_ref_time);
  std::time_t expected_use_time =
      std::chrono::system_clock::to_time_t(exp_use_time);

  // Calls the function under test.
  AccessToken token(token_string);
  // Verify test results.
  EXPECT_EQ(token.string(), token_string);
  EXPECT_EQ(Token::Header::kType, token.type());
  EXPECT_EQ(Config::get_jwt_issuer(), token.issuer());
  for (auto audience : token.audience()) {
    EXPECT_EQ(Config::get_jwt_audience(), audience);
  }
  EXPECT_EQ(Config::get_jwt_subject(), token.subject());
  EXPECT_EQ(expected_iss_time, token.issued_time());
  EXPECT_EQ(expected_exp_time, token.expiration_time());
  EXPECT_EQ(expected_ref_time, token.refresh_expiration_time());
  EXPECT_EQ(expected_use_time, token.available_time());
  EXPECT_EQ(role_name, token.user_name());
  EXPECT_TRUE(token.is_valid());
  EXPECT_TRUE(token.is_available());
}

/**
 * @brief Test of  access token expiration date.
 */
TEST_F(ApiTestAccessToken, access_token_expiration) {
  auto now_time = std::chrono::system_clock::now();

  {
    // Set the expiration date.
    auto iss_time = now_time - std::chrono::minutes{1};
    auto exp_time = now_time - std::chrono::minutes{1};  // <- target
    auto exp_ref_time = now_time + std::chrono::hours{24};
    auto exp_use_time = now_time + std::chrono::hours{72};

    // Generate an access token.
    std::string token_string =
        generate_token(iss_time, exp_time, exp_ref_time, exp_use_time);

    // Calls the function under test.
    AccessToken token(token_string);
    // Verify test results.
    EXPECT_FALSE(token.is_valid());
    EXPECT_TRUE(token.is_available());
  }

  {
    // Set the expiration date.
    auto iss_time = now_time - std::chrono::minutes{1};
    auto exp_time = now_time - std::chrono::minutes{1};
    auto exp_ref_time = now_time - std::chrono::minutes{1};  // <- target
    auto exp_use_time = now_time + std::chrono::hours{72};

    // Generate an access token.
    std::string token_string =
        generate_token(iss_time, exp_time, exp_ref_time, exp_use_time);

    // Calls the function under test.
    AccessToken token(token_string);
    // Verify test results.
    EXPECT_FALSE(token.is_valid());
    EXPECT_FALSE(token.is_available());
  }

  {
    // Set the expiration date.
    auto iss_time = now_time - std::chrono::minutes{1};
    auto exp_time = now_time + std::chrono::minutes{5};      // <- target
    auto exp_ref_time = now_time - std::chrono::minutes{1};  // <- target
    auto exp_use_time = now_time + std::chrono::hours{72};

    // Generate an access token.
    std::string token_string =
        generate_token(iss_time, exp_time, exp_ref_time, exp_use_time);

    // Calls the function under test.
    AccessToken token(token_string);
    // Verify test results.
    EXPECT_TRUE(token.is_valid());
    EXPECT_TRUE(token.is_available());
  }

  {
    // Set the expiration date.
    auto iss_time = now_time - std::chrono::minutes{1};
    auto exp_time = now_time - std::chrono::minutes{1};
    auto exp_ref_time = now_time - std::chrono::minutes{1};
    auto exp_use_time = now_time - std::chrono::minutes{1};  // <- target

    // Generate an access token.
    std::string token_string =
        generate_token(iss_time, exp_time, exp_ref_time, exp_use_time);

    // Calls the function under test.
    AccessToken token(token_string);
    // Verify test results.
    EXPECT_FALSE(token.is_valid());
    EXPECT_FALSE(token.is_available());
  }

  {
    // Set the expiration date.
    auto iss_time = now_time - std::chrono::minutes{1};
    auto exp_time = now_time + std::chrono::minutes{5};
    auto exp_ref_time = now_time + std::chrono::hours{24};
    auto exp_use_time = now_time - std::chrono::minutes{1};  // <- target

    // Generate an access token.
    std::string token_string =
        generate_token(iss_time, exp_time, exp_ref_time, exp_use_time);

    // Calls the function under test.
    AccessToken token(token_string);
    // Verify test results.
    EXPECT_FALSE(token.is_valid());
    EXPECT_FALSE(token.is_available());
  }
}

}  // namespace manager::authentication::testing
