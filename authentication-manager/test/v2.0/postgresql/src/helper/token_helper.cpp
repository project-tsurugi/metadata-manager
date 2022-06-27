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
#include "test/helper/token_helper.h"

#include "jwt-cpp/jwt.h"

#include "manager/authentication/common/config.h"
#include "manager/authentication/common/jwt_claims.h"

namespace manager::authentication::testing {

// using manager::authentication::Config;

/**
 * @brief Generate an access token.
 * @param (user_name)   [in]  user name.
 * @param (expiration)  [in]  expiration secconds.
 * @param (refresh)     [in]  refresh secconds.
 * @param (available)   [in]  available secconds.
 * @return token
 */
std::string TokenHelper::generate_token(std::string_view user_name,
                                        std::int32_t expiration,
                                        std::int32_t refresh,
                                        std::int32_t available) {
  // Cryptographic algorithms.
  auto algorithm = jwt::algorithm::hs256{Config::get_jwt_secret_key()};

  // Set the expiration date.
  auto now_time = std::chrono::system_clock::now();
  auto exp_time = now_time + std::chrono::seconds{expiration};
  auto exp_ref_time = now_time + std::chrono::seconds{refresh};
  auto exp_use_time = now_time + std::chrono::seconds{available};

  // Setting up data for token.
  auto jwt_builder =
      jwt::create()
          .set_type(Token::Header::kType)
          .set_issuer(Config::get_jwt_issuer())
          .set_audience(Config::get_jwt_audience())
          .set_subject(Config::get_jwt_subject())
          .set_issued_at(now_time)
          .set_expires_at(exp_time)
          .set_payload_claim(Token::Payload::kExpirationRefresh,
                             jwt::claim(exp_ref_time))
          .set_payload_claim(Token::Payload::kExpirationAvailable,
                             jwt::claim(exp_use_time))
          .set_payload_claim(Token::Payload::kAuthUserName,
                             jwt::claim(std::string(user_name)));
  // Sign the JWT token.
  auto signed_token = jwt_builder.sign(algorithm);

  return std::string(signed_token.c_str());
}

}  // namespace manager::authentication::testing
