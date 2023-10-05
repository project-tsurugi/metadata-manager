/*
 * Copyright 2021-2022 Project Tsurugi.
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

#include <string>
#include <vector>

#include "jwt-cpp/jwt.h"

#include "manager/metadata/common/config.h"

namespace manager::metadata::testing {

/**
 * @brief Generate and return a token with the specified information.
 * @param user_name User name to be included in the token.
 * @param exp Token expiration date.
 * @return Token string.
 */
std::string TokenHelper::generate_token(std::string_view user_name,
                                        int32_t exp) {
  // Cryptographic algorithms.
  auto algorithm = jwt::algorithm::hs256{Config::get_jwt_secret_key()};

  // Set the expiration date.
  auto now_time     = std::chrono::system_clock::now();
  auto exp_time     = now_time + std::chrono::seconds{exp};
  auto exp_ref_time = now_time + std::chrono::hours{1};
  auto exp_use_time = now_time + std::chrono::hours{48};

  // Setting up data for token.
  auto jwt_builder =
      jwt::create()
          .set_type("JWT")
          .set_issued_at(now_time)
          .set_expires_at(exp_time)
          .set_payload_claim("tsurugi/exp/refresh", jwt::claim(exp_ref_time))
          .set_payload_claim("tsurugi/exp/available", jwt::claim(exp_use_time))
          .set_payload_claim("tsurugi/auth/name",
                             jwt::claim(std::string(user_name)));
  // Sign the JWT token.
  auto signed_token = jwt_builder.sign(algorithm);

  return std::string(signed_token.c_str());
}

}  // namespace manager::metadata::testing
