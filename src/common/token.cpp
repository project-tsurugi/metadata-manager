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
#include "manager/metadata/common/token.h"

#include <regex>

#include <boost/format.hpp>
#include "jwt-cpp/jwt.h"

#include "manager/metadata/common/config.h"
#include "manager/metadata/common/jwt_claims.h"
#include "manager/metadata/common/message.h"
#include "manager/metadata/helper/logging_helper.h"

// =============================================================================
namespace manager::metadata {

/**
 * @brief Check if the token is valid.
 * @return true if valid, false otherwise.
 */
bool Token::is_valid() const {
  bool result = false;

  // Check the initialization of the access token.
  if (token_string_.empty()) {
    LOG_INFO << "Token is empty.";
    return result;
  }

  try {
    // Decode the access token.
    auto decoded_token = jwt::decode(token_string_);

    // Validation of required claims.
    result = validate_required(decoded_token);
    if (!result) {
      // Illegal token.
      LOG_ERROR << Message::INVALID_TOKEN;
      return result;
    }

    // Claim verification lambda.
    auto claim_verifier = [](const jwt::verify_context& ctx,
                             std::string_view regex) -> std::error_code {
      using verify_error = jwt::error::token_verification_error;
      std::error_code ec;

      // Get claims from jwt.
      auto claim_data = ctx.get_claim(false, ec);
      if (ec != verify_error::ok) {
        return ec;
      }
      // Check the type of the claim.
      if (claim_data.get_type() == jwt::json::type::string) {
        auto value = claim_data.as_string();
        auto regex_results =
            std::regex_match(claim_data.as_string(), std::regex(regex.data()));
        // Check the value of the claim.
        ec = (regex_results ? verify_error::ok
                            : verify_error::claim_value_missmatch);
      } else {
        ec = verify_error::claim_type_missmatch;
      }
      return ec;
    };

    // Cryptographic algorithms.
    auto algorithm = jwt::algorithm::hs256{Config::get_jwt_secret_key()};
    // Set the data for the token to be verified.
    auto verifier = jwt::verify()
                        .allow_algorithm(algorithm)
                        .issued_at_leeway(token::Leeway::kIssued)
                        .expires_at_leeway(token::Leeway::kExpiration)
                        .with_issuer(Config::get_jwt_issuer());

    // Token type (sub) verification.
    verifier.with_claim(
        token::Payload::kTokenType,
        [&claim_verifier](const jwt::verify_context& ctx, std::error_code& ec) {
          auto regex = boost::format("(%s|%s)") % token::TokenType::kAccess %
                       token::TokenType::kRefresh;
          // Claim verification.
          ec = claim_verifier(ctx, regex.str());
        });

    // User name verification.
    verifier.with_claim(
        token::Payload::kAuthUserName,
        [&claim_verifier](const jwt::verify_context& ctx, std::error_code& ec) {
          // Claim verification.
          ec = claim_verifier(ctx, ".+");
        });

    // Set the audience for the token to be verified.
    if (subject_ == token::TokenType::kAccess) {
      // For access tokens, verify that they are the same as the audience.
      verifier.with_audience(Config::get_jwt_audience());
    } else if (subject_ == token::TokenType::kRefresh) {
      // For refresh tokens, verify that they are the same as the issuer.
      verifier.with_audience(Config::get_jwt_issuer());
    }

    // Verify the JWT token.
    verifier.verify(decoded_token);

    // Ensure that the user name is defined.
    result = decoded_token.has_payload_claim(token::Payload::kAuthUserName);
  } catch (jwt::error::token_verification_exception ex) {
    LOG_ERROR << Message::INVALID_TOKEN << ex.what();
    result = false;
  } catch (...) {
    LOG_ERROR << Message::INVALID_TOKEN;
    result = false;
  }

  return result;
}

/**
 * @brief Returns whether or not the token is an access token.
 * @return true if an access token, false otherwise.
 */
bool Token::is_access_token() const {
  return subject_ == token::TokenType::kAccess;
}

/**
 * @brief Returns whether or not the token is a refresh token.
 * @return true if a refresh token, false otherwise.
 */
bool Token::is_refresh_token() const {
  return subject_ == token::TokenType::kRefresh;
}

/**
 * @brief Returns whether or not the token is a valid access token.
 *   Same as the result of executing both is_valid() and is_access_token().
 * @return true if an access token, false otherwise.
 */
bool Token::is_valid_access_token() const {
  bool result = is_valid();
  if (result) {
    result = is_access_token();
    if (!result) {
      LOG_ERROR << Message::PARAMETER_FAILED << "Token is a non-access token.";
    }
  }

  return result;
}

/**
 * @brief Returns whether or not the token is a valid refresh token.
 *   Same as the result of executing both is_valid() and is_refresh_token().
 * @return true if a refresh token, false otherwise.
 */
bool Token::is_valid_refresh_token() const {
  bool result = is_valid();
  if (result) {
    result = is_refresh_token();
    if (!result) {
      LOG_ERROR << Message::PARAMETER_FAILED << "Token is a non-refresh token.";
    }
  }

  return result;
}

/**
 * @brief Decodes tokens.
 * @param (token_string)  [in]  access token.
 * @return Decoded token.
 */
void Token::decode_token(std::string_view token_string) {
  try {
    // Decode a token.
    auto decoded_token = jwt::decode(std::string(token_string));

    // Set the value of the type claim.
    type_ = decoded_token.get_type();

    // Set the value of the issued-at claim.
    issued_time_ =
        std::chrono::system_clock::to_time_t(decoded_token.get_issued_at());

    // Set the value of the expires-at claim.
    expiration_time_ =
        std::chrono::system_clock::to_time_t(decoded_token.get_expires_at());

    // Set the value of the issuer claim.
    issuer_ = (decoded_token.has_issuer() ? decoded_token.get_issuer() : "");

    // Set the value of the audience claim.
    if (decoded_token.has_audience()) {
      audience_ = decoded_token.get_audience();
    } else {
      audience_.clear();
    }

    // Set the value of the subject  claim.
    subject_ = (decoded_token.has_subject() ? decoded_token.get_subject() : "");

    // Set the value of the token-type claim.
    user_name_ = decoded_token.get_payload_claim(token::Payload::kAuthUserName)
                     .as_string();

    // Set the access token string.
    token_string_ = token_string;
  } catch (...) {
    // Clear the access token data.
    token_string_    = "";
    type_            = "";
    issued_time_     = 0;
    expiration_time_ = 0;
    issuer_          = "";
    audience_.clear();
    subject_   = "";
    user_name_ = "";
  }
}

/**
 * @brief Check if the token is valid.
 * @retval true if valid.
 * @retval false if invalid.
 */
template <typename T>
bool Token::validate_required(T& decoded) const {
  bool result = false;

  try {
    // Check if algorithm is present ("alg").
    result = decoded.has_algorithm();
    // Check if type is present ("typ").
    result = (result ? decoded.has_type() : false);
    // Check if issued date is present ("iat").
    result = (result ? decoded.has_issued_at() : false);
    // Check if expires date is present ("exp").
    result = (result ? decoded.has_expires_at() : false);
    // Check if expires date is present ("iss").
    result = (result ? decoded.has_issuer() : false);
    // Check if expires date is present ("aud").
    result = (result ? decoded.has_audience() : false);
    // Check if expires date is present ("sub").
    result = (result ? decoded.has_subject() : false);
    // Check if a payload claim is present ("tsurugi/token_type").
    result = (result ? decoded.has_payload_claim(token::Payload::kTokenType)
                     : false);
  } catch (...) {
    result = false;
  }

  return result;
}

}  // namespace manager::metadata
