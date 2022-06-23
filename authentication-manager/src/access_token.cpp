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
#include "manager/authentication/access_token.h"

#include <limits>

#include "jwt-cpp/jwt.h"
#include "manager/authentication/common/config.h"
#include "manager/authentication/common/jwt_claims.h"

// =============================================================================
namespace manager::authentication {

/**
 * @brief Get an access token.
 * @return access token.
 */
std::string AccessToken::show() { return access_token_; }

/**
 * @brief Check if the token is valid.
 * @retval true if valid.
 * @retval false if invalid.
 */
bool AccessToken::is_valid() {
  bool result = false;

  // Check the initialization of the access token.
  if (access_token_.empty()) {
    return result;
  }

  jwt::date now_time = std::chrono::system_clock::now();
  jwt::date available_time;
  try {
    // Decode the access token.
    auto decoded_token = jwt::decode(access_token_);

    // Validation of required claims.
    result = validate_required(decoded_token);
    if (result) {
      // Cryptographic algorithms.
      auto algorithm = jwt::algorithm::hs256{Config::get_jwt_secret_key()};
      // Setting up data for token.
      auto verifier =
          jwt::verify().allow_algorithm(algorithm).expires_at_leeway(
              Token::Leeway::kExpiration);
      // Verify the JWT token.
      verifier.verify(decoded_token);

      // Extract the available date.
      available_time =
          decoded_token.get_payload_claim(Token::Payload::kExpirationAvailable)
              .as_date();
      result = true;
    }
  } catch (...) {
    result = false;
  }

  if (result) {
    // Extract the available dates and allow for leeway.
    available_time += std::chrono::seconds(
        static_cast<std::int64_t>(Token::Leeway::kExpirationAvailable));

    // Check if it is within the use expiration date.
    result = (available_time >= now_time);
  }

  return result;
}

/**
 * @brief Checks if a token is available.
 * @retval true if available.
 * @retval false if unavailable.
 */
bool AccessToken::is_available() {
  bool result = false;

  // Check the initialization of the access token.
  if (access_token_.empty()) {
    return result;
  }

  jwt::date now_time = std::chrono::system_clock::now();
  jwt::date exp_time;
  jwt::date refresh_exp_time;
  jwt::date available_exp_time;

  try {
    // Decode the access token.
    auto decoded_token = jwt::decode(access_token_);

    // Validation of required claims.
    result = validate_required(decoded_token);
    if (result) {
      // Cryptographic algorithms.
      auto algorithm = jwt::algorithm::hs256{Config::get_jwt_secret_key()};

      // Setting up data for token.
      auto verifier =
          jwt::verify().allow_algorithm(algorithm).expires_at_leeway(
              std::numeric_limits<int32_t>::max());

      // Verify the JWT token.
      verifier.verify(decoded_token);

      // Extract the expiration date.
      exp_time = decoded_token.get_expires_at();
      // Extract the refresh expiration date.
      refresh_exp_time =
          decoded_token.get_payload_claim(Token::Payload::kExpirationRefresh)
              .as_date();
      // Extract the available date.
      available_exp_time =
          decoded_token.get_payload_claim(Token::Payload::kExpirationAvailable)
              .as_date();
    }
  } catch (...) {
    // Illegal token.
    result = false;
  }

  if (result) {
    // Extract the expiration date and add leeway.
    exp_time += std::chrono::seconds(
        static_cast<std::int64_t>(Token::Leeway::kExpiration));
    // Extract the refresh expiration date and add leeway.
    refresh_exp_time += std::chrono::seconds(
        static_cast<std::int64_t>(Token::Leeway::kExpirationRefresh));
    // Extract the available dates and allow for leeway.
    available_exp_time += std::chrono::seconds(
        static_cast<std::int64_t>(Token::Leeway::kExpirationAvailable));

    // Check if it is within the use expiration date.
    if (available_exp_time >= now_time) {
      // Check if the expiration date or refresh period has expired.
      result = (exp_time >= now_time) || (refresh_exp_time >= now_time);
    } else {
      result = false;
    }
  }

  return result;
}

/**
 * @brief Get the value of the type claim.
 * @return type as a string.
 */
std::string AccessToken::type() { return type_; }

/**
 * @brief Get a value of the issuer claim.
 * @return issuer as string.
 */
std::string AccessToken::issuer() { return issuer_; }

/**
 * @brief Get a value of the audience claim.
 * @return audience as a set of strings.
 */
std::set<std::string> AccessToken::audience() { return audience_; }

/**
 * @brief Get a value of the subject claim.
 * @return subject as string.
 */
std::string AccessToken::subject() { return subject_; }

/**
 * @brief Get a value of the issued-at claim.
 * @return Date/time of issue (epoch time).
 */
std::time_t AccessToken::issued_time() { return issued_time_; }

/**
 * @brief Get a value of the expires-at claim.
 * @return Expiration date/time (epoch time).
 */
std::time_t AccessToken::expiration_time() { return expiration_time_; }

/**
 * @brief Get a value of the refresh-expiration claim.
 * @return Expiration date/time (epoch time).
 */
std::time_t AccessToken::refresh_expiration_time() {
  return refresh_expiration_time_;
}

/**
 * @brief Get a value of available-expiration claim.
 * @return Expiration date/time (epoch time).
 */
std::time_t AccessToken::available_time() { return available_time_; }

/**
 * @brief Get the value of the user-name claim.
 * @return user name.
 */
std::string AccessToken::user_name() { return user_name_; }

/**
 * @brief Decodes tokens.
 * @param (token_string)  [in]  access token.
 * @return Decoded token.
 */
void AccessToken::decode_token(std::string_view token_string) {
  try {
    // Decode a token.
    auto decoded_token = jwt::decode(std::string(token_string));

    // Set the value of the type claim.
    type_ = decoded_token.get_type();

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

    // Set the value of the issued-at claim.
    issued_time_ =
        std::chrono::system_clock::to_time_t(decoded_token.get_issued_at());

    // Set the value of the expires-at claim.
    expiration_time_ =
        std::chrono::system_clock::to_time_t(decoded_token.get_expires_at());

    // Set the value of the refresh-expiration claim.
    auto exp_refresh =
        decoded_token.get_payload_claim(Token::Payload::kExpirationRefresh);
    refresh_expiration_time_ =
        std::chrono::system_clock::to_time_t(exp_refresh.as_date());

    // Set the value of the available-expiration claim.
    auto exp_available =
        decoded_token.get_payload_claim(Token::Payload::kExpirationAvailable);
    available_time_ =
        std::chrono::system_clock::to_time_t(exp_available.as_date());

    // Set the value of the user-name claim.
    user_name_ = decoded_token.get_payload_claim(Token::Payload::kAuthUserName)
                     .as_string();

    // Set the access token string.
    access_token_ = token_string;
  } catch (...) {
    // Clear the access token data.
    access_token_ = "";
    type_ = "";
    issuer_ = "";
    audience_.clear();
    subject_ = "";
    issued_time_ = 0;
    expiration_time_ = 0;
    refresh_expiration_time_ = 0;
    available_time_ = 0;
    user_name_ = "";
  }
}

/**
 * @brief Check if the token is valid.
 * @retval true if valid.
 * @retval false if invalid.
 */
template <typename T>
bool AccessToken::validate_required(T& decoded) {
  bool result = false;

  try {
    // Check if algortihm is present ("alg").
    result = decoded.has_algorithm();
    // Check if type is present ("typ").
    result = (result ? decoded.has_type() : false);
    // Check if issued date is present ("iat").
    result = (result ? decoded.has_issued_at() : false);
    // Check if expires date is present ("iat").
    result = (result ? decoded.has_expires_at() : false);
    // Check if a payload claim is present (User Name).
    result = (result ? decoded.has_payload_claim(Token::Payload::kAuthUserName)
                     : false);
    // Check if a payload claim is present (Refresh Expiration).
    result =
        (result ? decoded.has_payload_claim(Token::Payload::kExpirationRefresh)
                : false);
    // Check if a payload claim is present (Token Use Expiration).
    result =
        (result
             ? decoded.has_payload_claim(Token::Payload::kExpirationAvailable)
             : false);
  } catch (...) {
    result = false;
  }

  return result;
}

}  // namespace manager::authentication
