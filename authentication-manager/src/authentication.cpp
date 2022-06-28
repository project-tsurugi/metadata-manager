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
#include "manager/authentication/authentication.h"

#include "jwt-cpp/jwt.h"
#include "manager/authentication/access_token.h"
#include "manager/authentication/common/config.h"
#include "manager/authentication/common/jwt_claims.h"
#include "manager/authentication/provider/authentication_provider.h"

// =============================================================================
namespace manager::authentication {

/**
 * @brief Authentication is performed based on the connection information of the
 *   specified database.
 * @param (connection_params)  [in]  connection parameters.
 * @retval ErrorCode::OK if success.
 * @retval ErrorCode::AUTHENTICATION_FAILURE if the authentication failed.
 * @retval ErrorCode::CONNECTION_FAILURE if the connection to the
 *   database failed.
 */
ErrorCode Authentication::auth_user(
    const boost::property_tree::ptree& connection_params) {
  ErrorCode error = ErrorCode::UNKNOWN;

  error = db::AuthenticationProvider::auth_user(connection_params);

  return error;
}

/**
 * @brief Authentication is performed based on the connection string of the
 *   specified database.
 * @param (connection_string)  [in]  connection string.
 * @retval ErrorCode::OK if success.
 * @retval ErrorCode::AUTHENTICATION_FAILURE if the authentication failed.
 * @retval ErrorCode::CONNECTION_FAILURE if the connection to the
 *   database failed.
 */
ErrorCode Authentication::auth_user(std::string_view connection_string) {
  ErrorCode error = ErrorCode::UNKNOWN;

  error = db::AuthenticationProvider::auth_user(connection_string);

  return error;
}

/**
 * @brief Authentication is performed based on the specified user name and
 *   password. If token is specified (not nullptr),
 *   the generated access token is set.
 * @param (user_name)  [in]  user name to authenticate.
 * @param (password)   [in]  passward.
 * @param (token)      [out] generated access token.
 *   nullptr is available.
 * @retval ErrorCode::OK if success.
 * @retval ErrorCode::AUTHENTICATION_FAILURE if the authentication failed.
 * @retval ErrorCode::CONNECTION_FAILURE if the connection to the
 *   database failed.
 */
ErrorCode Authentication::auth_user(std::string_view user_name,
                                    std::string_view password,
                                    std::string* token) {
  ErrorCode error = ErrorCode::UNKNOWN;

  // Authentication.
  error =
      db::AuthenticationProvider::auth_user(std::nullopt, user_name, password);

  if ((error == ErrorCode::OK) && (token != nullptr)) {
    // Generate and set an access token.
    *token = generate_token(user_name);
  }
  return error;
}

/**
 * @brief Authentication is performed based on the specified user name and
 *   password. If token is specified (not nullptr),
 *   the generated access token is set.
 * @param (connection_string)  [in]  connection string.
 * @param (user_name)          [in]  user name to authenticate.
 * @param (password)           [in]  passward.
 * @param (token)              [out] generated access token.
 *   nullptr is available.
 * @retval ErrorCode::OK if success.
 * @retval ErrorCode::AUTHENTICATION_FAILURE if the authentication failed.
 * @retval ErrorCode::CONNECTION_FAILURE if the connection to the
 *   database failed.
 */
ErrorCode Authentication::auth_user(std::string_view connection_string,
                                    std::string_view user_name,
                                    std::string_view password,
                                    std::string* token) {
  ErrorCode error = ErrorCode::UNKNOWN;

  // Authentication.
  error = db::AuthenticationProvider::auth_user(
      std::optional(connection_string.data()), user_name, password);

  if ((error == ErrorCode::OK) && (token != nullptr)) {
    // Generate and set an access token.
    *token = generate_token(user_name);
  }

  return error;
}

/**
 * @brief Extends the expiration date of the specified token for a
 *   specified time.
 *   The expiration date that can be extended shall not exceed a certain period
 *   of time from the date and time the token is issued, in which case
 *   the maximum expiration date shall be used as the expiration date.
 * @param (token_string)  [in/out] access token. Newly generated access token.
 * @param (extend_time)   [in]     time to extend.
 * @retval ErrorCode::OK if success.
 * @retval ErrorCode::INVALID_PARAMETER if an invalid token is specified.
 */
ErrorCode Authentication::refresh_token(std::string& token_string,
                                        std::chrono::seconds extend_time) {
  ErrorCode error = ErrorCode::UNKNOWN;

  // Check if the token is available.
  AccessToken token(token_string);
  if (!token.is_available()) {
    error = ErrorCode::INVALID_PARAMETER;
    return error;
  }

  auto now_time = std::chrono::system_clock::now();

  // Check again if the token is within the refresh time limit as
  // the condition is different from is_available().
  {
    auto refresh_exp_time = std::chrono::system_clock::from_time_t(
        token.refresh_expiration_time() + Token::Leeway::kExpirationRefresh);
    if (now_time > refresh_exp_time) {
      // Time limit is over.
      error = ErrorCode::INVALID_PARAMETER;
      return error;
    }
  }

  // Setting up data for token.
  auto jwt_builder = jwt::create();

  // Copy the type header parameter of the current token.
  jwt_builder.set_type(token.type());

  // Copy the issuer payload claim of the current token.
  if (!token.issuer().empty()) {
    jwt_builder.set_issuer(token.issuer());
  }

  // Copy the audience payload claim of the current token.
  if (!token.audience().empty()) {
    for (auto audience : token.audience()) {
      jwt_builder.set_audience(audience.c_str());
    }
  }

  // Copy the subject payload claim of the current token.
  if (!token.subject().empty()) {
    jwt_builder.set_subject(token.subject());
  }

  // Copy the issue date/time payload claim of the current token.
  jwt_builder.set_issued_at(
      std::chrono::system_clock::from_time_t(token.issued_time()));

  // Copy the available date/time payload claim of the current token.
  std::chrono::time_point available_time =
      std::chrono::system_clock::from_time_t(token.available_time());
  jwt_builder.set_payload_claim(Token::Payload::kExpirationAvailable,
                                jwt::claim(available_time));

  // Copy the user name payload claim of the current token.
  jwt_builder.set_payload_claim(Token::Payload::kAuthUserName,
                                jwt::claim(token.user_name()));

  // Extension of expiration date.
  {
    std::chrono::time_point expires_time = now_time + extend_time;

    // Check the extended expiration limit.
    std::chrono::time_point expansion_time_limit =
        std::chrono::system_clock::from_time_t(token.available_time());
    if (expires_time > expansion_time_limit) {
      // If the limit is exceeded, revise to the longest expiration date.
      expires_time = expansion_time_limit;
    }
    jwt_builder.set_expires_at(expires_time);
  }

  // Reset the refresh expiration date.
  {
    std::chrono::time_point expires_time =
        now_time + std::chrono::seconds{Config::get_jwt_expiration_refresh()};

    // Check the extended expiration limit.
    std::chrono::time_point expansion_time_limit =
        std::chrono::system_clock::from_time_t(token.available_time());
    if (expires_time > expansion_time_limit) {
      // If the limit is exceeded, revise to the longest expiration date.
      expires_time = expansion_time_limit;
    }
    jwt_builder.set_payload_claim(Token::Payload::kExpirationRefresh,
                                  jwt::claim(expires_time));
  }

  // Cryptographic algorithms.
  auto algorithm = jwt::algorithm::hs256{Config::get_jwt_secret_key()};
  // Sign the JWT token.
  auto signed_token = jwt_builder.sign(algorithm);

  // Set of JWT token.
  token_string = std::string(signed_token.c_str());

  error = ErrorCode::OK;
  return error;
}

/**
 * @brief Generate an access token.
 * @param (user_name)  [in]  user name.
 * @retval ErrorCode::OK if success.
 * @retval ErrorCode::AUTHENTICATION_FAILURE if the authentication failed.
 * @retval ErrorCode::CONNECTION_FAILURE if the connection to the
 *   database failed.
 */
std::string Authentication::generate_token(std::string_view user_name) {
  // Cryptographic algorithms.
  auto algorithm = jwt::algorithm::hs256{Config::get_jwt_secret_key()};

  // Set the expiration date.
  auto now_time = std::chrono::system_clock::now();
  auto exp_time = now_time + std::chrono::seconds{Config::get_jwt_expiration()};
  auto exp_ref_time =
      now_time + std::chrono::seconds{Config::get_jwt_expiration_refresh()};
  auto exp_use_time =
      now_time + std::chrono::seconds{Config::get_jwt_expiration_available()};

  // Setting up data for token.
  auto jwt_builder = jwt::create()
                         .set_type(Token::Header::kType)
                         .set_issuer(Config::get_jwt_issuer())
                         .set_audience(Config::get_jwt_audience())
                         .set_subject(Config::get_jwt_subject())
                         .set_issued_at(now_time)
                         .set_expires_at(exp_time)
                         .set_payload_claim(Token::Payload::kExpirationRefresh,
                                            jwt::claim(exp_ref_time))
                         .set_payload_claim(Token::Payload::kAuthUserName,
                                            jwt::claim(std::string(user_name)));

  // Setting up available date.
  if (Config::get_jwt_expiration_available() != 0) {
    jwt_builder.set_payload_claim(Token::Payload::kExpirationAvailable,
                                  jwt::claim(exp_use_time));
  } else {
    jwt_builder.set_payload_claim(Token::Payload::kExpirationAvailable,
                                  jwt::claim(0));
  }

  // Sign the JWT token.
  auto signed_token = jwt_builder.sign(algorithm);

  return std::string(signed_token.c_str());
}

}  // namespace manager::authentication
