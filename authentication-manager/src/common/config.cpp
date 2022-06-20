/*
 * Copyright 2020-2021 tsurugi project.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
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
#include "manager/authentication/common/config.h"

#include <map>
#include <regex>

#include <boost/format.hpp>

#include "manager/authentication/common/utilitys.h"

// =============================================================================
namespace {

static constexpr const char* const kRegexTime = R"(^(\d+)(s?|min|h|d)$)";
static constexpr std::int32_t kRegexTimeValuePos = 1;
static constexpr std::int32_t kRegexTimeUnitPos = 2;
static const std::map<std::string, std::int32_t> kConversionTimeUnit = {
    {"", 1}, {"s", 1}, {"min", 60}, {"h", 3600}, {"d", 86400}};

namespace key {

/**
 * @brief The name of an OS environment variable for a Connection Strings.
 */
static constexpr const char* const kTsurugiConnectionString =
    "TSURUGI_CONNECTION_STRING_AUTH";

/**
 * @brief The name of an OS environment variable for the JWT issuer claim value.
 */
static constexpr const char* const kJwtClaimIssuer = "TSURUGI_JWT_CLAIM_ISS";

/**
 * @brief The name of an OS environment variable for the
 *   JWT audience claim value.
 */
static constexpr const char* const kJwtClaimAudience = "TSURUGI_JWT_CLAIM_AUD";

/**
 * @brief The name of an OS environment variable for the
 *   JWT subject claim value.
 */
static constexpr const char* const kJwtClaimSubject = "TSURUGI_JWT_CLAIM_SUB";

/**
 * @brief The name of an OS environment variable for the JWT secret key.
 */
static constexpr const char* const kJwtSecretKey = "TSURUGI_JWT_SECRET_KEY";

/**
 * @brief The name of an OS environment variable for the
 *   JWT expiration.
 */
static constexpr const char* const kJwtExpiration = "TSURUGI_TOKEN_EXPIRATION";

/**
 * @brief The name of an OS environment variable for the
 *   JWT refresh expiration.
 */
static constexpr const char* const kJwtRefreshExpiration =
    "TSURUGI_TOKEN_EXPIRATION_REFRESH";

/**
 * @brief The name of an OS environment variable for the
 *   JWT use expiration.
 */
static constexpr const char* const kJwtAvailableExpiration =
    "TSURUGI_TOKEN_EXPIRATION_AVAILABLE";

}  // namespace key

namespace default_value {

/**
 * @brief Default Connection Strings.
 *   By default, several libpq functions parse this default connection strings
 *   to obtain connection parameters.
 */
static constexpr const char* const kConnectionString = "dbname=tsurugi";

/**
 * @brief Default value of the JWT issuer claim.
 */
static constexpr const char* const kJwtClaimIssuer = "authentication-manager";

/**
 * @brief Default value of the JWT audience claim.
 */
static constexpr const char* const kJwtClaimAudience = "metadata-manager";

/**
 * @brief Default value of the JWT subject claim.
 */
static constexpr const char* const kJwtClaimSubject = "AuthenticationToken";

/**
 * @brief Default value of the JWT secret key.
 */
static constexpr const char* const kJwtSecretKey =
    "qiZB8rXTdet7Z3HTaU9t2TtcpmV6FXy7";

/**
 * @brief Default value of the JWT expiration.
 *   (Unit: seconds)
 */
static constexpr std::int32_t kJwtExpiration = 300;

/**
 * @brief Default value of the JWT refresh expiration.
 *   (Unit: seconds)
 */
static constexpr std::int32_t kJwtRefreshExpiration = 86400;

/**
 * @brief Default value of the JWT use expiration.
 *   (Unit: seconds)
 */
static constexpr std::int32_t kJwtAvailableExpiration = (86400 * 7);

}  // namespace default_value

}  // namespace

// =============================================================================
namespace manager::authentication {

/**
 * @brief Gets connection string for authentication.
 * @return Connection String.
 */
std::string Config::get_connection_string() {
  const char* env_value = std::getenv(key::kTsurugiConnectionString);
  return (env_value != nullptr ? env_value : default_value::kConnectionString);
}

/**
 * @brief Gets JWT issuer value.
 * @return JWT issuer value.
 */
std::string Config::get_jwt_issuer() {
  const char* env_value = std::getenv(key::kJwtClaimIssuer);
  return (env_value != nullptr ? env_value : default_value::kJwtClaimIssuer);
}

/**
 * @brief Gets JWT audience value.
 * @return JWT audience value.
 */
std::string Config::get_jwt_audience() {
  const char* env_value = std::getenv(key::kJwtClaimAudience);
  return (env_value != nullptr ? env_value : default_value::kJwtClaimAudience);
}

/**
 * @brief Gets JWT subject value.
 * @return JWT subject value.
 */
std::string Config::get_jwt_subject() {
  const char* env_value = std::getenv(key::kJwtClaimSubject);
  return (env_value != nullptr ? env_value : default_value::kJwtClaimSubject);
}

/**
 * @brief Gets JWT secret key.
 * @return JWT secret key.
 */
std::string Config::get_jwt_secret_key() {
  const char* env_value = std::getenv(key::kJwtSecretKey);
  return (env_value != nullptr ? env_value : default_value::kJwtSecretKey);
}

/**
 * @brief Gets JWT expiration value.
 * @return JWT expiration value.
 */
std::int32_t Config::get_jwt_expiration() {
  return get_environment_expiration(key::kJwtExpiration,
                                    default_value::kJwtExpiration);
}

/**
 * @brief Gets JWT refresh expiration.
 * @return JWT refresh expiration.
 */
std::int32_t Config::get_jwt_expiration_refresh() {
  return get_environment_expiration(key::kJwtRefreshExpiration,
                                    default_value::kJwtRefreshExpiration);
}

/**
 * @brief Gets JWT available expiration.
 * @return JWT available expiration.
 */
std::int32_t Config::get_jwt_expiration_available() {
  return get_environment_expiration(key::kJwtAvailableExpiration,
                                    default_value::kJwtAvailableExpiration);
}

/**
 * @brief Get the value of the environment variable related to the expiration
 * date.
 * @return Environment variable value.
 */
std::int32_t Config::get_environment_expiration(std::string_view key_name,
                                                std::int32_t default_value) {
  std::int32_t result_value = default_value;

  const char* env_value = std::getenv(key_name.data());
  if (env_value != nullptr) {
    std::smatch regex_results;
    std::string env_value_string = env_value;

    // Convert time by dividing values and units.
    std::regex_match(env_value_string, regex_results, std::regex(kRegexTime));
    if (!regex_results.empty()) {
      // Convert to numeric values.
      std::int32_t env_value_num = 0;
      ErrorCode result_code = Utilitys::str_to_numeric(
          regex_results[kRegexTimeValuePos].str(), env_value_num);

      if (result_code == ErrorCode::OK) {
        // Convert units of time.
        auto conversion_value =
            kConversionTimeUnit.at(regex_results[kRegexTimeUnitPos].str());
        result_value = env_value_num * conversion_value;
      }
    }
  }

  return result_value;
}

}  // namespace manager::authentication
