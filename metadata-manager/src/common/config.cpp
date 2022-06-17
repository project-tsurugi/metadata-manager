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
#include "manager/metadata/common/config.h"

#include <boost/format.hpp>
#include <cstdlib>

// =============================================================================
namespace {

namespace key {

/**
 * @brief The name of an OS environment variable for a Connection Strings.
 */
static constexpr const char* const kTsurugiConnectionString =
    "TSURUGI_CONNECTION_STRING";

/**
 * @brief The name of the OS environment variable for the directory that
 *   stores the metadata.
 */
static constexpr const char* const kTsurugiMetadataDir = "TSURUGI_METADATA_DIR";

/**
 * @brief The name of the OS environment variable in the user's home
 *   directory.
 */
static constexpr const char* const kHomeDir = "HOME";

/**
 * @brief The name of an OS environment variable for the JWT secret key.
 */
static constexpr const char* const kJwtSecretKey = "TSURUGI_JWT_SECRET_KEY";

}  // namespace key

namespace default_value {

/**
 * @brief Default Connection Strings.
 *   By default, several libpq functions parse this default connection strings
 *   to obtain connection parameters.
 */
static constexpr const char* const kConnectionString = "dbname=tsurugi";

/**
 * @brief Default user's home directory.
 */
static constexpr const char* const kHomeDir = ".";

/**
 * @brief Default directory that stores the metadata.
 *   Metadata is stored under $HOME/[default directory].
 */
static constexpr const char* const kTsurugiMetadataDir =
    ".local/tsurugi/metadata";

/**
 * @brief Default JWT secret key.
 */
static constexpr const char* const kJwtSecretKey =
    "qiZB8rXTdet7Z3HTaU9t2TtcpmV6FXy7";

}  // namespace default_value

}  // namespace

// =============================================================================
namespace manager::metadata {

/**
 * @brief Gets connection string to the DB where the metadata is stored.
 * @return Connection Strings.
 */
std::string Config::get_connection_string() {
  const char* env_value = std::getenv(key::kTsurugiConnectionString);
  return (env_value != nullptr ? env_value : default_value::kConnectionString);
}

/**
 * @brief Gets the directory that stores the metadata.
 * @return Directory that stores the metadata.
 */
std::string Config::get_storage_dir_path() {
  std::string result_value = "";

  const char* env_value = std::getenv(key::kTsurugiMetadataDir);
  if (env_value != nullptr) {
    result_value = env_value;
  } else {
    const char* env_value_home = std::getenv(key::kHomeDir);
    env_value_home =
        (env_value_home != nullptr ? env_value_home : default_value::kHomeDir);

    // default value.
    boost::format storage_dir = boost::format("%s/%s") % env_value_home %
                                default_value::kTsurugiMetadataDir;
    result_value = storage_dir.str();
  }
  return result_value;
}

/**
 * @brief Gets JWT secret key.
 * @return JWT secret key.
 */
std::string Config::get_jwt_secret_key() {
  const char* env_value = std::getenv(key::kJwtSecretKey);
  return (env_value != nullptr ? env_value : default_value::kJwtSecretKey);
}

}  // namespace manager::metadata
