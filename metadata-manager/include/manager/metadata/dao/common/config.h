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

#ifndef MANAGER_METADATA_MANAGER_INCLUDE_MANAGER_METADATA_DAO_COMMON_CONFIG_H_
#define MANAGER_METADATA_MANAGER_INCLUDE_MANAGER_METADATA_DAO_COMMON_CONFIG_H_

#include <string>

namespace manager::metadata::db {

class Config {
 public:
  static std::string get_connection_string();
  static std::string get_storage_dir_path();

 private:
  /**
   * @brief The name of an OS environment variable
   * for a Connection Strings.
   * A Connection Strings is set to this environment variable.
   */
  static constexpr const char* const TSURUGI_CONNECTION_STRING =
      "TSURUGI_CONNECTION_STRING";

  /**
   * @brief Default Connection Strings.
   * By default, several libpq functions parse
   * this default connection strings
   * to obtain connection parameters.
   */
  static constexpr const char* const DEFAULT_CONNECTION_STRING =
      "dbname=tsurugi";

  /**
   * @brief The name of the OS environment variable for the directory that
   * stores the metadata. Directory that stores the metadata is set to this
   * environment variable.
   */
  static constexpr const char* const TSURUGI_METADATA_DIR =
      "TSURUGI_METADATA_DIR";

  /**
   * @brief The name of the OS environment variable in the user's home
   * directory.
   */
  static constexpr const char* const HOME_DIR = "HOME";

  /**
   * @brief Default directory that stores the metadata.
   * Metadata is stored under $HOME/[default directory].
   */
  static constexpr const char* const DEFAULT_TSURUGI_METADATA_DIR =
      ".local/tsurugi/metadata";
};  // class Config

}  // namespace manager::metadata::db

#endif  // MANAGER_METADATA_MANAGER_INCLUDE_MANAGER_METADATA_DAO_COMMON_CONFIG_H_
