/*
 * Copyright 2020 tsurugi project.
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
#include "manager/metadata/dao/common/config.h"

#include <boost/format.hpp>
#include <cstdlib>
#include <string>

// =============================================================================
namespace manager::metadata::db {

/**
 * @brief Gets Connection Strings.
 * @return Connection Strings.
 */
std::string Config::get_connection_string() {
  const char* tmp_cs = std::getenv(TSURUGI_CONNECTION_STRING);

  if (tmp_cs != nullptr) {
    return tmp_cs;
  }

  return DEFAULT_CONNECTION_STRING;
}

/**
 * @brief Gets the directory that stores the metadata.
 * @return Directory that stores the metadata.
 */
std::string Config::get_storage_dir_path() {
  const char* tmp_dir = std::getenv(TSURUGI_METADATA_DIR);
  if (tmp_dir != nullptr) {
    return tmp_dir;
  }
  // Returns the default value.
  boost::format storage_dir = boost::format("%s/%s") %
                              std::string(getenv(HOME_DIR)) %
                              DEFAULT_TSURUGI_METADATA_DIR;
  return storage_dir.str();
}

}  // namespace manager::metadata::db
