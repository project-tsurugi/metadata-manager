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

#include <cstdlib>
#include <string>

namespace manager::metadata::db {

/**
 * @brief Gets Connection Strings.
 * @return Connection Strings.
 */
std::string Config::get_connection_string() {
    const char *tmp_cs = std::getenv(TSURUGI_CONNECTION_STRING);

    if (tmp_cs != nullptr) {
        return tmp_cs;
    }

    return DEFAULT_CONNECTION_STRING;
}

}  // namespace manager::metadata::db
