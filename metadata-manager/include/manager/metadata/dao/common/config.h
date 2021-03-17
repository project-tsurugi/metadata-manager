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

#ifndef CONFIG_H_
#define CONFIG_H_

#include <string>

namespace manager::metadata::db {

class Config {
   public:
    static std::string get_connection_string();

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
};
}  // namespace manager::metadata::db

#endif  // CONFIG_H_
