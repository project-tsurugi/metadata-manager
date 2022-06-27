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
#ifndef MANAGER_AUTHENTICATION_MANAGER_INCLUDE_MANAGER_AUTHENTICATION_COMMON_CONFIG_H_
#define MANAGER_AUTHENTICATION_MANAGER_INCLUDE_MANAGER_AUTHENTICATION_COMMON_CONFIG_H_

#include <string>

namespace manager::authentication {

class Config {
 public:
  static std::string get_connection_string();
  static std::string get_jwt_issuer();
  static std::string get_jwt_audience();
  static std::string get_jwt_subject();
  static std::string get_jwt_secret_key();
  static std::int32_t get_jwt_expiration();
  static std::int32_t get_jwt_expiration_refresh();
  static std::int32_t get_jwt_expiration_available();

 private:
  static std::int32_t get_environment_expiration(std::string_view key,
                                                 std::int32_t default_value);
};  // class Config

}  // namespace manager::authentication

#endif  // MANAGER_AUTHENTICATION_MANAGER_INCLUDE_MANAGER_AUTHENTICATION_COMMON_CONFIG_H_
