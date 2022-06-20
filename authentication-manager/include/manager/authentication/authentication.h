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
#ifndef MANAGER_AUTHENTICATION_MANAGER_INCLUDE_MANAGER_AUTHENTICATION_AUTHENTICATION_H_
#define MANAGER_AUTHENTICATION_MANAGER_INCLUDE_MANAGER_AUTHENTICATION_AUTHENTICATION_H_

#include <chrono>
#include <string>
#include <string_view>

#include <boost/property_tree/ptree.hpp>

#include "manager/authentication/error_code.h"

namespace manager::authentication {

class Authentication {
 public:
  static ErrorCode auth_user(
      const boost::property_tree::ptree& connection_params);
  static ErrorCode auth_user(std::string_view connection_string);

  static ErrorCode auth_user(std::string_view user_name,
                             std::string_view password, std::string* token);
  static ErrorCode auth_user(std::string_view connection_string,
                             std::string_view user_name,
                             std::string_view password, std::string* token);

  static ErrorCode refresh_token(std::string& token,
                                 std::chrono::seconds extend_time);

 private:
  static std::string generate_token(std::string_view user_name);
};  // class Authentication

}  // namespace manager::authentication

#endif  // MANAGER_AUTHENTICATION_MANAGER_INCLUDE_MANAGER_AUTHENTICATION_AUTHENTICATION_H_
