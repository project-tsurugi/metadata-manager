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
#ifndef MANAGER_AUTHENTICATION_MANAGER_INCLUDE_MANAGER_AUTHENTICATION_ACCESS_TOKEN_H_
#define MANAGER_AUTHENTICATION_MANAGER_INCLUDE_MANAGER_AUTHENTICATION_ACCESS_TOKEN_H_

#include <chrono>
#include <set>
#include <string>
#include <string_view>

namespace manager::authentication {

class AccessToken {
 public:
  AccessToken() : access_token_("") {}
  explicit AccessToken(std::string_view token) { decode_token(token); }
  void operator=(std::string_view token) { decode_token(token); }
  AccessToken(const AccessToken&) = delete;
  AccessToken& operator=(const AccessToken&) = delete;

  std::string show();

  bool is_valid();
  bool is_available();

  std::string type();
  std::string issuer();
  std::set<std::string> audience();
  std::string subject();
  std::time_t issued_time();
  std::time_t expiration_time();
  std::time_t refresh_expiration_time();
  std::time_t available_time();
  std::string user_name();

 private:
  std::string access_token_;
  std::string type_;
  std::string issuer_;
  std::set<std::string> audience_;
  std::string subject_;
  std::time_t issued_time_;
  std::time_t expiration_time_;
  std::time_t refresh_expiration_time_;
  std::time_t available_time_;
  std::string user_name_;

  void decode_token(std::string_view token_string);
  template<typename T>
  bool validate_required(T& decoded);
};  // class AccessToken

}  // namespace manager::authentication

#endif  // MANAGER_AUTHENTICATION_MANAGER_INCLUDE_MANAGER_AUTHENTICATION_ACCESS_TOKEN_H_
