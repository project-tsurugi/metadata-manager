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
#ifndef MANAGER_METADATA_COMMON_TOKEN_H_
#define MANAGER_METADATA_COMMON_TOKEN_H_

#include <ctime>
#include <set>
#include <string>

namespace manager::metadata {

class Token {
 public:
  Token() : token_string_("") {}
  explicit Token(std::string_view token) { decode_token(token); }
  Token(const Token&) = delete;

  virtual ~Token() {}

  void operator=(std::string_view token) { decode_token(token); }
  Token& operator=(const Token&) = delete;

  /**
   * @brief Get an access token.
   * @return access token.
   */
  std::string string() const { return token_string_; }

  /**
   * @brief Get the value of the type claim.
   * @return type as a string.
   */
  std::string type() const { return type_; }

  /**
   * @brief Get a value of the issuer claim.
   * @return issuer as string.
   */
  std::string issuer() const { return issuer_; }

  /**
   * @brief Get a value of the audience claim.
   * @return audience as a set of strings.
   */
  std::set<std::string> audience() const { return audience_; }

  /**
   * @brief Get a value of the subject claim.
   * @return subject as string.
   */
  std::string subject() const { return subject_; }

  /**
   * @brief Get a value of the issued-at claim.
   * @return Date/time of issue (epoch time).
   */
  std::time_t issued_time() const { return issued_time_; }

  /**
   * @brief Get a value of the expires-at claim.
   * @return Expiration date/time (epoch time).
   */
  std::time_t expiration_time() const { return expiration_time_; }

  /**
   * @brief Get a value of the user name claim.
   * @return user name as string.
   */
  std::string user_name() const { return user_name_; }

  bool is_valid() const;
  bool is_access_token() const;
  bool is_refresh_token() const;
  bool is_valid_access_token() const;
  bool is_valid_refresh_token() const;

 private:
  std::string token_string_;
  std::string type_;
  std::string issuer_;
  std::set<std::string> audience_;
  std::string subject_;
  std::time_t issued_time_;
  std::time_t expiration_time_;
  std::string user_name_;

  void decode_token(std::string_view token_string);
};  // class Token

}  // namespace manager::metadata

#endif  // MANAGER_METADATA_COMMON_TOKEN_H_
