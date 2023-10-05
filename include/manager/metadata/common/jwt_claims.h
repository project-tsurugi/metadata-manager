/*
 * Copyright 2021 Project Tsurugi.
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
#ifndef MANAGER_METADATA_COMMON_JWT_CLAIMS_H_
#define MANAGER_METADATA_COMMON_JWT_CLAIMS_H_

#include <cstdint>

namespace manager::metadata::token {

struct Header {
  static constexpr const char* const kType = "JWT";
};  // struct Header

struct Payload {
  static constexpr const char* const kTokenType    = "sub";
  static constexpr const char* const kAuthUserName = "tsurugi/auth/name";
};  // struct Claim

struct Leeway {
  static constexpr std::int32_t kIssued              = 10;
  static constexpr std::int32_t kExpiration          = 10;
  static constexpr std::int32_t kExpirationRefresh   = 10;
  static constexpr std::int32_t kExpirationAvailable = 10;
};  // struct Leeway

struct TokenType {
  static constexpr const char* const kAccess  = "access";
};  // struct TokenType

}  // namespace manager::metadata::token

#endif  // MANAGER_METADATA_COMMON_JWT_CLAIMS_H_
