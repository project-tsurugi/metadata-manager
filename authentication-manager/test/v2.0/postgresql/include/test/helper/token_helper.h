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
#ifndef MANAGER_AUTHENTICATION_MANAGER_TEST_V2_0_POSTGRESQL_INCLUDE_TEST_HELPER_TOKEN_HELPER_H_
#define MANAGER_AUTHENTICATION_MANAGER_TEST_V2_0_POSTGRESQL_INCLUDE_TEST_HELPER_TOKEN_HELPER_H_

#include <string>
#include <string_view>

namespace manager::authentication::testing {

class TokenHelper {
 public:
  static std::string generate_token(std::string_view user_name,
                                    std::int32_t expiration,
                                    std::int32_t refresh,
                                    std::int32_t available);
};

}  // namespace manager::authentication::testing

#endif  // MANAGER_AUTHENTICATION_MANAGER_TEST_V2_0_POSTGRESQL_INCLUDE_TEST_HELPER_TOKEN_HELPER_H_
