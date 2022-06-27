/*
 * Copyright 2020-2021 tsurugi project.
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
#ifndef MANAGER_AUTHENTICATION_MANAGER_TEST_V1_0_POSTGRESQL_INCLUDE_TEST_UTILITY_UT_UTILS_H_
#define MANAGER_AUTHENTICATION_MANAGER_TEST_V1_0_POSTGRESQL_INCLUDE_TEST_UTILITY_UT_UTILS_H_

#include <iostream>

namespace manager::authentication::testing {

class UTUtils {
 public:
  static void print() {
#ifndef NDEBUG
    std::cout << std::endl;
#endif
  }

  template <class T, class... A>
  static void print([[maybe_unused]] const T& first,
                    [[maybe_unused]] const A&... rest) {
#ifndef NDEBUG
    std::cout << first;
    print(rest...);
#endif
  }

  template <class... A>
  static void print([[maybe_unused]] const A&... rest) {
#ifndef NDEBUG
    print(rest...);
#endif
  }
};  // class UTUtils

}  // namespace manager::authentication::testing

#endif  // MANAGER_AUTHENTICATION_MANAGER_TEST_V1_0_POSTGRESQL_INCLUDE_TEST_UTILITY_UT_UTILS_H_
