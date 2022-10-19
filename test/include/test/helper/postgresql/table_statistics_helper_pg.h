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
#ifndef TEST_INCLUDE_TEST_HELPER_POSTGRESQL_TABLE_STATISTICS_HELPER_PG_H_
#define TEST_INCLUDE_TEST_HELPER_POSTGRESQL_TABLE_STATISTICS_HELPER_PG_H_

#include <string>
#include <string_view>
#include <tuple>
#include <vector>

namespace manager::metadata::testing {

class TableStatisticsHelper {
 public:
  using BasicTestParameter = std::tuple<std::string, float, float>;

  static std::vector<BasicTestParameter> make_test_patterns_for_basic_tests(
      std::string_view test_number);
};

}  // namespace manager::metadata::testing

#endif  // TEST_INCLUDE_TEST_HELPER_POSTGRESQL_TABLE_STATISTICS_HELPER_PG_H_
