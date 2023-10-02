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
#include "test/helper/postgresql/table_statistics_helper_pg.h"

#include <gtest/gtest.h>

#include <limits>

namespace manager::metadata::testing {

std::vector<int64_t> reltuples_list = {
    -1,
    0,
    1,
    100000000,
    std::numeric_limits<int64_t>::infinity(),
    -std::numeric_limits<int64_t>::infinity(),
    std::numeric_limits<int64_t>::quiet_NaN(),
    INT64_MAX,
    INT64_MIN};

/**
 * @brief Create a test pattern for the basic test.
 * @param (test_number)  [in]  test number.
 * @return test pattern.
 */
std::vector<TableStatisticsHelper::BasicTestParameter>
TableStatisticsHelper::make_test_patterns_for_basic_tests(
    std::string_view test_number) {
  std::vector<BasicTestParameter> v;
  int next;
  for (int i = 0; static_cast<size_t>(i) < reltuples_list.size(); i++) {
    next = (i + 1) % reltuples_list.size();
    std::string table_name =
        "_TableStatistic_" + std::string(test_number) + "_" + std::to_string(i);
    v.emplace_back(table_name, reltuples_list[i], reltuples_list[next]);
  }
  return v;
}

}  // namespace manager::metadata::testing
