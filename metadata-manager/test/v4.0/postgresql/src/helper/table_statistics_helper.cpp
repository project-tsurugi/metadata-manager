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
#include "test/helper/table_statistics_helper.h"

#include <gtest/gtest.h>

#include <limits>

namespace manager::metadata::testing {

std::vector<float> reltuples_list = {-1,
                                     0,
                                     1,
                                     100000000,
                                     FLT_MAX,
                                     std::numeric_limits<float>::infinity(),
                                     -std::numeric_limits<float>::infinity(),
                                     std::numeric_limits<float>::quiet_NaN(),
                                     static_cast<float>(DBL_MAX),
                                     static_cast<float>(DBL_MIN)};

/**
 * @brief Create a test pattern for the basic test.
 * @param (test_number)  [in]  test number.
 * @return test pattern.
 */
std::vector<TestTableStatisticsType>
TableStatisticsHelper::make_test_patterns_for_basic_tests(
    std::string_view test_number) {
  std::vector<TestTableStatisticsType> v;
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
