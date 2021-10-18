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
#ifndef API_TEST_TABLE_STATISTICS_H_
#define API_TEST_TABLE_STATISTICS_H_

#include <gtest/gtest.h>
#include <string>
#include <string_view>
#include <tuple>
#include <vector>

namespace manager::metadata::testing {

typedef std::tuple<std::string, float, float> TupleApiTestTableStatistics;

class ApiTestTableStatistics {
 public:
  static std::vector<TupleApiTestTableStatistics> make_tuple_table_statistics(
      std::string_view test_number);
};

}  // namespace manager::metadata::testing

#endif  // API_TEST_TABLE_STATISTICS_H_
