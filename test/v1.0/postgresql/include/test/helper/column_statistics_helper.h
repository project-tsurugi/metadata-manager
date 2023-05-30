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
#ifndef TEST_POSTGRESQL_INCLUDE_TEST_HELPER_COLUMN_STATISTICS_HELPER_H_
#define TEST_POSTGRESQL_INCLUDE_TEST_HELPER_COLUMN_STATISTICS_HELPER_H_

#include <string>
#include <string_view>
#include <tuple>
#include <vector>

#include <boost/property_tree/ptree.hpp>

#include "manager/metadata/metadata.h"

namespace manager::metadata::testing {

class ColumnStatisticsHelper {
 public:
  using BasicTestParameter =
      std::tuple<std::string, std::vector<boost::property_tree::ptree>,
                 ObjectIdType>;
  using UpdateTestParameter =
      std::tuple<std::string, std::vector<boost::property_tree::ptree>,
                 std::vector<boost::property_tree::ptree>, ObjectIdType>;

  static std::vector<BasicTestParameter> make_test_patterns_for_basic_tests(
      std::string_view test_number);
  static std::vector<UpdateTestParameter>
  make_test_patterns_for_update_tests(std::string_view test_number);

  static void add_column_statistics(
      const ObjectIdType table_id,
      const std::vector<boost::property_tree::ptree>& column_statistics);

  static boost::property_tree::ptree generate_column_statistic();

 private:
  static constexpr int NUMBER_OF_ITERATIONS = 10;
  static constexpr int NUMBER_OF_RANDOM_CHARACTER = 10;
  static constexpr int UPPER_VALUE_100 = 100;
  static constexpr int UPPER_VALUE_20000 = 20000;
  static constexpr char ALPHA_NUM[] =
      "0123456789"
      "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
      "abcdefghijklmnopqrstuvwxyz";

  static boost::property_tree::ptree generate_histogram();
  static boost::property_tree::ptree generate_histogram_array();
  static std::string generate_random_string();
};

}  // namespace manager::metadata::testing

#endif  // TEST_POSTGRESQL_INCLUDE_TEST_HELPER_COLUMN_STATISTICS_HELPER_H_
