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
#include "test/helper/column_statistics_helper.h"

#include <gtest/gtest.h>

#include <random>

#include "manager/metadata/statistics.h"
#include "test/global_test_environment.h"
#include "test/utility/ut_utils.h"

namespace manager::metadata::testing {

using boost::property_tree::ptree;

/**
 * @brief Create a test pattern for the basic test.
 * @param (test_number)  [in]  test number.
 * @return test pattern.
 */
std::vector<TestColumnStatisticsBasicType>
ColumnStatisticsHelper::make_test_patterns_for_basic_tests(
    std::string_view test_number) {
  std::vector<ptree> column_statistics;
  for (int i = 0; i < 3; i++) {
    column_statistics.push_back(generate_column_statistic());
  }

  std::vector<ptree> empty_columns;
  ptree empty_column;
  for (int i = 0; i < 3; i++) {
    empty_columns.push_back(empty_column);
  }

  std::vector<TestColumnStatisticsBasicType> v;
  v.emplace_back("_ColumnStatistic_" + std::string(test_number) + "_1",
                 column_statistics, 1);
  v.emplace_back("_ColumnStatistic_" + std::string(test_number) + "_2",
                 empty_columns, 2);
  v.emplace_back("_ColumnStatistic_" + std::string(test_number) + "_3",
                 column_statistics, 3);
  return v;
}

/**
 * @brief Create a test pattern for the update test.
 * @param (test_number)  [in]  test number.
 * @return test pattern.
 */
std::vector<TestColumnStatisticsUpdateType>
ColumnStatisticsHelper::make_test_patterns_for_update_tests(
    std::string_view test_number) {
  std::vector<TestColumnStatisticsUpdateType> v;
  std::vector<int> number_of_columns = {1, 2, 2, 3};
  std::vector<ObjectIdType> ordinal_positions_to_remove = {1, 1, 2, 3};

  int test_case_no = 0;
  for (int noc : number_of_columns) {
    std::vector<ptree> cs;
    for (int i = 0; i < noc; i++) {
      cs.push_back(generate_column_statistic());
    }
    std::vector<ptree> empty_columns;
    ptree empty_column;
    for (int i = 0; i < noc; i++) {
      empty_columns.push_back(empty_column);
    }
    v.emplace_back("_ColumnStatistic_" + std::string(test_number) + "_" +
                       std::to_string(test_case_no),
                   cs, empty_columns,
                   ordinal_positions_to_remove[test_case_no]);
    test_case_no++;
  }

  return v;
}

/**
 * @brief Add column statistics based on the given table id and
 *   the given ptree type column statistics.
 * @param (table_id)           [in]  table id.
 * @param (column_statistics)  [in]  ptree type column statistics.
 * @return none.
 */
void ColumnStatisticsHelper::add_column_statistics(
    const ObjectIdType table_id,
    const std::vector<boost::property_tree::ptree>& column_statistics) {
  auto stats = std::make_unique<Statistics>(GlobalTestEnvironment::TEST_DB);

  ErrorCode error = stats->init();
  ASSERT_EQ(ErrorCode::OK, error);

  UTUtils::print("-- add column statistics by add_column_statistic start --");
  UTUtils::print(" id:", table_id);

  boost::property_tree::ptree statistic;

  for (std::int64_t ordinal_position = 1;
       static_cast<std::size_t>(ordinal_position) <= column_statistics.size();
       ordinal_position++) {
    // clear
    statistic.clear();
    // name
    std::string statistic_name = "TestColumnStatistics_" +
                                 std::to_string(table_id) + "-" +
                                 std::to_string(ordinal_position);
    statistic.put(Statistics::NAME, statistic_name);
    // table_id
    statistic.put(Statistics::TABLE_ID, table_id);
    // ordinal_position
    statistic.put(Statistics::ORDINAL_POSITION, ordinal_position);
    // column_statistic
    statistic.add_child(Statistics::COLUMN_STATISTIC,
                        column_statistics[ordinal_position - 1]);

    error = stats->add(statistic);
    ASSERT_EQ(ErrorCode::OK, error);

    UTUtils::print(" ordinal position: ", ordinal_position);
    UTUtils::print(
        " column statistics:" +
        UTUtils::get_tree_string(column_statistics[ordinal_position - 1]));
  }

  UTUtils::print("-- add column statistics by add_column_statistic end --\n");
}

/**
 * @brief Generate one column statistics used as test data.
 */
boost::property_tree::ptree
ColumnStatisticsHelper::generate_column_statistic() {
  std::random_device rd;
  std::mt19937 random_mt(rd());

  double null_frac = static_cast<double>(random_mt() / RAND_MAX);
  int avg_width = random_mt() % UPPER_VALUE_100 + 1;
  int n_distinct = random_mt() % UPPER_VALUE_100 + 1;
  double correlation = -1 * static_cast<double>(random_mt() / RAND_MAX);

  ptree column_statistic;
  column_statistic.put("null_frac", null_frac);
  column_statistic.put("avg_width", avg_width);
  column_statistic.put("most_common_vals", "mcv");
  column_statistic.put("n_distinct", n_distinct);
  column_statistic.put("most_common_freqs", "mcf");
  column_statistic.add_child("histogram_bounds", generate_histogram());
  column_statistic.put("correlation", correlation);
  column_statistic.put("most_common_elems", "mce");
  column_statistic.put("most_common_elem_freqs", "mcef");
  column_statistic.add_child("elem_count_histogram",
                             generate_histogram_array());

  return column_statistic;
}

/**
 * @brief Generate histogram of values used as column statistics test data.
 */
boost::property_tree::ptree ColumnStatisticsHelper::generate_histogram() {
  ptree values;
  std::random_device rd;
  std::mt19937 random_mt(rd());

  int random_number = random_mt();

  // If random number is even, generate random number histogram.
  // If random number is odd, generate random string histogram.
  if (random_number % 2 == 0) {
    for (int i = 0;
         i < static_cast<int>(random_mt() % NUMBER_OF_ITERATIONS + 1); i++) {
      ptree p_value;
      int i_value = random_mt() % UPPER_VALUE_20000 + 1;
      p_value.put("", i_value);
      values.push_back(std::make_pair("", p_value));
    }
  } else {
    for (int i = 0;
         i < static_cast<int>(random_mt() % NUMBER_OF_ITERATIONS + 1); i++) {
      ptree p_value;
      std::string random_string = generate_random_string();
      p_value.put("", random_string);
      values.push_back(std::make_pair("", p_value));
    }
  }

  return values;
}

/**
 * @brief Generate histogram of array elements used as column statistics test
 * data.
 */
boost::property_tree::ptree ColumnStatisticsHelper::generate_histogram_array() {
  ptree array_of_values;
  std::random_device rd;
  std::mt19937 random_mt(rd());

  for (int i = 0; i < static_cast<int>(random_mt() % NUMBER_OF_ITERATIONS + 1);
       i++) {
    ptree values;
    array_of_values.push_back(std::make_pair("", generate_histogram()));
  }

  return array_of_values;
}

/**
 * @brief Generate one random string.
 */
std::string ColumnStatisticsHelper::generate_random_string() {
  std::string random_string;
  std::random_device rd;
  std::mt19937 random_mt(rd());

  for (int i = 0;
       i < static_cast<int>(random_mt() % NUMBER_OF_RANDOM_CHARACTER + 1);
       i++) {
    random_string.push_back(ALPHANUM[random_mt() % (sizeof(ALPHANUM) - 1)]);
  }

  return random_string;
}

}  // namespace manager::metadata::testing
