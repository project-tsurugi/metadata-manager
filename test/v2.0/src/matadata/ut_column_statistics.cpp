/*
 * Copyright 2022-2023 tsurugi project.
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
#include "test/metadata/ut_column_statistics.h"

#include <random>

#include "manager/metadata/statistic.h"
#include "test/common/ut_utils.h"

namespace {

static constexpr int kNumberOfIterations      = 10;
static constexpr int kNumberOfRandomChar      = 10;
static constexpr int kUpperValueStatisticData = 100;
static constexpr int kUpperValueHistogram     = 20000;
static constexpr char kAlphaNum[] =
    "0123456789"
    "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
    "abcdefghijklmnopqrstuvwxyz";

}  // namespace

namespace manager::metadata::testing {

using boost::property_tree::ptree;

/**
 * @brief Verifies that the actual table metadata equals expected one.
 * @param expected  [in]  expected table metadata.
 * @param actual    [in]  actual table metadata.
 * @param file      [in]  file name of the caller.
 * @param line      [in]  line number of the caller.
 * @return none.
 */
void UtColumnStatistics::check_metadata_expected(
    const boost::property_tree::ptree& expected,
    const boost::property_tree::ptree& actual, const char* file,
    const int64_t line) const {
  // column statistics id
  auto opt_id_expected = expected.get_optional<ObjectId>(Statistic::ID);
  if (opt_id_expected) {
    check_expected<ObjectId>(expected, actual, Statistic::ID, file, line);
  } else {
    auto opt_id_actual = actual.get_optional<ObjectId>(Statistic::ID);
    EXPECT_GT_EX(opt_id_actual.get_value_or(INVALID_OBJECT_ID), 0, file, line);
  }

  // column statistics column id.
  auto opt_column_id_expected =
      expected.get_optional<ObjectId>(Statistic::COLUMN_ID);
  if (opt_column_id_expected) {
    check_expected<ObjectId>(expected, actual, Statistic::COLUMN_ID, file,
                             line);
  } else {
    auto opt_column_id_actual =
        actual.get_optional<ObjectId>(Statistic::COLUMN_ID);
    EXPECT_GT_EX(opt_column_id_actual.get_value_or(INVALID_OBJECT_ID), 0, file,
                 line);
  }

  // column statistics name.
  check_expected<std::string>(expected, actual, Statistic::NAME, file, line);

  // column statistics table id.
  check_expected<ObjectId>(expected, actual, Statistic::TABLE_ID, file, line);

  // column statistics column number.
  check_expected<int64_t>(expected, actual, Statistic::COLUMN_NUMBER, file,
                          line);

  // column statistics column name.
  check_expected<ObjectId>(expected, actual, Statistic::COLUMN_NAME, file,
                           line);

  // column statistics column statistic.
  check_child_expected(expected, actual, Statistic::COLUMN_STATISTIC, file,
                       line);
}

/**
 * @brief Get the column statistic object.
 * @return boost::property_tree::ptree - column statistic.
 */
boost::property_tree::ptree UtColumnStatistics::get_column_statistic() const {
  return metadata_ptree_.get_child(Statistic::COLUMN_STATISTIC);
}

/**
 * @brief Generate metadata for testing.
 */
void UtColumnStatistics::generate_test_metadata() {
  // Generate unique statistic name.
  std::string statistic_name =
      statistic_name_.empty()
          ? "statistic_name_" + UTUtils::generate_narrow_uid()
          : statistic_name_;

  // name
  metadata_ptree_.put(Statistic::NAME, statistic_name);
  // table_id
  metadata_ptree_.put(Statistic::TABLE_ID, table_id_);
  // column_number
  metadata_ptree_.put(Statistic::COLUMN_NUMBER, column_number_);
  // column_statistic
  {
    std::random_device rd;
    std::mt19937 random_mt(rd());
    ptree column_statistics;

    double null_frac   = static_cast<double>(random_mt() / RAND_MAX);
    int avg_width      = random_mt() % kUpperValueStatisticData + 1;
    int n_distinct     = random_mt() % kUpperValueStatisticData + 1;
    double correlation = -1 * static_cast<double>(random_mt() / RAND_MAX);

    column_statistics.put("null_frac", null_frac);
    column_statistics.put("avg_width", avg_width);
    column_statistics.put("most_common_vals", "mcv");
    column_statistics.put("n_distinct", n_distinct);
    column_statistics.put("most_common_freqs", "mcf");
    column_statistics.add_child("histogram_bounds", generate_histogram());
    column_statistics.put("correlation", correlation);
    column_statistics.put("most_common_elems", "mce");
    column_statistics.put("most_common_elem_freqs", "mcef");
    column_statistics.add_child("elem_count_histogram",
                                generate_histogram_array());

    metadata_ptree_.add_child(Statistic::COLUMN_STATISTIC, column_statistics);

    metadata_struct_->name = statistic_name;
  }
}

/**
 * @brief Generate histogram of values used as column statistics test data.
 */
boost::property_tree::ptree UtColumnStatistics::generate_histogram() {
  ptree values;
  std::random_device rd;
  std::mt19937 random_mt(rd());

  int random_number = random_mt();

  // If random number is even, generate random number histogram.
  // If random number is odd, generate random string histogram.
  if (random_number % 2 == 0) {
    for (int i = 0; i < static_cast<int>(random_mt() % kNumberOfIterations + 1);
         i++) {
      ptree p_value;
      int i_value = random_mt() % kUpperValueHistogram + 1;
      p_value.put("", i_value);
      values.push_back(std::make_pair("", p_value));
    }
  } else {
    for (int i = 0; i < static_cast<int>(random_mt() % kNumberOfIterations + 1);
         i++) {
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
boost::property_tree::ptree UtColumnStatistics::generate_histogram_array() {
  ptree array_of_values;
  std::random_device rd;
  std::mt19937 random_mt(rd());

  for (int i = 0; i < static_cast<int>(random_mt() % kNumberOfIterations + 1);
       i++) {
    ptree values;
    array_of_values.push_back(std::make_pair("", generate_histogram()));
  }

  return array_of_values;
}

/**
 * @brief Generate one random string.
 */
std::string UtColumnStatistics::generate_random_string() {
  std::string random_string;
  std::random_device rd;
  std::mt19937 random_mt(rd());

  for (int i = 0; i < static_cast<int>(random_mt() % kNumberOfRandomChar + 1);
       i++) {
    random_string.push_back(kAlphaNum[random_mt() % (sizeof(kAlphaNum) - 1)]);
  }

  return random_string;
}

}  // namespace manager::metadata::testing
