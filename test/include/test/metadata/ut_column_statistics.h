/*
 * Copyright 2022 Project Tsurugi.
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
#ifndef TEST_INCLUDE_TEST_METADATA_UT_COLUMN_STATISTICS_H_
#define TEST_INCLUDE_TEST_METADATA_UT_COLUMN_STATISTICS_H_

#include <string>

#include <boost/property_tree/ptree.hpp>

#include "test/common/dummy_object.h"
#include "test/metadata/ut_metadata.h"

namespace manager::metadata::testing {

class UtColumnStatistics : public UtMetadata<DummyObject> {
 public:
  using UtMetadata::UtMetadata;
  UtColumnStatistics() { this->generate_test_metadata(); }
  explicit UtColumnStatistics(ObjectId table_id)
      : table_id_(table_id), column_number_(1) {
    this->generate_test_metadata();
  }
  UtColumnStatistics(ObjectId table_id, int64_t column_number)
      : table_id_(table_id), column_number_(column_number) {
    this->generate_test_metadata();
  }
  UtColumnStatistics(ObjectId table_id, int64_t column_number,
                     std::string_view statistic_name)
      : table_id_(table_id),
        column_number_(column_number),
        statistic_name_(statistic_name) {
    this->generate_test_metadata();
  }

  void check_metadata_expected(const boost::property_tree::ptree& expected,
                               const boost::property_tree::ptree& actual,
                               const char* file,
                               const int64_t line) const override;

  /**
   * @brief Verifies that the actual metadata equals expected one.
   * @param actual    [in]  actual metadata.
   * @param file      [in]  file name of the caller.
   * @param line      [in]  line number of the caller.
   * @return none.
   */
  void check_metadata_expected(const boost::property_tree::ptree& actual,
                               const char* file, const int64_t line) const {
    check_metadata_expected(metadata_ptree_, actual, file, line);
  }

  boost::property_tree::ptree get_column_statistic() const;

 private:
  ObjectId table_id_          = NOT_INITIALIZED;
  std::int64_t column_number_ = NOT_INITIALIZED;
  std::string statistic_name_;

  void generate_test_metadata();
  boost::property_tree::ptree generate_histogram();
  boost::property_tree::ptree generate_histogram_array();
  std::string generate_random_string();
};

}  // namespace manager::metadata::testing

#endif  // TEST_INCLUDE_TEST_METADATA_UT_COLUMN_STATISTICS_H_
