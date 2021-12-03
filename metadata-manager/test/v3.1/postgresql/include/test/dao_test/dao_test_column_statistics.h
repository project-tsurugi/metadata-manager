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
#ifndef DAO_TEST_COLUMN_STATISTICS_H_
#define DAO_TEST_COLUMN_STATISTICS_H_

#include <gtest/gtest.h>
#include <boost/property_tree/ptree.hpp>
#include <vector>

#include "manager/metadata/metadata.h"

namespace manager::metadata::testing {

class DaoTestColumnStatistics : public ::testing::Test {
 public:
  static void add_column_statistics(
      ObjectIdType table_id,
      std::vector<boost::property_tree::ptree> column_statistics);

  static ErrorCode add_one_column_statistic(
      ObjectIdType table_id, ObjectIdType ordinal_position,
      boost::property_tree::ptree& column_statistic);
  static ErrorCode get_one_column_statistic(
      ObjectIdType table_id, ObjectIdType ordinal_position,
      const boost::property_tree::ptree& expected_column_statistic);
  static ErrorCode get_all_column_statistics(
      ObjectIdType table_id,
      std::vector<boost::property_tree::ptree> column_statistics_expected);
  static ErrorCode get_all_column_statistics(
      ObjectIdType table_id,
      std::vector<boost::property_tree::ptree> column_statistics_expected,
      ObjectIdType ordinal_position_removed);
  static ErrorCode remove_one_column_statistic(ObjectIdType table_id,
                                               ObjectIdType ordinal_position);
  static ErrorCode remove_all_column_statistics(ObjectIdType table_id);
};

}  // namespace manager::metadata::testing

#endif  // DAO_TEST_COLUMN_STATISTICS_H_
