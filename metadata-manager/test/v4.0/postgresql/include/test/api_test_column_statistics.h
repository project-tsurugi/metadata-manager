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
#ifndef API_TEST_COLUMN_STATISTICS_H_
#define API_TEST_COLUMN_STATISTICS_H_

#include <boost/property_tree/ptree.hpp>
#include <string>
#include <tuple>
#include <vector>

#include "manager/metadata/metadata.h"

namespace manager::metadata::testing {

typedef std::tuple<std::string, std::vector<boost::property_tree::ptree>,
                   ObjectIdType>
    TupleApiTestColumnStatisticsAllAPI;
typedef std::tuple<std::string, std::vector<boost::property_tree::ptree>,
                   std::vector<boost::property_tree::ptree>, ObjectIdType>
    TupleApiTestColumnStatisticsUpdate;

class ApiTestColumnStatistics {
 public:
  static void add_column_statistics(
      ObjectIdType table_id,
      std::vector<boost::property_tree::ptree> column_statistics);
  static void add_column_statistics(
      ObjectIdType column_id, boost::property_tree::ptree column_statistics);
  static std::vector<TupleApiTestColumnStatisticsAllAPI>
  make_tuple_for_api_test_column_statistics_all_api_happy(
      const std::string& test_number);
  static std::vector<TupleApiTestColumnStatisticsUpdate>
  make_tuple_for_api_test_column_statistics_update_happy(
      const std::string& test_number);
};

}  // namespace manager::metadata::testing

#endif  // API_TEST_COLUMN_STATISTICS_H_
