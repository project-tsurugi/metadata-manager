/*
 * Copyright 2020 tsurugi project.
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
#include "test/api_test_column_statistics.h"

#include <gtest/gtest.h>
#include <memory>
#include <string>
#include <unordered_map>
#include <vector>

#include "manager/metadata/error_code.h"
#include "manager/metadata/statistics.h"
#include "test/api_test_table_metadata.h"
#include "test/global_test_environment.h"
#include "test/utility/ut_table_metadata.h"
#include "test/utility/ut_utils.h"

using namespace manager::metadata;
using namespace boost::property_tree;

namespace manager::metadata::testing {

class ApiTestColumnStatisticsAllAPIHappy
    : public ::testing::TestWithParam<TupleApiTestColumnStatisticsAllAPI> {
  void SetUp() override { UTUtils::skip_if_connection_not_opened(); }
};
class ApiTestColumnStatisticsUpdateHappy
    : public ::testing::TestWithParam<TupleApiTestColumnStatisticsUpdate> {
  void SetUp() override { UTUtils::skip_if_connection_not_opened(); }
};
class ApiTestColumnStatisticsRemoveAllHappy
    : public ::testing::TestWithParam<std::string> {
  void SetUp() override { UTUtils::skip_if_connection_not_opened(); }
};
class ApiTestColumnStatisticsAllAPIException
    : public ::testing::TestWithParam<std::string> {
  void SetUp() override { UTUtils::skip_if_connection_not_opened(); }
};
class ApiTestColumnStatisticsAllAPIHappyWithoutInit
    : public ::testing::TestWithParam<TupleApiTestColumnStatisticsAllAPI> {
  void SetUp() override { UTUtils::skip_if_connection_not_opened(); }
};

std::vector<TupleApiTestColumnStatisticsAllAPI> ApiTestColumnStatistics::
    make_tuple_for_api_test_column_statistics_all_api_happy(
        const std::string& test_number) {
  std::vector<ptree> column_statistics;
  for (int i = 0; i < 3; i++) {
    column_statistics.push_back(UTUtils::generate_column_statistic());
  }

  std::vector<ptree> empty_columns;
  ptree empty_column;
  for (int i = 0; i < 3; i++) {
    empty_columns.push_back(empty_column);
  }

  std::vector<TupleApiTestColumnStatisticsAllAPI> v;
  v.emplace_back("_ColumnStatistic_" + test_number + "_1", column_statistics,
                 1);
  v.emplace_back("_ColumnStatistic_" + test_number + "_2", empty_columns, 2);
  v.emplace_back("_ColumnStatistic_" + test_number + "_3", column_statistics,
                 3);
  return v;
}

std::vector<TupleApiTestColumnStatisticsUpdate>
ApiTestColumnStatistics::make_tuple_for_api_test_column_statistics_update_happy(
    const std::string& test_number) {
  std::vector<TupleApiTestColumnStatisticsUpdate> v;
  std::vector<int> number_of_columns = {1, 2, 2, 3};
  std::vector<ObjectIdType> ordinal_positions_to_remove = {1, 1, 2, 3};

  int test_case_no = 0;
  for (int noc : number_of_columns) {
    std::vector<ptree> cs;
    for (int i = 0; i < noc; i++) {
      cs.push_back(UTUtils::generate_column_statistic());
    }
    std::vector<ptree> empty_columns;
    ptree empty_column;
    for (int i = 0; i < noc; i++) {
      empty_columns.push_back(empty_column);
    }
    v.emplace_back(
        "_ColumnStatistic_" + test_number + "_" + std::to_string(test_case_no),
        cs, empty_columns, ordinal_positions_to_remove[test_case_no]);
    test_case_no++;
  }

  return v;
}

INSTANTIATE_TEST_CASE_P(
    ParamtererizedTest, ApiTestColumnStatisticsAllAPIHappy,
    ::testing::ValuesIn(
        ApiTestColumnStatistics::
            make_tuple_for_api_test_column_statistics_all_api_happy("1")));
INSTANTIATE_TEST_CASE_P(
    ParamtererizedTest, ApiTestColumnStatisticsUpdateHappy,
    ::testing::ValuesIn(
        ApiTestColumnStatistics::
            make_tuple_for_api_test_column_statistics_update_happy("2")));
INSTANTIATE_TEST_CASE_P(ParamtererizedTest,
                        ApiTestColumnStatisticsRemoveAllHappy,
                        ::testing::Values("_ColumnStatistic_3"));
INSTANTIATE_TEST_CASE_P(ParamtererizedTest,
                        ApiTestColumnStatisticsAllAPIException,
                        ::testing::Values("_ColumnStatistic_4"));
INSTANTIATE_TEST_CASE_P(
    ParamtererizedTest, ApiTestColumnStatisticsAllAPIHappyWithoutInit,
    ::testing::ValuesIn(
        ApiTestColumnStatistics::
            make_tuple_for_api_test_column_statistics_all_api_happy("5")));

/**
 *  @brief  Add column statistics based on the given table id and
 *  the given ptree type column statistics.
 *  @param  (table_id)          [in]  table id.
 *  @param  (column_statistics)     [in]  ptree type column statistics.
 *  @return none.
 */
void ApiTestColumnStatistics::add_column_statistics(
    ObjectIdType table_id, std::vector<ptree> column_statistics) {
  auto stats = std::make_unique<Statistics>(GlobalTestEnvironment::TEST_DB);

  ErrorCode error = stats->init();
  EXPECT_EQ(ErrorCode::OK, error);

  UTUtils::print(
      " -- add column statistics by add_one_column_statistic start --");
  UTUtils::print("id:", table_id);

  for (ObjectIdType ordinal_position = 1;
       static_cast<size_t>(ordinal_position) <= column_statistics.size();
       ordinal_position++) {
    error = stats->add_one_column_statistic(
        table_id, ordinal_position, column_statistics[ordinal_position - 1]);
    EXPECT_EQ(ErrorCode::OK, error);

    UTUtils::print("ordinal position:", ordinal_position);
    UTUtils::print(
        "column statistics:" +
        UTUtils::get_tree_string(column_statistics[ordinal_position - 1]));
  }

  UTUtils::print(
      " -- add column statistics by add_one_column_statistic end -- \n");
}

/**
 * @brief happy test for all API.
 * 1. add/get/remove one column statistic
 * based on both existing table id and column ordinal position.
 *
 * 2. get/remove all column statistics
 * based on existing table id.
 *
 * -
 * add_one_column_statistic/get_one_column_statistic/remove_one_column_statistic
 * : based on both existing table id and column ordinal position.
 * - get_all_column_statistics/remove_all_column_statistics :
 *      based on existing table id.
 */
TEST_P(ApiTestColumnStatisticsAllAPIHappy, All_API_happy) {
  auto param = GetParam();

  UTTableMetadata* testdata_table_metadata =
      global->testdata_table_metadata.get();
  std::string table_name = testdata_table_metadata->name + std::get<0>(param);

  ObjectIdType ret_table_id;
  ApiTestTableMetadata::add_table(table_name, &ret_table_id);

  /**
   * add_one_column_statistic
   * based on both existing table id and column ordinal position.
   */
  std::vector<ptree> column_statistics = std::get<1>(param);
  ApiTestColumnStatistics::add_column_statistics(ret_table_id,
                                                 column_statistics);

  auto stats = std::make_unique<Statistics>(GlobalTestEnvironment::TEST_DB);

  ErrorCode error = stats->init();
  EXPECT_EQ(ErrorCode::OK, error);

  /**
   * get_one_column_statistic
   * based on both existing table id and column ordinal position.
   */

  UTUtils::print(
      " -- get column statistics by get_one_column_statistic start --");

  for (ObjectIdType ordinal_position = 1;
       static_cast<size_t>(ordinal_position) <= column_statistics.size();
       ordinal_position++) {
    ColumnStatistic cs_returned;

    error = stats->get_one_column_statistic(ret_table_id, ordinal_position,
                                            cs_returned);
    EXPECT_EQ(ErrorCode::OK, error);

    std::string s_cs_returned =
        UTUtils::get_tree_string(cs_returned.column_statistic);
    std::string s_cs_expected =
        UTUtils::get_tree_string(column_statistics[ordinal_position - 1]);

    EXPECT_EQ(s_cs_returned, s_cs_expected);

    UTUtils::print("ordinal position:", cs_returned.ordinal_position);
    UTUtils::print("column statistic:" + s_cs_returned);
  }

  UTUtils::print(
      " -- get column statistics by get_one_column_statistic end -- \n");

  /**
   * get_all_column_statistics
   * based on existing table id.
   */
  std::unordered_map<int64_t, ColumnStatistic> hashmap_cs_returned;
  error = stats->get_all_column_statistics(ret_table_id, hashmap_cs_returned);
  EXPECT_EQ(ErrorCode::OK, error);

  UTUtils::print(
      " -- get column statistics by get_all_column_statistics start --");

  for (ObjectIdType ordinal_position = 1;
       static_cast<size_t>(ordinal_position) <= hashmap_cs_returned.size();
       ordinal_position++) {
    ColumnStatistic c_cs_returned = hashmap_cs_returned[ordinal_position];

    std::string s_cs_returned =
        UTUtils::get_tree_string(c_cs_returned.column_statistic);
    std::string s_cs_expected =
        UTUtils::get_tree_string(column_statistics[ordinal_position - 1]);

    EXPECT_EQ(s_cs_expected, s_cs_returned);

    UTUtils::print("ordinal position:", c_cs_returned.ordinal_position);
    UTUtils::print("column statistic:" + s_cs_returned);
  }

  UTUtils::print(
      " -- get column statistics by get_all_column_statistics end -- \n");

  /**
   * remove_one_column_statistic
   * based on both existing table id and column ordinal position.
   */
  ObjectIdType ordinal_position_to_remove = std::get<2>(param);
  error = stats->remove_one_column_statistic(ret_table_id,
                                             ordinal_position_to_remove);
  EXPECT_EQ(ErrorCode::OK, error);

  UTUtils::print(
      " -- After removing ordinal position=", ordinal_position_to_remove,
      " get column statistics by get_one_column_statistic start --");

  for (ObjectIdType ordinal_position = 1;
       static_cast<size_t>(ordinal_position) <= column_statistics.size();
       ordinal_position++) {
    ColumnStatistic cs_returned;

    error = stats->get_one_column_statistic(ret_table_id, ordinal_position,
                                            cs_returned);

    if (ordinal_position_to_remove == ordinal_position) {
      EXPECT_EQ(ErrorCode::ID_NOT_FOUND, error);
    } else {
      EXPECT_EQ(ErrorCode::OK, error);

      std::string s_cs_returned =
          UTUtils::get_tree_string(cs_returned.column_statistic);
      std::string s_cs_expected =
          UTUtils::get_tree_string(column_statistics[ordinal_position - 1]);

      EXPECT_EQ(s_cs_returned, s_cs_expected);

      UTUtils::print("ordinal position:", cs_returned.ordinal_position);
      UTUtils::print("column statistic:" + s_cs_returned);
    }
  }

  UTUtils::print(
      " -- After removing ordinal position=", ordinal_position_to_remove,
      " get column statistics by get_one_column_statistic end -- \n");

  std::unordered_map<int64_t, ColumnStatistic> hashmap_cs_removed_returned;
  error = stats->get_all_column_statistics(ret_table_id,
                                           hashmap_cs_removed_returned);
  EXPECT_EQ(ErrorCode::OK, error);
  EXPECT_EQ(column_statistics.size() - 1, hashmap_cs_removed_returned.size());

  UTUtils::print(
      " -- After removing ordinal position=", ordinal_position_to_remove,
      " get column statistics by get_all_column_statistics start --");

  for (ObjectIdType ordinal_position = 1;
       static_cast<size_t>(ordinal_position) <= column_statistics.size();
       ordinal_position++) {
    auto got = hashmap_cs_removed_returned.find(ordinal_position);

    if (got == hashmap_cs_removed_returned.end()) {
      EXPECT_TRUE(ordinal_position_to_remove == ordinal_position);
    } else {
      ColumnStatistic c_cs_returned = got->second;
      std::string s_cs_returned =
          UTUtils::get_tree_string(c_cs_returned.column_statistic);
      std::string s_cs_expected =
          UTUtils::get_tree_string(column_statistics[ordinal_position - 1]);

      EXPECT_EQ(s_cs_expected, s_cs_returned);

      UTUtils::print("ordinal position:", c_cs_returned.ordinal_position);
      UTUtils::print("column statistic:" + s_cs_returned);
    }
  }

  UTUtils::print(
      " -- After removing ordinal position=", ordinal_position_to_remove,
      " get column statistics by get_all_column_statistics end --");

  /**
   * remove_all_column_statistics
   * based on existing table.
   */
  error = stats->remove_all_column_statistics(ret_table_id);
  EXPECT_EQ(ErrorCode::OK, error);

  std::unordered_map<int64_t, ColumnStatistic> all_column_statistics_removed;
  error = stats->get_all_column_statistics(ret_table_id,
                                           all_column_statistics_removed);
  EXPECT_EQ(ErrorCode::ID_NOT_FOUND, error);

  EXPECT_EQ(all_column_statistics_removed.size(), 0);

  for (ObjectIdType ordinal_position = 1;
       static_cast<size_t>(ordinal_position) <= column_statistics.size();
       ordinal_position++) {
    ColumnStatistic cs_returned;

    error = stats->get_one_column_statistic(ret_table_id, ordinal_position,
                                            cs_returned);
    EXPECT_EQ(ErrorCode::ID_NOT_FOUND, error);
  }
}

/**
 * @brief happy test to update column statistics
 * based on both existing table id and column ordinal position.
 *
 * - add_one_column_statistic:
 *      update column statistics
 *      based on both existing table id and column ordinal position.
 */
TEST_P(ApiTestColumnStatisticsUpdateHappy, update_column_statistics) {
  auto param = GetParam();

  UTTableMetadata* testdata_table_metadata =
      global->testdata_table_metadata.get();
  std::string table_name = testdata_table_metadata->name + std::get<0>(param);

  ObjectIdType ret_table_id;
  ApiTestTableMetadata::add_table(table_name, &ret_table_id);

  /**
   * add new column statistics
   * based on both existing table id and column ordinal position.
   */
  std::vector<ptree> column_statistics = std::get<1>(param);
  ApiTestColumnStatistics::add_column_statistics(ret_table_id,
                                                 column_statistics);

  auto stats = std::make_unique<Statistics>(GlobalTestEnvironment::TEST_DB);

  ErrorCode error = stats->init();
  EXPECT_EQ(ErrorCode::OK, error);

  /**
   * check if results of column statistics are expected or not.
   */

  UTUtils::print(
      " -- get column statistics by get_one_column_statistic start --");

  for (ObjectIdType ordinal_position = 1;
       static_cast<size_t>(ordinal_position) <= column_statistics.size();
       ordinal_position++) {
    ColumnStatistic cs_returned;

    error = stats->get_one_column_statistic(ret_table_id, ordinal_position,
                                            cs_returned);
    EXPECT_EQ(ErrorCode::OK, error);

    std::string s_cs_returned =
        UTUtils::get_tree_string(cs_returned.column_statistic);
    std::string s_cs_expected =
        UTUtils::get_tree_string(column_statistics[ordinal_position - 1]);

    EXPECT_EQ(s_cs_returned, s_cs_expected);

    UTUtils::print("ordinal position:", cs_returned.ordinal_position);
    UTUtils::print("column statistic:" + s_cs_returned);
  }

  UTUtils::print(
      " -- get column statistics by get_one_column_statistic end -- \n");

  std::unordered_map<int64_t, ColumnStatistic> hashmap_cs_returned;
  error = stats->get_all_column_statistics(ret_table_id, hashmap_cs_returned);
  EXPECT_EQ(ErrorCode::OK, error);
  EXPECT_EQ(column_statistics.size(), hashmap_cs_returned.size());

  UTUtils::print(
      " -- get column statistics by get_all_column_statistics start --");

  for (ObjectIdType ordinal_position = 1;
       static_cast<size_t>(ordinal_position) <= hashmap_cs_returned.size();
       ordinal_position++) {
    ColumnStatistic c_cs_returned = hashmap_cs_returned[ordinal_position];

    std::string s_cs_returned =
        UTUtils::get_tree_string(c_cs_returned.column_statistic);
    std::string s_cs_expected =
        UTUtils::get_tree_string(column_statistics[ordinal_position - 1]);

    EXPECT_EQ(s_cs_expected, s_cs_returned);

    UTUtils::print("ordinal position:", c_cs_returned.ordinal_position);
    UTUtils::print("column statistic:" + s_cs_returned);
  }

  UTUtils::print(
      " -- get column statistics by get_all_column_statistics end -- \n");

  /**
   * update column statistics
   * based on both existing table id and column ordinal position.
   */
  std::vector<ptree> column_statistics_to_update = std::get<2>(param);
  ApiTestColumnStatistics::add_column_statistics(ret_table_id,
                                                 column_statistics_to_update);

  /**
   * check if results of column statistics are expected or not.
   */

  UTUtils::print(
      " -- After updating all column statistics, get column statistics by ",
      "get_one_column_statistic start --");

  for (ObjectIdType ordinal_position = 1;
       static_cast<size_t>(ordinal_position) <=
       column_statistics_to_update.size();
       ordinal_position++) {
    ColumnStatistic cs_returned;

    error = stats->get_one_column_statistic(ret_table_id, ordinal_position,
                                            cs_returned);
    EXPECT_EQ(ErrorCode::OK, error);

    std::string s_cs_returned =
        UTUtils::get_tree_string(cs_returned.column_statistic);
    std::string s_cs_expected = UTUtils::get_tree_string(
        column_statistics_to_update[ordinal_position - 1]);

    EXPECT_EQ(s_cs_returned, s_cs_expected);

    UTUtils::print("ordinal position:", cs_returned.ordinal_position);
    UTUtils::print("column statistic:", s_cs_returned);
  }

  UTUtils::print(
      " -- After updating all column statistics, get column statistics by ",
      "get_one_column_statistic end -- \n");

  std::unordered_map<int64_t, ColumnStatistic> hashmap_cs_updated_returned;
  error = stats->get_all_column_statistics(ret_table_id,
                                           hashmap_cs_updated_returned);
  EXPECT_EQ(ErrorCode::OK, error);
  EXPECT_EQ(column_statistics_to_update.size(),
            hashmap_cs_updated_returned.size());

  UTUtils::print(
      "-- After updating all column statistics, get column statistics by ",
      "get_all_column_statistics start --");

  for (ObjectIdType ordinal_position = 1;
       static_cast<size_t>(ordinal_position) <=
       hashmap_cs_updated_returned.size();
       ordinal_position++) {
    ColumnStatistic c_cs_returned =
        hashmap_cs_updated_returned[ordinal_position];

    std::string s_cs_returned =
        UTUtils::get_tree_string(c_cs_returned.column_statistic);
    std::string s_cs_expected = UTUtils::get_tree_string(
        column_statistics_to_update[ordinal_position - 1]);

    EXPECT_EQ(s_cs_expected, s_cs_returned);

    UTUtils::print("ordinal position:", c_cs_returned.ordinal_position);
    UTUtils::print("column statistic:", s_cs_returned);
  }

  UTUtils::print(
      "-- After updating all column statistics, get column statistics by ",
      "get_all_column_statistics end -- \n");

  /**
   * remove_one_column_statistic
   * based on both existing table id and column ordinal position.
   */
  ObjectIdType ordinal_position_to_remove = std::get<3>(param);
  error = stats->remove_one_column_statistic(ret_table_id,
                                             ordinal_position_to_remove);
  EXPECT_EQ(ErrorCode::OK, error);

  UTUtils::print(
      " -- After removing ordinal position=", ordinal_position_to_remove,
      " get column statistics by get_one_column_statistic start --");

  for (ObjectIdType ordinal_position = 1;
       static_cast<size_t>(ordinal_position) <=
       column_statistics_to_update.size();
       ordinal_position++) {
    ColumnStatistic cs_returned;

    error = stats->get_one_column_statistic(ret_table_id, ordinal_position,
                                            cs_returned);

    if (ordinal_position_to_remove == ordinal_position) {
      EXPECT_EQ(ErrorCode::ID_NOT_FOUND, error);
    } else {
      EXPECT_EQ(ErrorCode::OK, error);

      std::string s_cs_returned =
          UTUtils::get_tree_string(cs_returned.column_statistic);
      std::string s_cs_expected = UTUtils::get_tree_string(
          column_statistics_to_update[ordinal_position - 1]);

      EXPECT_EQ(s_cs_returned, s_cs_expected);

      UTUtils::print("ordinal position:", cs_returned.ordinal_position);
      UTUtils::print("column statistic:", s_cs_returned);
    }
  }

  UTUtils::print(
      " -- After removing ordinal position=", ordinal_position_to_remove,
      " get column statistics by get_one_column_statistic end -- \n");

  std::unordered_map<int64_t, ColumnStatistic> hashmap_cs_removed_returned;
  error = stats->get_all_column_statistics(ret_table_id,
                                           hashmap_cs_removed_returned);

  if (column_statistics_to_update.size() == 1) {
    EXPECT_EQ(ErrorCode::ID_NOT_FOUND, error);
  } else {
    EXPECT_EQ(ErrorCode::OK, error);
  }

  EXPECT_EQ(column_statistics_to_update.size() - 1,
            hashmap_cs_removed_returned.size());

  UTUtils::print(
      " -- After removing ordinal position=", ordinal_position_to_remove,
      " get column statistics by get_all_column_statistics start --");

  for (ObjectIdType ordinal_position = 1;
       static_cast<size_t>(ordinal_position) <=
       column_statistics_to_update.size();
       ordinal_position++) {
    auto got = hashmap_cs_removed_returned.find(ordinal_position);

    if (got == hashmap_cs_removed_returned.end()) {
      EXPECT_TRUE(ordinal_position_to_remove == ordinal_position);
    } else {
      ColumnStatistic c_cs_returned = got->second;
      std::string s_cs_returned =
          UTUtils::get_tree_string(c_cs_returned.column_statistic);
      std::string s_cs_expected = UTUtils::get_tree_string(
          column_statistics_to_update[ordinal_position - 1]);

      EXPECT_EQ(s_cs_expected, s_cs_returned);

      UTUtils::print("ordinal position:", c_cs_returned.ordinal_position);
      UTUtils::print("column statistic:", s_cs_returned);
    }
  }

  UTUtils::print(
      " -- After removing ordinal position=", ordinal_position_to_remove,
      " get column statistics by get_all_column_statistics end --");

  /**
   * remove_all_column_statistics
   * based on existing table id.
   */
  error = stats->remove_all_column_statistics(ret_table_id);

  if (column_statistics_to_update.size() == 1) {
    EXPECT_EQ(ErrorCode::ID_NOT_FOUND, error);
  } else {
    EXPECT_EQ(ErrorCode::OK, error);
  }

  std::unordered_map<int64_t, ColumnStatistic> all_column_statistics_removed;
  error = stats->get_all_column_statistics(ret_table_id,
                                           all_column_statistics_removed);
  EXPECT_EQ(ErrorCode::ID_NOT_FOUND, error);

  EXPECT_EQ(all_column_statistics_removed.size(), 0);

  for (ObjectIdType ordinal_position = 1;
       static_cast<size_t>(ordinal_position) <=
       column_statistics_to_update.size();
       ordinal_position++) {
    ColumnStatistic cs_returned;

    error = stats->get_one_column_statistic(ret_table_id, ordinal_position,
                                            cs_returned);
    EXPECT_EQ(ErrorCode::ID_NOT_FOUND, error);
  }
}

/**
 * @brief happy test to remove all column statistics
 * based on both existing table id.
 *
 * - add_one_column_statistic:
 *      remove all column statistics
 *      based on both existing table id.
 */
TEST_P(ApiTestColumnStatisticsRemoveAllHappy, remove_all_column_statistics) {
  auto param = GetParam();

  UTTableMetadata* testdata_table_metadata =
      global->testdata_table_metadata.get();
  std::string table_name = testdata_table_metadata->name + param;

  ObjectIdType ret_table_id;
  ApiTestTableMetadata::add_table(table_name, &ret_table_id);

  /**
   * add new column statistics
   * based on both existing table id and column ordinal position.
   */
  std::vector<ptree> column_statistics = global->column_statistics;
  ApiTestColumnStatistics::add_column_statistics(ret_table_id,
                                                 column_statistics);

  auto stats = std::make_unique<Statistics>(GlobalTestEnvironment::TEST_DB);

  ErrorCode error = stats->init();
  EXPECT_EQ(ErrorCode::OK, error);

  /**
   * check if results of column statistics are expected or not.
   */

  UTUtils::print(
      " -- get column statistics by get_one_column_statistic start --");

  for (ObjectIdType ordinal_position = 1;
       static_cast<size_t>(ordinal_position) <= column_statistics.size();
       ordinal_position++) {
    ColumnStatistic cs_returned;

    error = stats->get_one_column_statistic(ret_table_id, ordinal_position,
                                            cs_returned);
    EXPECT_EQ(ErrorCode::OK, error);

    std::string s_cs_returned =
        UTUtils::get_tree_string(cs_returned.column_statistic);
    std::string s_cs_expected =
        UTUtils::get_tree_string(column_statistics[ordinal_position - 1]);

    EXPECT_EQ(s_cs_returned, s_cs_expected);

    UTUtils::print("ordinal position:", cs_returned.ordinal_position);
    UTUtils::print("column statistic:", s_cs_returned);
  }

  UTUtils::print(
      " -- get column statistics by get_one_column_statistic end -- \n");

  std::unordered_map<int64_t, ColumnStatistic> hashmap_cs_returned;
  error = stats->get_all_column_statistics(ret_table_id, hashmap_cs_returned);
  EXPECT_EQ(ErrorCode::OK, error);
  EXPECT_EQ(column_statistics.size(), hashmap_cs_returned.size());

  UTUtils::print(
      " -- get column statistics by get_all_column_statistics start --");

  for (ObjectIdType ordinal_position = 1;
       static_cast<size_t>(ordinal_position) <= hashmap_cs_returned.size();
       ordinal_position++) {
    ColumnStatistic c_cs_returned = hashmap_cs_returned[ordinal_position];

    std::string s_cs_returned =
        UTUtils::get_tree_string(c_cs_returned.column_statistic);
    std::string s_cs_expected =
        UTUtils::get_tree_string(column_statistics[ordinal_position - 1]);

    EXPECT_EQ(s_cs_expected, s_cs_returned);

    UTUtils::print("ordinal position:", c_cs_returned.ordinal_position);
    UTUtils::print("column statistic:", s_cs_returned);
  }

  UTUtils::print(
      " -- get column statistics by get_all_column_statistics end -- \n");

  /**
   * remove_all_column_statistics
   * based on existing table id.
   */
  error = stats->remove_all_column_statistics(ret_table_id);
  EXPECT_EQ(ErrorCode::OK, error);

  std::unordered_map<int64_t, ColumnStatistic> all_column_statistics_removed;
  error = stats->get_all_column_statistics(ret_table_id,
                                           all_column_statistics_removed);
  EXPECT_EQ(ErrorCode::ID_NOT_FOUND, error);

  EXPECT_EQ(all_column_statistics_removed.size(), 0);

  for (ObjectIdType ordinal_position = 1;
       static_cast<size_t>(ordinal_position) <= column_statistics.size();
       ordinal_position++) {
    ColumnStatistic cs_returned;

    error = stats->get_one_column_statistic(ret_table_id, ordinal_position,
                                            cs_returned);
    EXPECT_EQ(ErrorCode::ID_NOT_FOUND, error);
  }
}

/**
 * @brief Exception path test for all API.
 * 1. add/get/remove one column statistic
 * based on non-existing table id or
 * non-existing column ordinal position.
 *
 * 2. get/remove all column statistics
 * based on non-existing table id.
 *
 * -
 * add_one_column_statistic/get_one_column_statistic/remove_one_column_statistic:
 *      - based on non-existing column ordinal position
 *                 and existing table id.
 *      - based on non-existing table id
 *                 and existing column ordinal position.
 *      - based on both non-existing table id and column ordinal position.
 * -  get_all_column_statistics/remove_all_column_statistics:
 *      - based on non-existing table id.
 */
TEST_P(ApiTestColumnStatisticsAllAPIException, All_API_exception) {
  auto param = GetParam();
  UTTableMetadata* testdata_table_metadata =
      global->testdata_table_metadata.get();
  std::string table_name = testdata_table_metadata->name + param;

  ObjectIdType ret_table_id;
  ApiTestTableMetadata::add_table(table_name, &ret_table_id);

  std::vector<ptree> column_statistics = global->column_statistics;
  ApiTestColumnStatistics::add_column_statistics(ret_table_id,
                                                 column_statistics);

  auto stats = std::make_unique<Statistics>(GlobalTestEnvironment::TEST_DB);

  ErrorCode error = stats->init();
  EXPECT_EQ(ErrorCode::OK, error);

  for (ObjectIdType ordinal_position = 1;
       static_cast<size_t>(ordinal_position) <= column_statistics.size();
       ordinal_position++) {
    ColumnStatistic cs_returned;

    error = stats->get_one_column_statistic(ret_table_id, ordinal_position,
                                            cs_returned);
    EXPECT_EQ(ErrorCode::OK, error);

    std::string s_cs_returned =
        UTUtils::get_tree_string(cs_returned.column_statistic);
    std::string s_cs_expected =
        UTUtils::get_tree_string(column_statistics[ordinal_position - 1]);

    EXPECT_EQ(s_cs_returned, s_cs_expected);
  }

  /**
   * add_one_column_statistic
   * based on non-existing column ordinal position
   * or non-existing table id.
   */
  for (ObjectIdType ordinal_position : global->ordinal_position_not_exists) {
    // ordinal position only not exists
    error = stats->add_one_column_statistic(ret_table_id, ordinal_position,
                                            column_statistics[0]);
    EXPECT_EQ(ErrorCode::INVALID_PARAMETER, error);

    for (ObjectIdType table_id : global->table_id_not_exists) {
      // table id and ordinal position not exists
      error = stats->add_one_column_statistic(table_id, ordinal_position,
                                              column_statistics[0]);
      EXPECT_EQ(ErrorCode::INVALID_PARAMETER, error);
    }
  }

  ObjectIdType ordinal_position_exists = 1;
  for (ObjectIdType table_id : global->table_id_not_exists) {
    // table id only not exists
    error = stats->add_one_column_statistic(table_id, ordinal_position_exists,
                                            column_statistics[0]);
    EXPECT_EQ(ErrorCode::INVALID_PARAMETER, error);
  }

  /**
   * get_all_column_statistics
   * based on non-existing table id.
   */
  for (ObjectIdType table_id : global->table_id_not_exists) {
    // table id only not exists
    std::unordered_map<int64_t, ColumnStatistic> hashmap_cs_returned;
    error = stats->get_all_column_statistics(table_id, hashmap_cs_returned);
    EXPECT_EQ(ErrorCode::ID_NOT_FOUND, error);
    EXPECT_EQ(hashmap_cs_returned.size(), 0);
  }

  /**
   * get_one_column_statistic
   * based on non-existing column ordinal position
   * or non-existing table id.
   */
  ColumnStatistic cs_returned;
  for (ObjectIdType ordinal_position : global->ordinal_position_not_exists) {
    // ordinal position only not exists
    error = stats->get_one_column_statistic(ret_table_id, ordinal_position,
                                            cs_returned);
    EXPECT_EQ(ErrorCode::ID_NOT_FOUND, error);

    for (ObjectIdType table_id : global->table_id_not_exists) {
      // table id and ordinal position not exists
      error = stats->get_one_column_statistic(table_id, ordinal_position,
                                              cs_returned);
      EXPECT_EQ(ErrorCode::ID_NOT_FOUND, error);
    }
  }

  for (ObjectIdType table_id : global->table_id_not_exists) {
    // table id only not exists
    error = stats->get_one_column_statistic(table_id, ordinal_position_exists,
                                            cs_returned);
    EXPECT_EQ(ErrorCode::ID_NOT_FOUND, error);
  }

  /**
   * remove_one_column_statistic
   * based on non-existing column ordinal position
   * or non-existing table id.
   */
  for (ObjectIdType ordinal_position : global->ordinal_position_not_exists) {
    // ordinal position only not exists
    error = stats->remove_one_column_statistic(ret_table_id, ordinal_position);
    EXPECT_EQ(ErrorCode::ID_NOT_FOUND, error);

    for (ObjectIdType table_id : global->table_id_not_exists) {
      // table id and ordinal position not exists
      error = stats->remove_one_column_statistic(table_id, ordinal_position);
      EXPECT_EQ(ErrorCode::ID_NOT_FOUND, error);
    }
  }

  for (ObjectIdType table_id : global->table_id_not_exists) {
    // table id only not exists
    error =
        stats->remove_one_column_statistic(table_id, ordinal_position_exists);
    EXPECT_EQ(ErrorCode::ID_NOT_FOUND, error);
  }

  /**
   * remove_all_column_statistics
   * based on non-existing table id.
   */
  for (ObjectIdType table_id : global->table_id_not_exists) {
    // table id not exists
    error = stats->remove_all_column_statistics(table_id);
    EXPECT_EQ(ErrorCode::ID_NOT_FOUND, error);
  }
}

/**
 * @brief happy test for all API without init().
 * 1. add/get/remove one column statistic without init()
 * based on both existing table id and column ordinal position.
 *
 * 2. get/remove all column statistics without init()
 * based on existing table id.
 *
 * -
 * add_one_column_statistic/get_one_column_statistic/remove_one_column_statistic
 * : based on both existing table id and column ordinal position.
 * - get_all_column_statistics/remove_all_column_statistics :
 *      based on existing table id.
 */
TEST_P(ApiTestColumnStatisticsAllAPIHappyWithoutInit,
       All_API_happy_without_init) {
  auto param = GetParam();

  UTTableMetadata* testdata_table_metadata =
      global->testdata_table_metadata.get();
  std::string table_name = testdata_table_metadata->name + std::get<0>(param);

  ObjectIdType ret_table_id;
  ApiTestTableMetadata::add_table(table_name, &ret_table_id);

  /**
   * add_one_column_statistic without init()
   * based on both existing table id and column ordinal position.
   */
  auto stats_add = std::make_unique<Statistics>(GlobalTestEnvironment::TEST_DB);

  UTUtils::print(
      " -- add column statistics by add_one_column_statistic start --");
  UTUtils::print("id:", ret_table_id);

  ErrorCode error;
  std::vector<ptree> column_statistics = std::get<1>(param);
  for (ObjectIdType ordinal_position = 1;
       static_cast<size_t>(ordinal_position) <= column_statistics.size();
       ordinal_position++) {
    error = stats_add->add_one_column_statistic(
        ret_table_id, ordinal_position,
        column_statistics[ordinal_position - 1]);
    EXPECT_EQ(ErrorCode::OK, error);

    UTUtils::print("ordinal position:", ordinal_position);
    UTUtils::print(
        "column statistics:",
        UTUtils::get_tree_string(column_statistics[ordinal_position - 1]));
  }

  UTUtils::print(
      " -- add column statistics by add_one_column_statistic end -- \n");

  /**
   * get_one_column_statistic without init()
   * based on both existing table id and column ordinal position.
   */
  auto stats_get_one_cs =
      std::make_unique<Statistics>(GlobalTestEnvironment::TEST_DB);

  UTUtils::print(
      " -- get column statistics by get_one_column_statistic start --");

  for (ObjectIdType ordinal_position = 1;
       static_cast<size_t>(ordinal_position) <= column_statistics.size();
       ordinal_position++) {
    ColumnStatistic cs_returned;

    error = stats_get_one_cs->get_one_column_statistic(
        ret_table_id, ordinal_position, cs_returned);
    EXPECT_EQ(ErrorCode::OK, error);

    std::string s_cs_returned =
        UTUtils::get_tree_string(cs_returned.column_statistic);
    std::string s_cs_expected =
        UTUtils::get_tree_string(column_statistics[ordinal_position - 1]);

    EXPECT_EQ(s_cs_returned, s_cs_expected);

    UTUtils::print("ordinal position:", cs_returned.ordinal_position);
    UTUtils::print("column statistic:", s_cs_returned);
  }

  UTUtils::print(
      " -- get column statistics by get_one_column_statistic end -- \n");

  /**
   * get_all_column_statistics without init()
   * based on existing table id.
   */
  auto stats_get_all_cs =
      std::make_unique<Statistics>(GlobalTestEnvironment::TEST_DB);

  std::unordered_map<int64_t, ColumnStatistic> hashmap_cs_returned;
  error = stats_get_all_cs->get_all_column_statistics(ret_table_id,
                                                      hashmap_cs_returned);
  EXPECT_EQ(ErrorCode::OK, error);

  UTUtils::print(
      " -- get column statistics by get_all_column_statistics start --");

  for (ObjectIdType ordinal_position = 1;
       static_cast<size_t>(ordinal_position) <= hashmap_cs_returned.size();
       ordinal_position++) {
    ColumnStatistic c_cs_returned = hashmap_cs_returned[ordinal_position];

    std::string s_cs_returned =
        UTUtils::get_tree_string(c_cs_returned.column_statistic);
    std::string s_cs_expected =
        UTUtils::get_tree_string(column_statistics[ordinal_position - 1]);

    EXPECT_EQ(s_cs_expected, s_cs_returned);

    UTUtils::print("ordinal position:", c_cs_returned.ordinal_position);
    UTUtils::print("column statistic:", s_cs_returned);
  }

  UTUtils::print(
      " -- get column statistics by get_all_column_statistics end -- \n");

  /**
   * remove_one_column_statistic without init()
   * based on both existing table id and column ordinal position.
   */
  auto stats_remove_one_cs =
      std::make_unique<Statistics>(GlobalTestEnvironment::TEST_DB);

  ObjectIdType ordinal_position_to_remove = std::get<2>(param);
  error = stats_remove_one_cs->remove_one_column_statistic(
      ret_table_id, ordinal_position_to_remove);
  EXPECT_EQ(ErrorCode::OK, error);

  UTUtils::print(
      " -- After removing ordinal position=", ordinal_position_to_remove,
      " get column statistics by get_one_column_statistic start --");

  for (ObjectIdType ordinal_position = 1;
       static_cast<size_t>(ordinal_position) <= column_statistics.size();
       ordinal_position++) {
    ColumnStatistic cs_returned;

    error = stats_remove_one_cs->get_one_column_statistic(
        ret_table_id, ordinal_position, cs_returned);

    if (ordinal_position_to_remove == ordinal_position) {
      EXPECT_EQ(ErrorCode::ID_NOT_FOUND, error);
    } else {
      EXPECT_EQ(ErrorCode::OK, error);

      std::string s_cs_returned =
          UTUtils::get_tree_string(cs_returned.column_statistic);
      std::string s_cs_expected =
          UTUtils::get_tree_string(column_statistics[ordinal_position - 1]);

      EXPECT_EQ(s_cs_returned, s_cs_expected);

      UTUtils::print("ordinal position:", cs_returned.ordinal_position);
      UTUtils::print("column statistic:", s_cs_returned);
    }
  }

  UTUtils::print(
      " -- After removing ordinal position=", ordinal_position_to_remove,
      " get column statistics by get_one_column_statistic end -- \n");

  std::unordered_map<int64_t, ColumnStatistic> hashmap_cs_removed_returned;
  error = stats_remove_one_cs->get_all_column_statistics(
      ret_table_id, hashmap_cs_removed_returned);
  EXPECT_EQ(ErrorCode::OK, error);
  EXPECT_EQ(column_statistics.size() - 1, hashmap_cs_removed_returned.size());

  UTUtils::print(
      " -- After removing ordinal position=", ordinal_position_to_remove,
      " get column statistics by get_all_column_statistics start --");

  for (ObjectIdType ordinal_position = 1;
       static_cast<size_t>(ordinal_position) <= column_statistics.size();
       ordinal_position++) {
    auto got = hashmap_cs_removed_returned.find(ordinal_position);

    if (got == hashmap_cs_removed_returned.end()) {
      EXPECT_TRUE(ordinal_position_to_remove == ordinal_position);
    } else {
      ColumnStatistic c_cs_returned = got->second;
      std::string s_cs_returned =
          UTUtils::get_tree_string(c_cs_returned.column_statistic);
      std::string s_cs_expected =
          UTUtils::get_tree_string(column_statistics[ordinal_position - 1]);

      EXPECT_EQ(s_cs_expected, s_cs_returned);

      UTUtils::print("ordinal position:", c_cs_returned.ordinal_position);
      UTUtils::print("column statistic:", s_cs_returned);
    }
  }

  UTUtils::print(
      " -- After removing ordinal position=", ordinal_position_to_remove,
      " get column statistics by get_all_column_statistics end --");

  /**
   * remove_all_column_statistics without init()
   * based on existing table.
   */
  auto stats_remove_all_cs =
      std::make_unique<Statistics>(GlobalTestEnvironment::TEST_DB);

  error = stats_remove_all_cs->remove_all_column_statistics(ret_table_id);
  EXPECT_EQ(ErrorCode::OK, error);

  std::unordered_map<int64_t, ColumnStatistic> all_column_statistics_removed;
  error = stats_remove_all_cs->get_all_column_statistics(
      ret_table_id, all_column_statistics_removed);
  EXPECT_EQ(ErrorCode::ID_NOT_FOUND, error);

  EXPECT_EQ(all_column_statistics_removed.size(), 0);

  for (ObjectIdType ordinal_position = 1;
       static_cast<size_t>(ordinal_position) <= column_statistics.size();
       ordinal_position++) {
    ColumnStatistic cs_returned;

    error = stats_remove_all_cs->get_one_column_statistic(
        ret_table_id, ordinal_position, cs_returned);
    EXPECT_EQ(ErrorCode::ID_NOT_FOUND, error);
  }
}

}  // namespace manager::metadata::testing
