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
 * @brief Add column statistics based on the given table id and
 *  the given ptree type column statistics.
 * @param (table_id)          [in]  table id.
 * @param (column_statistics)     [in]  ptree type column statistics.
 * @return none.
 */
void ApiTestColumnStatistics::add_column_statistics(
    ObjectIdType table_id, std::vector<ptree> column_statistics) {
  auto stats = std::make_unique<Statistics>(GlobalTestEnvironment::TEST_DB);

  ErrorCode error = stats->init();
  EXPECT_EQ(ErrorCode::OK, error);

  UTUtils::print(" -- add column statistics by add_column_statistic start --");
  UTUtils::print("id:", table_id);

  boost::property_tree::ptree statistic;

  for (ObjectIdType ordinal_position = 1;
       static_cast<size_t>(ordinal_position) <= column_statistics.size();
       ordinal_position++) {
    // clear
    statistic.clear();
    // name
    std::string statistic_name = "ApiTestColumnStatistics_" +
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
    EXPECT_EQ(ErrorCode::OK, error);

    UTUtils::print("ordinal position:", ordinal_position);
    UTUtils::print(
        "column statistics:" +
        UTUtils::get_tree_string(column_statistics[ordinal_position - 1]));
  }

  UTUtils::print(" -- add column statistics by add_column_statistic end -- \n");
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
 * add/get_by_column_number/remove_by_column_number
 * : based on both existing table id and column ordinal position.
 * - get_all/remove_by_table_id :
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
   * add
   * based on both existing table id and column ordinal position.
   */
  std::vector<ptree> column_statistics = std::get<1>(param);
  ApiTestColumnStatistics::add_column_statistics(ret_table_id,
                                                 column_statistics);

  auto stats = std::make_unique<Statistics>(GlobalTestEnvironment::TEST_DB);

  ErrorCode error = stats->init();
  EXPECT_EQ(ErrorCode::OK, error);

  /**
   * get_by_column_number
   * based on both existing table id and column ordinal position.
   */

  UTUtils::print(" -- get column statistics by get_by_column_number start --");

  for (ObjectIdType ordinal_position = 1;
       static_cast<size_t>(ordinal_position) <= column_statistics.size();
       ordinal_position++) {
    ptree cs_returned;

    error = stats->get_by_column_number(ret_table_id, ordinal_position,
                                        cs_returned);
    EXPECT_EQ(ErrorCode::OK, error);

    boost::optional<ptree&> optional_column_statistic =
        cs_returned.get_child_optional(Statistics::COLUMN_STATISTIC);
    EXPECT_TRUE(optional_column_statistic);

    std::string s_cs_returned =
        UTUtils::get_tree_string(optional_column_statistic.get());
    std::string s_cs_expected =
        UTUtils::get_tree_string(column_statistics[ordinal_position - 1]);
    boost::optional<std::int64_t> optional_ordinal_position =
        cs_returned.get_optional<std::int64_t>(Statistics::ORDINAL_POSITION);

    UTUtils::print("ordinal position:", optional_ordinal_position.get());
    UTUtils::print("column statistic: ", s_cs_returned);

    EXPECT_EQ(s_cs_returned, s_cs_expected);
  }

  UTUtils::print(" -- get column statistics by get_by_column_number end -- \n");

  /**
   * get_all
   * based on existing table id.
   */

  std::vector<ptree> vector_cs_returned;
  error = stats->get_all(ret_table_id, vector_cs_returned);
  EXPECT_EQ(ErrorCode::OK, error);

  UTUtils::print(" -- get column statistics by get_all start --");

  for (ObjectIdType ordinal_position = 0;
       static_cast<size_t>(ordinal_position) < vector_cs_returned.size();
       ordinal_position++) {
    ptree c_cs_returned = vector_cs_returned[ordinal_position];

    boost::optional<ptree&> optional_column_statistic =
        c_cs_returned.get_child_optional(Statistics::COLUMN_STATISTIC);
    EXPECT_TRUE(optional_column_statistic);

    std::string s_cs_returned =
        UTUtils::get_tree_string(optional_column_statistic.get());
    std::string s_cs_expected =
        UTUtils::get_tree_string(column_statistics[ordinal_position]);

    EXPECT_EQ(s_cs_expected, s_cs_returned);

    boost::optional<std::int64_t> optional_ordinal_position =
        c_cs_returned.get_optional<std::int64_t>(Statistics::ORDINAL_POSITION);

    UTUtils::print("ordinal position:", optional_ordinal_position);
    UTUtils::print("column statistic:" + s_cs_returned);
  }

  UTUtils::print(" -- get column statistics by get_all end -- \n");

  /**
   * remove_by_column_number
   * based on both existing table id and column ordinal position.
   */
  std::int64_t ordinal_position_to_remove = std::get<2>(param);
  error =
      stats->remove_by_column_number(ret_table_id, ordinal_position_to_remove);
  EXPECT_EQ(ErrorCode::OK, error);

  UTUtils::print(
      " -- After removing ordinal position=", ordinal_position_to_remove,
      " get column statistics by get_by_column_number start --");

  for (std::int64_t ordinal_position = 1;
       static_cast<size_t>(ordinal_position) <= column_statistics.size();
       ordinal_position++) {
    ptree cs_returned;

    error = stats->get_by_column_number(ret_table_id, ordinal_position,
                                        cs_returned);

    if (ordinal_position_to_remove == ordinal_position) {
      EXPECT_EQ(ErrorCode::ID_NOT_FOUND, error);
    } else {
      EXPECT_EQ(ErrorCode::OK, error);

      boost::optional<ptree&> optional_column_statistic =
          cs_returned.get_child_optional(Statistics::COLUMN_STATISTIC);
      EXPECT_TRUE(optional_column_statistic);

      std::string s_cs_returned =
          UTUtils::get_tree_string(optional_column_statistic.get());
      std::string s_cs_expected =
          UTUtils::get_tree_string(column_statistics[ordinal_position - 1]);

      EXPECT_EQ(s_cs_returned, s_cs_expected);

      boost::optional<std::int64_t> optional_ordinal_position =
          cs_returned.get_optional<std::int64_t>(Statistics::ORDINAL_POSITION);

      UTUtils::print("ordinal position:", optional_ordinal_position.get());
      UTUtils::print("column statistic:" + s_cs_returned);
    }
  }

  UTUtils::print(
      " -- After removing ordinal position=", ordinal_position_to_remove,
      " get column statistics by get_by_column_number end -- \n");

  std::vector<ptree> vector_cs_removed_returned;
  error = stats->get_all(ret_table_id, vector_cs_removed_returned);
  EXPECT_EQ(ErrorCode::OK, error);
  EXPECT_EQ(column_statistics.size() - 1, vector_cs_removed_returned.size());

  UTUtils::print(
      " -- After removing ordinal position=", ordinal_position_to_remove,
      " get column statistics by get_all start --");

  int ordinal_position = 1;
  for (ptree statistic : vector_cs_removed_returned) {
    boost::optional<ptree&> optional_column_statistic =
        statistic.get_child_optional(Statistics::COLUMN_STATISTIC);
    EXPECT_TRUE(optional_column_statistic);

    if (ordinal_position_to_remove == ordinal_position) {
      ordinal_position++;
    }
    std::string s_cs_returned =
        UTUtils::get_tree_string(optional_column_statistic.get());
    std::string s_cs_expected =
        UTUtils::get_tree_string(column_statistics[ordinal_position - 1]);
    ordinal_position++;

    EXPECT_EQ(s_cs_expected, s_cs_returned);

    boost::optional<std::int64_t> optional_ordinal_position =
        statistic.get_optional<std::int64_t>(Statistics::ORDINAL_POSITION);
    EXPECT_NE(ordinal_position_to_remove, optional_ordinal_position.get());

    UTUtils::print("ordinal position:", optional_ordinal_position.get());
    UTUtils::print("column statistic:" + s_cs_returned);
  }

  UTUtils::print(
      " -- After removing ordinal position=", ordinal_position_to_remove,
      " get column statistics by get_all end --");

  /**
   * remove_by_table_id
   * based on existing table.
   */
  error = stats->remove_by_table_id(ret_table_id);
  EXPECT_EQ(ErrorCode::OK, error);

  std::vector<ptree> all_column_statistics_removed;
  error = stats->get_all(ret_table_id, all_column_statistics_removed);
  EXPECT_EQ(ErrorCode::ID_NOT_FOUND, error);

  EXPECT_EQ(all_column_statistics_removed.size(), 0);

  for (ObjectIdType ordinal_position = 1;
       static_cast<size_t>(ordinal_position) <= column_statistics.size();
       ordinal_position++) {
    ptree cs_returned;

    error = stats->get_by_column_number(ret_table_id, ordinal_position,
                                        cs_returned);
    EXPECT_EQ(ErrorCode::ID_NOT_FOUND, error);
  }
}

/**
 * @brief happy test to update column statistics
 * based on both existing table id and column ordinal position.
 *
 * - add:
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

  UTUtils::print(" -- get column statistics by get_by_column_number start --");

  for (ObjectIdType ordinal_position = 1;
       static_cast<size_t>(ordinal_position) <= column_statistics.size();
       ordinal_position++) {
    ptree cs_returned;

    error = stats->get_by_column_number(ret_table_id, ordinal_position,
                                        cs_returned);
    EXPECT_EQ(ErrorCode::OK, error);

    boost::optional<ptree&> optional_column_statistic =
        cs_returned.get_child_optional(Statistics::COLUMN_STATISTIC);
    EXPECT_TRUE(optional_column_statistic);

    std::string s_cs_returned =
        UTUtils::get_tree_string(optional_column_statistic.get());
    std::string s_cs_expected =
        UTUtils::get_tree_string(column_statistics[ordinal_position - 1]);

    EXPECT_EQ(s_cs_returned, s_cs_expected);

    boost::optional<std::int64_t> optional_ordinal_position =
        cs_returned.get_optional<std::int64_t>(Statistics::ORDINAL_POSITION);

    UTUtils::print("ordinal position:", optional_ordinal_position.get());
    UTUtils::print("column statistic:" + s_cs_returned);
  }

  UTUtils::print(" -- get column statistics by get_by_column_number end -- \n");

  std::vector<ptree> vector_cs_returned;
  error = stats->get_all(ret_table_id, vector_cs_returned);
  EXPECT_EQ(ErrorCode::OK, error);
  EXPECT_EQ(column_statistics.size(), vector_cs_returned.size());

  UTUtils::print(" -- get column statistics by get_all start --");

  for (ObjectIdType ordinal_position = 1;
       static_cast<size_t>(ordinal_position) <= vector_cs_returned.size();
       ordinal_position++) {
    ptree c_cs_returned = vector_cs_returned[ordinal_position - 1];

    boost::optional<ptree&> optional_column_statistic =
        c_cs_returned.get_child_optional(Statistics::COLUMN_STATISTIC);
    EXPECT_TRUE(optional_column_statistic);

    std::string s_cs_returned =
        UTUtils::get_tree_string(optional_column_statistic.get());
    std::string s_cs_expected =
        UTUtils::get_tree_string(column_statistics[ordinal_position - 1]);

    EXPECT_EQ(s_cs_expected, s_cs_returned);

    boost::optional<std::int64_t> optional_ordinal_position =
        c_cs_returned.get_optional<std::int64_t>(Statistics::ORDINAL_POSITION);

    UTUtils::print("ordinal position:", optional_ordinal_position.get());
    UTUtils::print("column statistic:" + s_cs_returned);
  }

  UTUtils::print(" -- get column statistics by get_all end -- \n");

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
      "get_by_column_number start --");

  for (ObjectIdType ordinal_position = 1;
       static_cast<size_t>(ordinal_position) <=
       column_statistics_to_update.size();
       ordinal_position++) {
    ptree cs_returned;

    error = stats->get_by_column_number(ret_table_id, ordinal_position,
                                        cs_returned);
    EXPECT_EQ(ErrorCode::OK, error);

    boost::optional<ptree&> optional_column_statistic =
        cs_returned.get_child_optional(Statistics::COLUMN_STATISTIC);
    EXPECT_TRUE(optional_column_statistic);

    std::string s_cs_returned =
        UTUtils::get_tree_string(optional_column_statistic.get());
    std::string s_cs_expected = UTUtils::get_tree_string(
        column_statistics_to_update[ordinal_position - 1]);

    EXPECT_EQ(s_cs_returned, s_cs_expected);

    boost::optional<std::int64_t> optional_ordinal_position =
        cs_returned.get_optional<std::int64_t>(Statistics::ORDINAL_POSITION);

    UTUtils::print("ordinal position:", optional_ordinal_position.get());
    UTUtils::print("column statistic:", s_cs_returned);
  }

  UTUtils::print(
      " -- After updating all column statistics, get column statistics by ",
      "get_by_column_number end -- \n");

  std::vector<ptree> vector_cs_updated_returned;
  error = stats->get_all(ret_table_id, vector_cs_updated_returned);
  EXPECT_EQ(ErrorCode::OK, error);
  EXPECT_EQ(column_statistics_to_update.size(),
            vector_cs_updated_returned.size());

  UTUtils::print(
      "-- After updating all column statistics, get column statistics by ",
      "get_all start --");

  for (ObjectIdType ordinal_position = 1;
       static_cast<size_t>(ordinal_position) <=
       vector_cs_updated_returned.size();
       ordinal_position++) {
    ptree c_cs_returned = vector_cs_updated_returned[ordinal_position - 1];

    boost::optional<ptree&> optional_column_statistic =
        c_cs_returned.get_child_optional(Statistics::COLUMN_STATISTIC);
    EXPECT_TRUE(optional_column_statistic);

    std::string s_cs_returned =
        UTUtils::get_tree_string(optional_column_statistic.get());
    std::string s_cs_expected = UTUtils::get_tree_string(
        column_statistics_to_update[ordinal_position - 1]);

    EXPECT_EQ(s_cs_expected, s_cs_returned);

    boost::optional<std::int64_t> optional_ordinal_position =
        c_cs_returned.get_optional<std::int64_t>(Statistics::ORDINAL_POSITION);

    UTUtils::print("ordinal position:", optional_ordinal_position.get());
    UTUtils::print("column statistic:", s_cs_returned);
  }

  UTUtils::print(
      "-- After updating all column statistics, get column statistics by ",
      "get_all end -- \n");

  /**
   * remove_by_column_number
   * based on both existing table id and column ordinal position.
   */
  ObjectIdType ordinal_position_to_remove = std::get<3>(param);
  error =
      stats->remove_by_column_number(ret_table_id, ordinal_position_to_remove);
  EXPECT_EQ(ErrorCode::OK, error);

  UTUtils::print(
      " -- After removing ordinal position=", ordinal_position_to_remove,
      " get column statistics by get_by_column_number start --");

  for (ObjectIdType ordinal_position = 1;
       static_cast<size_t>(ordinal_position) <=
       column_statistics_to_update.size();
       ordinal_position++) {
    ptree cs_returned;

    error = stats->get_by_column_number(ret_table_id, ordinal_position,
                                        cs_returned);

    if (ordinal_position_to_remove == ordinal_position) {
      EXPECT_EQ(ErrorCode::ID_NOT_FOUND, error);
    } else {
      EXPECT_EQ(ErrorCode::OK, error);

      boost::optional<ptree&> optional_column_statistic =
          cs_returned.get_child_optional(Statistics::COLUMN_STATISTIC);
      EXPECT_TRUE(optional_column_statistic);

      std::string s_cs_returned =
          UTUtils::get_tree_string(optional_column_statistic.get());
      std::string s_cs_expected = UTUtils::get_tree_string(
          column_statistics_to_update[ordinal_position - 1]);

      EXPECT_EQ(s_cs_returned, s_cs_expected);

      boost::optional<std::int64_t> optional_ordinal_position =
          cs_returned.get_optional<std::int64_t>(Statistics::ORDINAL_POSITION);

      UTUtils::print("ordinal position:", optional_ordinal_position.get());
      UTUtils::print("column statistic:", s_cs_returned);
    }
  }

  UTUtils::print(
      " -- After removing ordinal position=", ordinal_position_to_remove,
      " get column statistics by get_by_column_number end -- \n");

  std::vector<ptree> vector_cs_removed_returned;
  error = stats->get_all(ret_table_id, vector_cs_removed_returned);

  if (column_statistics_to_update.size() == 1) {
    EXPECT_EQ(ErrorCode::ID_NOT_FOUND, error);
  } else {
    EXPECT_EQ(ErrorCode::OK, error);
  }

  EXPECT_EQ(column_statistics_to_update.size() - 1,
            vector_cs_removed_returned.size());

  UTUtils::print(
      " -- After removing ordinal position=", ordinal_position_to_remove,
      " get column statistics by get_all start --");

  int ordinal_position = 1;
  for (ptree statistic : vector_cs_removed_returned) {
    boost::optional<ptree&> optional_column_statistic =
        statistic.get_child_optional(Statistics::COLUMN_STATISTIC);
    EXPECT_TRUE(optional_column_statistic);

    boost::optional<std::int64_t> optional_ordinal_position =
        statistic.get_optional<std::int64_t>(Statistics::ORDINAL_POSITION);
    EXPECT_NE(ordinal_position_to_remove, optional_ordinal_position.get());

    if (ordinal_position_to_remove == ordinal_position) {
      ordinal_position++;
    }
    std::string s_cs_returned =
        UTUtils::get_tree_string(optional_column_statistic.get());
    std::string s_cs_expected = UTUtils::get_tree_string(
        column_statistics_to_update[ordinal_position - 1]);
    ordinal_position++;

    EXPECT_EQ(s_cs_expected, s_cs_returned);

    UTUtils::print("ordinal position:", optional_ordinal_position.get());
    UTUtils::print("column statistic:", s_cs_returned);
  }

  UTUtils::print(
      " -- After removing ordinal position=", ordinal_position_to_remove,
      " get column statistics by get_all end --");

  /**
   * remove_by_table_id
   * based on existing table id.
   */
  error = stats->remove_by_table_id(ret_table_id);

  if (column_statistics_to_update.size() == 1) {
    EXPECT_EQ(ErrorCode::ID_NOT_FOUND, error);
  } else {
    EXPECT_EQ(ErrorCode::OK, error);
  }

  std::vector<ptree> all_column_statistics_removed;
  error = stats->get_all(ret_table_id, all_column_statistics_removed);
  EXPECT_EQ(ErrorCode::ID_NOT_FOUND, error);

  EXPECT_EQ(all_column_statistics_removed.size(), 0);

  for (ObjectIdType ordinal_position = 1;
       static_cast<size_t>(ordinal_position) <=
       column_statistics_to_update.size();
       ordinal_position++) {
    ptree cs_returned;

    error = stats->get_by_column_number(ret_table_id, ordinal_position,
                                        cs_returned);
    EXPECT_EQ(ErrorCode::ID_NOT_FOUND, error);
  }
}

/**
 * @brief happy test to remove all column statistics
 * based on both existing table id.
 *
 * - add:
 *      remove all column statistics
 *      based on both existing table id.
 */
TEST_P(ApiTestColumnStatisticsRemoveAllHappy, remove_by_table_id) {
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

  UTUtils::print(" -- get column statistics by get_by_column_number start --");

  for (ObjectIdType ordinal_position = 1;
       static_cast<size_t>(ordinal_position) <= column_statistics.size();
       ordinal_position++) {
    ptree cs_returned;

    error = stats->get_by_column_number(ret_table_id, ordinal_position,
                                        cs_returned);
    EXPECT_EQ(ErrorCode::OK, error);

    boost::optional<ptree&> optional_column_statistic =
        cs_returned.get_child_optional(Statistics::COLUMN_STATISTIC);
    EXPECT_TRUE(optional_column_statistic);

    std::string s_cs_returned =
        UTUtils::get_tree_string(optional_column_statistic.get());
    std::string s_cs_expected =
        UTUtils::get_tree_string(column_statistics[ordinal_position - 1]);

    EXPECT_EQ(s_cs_returned, s_cs_expected);

    boost::optional<std::int64_t> optional_ordinal_position =
        cs_returned.get_optional<std::int64_t>(Statistics::ORDINAL_POSITION);

    UTUtils::print("ordinal position:", optional_ordinal_position.get());
    UTUtils::print("column statistic:", s_cs_returned);
  }

  UTUtils::print(" -- get column statistics by get_by_column_number end -- \n");

  std::vector<ptree> vector_cs_returned;
  error = stats->get_all(ret_table_id, vector_cs_returned);
  EXPECT_EQ(ErrorCode::OK, error);
  EXPECT_EQ(column_statistics.size(), vector_cs_returned.size());

  UTUtils::print(" -- get column statistics by get_all start --");

  for (ObjectIdType ordinal_position = 1;
       static_cast<size_t>(ordinal_position) <= vector_cs_returned.size();
       ordinal_position++) {
    ptree c_cs_returned = vector_cs_returned[ordinal_position - 1];

    boost::optional<ptree&> optional_column_statistic =
        c_cs_returned.get_child_optional(Statistics::COLUMN_STATISTIC);
    EXPECT_TRUE(optional_column_statistic);

    std::string s_cs_returned =
        UTUtils::get_tree_string(optional_column_statistic.get());
    std::string s_cs_expected =
        UTUtils::get_tree_string(column_statistics[ordinal_position - 1]);

    EXPECT_EQ(s_cs_expected, s_cs_returned);

    boost::optional<std::int64_t> optional_ordinal_position =
        c_cs_returned.get_optional<std::int64_t>(Statistics::ORDINAL_POSITION);

    UTUtils::print("ordinal position:", optional_ordinal_position.get());
    UTUtils::print("column statistic:", s_cs_returned);
  }

  UTUtils::print(" -- get column statistics by get_all end -- \n");

  /**
   * remove_by_table_id
   * based on existing table id.
   */
  error = stats->remove_by_table_id(ret_table_id);
  EXPECT_EQ(ErrorCode::OK, error);

  std::vector<ptree> all_column_statistics_removed;
  error = stats->get_all(ret_table_id, all_column_statistics_removed);
  EXPECT_EQ(ErrorCode::ID_NOT_FOUND, error);

  EXPECT_EQ(all_column_statistics_removed.size(), 0);

  for (ObjectIdType ordinal_position = 1;
       static_cast<size_t>(ordinal_position) <= column_statistics.size();
       ordinal_position++) {
    ptree cs_returned;

    error = stats->get_by_column_number(ret_table_id, ordinal_position,
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
 * add/get_by_column_number/remove_by_column_number:
 *      - based on non-existing column ordinal position
 *                 and existing table id.
 *      - based on non-existing table id
 *                 and existing column ordinal position.
 *      - based on both non-existing table id and column ordinal position.
 * -  get_all/remove_by_table_id:
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
    ptree cs_returned;

    error = stats->get_by_column_number(ret_table_id, ordinal_position,
                                        cs_returned);
    EXPECT_EQ(ErrorCode::OK, error);

    boost::optional<ptree&> optional_column_statistic =
        cs_returned.get_child_optional(Statistics::COLUMN_STATISTIC);
    EXPECT_TRUE(optional_column_statistic);

    std::string s_cs_returned =
        UTUtils::get_tree_string(optional_column_statistic.get());
    std::string s_cs_expected =
        UTUtils::get_tree_string(column_statistics[ordinal_position - 1]);

    EXPECT_EQ(s_cs_returned, s_cs_expected);
  }

  /**
   * add
   * based on non-existing column ordinal position
   * or non-existing table id.
   */
  for (ObjectIdType ordinal_position : global->ordinal_position_not_exists) {
    // ordinal position only not exists
    {
      ptree statistic;
      // name
      std::string statistic_name = "ApiTestColumnStatisticsAllAPIException_" +
                                   std::to_string(ret_table_id) + "-" +
                                   std::to_string(ordinal_position);
      statistic.put(Statistics::NAME, statistic_name);
      // table_id
      statistic.put(Statistics::TABLE_ID, ret_table_id);
      // ordinal_position
      statistic.put(Statistics::ORDINAL_POSITION, ordinal_position);
      // column_statistic
      statistic.add_child(Statistics::COLUMN_STATISTIC, column_statistics[0]);

      error = stats->add(statistic);
      EXPECT_EQ(ErrorCode::INVALID_PARAMETER, error);
    }

    // table id and ordinal position not exists
    for (ObjectIdType table_id : global->table_id_not_exists) {
      ptree statistic;
      // name
      std::string statistic_name = "ApiTestColumnStatisticsAllAPIException_" +
                                   std::to_string(table_id) + "-" +
                                   std::to_string(ordinal_position);
      statistic.put(Statistics::NAME, statistic_name);
      // table_id
      statistic.put(Statistics::TABLE_ID, table_id);
      // ordinal_position
      statistic.put(Statistics::ORDINAL_POSITION, ordinal_position);
      // column_statistic
      statistic.add_child(Statistics::COLUMN_STATISTIC, column_statistics[0]);

      error = stats->add(statistic);
      EXPECT_EQ(ErrorCode::INVALID_PARAMETER, error);
    }
  }

  // table id only not exists
  ObjectIdType ordinal_position_exists = 1;
  for (ObjectIdType table_id : global->table_id_not_exists) {
    ptree statistic;
    // name
    std::string statistic_name = "ApiTestColumnStatisticsAllAPIException_" +
                                 std::to_string(table_id) + "-" +
                                 std::to_string(ordinal_position_exists);
    statistic.put(Statistics::NAME, statistic_name);
    // table_id
    statistic.put(Statistics::TABLE_ID, table_id);
    // ordinal_position
    statistic.put(Statistics::ORDINAL_POSITION, ordinal_position_exists);
    // column_statistic
    statistic.add_child(Statistics::COLUMN_STATISTIC, column_statistics[0]);

    error = stats->add(statistic);
    EXPECT_EQ(ErrorCode::INVALID_PARAMETER, error);
  }

  /**
   * get_all
   * based on non-existing table id.
   */
  for (ObjectIdType table_id : global->table_id_not_exists) {
    // table id only not exists
    std::vector<ptree> vector_cs_returned;
    error = stats->get_all(table_id, vector_cs_returned);
    EXPECT_EQ(ErrorCode::ID_NOT_FOUND, error);
    EXPECT_EQ(vector_cs_returned.size(), 0);
  }

  /**
   * get_by_column_number
   * based on non-existing column ordinal position
   * or non-existing table id.
   */
  ptree cs_returned;
  for (std::int64_t ordinal_position : global->ordinal_position_not_exists) {
    // ordinal position only not exists
    error = stats->get_by_column_number(ret_table_id, ordinal_position,
                                        cs_returned);
    // if (ordinal_position >= INT64_MAX) {
    //   EXPECT_EQ(ErrorCode::INVALID_PARAMETER, error);
    // } else {
      EXPECT_EQ(ErrorCode::ID_NOT_FOUND, error);
    // }

    for (ObjectIdType table_id : global->table_id_not_exists) {
      // table id and ordinal position not exists
      error =
          stats->get_by_column_number(table_id, ordinal_position, cs_returned);
      EXPECT_EQ(ErrorCode::ID_NOT_FOUND, error);
    }
  }

  for (ObjectIdType table_id : global->table_id_not_exists) {
    // table id only not exists
    error = stats->get_by_column_number(table_id, ordinal_position_exists,
                                        cs_returned);
    EXPECT_EQ(ErrorCode::ID_NOT_FOUND, error);
  }

  /**
   * remove_by_column_number
   * based on non-existing column ordinal position
   * or non-existing table id.
   */
  for (ObjectIdType ordinal_position : global->ordinal_position_not_exists) {
    // ordinal position only not exists
    error = stats->remove_by_column_number(ret_table_id, ordinal_position);
    EXPECT_EQ(ErrorCode::ID_NOT_FOUND, error);

    for (ObjectIdType table_id : global->table_id_not_exists) {
      // table id and ordinal position not exists
      error = stats->remove_by_column_number(table_id, ordinal_position);
      EXPECT_EQ(ErrorCode::ID_NOT_FOUND, error);
    }
  }

  for (ObjectIdType table_id : global->table_id_not_exists) {
    // table id only not exists
    error = stats->remove_by_column_number(table_id, ordinal_position_exists);
    EXPECT_EQ(ErrorCode::ID_NOT_FOUND, error);
  }

  /**
   * remove_by_table_id
   * based on non-existing table id.
   */
  for (ObjectIdType table_id : global->table_id_not_exists) {
    // table id not exists
    error = stats->remove_by_table_id(table_id);
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
 * add/get_by_column_number/remove_by_column_number
 * : based on both existing table id and column ordinal position.
 * - get_all/remove_by_table_id :
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
   * add without init()
   * based on both existing table id and column ordinal position.
   */
  auto stats_add = std::make_unique<Statistics>(GlobalTestEnvironment::TEST_DB);

  UTUtils::print(" -- add column statistics by add start --");
  UTUtils::print("id:", ret_table_id);

  ErrorCode error;
  std::vector<ptree> column_statistics = std::get<1>(param);
  for (ObjectIdType ordinal_position = 1;
       static_cast<size_t>(ordinal_position) <= column_statistics.size();
       ordinal_position++) {
    ptree statistic;
    // name
    std::string statistic_name = "ApiTestColumnStatisticsAllAPIException_" +
                                 std::to_string(ret_table_id) + "-" +
                                 std::to_string(ordinal_position);
    statistic.put(Statistics::NAME, statistic_name);
    // table_id
    statistic.put(Statistics::TABLE_ID, ret_table_id);
    // ordinal_position
    statistic.put(Statistics::ORDINAL_POSITION, ordinal_position);
    // column_statistic
    statistic.add_child(Statistics::COLUMN_STATISTIC,
                        column_statistics[ordinal_position - 1]);

    error = stats_add->add(statistic);

    EXPECT_EQ(ErrorCode::OK, error);

    UTUtils::print("ordinal position:", ordinal_position);
    UTUtils::print(
        "column statistics:",
        UTUtils::get_tree_string(column_statistics[ordinal_position - 1]));
  }

  UTUtils::print(" -- add column statistics by add end -- \n");

  /**
   * get_by_column_number without init()
   * based on both existing table id and column ordinal position.
   */
  auto stats_get_one_cs =
      std::make_unique<Statistics>(GlobalTestEnvironment::TEST_DB);

  UTUtils::print(" -- get column statistics by get_by_column_number start --");

  for (ObjectIdType ordinal_position = 1;
       static_cast<size_t>(ordinal_position) <= column_statistics.size();
       ordinal_position++) {
    ptree cs_returned;

    error = stats_get_one_cs->get_by_column_number(
        ret_table_id, ordinal_position, cs_returned);
    EXPECT_EQ(ErrorCode::OK, error);

    boost::optional<ptree&> optional_column_statistic =
        cs_returned.get_child_optional(Statistics::COLUMN_STATISTIC);
    EXPECT_TRUE(optional_column_statistic);

    std::string s_cs_returned =
        UTUtils::get_tree_string(optional_column_statistic.get());
    std::string s_cs_expected =
        UTUtils::get_tree_string(column_statistics[ordinal_position - 1]);

    EXPECT_EQ(s_cs_returned, s_cs_expected);

    boost::optional<std::int64_t> optional_ordinal_position =
        cs_returned.get_optional<std::int64_t>(Statistics::ORDINAL_POSITION);

    UTUtils::print("ordinal position:", optional_ordinal_position.get());
    UTUtils::print("column statistic:", s_cs_returned);
  }

  UTUtils::print(" -- get column statistics by get_by_column_number end -- \n");

  /**
   * get_all without init()
   * based on existing table id.
   */
  auto stats_get_all_cs =
      std::make_unique<Statistics>(GlobalTestEnvironment::TEST_DB);

  std::vector<ptree> vector_cs_returned;
  error = stats_get_all_cs->get_all(ret_table_id, vector_cs_returned);
  EXPECT_EQ(ErrorCode::OK, error);

  UTUtils::print(" -- get column statistics by get_all start --");

  for (ObjectIdType ordinal_position = 1;
       static_cast<size_t>(ordinal_position) <= vector_cs_returned.size();
       ordinal_position++) {
    ptree c_cs_returned = vector_cs_returned[ordinal_position - 1];

    boost::optional<ptree&> optional_column_statistic =
        c_cs_returned.get_child_optional(Statistics::COLUMN_STATISTIC);
    EXPECT_TRUE(optional_column_statistic);

    std::string s_cs_returned =
        UTUtils::get_tree_string(optional_column_statistic.get());
    std::string s_cs_expected =
        UTUtils::get_tree_string(column_statistics[ordinal_position - 1]);

    EXPECT_EQ(s_cs_expected, s_cs_returned);

    boost::optional<std::int64_t> optional_ordinal_position =
        c_cs_returned.get_optional<std::int64_t>(Statistics::ORDINAL_POSITION);

    UTUtils::print("ordinal position:", optional_ordinal_position.get());
    UTUtils::print("column statistic:", s_cs_returned);
  }

  UTUtils::print(" -- get column statistics by get_all end -- \n");

  /**
   * remove_by_column_number without init()
   * based on both existing table id and column ordinal position.
   */
  auto stats_remove_one_cs =
      std::make_unique<Statistics>(GlobalTestEnvironment::TEST_DB);

  ObjectIdType ordinal_position_to_remove = std::get<2>(param);
  error = stats_remove_one_cs->remove_by_column_number(
      ret_table_id, ordinal_position_to_remove);
  EXPECT_EQ(ErrorCode::OK, error);

  UTUtils::print(
      " -- After removing ordinal position=", ordinal_position_to_remove,
      " get column statistics by get_by_column_number start --");

  for (ObjectIdType ordinal_position = 1;
       static_cast<size_t>(ordinal_position) <= column_statistics.size();
       ordinal_position++) {
    ptree cs_returned;

    error = stats_remove_one_cs->get_by_column_number(
        ret_table_id, ordinal_position, cs_returned);

    if (ordinal_position_to_remove == ordinal_position) {
      EXPECT_EQ(ErrorCode::ID_NOT_FOUND, error);
    } else {
      EXPECT_EQ(ErrorCode::OK, error);

      boost::optional<ptree&> optional_column_statistic =
          cs_returned.get_child_optional(Statistics::COLUMN_STATISTIC);
      EXPECT_TRUE(optional_column_statistic);

      std::string s_cs_returned =
          UTUtils::get_tree_string(optional_column_statistic.get());
      std::string s_cs_expected =
          UTUtils::get_tree_string(column_statistics[ordinal_position - 1]);

      EXPECT_EQ(s_cs_returned, s_cs_expected);

      boost::optional<std::int64_t> optional_ordinal_position =
          cs_returned.get_optional<std::int64_t>(Statistics::ORDINAL_POSITION);

      UTUtils::print("ordinal position:", optional_ordinal_position.get());
      UTUtils::print("column statistic:", s_cs_returned);
    }
  }

  UTUtils::print(
      " -- After removing ordinal position=", ordinal_position_to_remove,
      " get column statistics by get_by_column_number end -- \n");

  std::vector<ptree> vector_cs_removed_returned;
  error =
      stats_remove_one_cs->get_all(ret_table_id, vector_cs_removed_returned);
  EXPECT_EQ(ErrorCode::OK, error);
  EXPECT_EQ(column_statistics.size() - 1, vector_cs_removed_returned.size());

  UTUtils::print(
      " -- After removing ordinal position=", ordinal_position_to_remove,
      " get column statistics by get_all start --");

  int ordinal_position = 1;
  for (ptree statistic : vector_cs_removed_returned) {
    boost::optional<ptree&> optional_column_statistic =
        statistic.get_child_optional(Statistics::COLUMN_STATISTIC);
    EXPECT_TRUE(optional_column_statistic);

    if (ordinal_position_to_remove == ordinal_position) {
      ordinal_position++;
    }
    std::string s_cs_returned =
        UTUtils::get_tree_string(optional_column_statistic.get());
    std::string s_cs_expected =
        UTUtils::get_tree_string(column_statistics[ordinal_position - 1]);
    ordinal_position++;

    EXPECT_EQ(s_cs_expected, s_cs_returned);

    boost::optional<std::int64_t> optional_ordinal_position =
        statistic.get_optional<std::int64_t>(Statistics::ORDINAL_POSITION);
    EXPECT_NE(ordinal_position_to_remove, optional_ordinal_position.get());

    UTUtils::print("ordinal position:", optional_ordinal_position.get());
    UTUtils::print("column statistic:", s_cs_returned);
  }

  UTUtils::print(
      " -- After removing ordinal position=", ordinal_position_to_remove,
      " get column statistics by get_all end --");

  /**
   * remove_by_table_id without init()
   * based on existing table.
   */
  auto stats_remove_all_cs =
      std::make_unique<Statistics>(GlobalTestEnvironment::TEST_DB);

  error = stats_remove_all_cs->remove_by_table_id(ret_table_id);
  EXPECT_EQ(ErrorCode::OK, error);

  std::vector<ptree> all_column_statistics_removed;
  error =
      stats_remove_all_cs->get_all(ret_table_id, all_column_statistics_removed);
  EXPECT_EQ(ErrorCode::ID_NOT_FOUND, error);

  EXPECT_EQ(all_column_statistics_removed.size(), 0);

  for (ObjectIdType ordinal_position = 1;
       static_cast<size_t>(ordinal_position) <= column_statistics.size();
       ordinal_position++) {
    ptree cs_returned;

    error = stats_remove_all_cs->get_by_column_number(
        ret_table_id, ordinal_position, cs_returned);
    EXPECT_EQ(ErrorCode::ID_NOT_FOUND, error);
  }
}

}  // namespace manager::metadata::testing
