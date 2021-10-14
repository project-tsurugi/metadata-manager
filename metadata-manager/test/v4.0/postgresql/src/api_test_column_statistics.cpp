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

  UTUtils::print("-- add column statistics by add_column_statistic start --");
  UTUtils::print(" id:", table_id);

  boost::property_tree::ptree statistic;

  for (std::int64_t ordinal_position = 1;
       static_cast<std::size_t>(ordinal_position) <= column_statistics.size();
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

    UTUtils::print(" ordinal position: ", ordinal_position);
    UTUtils::print(
        " column statistics:" +
        UTUtils::get_tree_string(column_statistics[ordinal_position - 1]));
  }

  UTUtils::print("-- add column statistics by add_column_statistic end --\n");
}

/**
 * @brief Add column statistics based on the given table id and
 *  the given ptree type column statistics.
 * @param (table_id)          [in]  table id.
 * @param (column_statistics)     [in]  ptree type column statistics.
 * @return none.
 */
void ApiTestColumnStatistics::add_column_statistics(ObjectIdType column_id,
                                                    ptree column_statistics) {
  auto stats = std::make_unique<Statistics>(GlobalTestEnvironment::TEST_DB);

  ErrorCode error = stats->init();
  EXPECT_EQ(ErrorCode::OK, error);

  UTUtils::print("-- add column statistics by add_column_statistic start --");
  UTUtils::print(" column_id:", column_id);
  UTUtils::print(" column statistics:" +
                 UTUtils::get_tree_string(column_statistics));

  boost::property_tree::ptree statistic;
  // name
  std::string statistic_name =
      "ApiTestColumnStatistics_" + std::to_string(column_id);
  statistic.put(Statistics::NAME, statistic_name);
  // column_id
  statistic.put(Statistics::COLUMN_ID, column_id);
  // column_statistic
  statistic.add_child(Statistics::COLUMN_STATISTIC, column_statistics);

  error = stats->add(statistic);
  EXPECT_EQ(ErrorCode::OK, error);

  UTUtils::print("-- add column statistics by add_column_statistic end --\n");
}

/**
 * @brief happy test for add/get_all/remove API.
 *
 * - add:
 *     based on existing table id and ordinal position.
 * - get_all/remove_by_table_id:
 *     based on existing table id.
 */
TEST_P(ApiTestColumnStatisticsAllAPIHappy, get_all_api_by_table_id) {
  auto param = GetParam();

  UTTableMetadata* testdata_table_metadata =
      global->testdata_table_metadata.get();
  std::string table_name_base =
      testdata_table_metadata->name + std::get<0>(param) + "_";

  // add table metadata.
  std::string table_name_1 = table_name_base + "1";
  ObjectIdType ret_table_id_1;
  ApiTestTableMetadata::add_table(table_name_1, &ret_table_id_1);
  // add table metadata.
  std::string table_name_2 = table_name_base + "2";
  ObjectIdType ret_table_id_2;
  ApiTestTableMetadata::add_table(table_name_2, &ret_table_id_2);

  /**
   * add
   * based on both existing table id and column ordinal position.
   */
  std::vector<ptree> column_statistics = std::get<1>(param);
  ApiTestColumnStatistics::add_column_statistics(ret_table_id_1,
                                                 column_statistics);
  ApiTestColumnStatistics::add_column_statistics(ret_table_id_2,
                                                 column_statistics);

  auto stats = std::make_unique<Statistics>(GlobalTestEnvironment::TEST_DB);
  ErrorCode error = stats->init();
  EXPECT_EQ(ErrorCode::OK, error);

  /**
   * get_all
   * based on existing table id.
   */
  UTUtils::print("-- get column statistics by get_all start --");
  std::vector<ptree> ret_statistics;
  error = stats->get_all(ret_table_id_1, ret_statistics);
  EXPECT_EQ(ErrorCode::OK, error);

  for (std::size_t ordinal_position = 0;
       ordinal_position < ret_statistics.size(); ordinal_position++) {
    auto optional_column_statistic =
        ret_statistics[ordinal_position].get_child_optional(
            Statistics::COLUMN_STATISTIC);
    EXPECT_TRUE(optional_column_statistic);

    std::string s_statistics_expected =
        UTUtils::get_tree_string(column_statistics[ordinal_position]);
    std::string s_statistics_actual =
        UTUtils::get_tree_string(optional_column_statistic.get());
    EXPECT_EQ(s_statistics_expected, s_statistics_actual);

    auto optional_ordinal_position =
        ret_statistics[ordinal_position].get_optional<std::int64_t>(
            Statistics::ORDINAL_POSITION);

    UTUtils::print(" ordinal position: ", optional_ordinal_position.get());
    UTUtils::print(" column statistic: ", s_statistics_actual);
  }
  UTUtils::print("-- get column statistics by get_all end --\n");

  /**
   * remove_by_table_id
   * based on existing table.
   */
  UTUtils::print("-- remove column statistics by remove_by_table_id start --");
  error = stats->remove_by_table_id(ret_table_id_1);
  EXPECT_EQ(ErrorCode::OK, error);

  error = stats->remove_by_table_id(ret_table_id_1);
  EXPECT_EQ(ErrorCode::ID_NOT_FOUND, error);

  std::vector<ptree> all_column_statistics_removed;
  error = stats->get_all(ret_table_id_1, all_column_statistics_removed);
  EXPECT_EQ(ErrorCode::ID_NOT_FOUND, error);
  EXPECT_EQ(all_column_statistics_removed.size(), 0);

  for (ObjectIdType ordinal_position = 1;
       static_cast<size_t>(ordinal_position) <= column_statistics.size();
       ordinal_position++) {
    ptree cs_returned;
    error = stats->get_by_column_number(ret_table_id_1, ordinal_position,
                                        cs_returned);
    EXPECT_EQ(ErrorCode::ID_NOT_FOUND, error);
  }
  UTUtils::print("-- remove column statistics by remove_by_table_id end --\n");

  // remove table metadata.
  ApiTestTableMetadata::remove_table(ret_table_id_1);
  ApiTestTableMetadata::remove_table(ret_table_id_2);
}

/**
 * @brief happy test for add/get_all/remove API.
 *
 * - add:
 *     based on existing table id and ordinal position.
 * - get_all:
 *     all metadata.
 * - remove_by_table_id:
 *     based on existing table id.
 */
TEST_P(ApiTestColumnStatisticsAllAPIHappy, get_all_api) {
  auto param = GetParam();

  UTTableMetadata* testdata_table_metadata =
      global->testdata_table_metadata.get();
  std::string table_name_base =
      testdata_table_metadata->name + std::get<0>(param) + "_";

  // add table metadata.
  std::string table_name_1 = table_name_base + "1";
  ObjectIdType ret_table_id_1;
  ApiTestTableMetadata::add_table(table_name_1, &ret_table_id_1);
  // add table metadata.
  std::string table_name_2 = table_name_base + "2";
  ObjectIdType ret_table_id_2;
  ApiTestTableMetadata::add_table(table_name_2, &ret_table_id_2);

  /**
   * add
   * based on both existing table id and column ordinal position.
   */
  std::vector<ptree> column_statistics = std::get<1>(param);
  ApiTestColumnStatistics::add_column_statistics(ret_table_id_1,
                                                 column_statistics);
  ApiTestColumnStatistics::add_column_statistics(ret_table_id_2,
                                                 column_statistics);

  auto stats = std::make_unique<Statistics>(GlobalTestEnvironment::TEST_DB);
  ErrorCode error = stats->init();
  EXPECT_EQ(ErrorCode::OK, error);

  /**
   * get_all
   * based on existing table id.
   */
  UTUtils::print("-- get column statistics by get_all start --");
  std::vector<ptree> ret_statistics;
  error = stats->get_all(ret_statistics);
  EXPECT_EQ(ErrorCode::OK, error);

  // remove data that is not under test.
  auto itr = ret_statistics.begin();
  while (itr != ret_statistics.end()) {
    auto optional_table_id =
        (*itr).get_optional<ObjectIdType>(Statistics::TABLE_ID);
    EXPECT_TRUE(optional_table_id);

    if ((optional_table_id.value() == ret_table_id_1) ||
        (optional_table_id.value() == ret_table_id_2)) {
      itr++;
    } else {
      itr = ret_statistics.erase(itr);
    }
  }

  EXPECT_EQ((column_statistics.size() * 2), ret_statistics.size());

  for (std::size_t index = 0; index < ret_statistics.size(); index++) {
    ptree statistics_expected =
        column_statistics[index % column_statistics.size()];

    auto optional_column_statistic =
        ret_statistics[index].get_child_optional(Statistics::COLUMN_STATISTIC);
    EXPECT_TRUE(optional_column_statistic);

    std::string s_statistics_expected =
        UTUtils::get_tree_string(statistics_expected);
    std::string s_statistics_actual =
        UTUtils::get_tree_string(optional_column_statistic.get());
    EXPECT_EQ(s_statistics_expected, s_statistics_actual);

    auto optional_ordinal_position =
        ret_statistics[index].get_optional<std::int64_t>(
            Statistics::ORDINAL_POSITION);

    UTUtils::print(" ordinal position: ", optional_ordinal_position.get());
    UTUtils::print(" column statistic: ", s_statistics_actual);
  }
  UTUtils::print("-- get column statistics by get_all end --\n");

  /**
   * remove_by_table_id
   * based on existing table.
   */
  UTUtils::print("-- remove column statistics by remove_by_table_id start --");
  error = stats->remove_by_table_id(ret_table_id_1);
  EXPECT_EQ(ErrorCode::OK, error);
  error = stats->remove_by_table_id(ret_table_id_2);
  EXPECT_EQ(ErrorCode::OK, error);

  error = stats->remove_by_table_id(ret_table_id_1);
  EXPECT_EQ(ErrorCode::ID_NOT_FOUND, error);
  error = stats->remove_by_table_id(ret_table_id_2);
  EXPECT_EQ(ErrorCode::ID_NOT_FOUND, error);

  std::vector<ptree> all_column_statistics_removed;
  error = stats->get_all(all_column_statistics_removed);
  EXPECT_EQ(ErrorCode::OK, error);

  // remove data that is not under test.
  itr = all_column_statistics_removed.begin();
  while (itr != all_column_statistics_removed.end()) {
    auto optional_table_id =
        (*itr).get_optional<ObjectIdType>(Statistics::TABLE_ID);
    EXPECT_TRUE(optional_table_id);

    if ((optional_table_id == ret_table_id_1) ||
        (optional_table_id == ret_table_id_2)) {
      itr++;
    } else {
      itr = all_column_statistics_removed.erase(itr);
    }
  }
  EXPECT_EQ(all_column_statistics_removed.size(), 0);
  UTUtils::print("-- remove column statistics by remove_by_table_id end --\n");

  // remove table metadata.
  ApiTestTableMetadata::remove_table(ret_table_id_1);
  ApiTestTableMetadata::remove_table(ret_table_id_2);
}

/**
 * @brief happy test for add/get/remove API by statistic id.
 *   add/get/remove one column statistic based on both existing statistic id.
 *
 * - add:
 *      based on existing ordinal position.
 * - get/remove:
 *      based on existing statistic id.
 */
TEST_P(ApiTestColumnStatisticsAllAPIHappy, get_remove_api_by_statistic_id) {
  auto param = GetParam();

  UTTableMetadata* testdata_table_metadata =
      global->testdata_table_metadata.get();
  std::string table_name = testdata_table_metadata->name + std::get<0>(param);
  std::vector<ptree> column_statistics = std::get<1>(param);

  ErrorCode error = ErrorCode::UNKNOWN;

  // add table metadata.
  ObjectIdType ret_table_id;
  ApiTestTableMetadata::add_table(table_name, &ret_table_id);

  auto stats = std::make_unique<Statistics>(GlobalTestEnvironment::TEST_DB);
  error = stats->init();
  EXPECT_EQ(ErrorCode::OK, error);

  /**
   * add(by ordinal position)
   * based on both existing table id and column ordinal position.
   */
  UTUtils::print(
      "-- add column statistics by add (by ordinal position) start --");
  std::vector<ObjectIdType> statistic_ids = {};
  for (std::size_t index = 0; index < column_statistics.size(); index++) {
    boost::property_tree::ptree statistic;
    // name
    std::string statistic_name =
        "ApiTestColumnStatistics_" + std::to_string(index);
    statistic.put(Statistics::NAME, statistic_name);
    // table_id
    statistic.put(Statistics::TABLE_ID, ret_table_id);
    // ordinal_position
    statistic.put(Statistics::ORDINAL_POSITION, (index + 1));
    // column_statistic
    statistic.add_child(Statistics::COLUMN_STATISTIC, column_statistics[index]);

    ObjectIdType statistic_id;
    error = stats->add(statistic, &statistic_id);
    EXPECT_EQ(ErrorCode::OK, error);

    statistic_ids.push_back(statistic_id);
  }
  UTUtils::print(
      "-- add column statistics by add (by ordinal position) end --\n");

  /**
   * get
   * based on both existing statistic id.
   */
  UTUtils::print("-- get column statistics by get (by statistic id) start --");
  for (ObjectIdType statistic_id : statistic_ids) {
    ptree cs_returned;

    error = stats->get(statistic_id, cs_returned);
    EXPECT_EQ(ErrorCode::OK, error);

    // column metadata ordinal position
    auto optional_ordinal_position = cs_returned.get_optional<std::int64_t>(
        Tables::Column::ORDINAL_POSITION);
    EXPECT_TRUE(optional_ordinal_position);

    // column metadata column statistic
    auto optional_column_statistic =
        cs_returned.get_child_optional(Statistics::COLUMN_STATISTIC);
    EXPECT_TRUE(optional_column_statistic);

    std::string s_cs_returned =
        UTUtils::get_tree_string(optional_column_statistic.get());
    std::string s_cs_expected = UTUtils::get_tree_string(
        column_statistics[optional_ordinal_position.get() - 1]);

    UTUtils::print(" ordinal position: ", optional_ordinal_position.get());
    UTUtils::print(" column statistic: ", s_cs_returned);

    EXPECT_EQ(s_cs_returned, s_cs_expected);
  }
  UTUtils::print("-- get column statistics by get (by statistic id) end --\n");

  /**
   * remove
   * based on both existing statistic id.
   */
  UTUtils::print(
      "-- remove column statistics by remove (by statistic id) start --");
  for (ObjectIdType statistic_id : statistic_ids) {
    error = stats->remove(statistic_id);
    EXPECT_EQ(ErrorCode::OK, error);

    error = stats->remove(statistic_id);
    EXPECT_EQ(ErrorCode::ID_NOT_FOUND, error);

    ptree cs_returned;
    error = stats->get(statistic_id, cs_returned);
    EXPECT_EQ(ErrorCode::ID_NOT_FOUND, error);
  }
  UTUtils::print(
      "-- remove column statistics by remove (by statistic id) end --\n");

  // remove table metadata.
  ApiTestTableMetadata::remove_table(ret_table_id);
}

/**
 * @brief happy test for add/get/remove API by statistic name.
 *   add/get/remove one column statistic based on both existing statistic name.
 *
 * - add:
 *      based on existing ordinal position.
 * - get/remove:
 *      based on existing statistic name.
 */
TEST_P(ApiTestColumnStatisticsAllAPIHappy, get_remove_api_by_statistic_name) {
  auto param = GetParam();

  UTTableMetadata* testdata_table_metadata =
      global->testdata_table_metadata.get();
  std::string table_name = testdata_table_metadata->name + std::get<0>(param);
  std::vector<ptree> column_statistics = std::get<1>(param);

  ErrorCode error = ErrorCode::UNKNOWN;

  // add table metadata.
  ObjectIdType ret_table_id;
  ApiTestTableMetadata::add_table(table_name, &ret_table_id);

  auto stats = std::make_unique<Statistics>(GlobalTestEnvironment::TEST_DB);
  error = stats->init();
  EXPECT_EQ(ErrorCode::OK, error);

  /**
   * add(by ordinal position)
   * based on both existing table id and column ordinal position.
   */
  UTUtils::print(
      "-- add column statistics by add (by ordinal position) start --");
  std::vector<ObjectIdType> statistic_ids = {};
  std::vector<std::string> statistic_names = {};
  for (std::size_t index = 0; index < column_statistics.size(); index++) {
    boost::property_tree::ptree statistic;
    // name
    std::string statistic_name =
        "ApiTestColumnStatistics_" + std::to_string(index);
    statistic.put(Statistics::NAME, statistic_name);
    // table_id
    statistic.put(Statistics::TABLE_ID, ret_table_id);
    // ordinal_position
    statistic.put(Statistics::ORDINAL_POSITION, (index + 1));
    // column_statistic
    statistic.add_child(Statistics::COLUMN_STATISTIC, column_statistics[index]);

    ObjectIdType statistic_id;
    error = stats->add(statistic, &statistic_id);
    EXPECT_EQ(ErrorCode::OK, error);

    statistic_ids.push_back(statistic_id);
    statistic_names.push_back(statistic_name);
  }
  UTUtils::print(
      "-- add column statistics by add (by ordinal position) end --\n");

  /**
   * get
   * based on both existing statistic name.
   */
  UTUtils::print(
      "-- get column statistics by get (by statistic name) start --");
  for (std::string statistic_name : statistic_names) {
    ptree cs_returned;

    error = stats->get(statistic_name, cs_returned);
    EXPECT_EQ(ErrorCode::OK, error);

    // column metadata ordinal position
    auto optional_ordinal_position = cs_returned.get_optional<std::int64_t>(
        Tables::Column::ORDINAL_POSITION);
    EXPECT_TRUE(optional_ordinal_position);

    // column metadata column statistic
    auto optional_column_statistic =
        cs_returned.get_child_optional(Statistics::COLUMN_STATISTIC);
    EXPECT_TRUE(optional_column_statistic);

    std::string s_cs_returned =
        UTUtils::get_tree_string(optional_column_statistic.get());
    std::string s_cs_expected = UTUtils::get_tree_string(
        column_statistics[optional_ordinal_position.get() - 1]);

    UTUtils::print(" ordinal position: ", optional_ordinal_position.get());
    UTUtils::print(" column statistic: ", s_cs_returned);

    EXPECT_EQ(s_cs_returned, s_cs_expected);
  }
  UTUtils::print(
      "-- get column statistics by get (by statistic name) end --\n");

  /**
   * remove
   * based on both existing statistic name.
   */
  UTUtils::print(
      "-- remove column statistics by remove (by statistic name) start --");
  for (std::size_t index = 0; index < statistic_names.size(); index++) {
    ObjectIdType ret_statistic_id;
    error = stats->remove(statistic_names[index], &ret_statistic_id);
    EXPECT_EQ(ErrorCode::OK, error);
    EXPECT_EQ(statistic_ids[index], ret_statistic_id);

    error = stats->remove(statistic_names[index], &ret_statistic_id);
    EXPECT_EQ(ErrorCode::NAME_NOT_FOUND, error);

    ptree cs_returned;
    error = stats->get(statistic_names[index], cs_returned);
    EXPECT_EQ(ErrorCode::NAME_NOT_FOUND, error);
  }
  UTUtils::print(
      "-- remove column statistics by remove (by statistic name) end --\n");

  // remove table metadata.
  ApiTestTableMetadata::remove_table(ret_table_id);
}

/**
 * @brief happy test for add/get/remove API by column id.
 *   add/get/remove one column statistic based on both existing column id.
 *
 * - add/get_by_column_id/remove_by_culumn_id:
 *      based on existing column id.
 */
TEST_P(ApiTestColumnStatisticsAllAPIHappy, get_remove_api_by_column_id) {
  auto param = GetParam();

  UTTableMetadata* testdata_table_metadata =
      global->testdata_table_metadata.get();
  std::string table_name = testdata_table_metadata->name + std::get<0>(param);
  std::vector<ptree> column_statistics = std::get<1>(param);

  ErrorCode error = ErrorCode::UNKNOWN;

  // add table metadata.
  ObjectIdType ret_table_id;
  ApiTestTableMetadata::add_table(table_name, &ret_table_id);

  auto tables = std::make_unique<Tables>(GlobalTestEnvironment::TEST_DB);
  error = tables->init();
  EXPECT_EQ(ErrorCode::OK, error);

  // get table metadata.
  ptree table_metadata;
  error = tables->get(ret_table_id, table_metadata);
  EXPECT_EQ(ErrorCode::OK, error);

  // get column metadata.
  auto o_columns = table_metadata.get_child_optional(Tables::COLUMNS_NODE);
  if (!o_columns) {
    GTEST_FAIL();
  }
  EXPECT_EQ(column_statistics.size(), o_columns.value().size());

  // get column metadata
  std::vector<ObjectIdType> column_ids = {};
  for (auto node : o_columns.value()) {
    ptree column_metadata = node.second;
    // column metadata id
    auto column_id =
        column_metadata.get_optional<ObjectIdType>(Tables::Column::ID);
    EXPECT_TRUE(column_id);

    column_ids.push_back(column_id.get());
  }

  auto stats = std::make_unique<Statistics>(GlobalTestEnvironment::TEST_DB);
  error = stats->init();
  EXPECT_EQ(ErrorCode::OK, error);

  /**
   * add(by column id)
   * based on both existing column id.
   */
  UTUtils::print("-- add column statistics by add (by column id) start --");
  for (std::size_t index = 0; index < column_ids.size(); index++) {
    ObjectIdType column_id = column_ids.at(index);

    boost::property_tree::ptree statistic;
    // name
    std::string statistic_name =
        "ApiTestColumnStatistics_" + std::to_string(column_id);
    statistic.put(Statistics::NAME, statistic_name);
    // column_id
    statistic.put(Statistics::COLUMN_ID, column_id);
    // column_statistic
    statistic.add_child(Statistics::COLUMN_STATISTIC, column_statistics[index]);

    error = stats->add(statistic);
    EXPECT_EQ(ErrorCode::OK, error);
  }
  UTUtils::print("-- add column statistics by add (by column id) end --\n");

  /**
   * get_by_column_id
   * based on both existing column id.
   */
  UTUtils::print("-- get column statistics by get_by_column_id start --");
  for (ObjectIdType column_id : column_ids) {
    ptree cs_returned;

    error = stats->get_by_column_id(column_id, cs_returned);
    EXPECT_EQ(ErrorCode::OK, error);

    // column metadata ordinal position
    auto optional_ordinal_position = cs_returned.get_optional<std::int64_t>(
        Tables::Column::ORDINAL_POSITION);
    EXPECT_TRUE(optional_ordinal_position);

    // column metadata column statistic
    auto optional_column_statistic =
        cs_returned.get_child_optional(Statistics::COLUMN_STATISTIC);
    EXPECT_TRUE(optional_column_statistic);

    std::string s_cs_returned =
        UTUtils::get_tree_string(optional_column_statistic.get());
    std::string s_cs_expected = UTUtils::get_tree_string(
        column_statistics[optional_ordinal_position.get() - 1]);

    UTUtils::print(" ordinal position: ", optional_ordinal_position.get());
    UTUtils::print(" column statistic: ", s_cs_returned);

    EXPECT_EQ(s_cs_returned, s_cs_expected);
  }
  UTUtils::print("-- get column statistics by get_by_column_id end --\n");

  /**
   * remove_by_column_id
   * based on both existing column id.
   */
  UTUtils::print("-- remove column statistics by remove_by_column_id start --");
  for (ObjectIdType column_id : column_ids) {
    error = stats->remove_by_column_id(column_id);
    EXPECT_EQ(ErrorCode::OK, error);

    error = stats->remove_by_column_id(column_id);
    EXPECT_EQ(ErrorCode::ID_NOT_FOUND, error);

    ptree cs_returned;
    error = stats->get_by_column_id(column_id, cs_returned);
    EXPECT_EQ(ErrorCode::ID_NOT_FOUND, error);
  }
  UTUtils::print("-- remove column statistics by remove_by_column_id end --\n");

  // remove table metadata.
  ApiTestTableMetadata::remove_table(ret_table_id);
}

/**
 * @brief happy test for add/get/remove API by column number.
 *   add/get/remove one column statistic based on both existing column number.
 *
 * - add/get_by_column_number/remove_by_column_number:
 *      based on existing table id and column number.
 */
TEST_P(ApiTestColumnStatisticsAllAPIHappy, get_remove_api_by_column_number) {
  auto param = GetParam();

  UTTableMetadata* testdata_table_metadata =
      global->testdata_table_metadata.get();
  std::string table_name = testdata_table_metadata->name + std::get<0>(param);
  std::vector<ptree> column_statistics = std::get<1>(param);

  ErrorCode error = ErrorCode::UNKNOWN;

  // add table metadata.
  ObjectIdType ret_table_id;
  ApiTestTableMetadata::add_table(table_name, &ret_table_id);

  auto stats = std::make_unique<Statistics>(GlobalTestEnvironment::TEST_DB);
  error = stats->init();
  EXPECT_EQ(ErrorCode::OK, error);

  /**
   * add(by ordinal position)
   * based on both existing table id and column ordinal position.
   */
  UTUtils::print(
      "-- add column statistics by add (by ordinal position) start --");
  for (std::size_t index = 0; index < column_statistics.size(); index++) {
    boost::property_tree::ptree statistic;
    // name
    std::string statistic_name =
        "ApiTestColumnStatistics_" + std::to_string(index);
    statistic.put(Statistics::NAME, statistic_name);
    // table_id
    statistic.put(Statistics::TABLE_ID, ret_table_id);
    // ordinal_position
    statistic.put(Statistics::ORDINAL_POSITION, (index + 1));
    // column_statistic
    statistic.add_child(Statistics::COLUMN_STATISTIC, column_statistics[index]);

    error = stats->add(statistic);
    EXPECT_EQ(ErrorCode::OK, error);
  }
  UTUtils::print(
      "-- add column statistics by add (by ordinal position) end --\n");

  /**
   * get_by_column_number
   * based on both existing table id and ordinal position.
   */
  UTUtils::print("-- get column statistics by get_by_column_number start --");
  for (std::int64_t ordinal_position = 1;
       static_cast<std::size_t>(ordinal_position) <= column_statistics.size();
       ordinal_position++) {
    ptree cs_returned;

    error = stats->get_by_column_number(ret_table_id, ordinal_position,
                                        cs_returned);
    EXPECT_EQ(ErrorCode::OK, error);

    // column metadata column statistic
    auto optional_column_statistic =
        cs_returned.get_child_optional(Statistics::COLUMN_STATISTIC);
    EXPECT_TRUE(optional_column_statistic);

    std::string s_cs_returned =
        UTUtils::get_tree_string(optional_column_statistic.get());
    std::string s_cs_expected =
        UTUtils::get_tree_string(column_statistics[ordinal_position - 1]);

    UTUtils::print(" ordinal position: ", ordinal_position);
    UTUtils::print(" column statistic: ", s_cs_returned);

    EXPECT_EQ(s_cs_returned, s_cs_expected);
  }
  UTUtils::print("-- get column statistics by get_by_column_number end --\n");

  /**
   * remove_by_column_number
   * based on both existing table id and column number.
   */
  UTUtils::print(
      "-- remove column statistics by remove_by_column_number start --");
  for (std::size_t ordinal_position = 1;
       ordinal_position <= column_statistics.size(); ordinal_position++) {
    error = stats->remove_by_column_number(ret_table_id, ordinal_position);
    EXPECT_EQ(ErrorCode::OK, error);

    error = stats->remove_by_column_number(ret_table_id, ordinal_position);
    EXPECT_EQ(ErrorCode::ID_NOT_FOUND, error);

    ptree cs_returned;
    error = stats->get_by_column_number(ret_table_id, ordinal_position,
                                        cs_returned);
    EXPECT_EQ(ErrorCode::ID_NOT_FOUND, error);
  }
  UTUtils::print(
      "-- remove column statistics by remove_by_column_numebr end --\n");

  // remove table metadata.
  ApiTestTableMetadata::remove_table(ret_table_id);
}

/**
 * @brief happy test for add/get/remove API by column name.
 *   add/get/remove one column statistic based on both existing column name.
 *
 * - add/get_by_column_name/remove_by_column_name:
 *      based on existing table id and column name.
 */
TEST_P(ApiTestColumnStatisticsAllAPIHappy, get_remove_api_by_column_name) {
  auto param = GetParam();

  UTTableMetadata* testdata_table_metadata =
      global->testdata_table_metadata.get();
  std::string table_name = testdata_table_metadata->name + std::get<0>(param);
  std::vector<ptree> column_statistics = std::get<1>(param);

  ErrorCode error = ErrorCode::UNKNOWN;

  // add table metadata.
  ObjectIdType ret_table_id;
  ApiTestTableMetadata::add_table(table_name, &ret_table_id);

  auto tables = std::make_unique<Tables>(GlobalTestEnvironment::TEST_DB);
  error = tables->init();
  EXPECT_EQ(ErrorCode::OK, error);

  // get table metadata.
  ptree table_metadata;
  error = tables->get(ret_table_id, table_metadata);
  EXPECT_EQ(ErrorCode::OK, error);

  // get column metadata.
  auto o_columns = table_metadata.get_child_optional(Tables::COLUMNS_NODE);
  if (!o_columns) {
    GTEST_FAIL();
  }
  EXPECT_EQ(column_statistics.size(), o_columns.value().size());

  // get column metadata
  std::vector<std::string> column_names = {};
  for (auto node : o_columns.value()) {
    ptree column_metadata = node.second;
    // column metadata name
    auto column_name =
        column_metadata.get_optional<std::string>(Tables::Column::NAME);
    EXPECT_TRUE(column_name);

    column_names.push_back(column_name.get());
  }

  auto stats = std::make_unique<Statistics>(GlobalTestEnvironment::TEST_DB);
  error = stats->init();
  EXPECT_EQ(ErrorCode::OK, error);

  /**
   * add(by column name)
   * based on both existing table id and column name.
   */
  UTUtils::print("-- add column statistics by add (by column name) start --");
  for (std::size_t index = 0; index < column_names.size(); index++) {
    std::string column_name = column_names.at(index);

    boost::property_tree::ptree statistic;
    // name
    std::string statistic_name =
        "ApiTestColumnStatistics_" + column_name + "_" + std::to_string(index);
    statistic.put(Statistics::NAME, statistic_name);
    // table_id
    statistic.put(Statistics::TABLE_ID, ret_table_id);
    // column_name
    statistic.put(Statistics::COLUMN_NAME, column_name);
    // column_statistic
    statistic.add_child(Statistics::COLUMN_STATISTIC, column_statistics[index]);

    error = stats->add(statistic);
    EXPECT_EQ(ErrorCode::OK, error);
  }
  UTUtils::print("-- add column statistics by add (by column name) end --\n");

  /**
   * get_by_column_name
   * based on both existing table id and column name.
   */
  UTUtils::print("-- get column statistics by get_by_column_name start --");
  for (std::string column_name : column_names) {
    ptree cs_returned;

    error = stats->get_by_column_name(ret_table_id, column_name, cs_returned);
    EXPECT_EQ(ErrorCode::OK, error);

    // column metadata ordinal position
    auto optional_ordinal_position = cs_returned.get_optional<std::int64_t>(
        Tables::Column::ORDINAL_POSITION);
    EXPECT_TRUE(optional_ordinal_position);

    // column metadata column statistic
    auto optional_column_statistic =
        cs_returned.get_child_optional(Statistics::COLUMN_STATISTIC);
    EXPECT_TRUE(optional_column_statistic);

    std::string s_cs_returned =
        UTUtils::get_tree_string(optional_column_statistic.get());
    std::string s_cs_expected = UTUtils::get_tree_string(
        column_statistics[optional_ordinal_position.get() - 1]);

    UTUtils::print(" ordinal position: ", optional_ordinal_position.get());
    UTUtils::print(" column statistic: ", s_cs_returned);

    EXPECT_EQ(s_cs_returned, s_cs_expected);
  }
  UTUtils::print("-- get column statistics by get_by_column_name end --\n");

  /**
   * remove_by_column_name
   * based on both existing table id and column name.
   */
  UTUtils::print(
      "-- remove column statistics by remove_by_column_name start --");
  for (std::string column_name : column_names) {
    error = stats->remove_by_column_name(ret_table_id, column_name);
    EXPECT_EQ(ErrorCode::OK, error);

    error = stats->remove_by_column_name(ret_table_id, column_name);
    EXPECT_EQ(ErrorCode::NAME_NOT_FOUND, error);

    ptree cs_returned;
    error = stats->get_by_column_name(ret_table_id, column_name, cs_returned);
    EXPECT_EQ(ErrorCode::NAME_NOT_FOUND, error);
  }
  UTUtils::print(
      "-- remove column statistics by remove_by_column_name end --\n");

  // remove table metadata.
  ApiTestTableMetadata::remove_table(ret_table_id);
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

  UTUtils::print("-- get column statistics by get_by_column_number start --");

  for (ObjectIdType ordinal_position = 1;
       static_cast<size_t>(ordinal_position) <= column_statistics.size();
       ordinal_position++) {
    ptree cs_returned;

    error = stats->get_by_column_number(ret_table_id, ordinal_position,
                                        cs_returned);
    EXPECT_EQ(ErrorCode::OK, error);

    auto optional_column_statistic =
        cs_returned.get_child_optional(Statistics::COLUMN_STATISTIC);
    EXPECT_TRUE(optional_column_statistic);

    std::string s_cs_returned =
        UTUtils::get_tree_string(optional_column_statistic.get());
    std::string s_cs_expected =
        UTUtils::get_tree_string(column_statistics[ordinal_position - 1]);

    EXPECT_EQ(s_cs_returned, s_cs_expected);

    auto optional_ordinal_position =
        cs_returned.get_optional<std::int64_t>(Statistics::ORDINAL_POSITION);

    UTUtils::print(" ordinal position: ", optional_ordinal_position.get());
    UTUtils::print(" column statistic: " + s_cs_returned);
  }

  UTUtils::print("-- get column statistics by get_by_column_number end -- \n");

  std::vector<ptree> vector_cs_returned;
  error = stats->get_all(ret_table_id, vector_cs_returned);
  EXPECT_EQ(ErrorCode::OK, error);
  EXPECT_EQ(column_statistics.size(), vector_cs_returned.size());

  UTUtils::print("-- get column statistics by get_all start --");

  for (ObjectIdType ordinal_position = 1;
       static_cast<size_t>(ordinal_position) <= vector_cs_returned.size();
       ordinal_position++) {
    ptree c_cs_returned = vector_cs_returned[ordinal_position - 1];

    auto optional_column_statistic =
        c_cs_returned.get_child_optional(Statistics::COLUMN_STATISTIC);
    EXPECT_TRUE(optional_column_statistic);

    std::string s_cs_returned =
        UTUtils::get_tree_string(optional_column_statistic.get());
    std::string s_cs_expected =
        UTUtils::get_tree_string(column_statistics[ordinal_position - 1]);

    EXPECT_EQ(s_cs_expected, s_cs_returned);

    auto optional_ordinal_position =
        c_cs_returned.get_optional<std::int64_t>(Statistics::ORDINAL_POSITION);

    UTUtils::print(" ordinal position: ", optional_ordinal_position.get());
    UTUtils::print(" column statistic: " + s_cs_returned);
  }

  UTUtils::print("-- get column statistics by get_all end -- \n");

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
      "-- After updating all column statistics, get column statistics by ",
      "get_by_column_number start --");

  for (ObjectIdType ordinal_position = 1;
       static_cast<size_t>(ordinal_position) <=
       column_statistics_to_update.size();
       ordinal_position++) {
    ptree cs_returned;

    error = stats->get_by_column_number(ret_table_id, ordinal_position,
                                        cs_returned);
    EXPECT_EQ(ErrorCode::OK, error);

    auto optional_column_statistic =
        cs_returned.get_child_optional(Statistics::COLUMN_STATISTIC);
    EXPECT_TRUE(optional_column_statistic);

    std::string s_cs_returned =
        UTUtils::get_tree_string(optional_column_statistic.get());
    std::string s_cs_expected = UTUtils::get_tree_string(
        column_statistics_to_update[ordinal_position - 1]);

    EXPECT_EQ(s_cs_returned, s_cs_expected);

    auto optional_ordinal_position =
        cs_returned.get_optional<std::int64_t>(Statistics::ORDINAL_POSITION);

    UTUtils::print(" ordinal position: ", optional_ordinal_position.get());
    UTUtils::print(" column statistic: ", s_cs_returned);
  }

  UTUtils::print(
      "-- After updating all column statistics, get column statistics by ",
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

    auto optional_column_statistic =
        c_cs_returned.get_child_optional(Statistics::COLUMN_STATISTIC);
    EXPECT_TRUE(optional_column_statistic);

    std::string s_cs_returned =
        UTUtils::get_tree_string(optional_column_statistic.get());
    std::string s_cs_expected = UTUtils::get_tree_string(
        column_statistics_to_update[ordinal_position - 1]);

    EXPECT_EQ(s_cs_expected, s_cs_returned);

    auto optional_ordinal_position =
        c_cs_returned.get_optional<std::int64_t>(Statistics::ORDINAL_POSITION);

    UTUtils::print(" ordinal position: ", optional_ordinal_position.get());
    UTUtils::print(" column statistic: ", s_cs_returned);
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
      "-- After removing ordinal position=", ordinal_position_to_remove,
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

      auto optional_column_statistic =
          cs_returned.get_child_optional(Statistics::COLUMN_STATISTIC);
      EXPECT_TRUE(optional_column_statistic);

      std::string s_cs_returned =
          UTUtils::get_tree_string(optional_column_statistic.get());
      std::string s_cs_expected = UTUtils::get_tree_string(
          column_statistics_to_update[ordinal_position - 1]);

      EXPECT_EQ(s_cs_returned, s_cs_expected);

      auto optional_ordinal_position =
          cs_returned.get_optional<std::int64_t>(Statistics::ORDINAL_POSITION);

      UTUtils::print(" ordinal position: ", optional_ordinal_position.get());
      UTUtils::print(" column statistic: ", s_cs_returned);
    }
  }

  UTUtils::print(
      "-- After removing ordinal position=", ordinal_position_to_remove,
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
      "-- After removing ordinal position=", ordinal_position_to_remove,
      " get column statistics by get_all start --");

  int ordinal_position = 1;
  for (ptree statistic : vector_cs_removed_returned) {
    auto optional_column_statistic =
        statistic.get_child_optional(Statistics::COLUMN_STATISTIC);
    EXPECT_TRUE(optional_column_statistic);

    auto optional_ordinal_position =
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

    UTUtils::print(" ordinal position: ", optional_ordinal_position.get());
    UTUtils::print(" column statistic: ", s_cs_returned);
  }

  UTUtils::print(
      "-- After removing ordinal position=", ordinal_position_to_remove,
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

  // remove table metadata.
  ApiTestTableMetadata::remove_table(ret_table_id);
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

  UTUtils::print("-- get column statistics by get_by_column_number start --");

  for (ObjectIdType ordinal_position = 1;
       static_cast<size_t>(ordinal_position) <= column_statistics.size();
       ordinal_position++) {
    ptree cs_returned;

    error = stats->get_by_column_number(ret_table_id, ordinal_position,
                                        cs_returned);
    EXPECT_EQ(ErrorCode::OK, error);

    auto optional_column_statistic =
        cs_returned.get_child_optional(Statistics::COLUMN_STATISTIC);
    EXPECT_TRUE(optional_column_statistic);

    std::string s_cs_returned =
        UTUtils::get_tree_string(optional_column_statistic.get());
    std::string s_cs_expected =
        UTUtils::get_tree_string(column_statistics[ordinal_position - 1]);

    EXPECT_EQ(s_cs_returned, s_cs_expected);

    auto optional_ordinal_position =
        cs_returned.get_optional<std::int64_t>(Statistics::ORDINAL_POSITION);

    UTUtils::print(" ordinal position: ", optional_ordinal_position.get());
    UTUtils::print(" column statistic: ", s_cs_returned);
  }

  UTUtils::print("-- get column statistics by get_by_column_number end -- \n");

  std::vector<ptree> vector_cs_returned;
  error = stats->get_all(ret_table_id, vector_cs_returned);
  EXPECT_EQ(ErrorCode::OK, error);
  EXPECT_EQ(column_statistics.size(), vector_cs_returned.size());

  UTUtils::print("-- get column statistics by get_all start --");

  for (ObjectIdType ordinal_position = 1;
       static_cast<size_t>(ordinal_position) <= vector_cs_returned.size();
       ordinal_position++) {
    ptree c_cs_returned = vector_cs_returned[ordinal_position - 1];

    auto optional_column_statistic =
        c_cs_returned.get_child_optional(Statistics::COLUMN_STATISTIC);
    EXPECT_TRUE(optional_column_statistic);

    std::string s_cs_returned =
        UTUtils::get_tree_string(optional_column_statistic.get());
    std::string s_cs_expected =
        UTUtils::get_tree_string(column_statistics[ordinal_position - 1]);

    EXPECT_EQ(s_cs_expected, s_cs_returned);

    auto optional_ordinal_position =
        c_cs_returned.get_optional<std::int64_t>(Statistics::ORDINAL_POSITION);

    UTUtils::print(" ordinal position: ", optional_ordinal_position.get());
    UTUtils::print(" column statistic: ", s_cs_returned);
  }

  UTUtils::print("-- get column statistics by get_all end -- \n");

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

  // remove table metadata.
  ApiTestTableMetadata::remove_table(ret_table_id);
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
TEST_P(ApiTestColumnStatisticsAllAPIException, all_api_exception) {
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

    auto optional_column_statistic =
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

  // remove table metadata.
  ApiTestTableMetadata::remove_table(ret_table_id);
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
       all_api_happy_without_init) {
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

  UTUtils::print("-- add column statistics by add start --");
  UTUtils::print(" id:", ret_table_id);

  ErrorCode error;
  std::vector<ptree> column_statistics = std::get<1>(param);
  for (std::int64_t ordinal_position = 1;
       static_cast<std::size_t>(ordinal_position) <= column_statistics.size();
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

    UTUtils::print(" ordinal position: ", ordinal_position);
    UTUtils::print(
        " column statistics:",
        UTUtils::get_tree_string(column_statistics[ordinal_position - 1]));
  }

  UTUtils::print("-- add column statistics by add end -- \n");

  /**
   * get_by_column_number without init()
   * based on both existing table id and column ordinal position.
   */
  auto stats_get_one_cs =
      std::make_unique<Statistics>(GlobalTestEnvironment::TEST_DB);

  UTUtils::print("-- get column statistics by get_by_column_number start --");

  for (ObjectIdType ordinal_position = 1;
       static_cast<size_t>(ordinal_position) <= column_statistics.size();
       ordinal_position++) {
    ptree cs_returned;

    error = stats_get_one_cs->get_by_column_number(
        ret_table_id, ordinal_position, cs_returned);
    EXPECT_EQ(ErrorCode::OK, error);

    auto optional_column_statistic =
        cs_returned.get_child_optional(Statistics::COLUMN_STATISTIC);
    EXPECT_TRUE(optional_column_statistic);

    std::string s_cs_returned =
        UTUtils::get_tree_string(optional_column_statistic.get());
    std::string s_cs_expected =
        UTUtils::get_tree_string(column_statistics[ordinal_position - 1]);

    EXPECT_EQ(s_cs_returned, s_cs_expected);

    auto optional_ordinal_position =
        cs_returned.get_optional<std::int64_t>(Statistics::ORDINAL_POSITION);

    UTUtils::print(" ordinal position: ", optional_ordinal_position.get());
    UTUtils::print(" column statistic: ", s_cs_returned);
  }

  UTUtils::print("-- get column statistics by get_by_column_number end -- \n");

  /**
   * get_all without init()
   * based on existing table id.
   */
  auto stats_get_all_cs =
      std::make_unique<Statistics>(GlobalTestEnvironment::TEST_DB);

  std::vector<ptree> vector_cs_returned;
  error = stats_get_all_cs->get_all(ret_table_id, vector_cs_returned);
  EXPECT_EQ(ErrorCode::OK, error);

  UTUtils::print("-- get column statistics by get_all start --");

  for (ObjectIdType ordinal_position = 1;
       static_cast<size_t>(ordinal_position) <= vector_cs_returned.size();
       ordinal_position++) {
    ptree c_cs_returned = vector_cs_returned[ordinal_position - 1];

    auto optional_column_statistic =
        c_cs_returned.get_child_optional(Statistics::COLUMN_STATISTIC);
    EXPECT_TRUE(optional_column_statistic);

    std::string s_cs_returned =
        UTUtils::get_tree_string(optional_column_statistic.get());
    std::string s_cs_expected =
        UTUtils::get_tree_string(column_statistics[ordinal_position - 1]);

    EXPECT_EQ(s_cs_expected, s_cs_returned);

    auto optional_ordinal_position =
        c_cs_returned.get_optional<std::int64_t>(Statistics::ORDINAL_POSITION);

    UTUtils::print(" ordinal position: ", optional_ordinal_position.get());
    UTUtils::print(" column statistic: ", s_cs_returned);
  }

  UTUtils::print("-- get column statistics by get_all end -- \n");

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
      "-- After removing ordinal position=", ordinal_position_to_remove,
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

      auto optional_column_statistic =
          cs_returned.get_child_optional(Statistics::COLUMN_STATISTIC);
      EXPECT_TRUE(optional_column_statistic);

      std::string s_cs_returned =
          UTUtils::get_tree_string(optional_column_statistic.get());
      std::string s_cs_expected =
          UTUtils::get_tree_string(column_statistics[ordinal_position - 1]);

      EXPECT_EQ(s_cs_returned, s_cs_expected);

      auto optional_ordinal_position =
          cs_returned.get_optional<std::int64_t>(Statistics::ORDINAL_POSITION);

      UTUtils::print(" ordinal position: ", optional_ordinal_position.get());
      UTUtils::print(" column statistic: ", s_cs_returned);
    }
  }

  UTUtils::print(
      "-- After removing ordinal position=", ordinal_position_to_remove,
      " get column statistics by get_by_column_number end -- \n");

  std::vector<ptree> vector_cs_removed_returned;
  error =
      stats_remove_one_cs->get_all(ret_table_id, vector_cs_removed_returned);
  EXPECT_EQ(ErrorCode::OK, error);
  EXPECT_EQ(column_statistics.size() - 1, vector_cs_removed_returned.size());

  UTUtils::print(
      "-- After removing ordinal position=", ordinal_position_to_remove,
      " get column statistics by get_all start --");

  int ordinal_position = 1;
  for (ptree statistic : vector_cs_removed_returned) {
    auto optional_column_statistic =
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

    auto optional_ordinal_position =
        statistic.get_optional<std::int64_t>(Statistics::ORDINAL_POSITION);
    EXPECT_NE(ordinal_position_to_remove, optional_ordinal_position.get());

    UTUtils::print(" ordinal position: ", optional_ordinal_position.get());
    UTUtils::print(" column statistic: ", s_cs_returned);
  }

  UTUtils::print(
      "-- After removing ordinal position=", ordinal_position_to_remove,
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

  // remove table metadata.
  ApiTestTableMetadata::remove_table(ret_table_id);
}

}  // namespace manager::metadata::testing
