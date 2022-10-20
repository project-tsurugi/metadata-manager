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
#include <gtest/gtest.h>

#include <memory>
#include <string>
#include <string_view>
#include <vector>

#include "manager/metadata/statistics.h"
#include "manager/metadata/tables.h"
#include "test/environment/global_test_environment.h"
#include "test/common/ut_utils.h"
#include "test/helper/column_statistics_helper.h"
#include "test/helper/table_metadata_helper.h"
#include "test/metadata/ut_table_metadata.h"

namespace manager::metadata::testing {

using boost::property_tree::ptree;

class ApiTestColumnStatisticsAllAPIHappy
    : public ::testing::TestWithParam<
          ColumnStatisticsHelper::BasicTestParameter> {
  void SetUp() override { UTUtils::skip_if_connection_not_opened(); }
};
class ApiTestColumnStatisticsUpdateHappy
    : public ::testing::TestWithParam<
          ColumnStatisticsHelper::UpdateTestParameter> {
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
    : public ::testing::TestWithParam<
          ColumnStatisticsHelper::BasicTestParameter> {
  void SetUp() override { UTUtils::skip_if_connection_not_opened(); }
};

INSTANTIATE_TEST_CASE_P(
    ParameterizedTest, ApiTestColumnStatisticsAllAPIHappy,
    ::testing::ValuesIn(
        ColumnStatisticsHelper::make_test_patterns_for_basic_tests("1")));
INSTANTIATE_TEST_CASE_P(
    ParameterizedTest, ApiTestColumnStatisticsUpdateHappy,
    ::testing::ValuesIn(
        ColumnStatisticsHelper::make_test_patterns_for_update_tests("2")));
INSTANTIATE_TEST_CASE_P(ParameterizedTest,
                        ApiTestColumnStatisticsRemoveAllHappy,
                        ::testing::Values("_ColumnStatistic_3"));
INSTANTIATE_TEST_CASE_P(ParameterizedTest,
                        ApiTestColumnStatisticsAllAPIException,
                        ::testing::Values("_ColumnStatistic_4"));
INSTANTIATE_TEST_CASE_P(
    ParameterizedTest, ApiTestColumnStatisticsAllAPIHappyWithoutInit,
    ::testing::ValuesIn(
        ColumnStatisticsHelper::make_test_patterns_for_basic_tests("5")));

/**
 * @brief happy test for add/get_all/remove API.
 *
 * - add:
 *     based on existing table id and column number.
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
  TableMetadataHelper::add_table(table_name_1, &ret_table_id_1);
  // add table metadata.
  std::string table_name_2 = table_name_base + "2";
  ObjectIdType ret_table_id_2;
  TableMetadataHelper::add_table(table_name_2, &ret_table_id_2);

  /**
   * add
   * based on both existing table id and column number.
   */
  std::vector<ptree> column_statistics = std::get<1>(param);
  ColumnStatisticsHelper::add_column_statistics(ret_table_id_1,
                                                column_statistics);
  ColumnStatisticsHelper::add_column_statistics(ret_table_id_2,
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

  for (std::size_t column_number = 0; column_number < ret_statistics.size();
       column_number++) {
    auto optional_column_statistic =
        ret_statistics[column_number].get_child_optional(
            Statistics::COLUMN_STATISTIC);
    EXPECT_TRUE(optional_column_statistic);

    std::string s_statistics_expected =
        UTUtils::get_tree_string(column_statistics[column_number]);
    std::string s_statistics_actual =
        UTUtils::get_tree_string(optional_column_statistic.get());
    EXPECT_EQ(s_statistics_expected, s_statistics_actual);

    auto optional_column_number =
        ret_statistics[column_number].get_optional<std::int64_t>(
            Statistics::COLUMN_NUMBER);

    UTUtils::print(" column number: ", optional_column_number.get());
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

  for (ObjectIdType column_number = 1;
       static_cast<size_t>(column_number) <= column_statistics.size();
       column_number++) {
    ptree cs_returned;
    error =
        stats->get_by_column_number(ret_table_id_1, column_number, cs_returned);
    EXPECT_EQ(ErrorCode::ID_NOT_FOUND, error);
  }
  UTUtils::print("-- remove column statistics by remove_by_table_id end --\n");

  // remove table metadata.
  TableMetadataHelper::remove_table(ret_table_id_1);
  TableMetadataHelper::remove_table(ret_table_id_2);
}

/**
 * @brief happy test for add/get_all/remove API.
 *
 * - add:
 *     based on existing table id and column number.
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
  TableMetadataHelper::add_table(table_name_1, &ret_table_id_1);
  // add table metadata.
  std::string table_name_2 = table_name_base + "2";
  ObjectIdType ret_table_id_2;
  TableMetadataHelper::add_table(table_name_2, &ret_table_id_2);

  /**
   * add
   * based on both existing table id and column number.
   */
  std::vector<ptree> column_statistics = std::get<1>(param);
  ColumnStatisticsHelper::add_column_statistics(ret_table_id_1,
                                                column_statistics);
  ColumnStatisticsHelper::add_column_statistics(ret_table_id_2,
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

    auto optional_column_number =
        ret_statistics[index].get_optional<std::int64_t>(
            Statistics::COLUMN_NUMBER);

    UTUtils::print(" column number: ", optional_column_number.get());
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
  TableMetadataHelper::remove_table(ret_table_id_1);
  TableMetadataHelper::remove_table(ret_table_id_2);
}

/**
 * @brief happy test for add/get/remove API by statistic id.
 *   add/get/remove one column statistic based on both existing statistic id.
 *
 * - add:
 *      based on existing column number.
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
  TableMetadataHelper::add_table(table_name, &ret_table_id);

  auto stats = std::make_unique<Statistics>(GlobalTestEnvironment::TEST_DB);
  error      = stats->init();
  EXPECT_EQ(ErrorCode::OK, error);

  /**
   * add(by column number)
   * based on both existing table id and column number.
   */
  UTUtils::print("-- add column statistics by add (by column number) start --");
  std::vector<ObjectIdType> statistic_ids = {};
  for (std::size_t index = 0; index < column_statistics.size(); index++) {
    boost::property_tree::ptree statistic;
    // name
    std::string statistic_name =
        "ApiTestColumnStatistics_" + std::to_string(index);
    statistic.put(Statistics::NAME, statistic_name);
    // table_id
    statistic.put(Statistics::TABLE_ID, ret_table_id);
    // column_number
    statistic.put(Statistics::COLUMN_NUMBER, (index + 1));
    // column_statistic
    statistic.add_child(Statistics::COLUMN_STATISTIC, column_statistics[index]);

    ObjectIdType statistic_id;
    error = stats->add(statistic, &statistic_id);
    EXPECT_EQ(ErrorCode::OK, error);

    statistic_ids.push_back(statistic_id);
  }
  UTUtils::print("-- add column statistics by add (by column number) end --\n");

  /**
   * get
   * based on both existing statistic id.
   */
  UTUtils::print("-- get column statistics by get (by statistic id) start --");
  for (ObjectIdType statistic_id : statistic_ids) {
    ptree cs_returned;

    error = stats->get(statistic_id, cs_returned);
    EXPECT_EQ(ErrorCode::OK, error);

    UTUtils::print("@@@@@", UTUtils::get_tree_string(cs_returned));

    // column metadata column number
    auto optional_column_number =
        cs_returned.get_optional<std::int64_t>(Column::COLUMN_NUMBER);
    EXPECT_TRUE(optional_column_number);

    // column metadata column statistic
    auto optional_column_statistic =
        cs_returned.get_child_optional(Statistics::COLUMN_STATISTIC);
    EXPECT_TRUE(optional_column_statistic);

    std::string s_cs_returned =
        UTUtils::get_tree_string(optional_column_statistic.get());
    std::string s_cs_expected = UTUtils::get_tree_string(
        column_statistics[optional_column_number.get() - 1]);

    UTUtils::print(" column number: ", optional_column_number.get());
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
  TableMetadataHelper::remove_table(ret_table_id);
}

/**
 * @brief happy test for add/get/remove API by statistic name.
 *   add/get/remove one column statistic based on both existing statistic name.
 *
 * - add:
 *      based on existing column number.
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
  TableMetadataHelper::add_table(table_name, &ret_table_id);

  auto stats = std::make_unique<Statistics>(GlobalTestEnvironment::TEST_DB);
  error      = stats->init();
  EXPECT_EQ(ErrorCode::OK, error);

  /**
   * add(by column number)
   * based on both existing table id and column number.
   */
  UTUtils::print("-- add column statistics by add (by column number) start --");
  std::vector<ObjectIdType> statistic_ids  = {};
  std::vector<std::string> statistic_names = {};
  std::string statistic_name_prefix =
      "ApiTestColumnStatistics-" + std::to_string(time(NULL)) + "-";

  for (std::size_t index = 0; index < column_statistics.size(); index++) {
    boost::property_tree::ptree statistic;
    // name
    std::string statistic_name = statistic_name_prefix + std::to_string(index);
    statistic.put(Statistics::NAME, statistic_name);
    // table_id
    statistic.put(Statistics::TABLE_ID, ret_table_id);
    // column_number
    statistic.put(Statistics::COLUMN_NUMBER, (index + 1));
    // column_statistic
    statistic.add_child(Statistics::COLUMN_STATISTIC, column_statistics[index]);

    ObjectIdType statistic_id;
    error = stats->add(statistic, &statistic_id);
    EXPECT_EQ(ErrorCode::OK, error);

    statistic_ids.push_back(statistic_id);
    statistic_names.push_back(statistic_name);
  }
  UTUtils::print("-- add column statistics by add (by column number) end --\n");

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

    // column metadata column number
    auto optional_column_number =
        cs_returned.get_optional<std::int64_t>(Column::COLUMN_NUMBER);
    EXPECT_TRUE(optional_column_number);

    // column metadata column statistic
    auto optional_column_statistic =
        cs_returned.get_child_optional(Statistics::COLUMN_STATISTIC);
    EXPECT_TRUE(optional_column_statistic);

    std::string s_cs_returned =
        UTUtils::get_tree_string(optional_column_statistic.get());
    std::string s_cs_expected = UTUtils::get_tree_string(
        column_statistics[optional_column_number.get() - 1]);

    UTUtils::print(" column number: ", optional_column_number.get());
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
  TableMetadataHelper::remove_table(ret_table_id);
}

/**
 * @brief happy test for add/get/remove API by column id.
 *   add/get/remove one column statistic based on both existing column id.
 *
 * - add/get_by_column_id/remove_by_column_id:
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
  TableMetadataHelper::add_table(table_name, &ret_table_id);

  auto tables = std::make_unique<Tables>(GlobalTestEnvironment::TEST_DB);
  error       = tables->init();
  EXPECT_EQ(ErrorCode::OK, error);

  // get table metadata.
  ptree table_metadata;
  error = tables->get(ret_table_id, table_metadata);
  EXPECT_EQ(ErrorCode::OK, error);

  // get column metadata.
  auto o_columns = table_metadata.get_child_optional(Table::COLUMNS_NODE);
  if (!o_columns) {
    GTEST_FAIL();
  }
  EXPECT_EQ(column_statistics.size(), o_columns.value().size());

  // get column metadata
  std::vector<ObjectIdType> column_ids = {};
  for (auto node : o_columns.value()) {
    ptree column_metadata = node.second;
    // column metadata id
    auto column_id = column_metadata.get_optional<ObjectIdType>(Column::ID);
    EXPECT_TRUE(column_id);

    column_ids.push_back(column_id.get());
  }

  auto stats = std::make_unique<Statistics>(GlobalTestEnvironment::TEST_DB);
  error      = stats->init();
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

    // column metadata column number
    auto optional_column_number =
        cs_returned.get_optional<std::int64_t>(Column::COLUMN_NUMBER);
    EXPECT_TRUE(optional_column_number);

    // column metadata column statistic
    auto optional_column_statistic =
        cs_returned.get_child_optional(Statistics::COLUMN_STATISTIC);
    EXPECT_TRUE(optional_column_statistic);

    std::string s_cs_returned =
        UTUtils::get_tree_string(optional_column_statistic.get());
    std::string s_cs_expected = UTUtils::get_tree_string(
        column_statistics[optional_column_number.get() - 1]);

    UTUtils::print(" column number: ", optional_column_number.get());
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
  TableMetadataHelper::remove_table(ret_table_id);
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
  TableMetadataHelper::add_table(table_name, &ret_table_id);

  auto stats = std::make_unique<Statistics>(GlobalTestEnvironment::TEST_DB);
  error      = stats->init();
  EXPECT_EQ(ErrorCode::OK, error);

  /**
   * add(by column number)
   * based on both existing table id and column number.
   */
  UTUtils::print("-- add column statistics by add (by column number) start --");
  for (std::size_t index = 0; index < column_statistics.size(); index++) {
    boost::property_tree::ptree statistic;
    // name
    std::string statistic_name =
        "ApiTestColumnStatistics_" + std::to_string(index);
    statistic.put(Statistics::NAME, statistic_name);
    // table_id
    statistic.put(Statistics::TABLE_ID, ret_table_id);
    // column_number
    statistic.put(Statistics::COLUMN_NUMBER, (index + 1));
    // column_statistic
    statistic.add_child(Statistics::COLUMN_STATISTIC, column_statistics[index]);

    error = stats->add(statistic);
    EXPECT_EQ(ErrorCode::OK, error);
  }
  UTUtils::print("-- add column statistics by add (by column number) end --\n");

  /**
   * get_by_column_number
   * based on both existing table id and column number.
   */
  UTUtils::print("-- get column statistics by get_by_column_number start --");
  for (std::int64_t column_number = 1;
       static_cast<std::size_t>(column_number) <= column_statistics.size();
       column_number++) {
    ptree cs_returned;

    error =
        stats->get_by_column_number(ret_table_id, column_number, cs_returned);
    EXPECT_EQ(ErrorCode::OK, error);

    // column metadata column statistic
    auto optional_column_statistic =
        cs_returned.get_child_optional(Statistics::COLUMN_STATISTIC);
    EXPECT_TRUE(optional_column_statistic);

    std::string s_cs_returned =
        UTUtils::get_tree_string(optional_column_statistic.get());
    std::string s_cs_expected =
        UTUtils::get_tree_string(column_statistics[column_number - 1]);

    UTUtils::print(" column number: ", column_number);
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
  for (std::size_t column_number = 1; column_number <= column_statistics.size();
       column_number++) {
    error = stats->remove_by_column_number(ret_table_id, column_number);
    EXPECT_EQ(ErrorCode::OK, error);

    error = stats->remove_by_column_number(ret_table_id, column_number);
    EXPECT_EQ(ErrorCode::ID_NOT_FOUND, error);

    ptree cs_returned;
    error =
        stats->get_by_column_number(ret_table_id, column_number, cs_returned);
    EXPECT_EQ(ErrorCode::ID_NOT_FOUND, error);
  }
  UTUtils::print(
      "-- remove column statistics by remove_by_column_number end --\n");

  // remove table metadata.
  TableMetadataHelper::remove_table(ret_table_id);
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
  TableMetadataHelper::add_table(table_name, &ret_table_id);

  auto tables = std::make_unique<Tables>(GlobalTestEnvironment::TEST_DB);
  error       = tables->init();
  EXPECT_EQ(ErrorCode::OK, error);

  // get table metadata.
  ptree table_metadata;
  error = tables->get(ret_table_id, table_metadata);
  EXPECT_EQ(ErrorCode::OK, error);

  // get column metadata.
  auto o_columns = table_metadata.get_child_optional(Table::COLUMNS_NODE);
  if (!o_columns) {
    GTEST_FAIL();
  }
  EXPECT_EQ(column_statistics.size(), o_columns.value().size());

  // get column metadata
  std::vector<std::string> column_names = {};
  for (auto node : o_columns.value()) {
    ptree column_metadata = node.second;
    // column metadata name
    auto column_name = column_metadata.get_optional<std::string>(Column::NAME);
    EXPECT_TRUE(column_name);

    column_names.push_back(column_name.get());
  }

  auto stats = std::make_unique<Statistics>(GlobalTestEnvironment::TEST_DB);
  error      = stats->init();
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

    // column metadata column number
    auto optional_column_number =
        cs_returned.get_optional<std::int64_t>(Column::COLUMN_NUMBER);
    EXPECT_TRUE(optional_column_number);

    // column metadata column statistic
    auto optional_column_statistic =
        cs_returned.get_child_optional(Statistics::COLUMN_STATISTIC);
    EXPECT_TRUE(optional_column_statistic);

    std::string s_cs_returned =
        UTUtils::get_tree_string(optional_column_statistic.get());
    std::string s_cs_expected = UTUtils::get_tree_string(
        column_statistics[optional_column_number.get() - 1]);

    UTUtils::print(" column number: ", optional_column_number.get());
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
  TableMetadataHelper::remove_table(ret_table_id);
}

/**
 * @brief happy test to update column statistics
 * based on both existing table id and column number.
 *
 * - add:
 *      update column statistics
 *      based on both existing table id and column number.
 */
TEST_P(ApiTestColumnStatisticsUpdateHappy, update_column_statistics) {
  auto param = GetParam();

  UTTableMetadata* testdata_table_metadata =
      global->testdata_table_metadata.get();
  std::string table_name = testdata_table_metadata->name + std::get<0>(param);

  ObjectIdType ret_table_id;
  TableMetadataHelper::add_table(table_name, &ret_table_id);

  /**
   * add new column statistics
   * based on both existing table id and column number.
   */
  std::vector<ptree> column_statistics = std::get<1>(param);
  ColumnStatisticsHelper::add_column_statistics(ret_table_id,
                                                column_statistics);

  auto stats = std::make_unique<Statistics>(GlobalTestEnvironment::TEST_DB);

  ErrorCode error = stats->init();
  EXPECT_EQ(ErrorCode::OK, error);

  /**
   * check if results of column statistics are expected or not.
   */

  UTUtils::print("-- get column statistics by get_by_column_number start --");

  for (ObjectIdType column_number = 1;
       static_cast<size_t>(column_number) <= column_statistics.size();
       column_number++) {
    ptree cs_returned;

    error =
        stats->get_by_column_number(ret_table_id, column_number, cs_returned);
    EXPECT_EQ(ErrorCode::OK, error);

    auto optional_column_statistic =
        cs_returned.get_child_optional(Statistics::COLUMN_STATISTIC);
    EXPECT_TRUE(optional_column_statistic);

    std::string s_cs_returned =
        UTUtils::get_tree_string(optional_column_statistic.get());
    std::string s_cs_expected =
        UTUtils::get_tree_string(column_statistics[column_number - 1]);

    EXPECT_EQ(s_cs_returned, s_cs_expected);

    auto optional_column_number =
        cs_returned.get_optional<std::int64_t>(Statistics::COLUMN_NUMBER);

    UTUtils::print(" column number: ", optional_column_number.get());
    UTUtils::print(" column statistic: " + s_cs_returned);
  }

  UTUtils::print("-- get column statistics by get_by_column_number end -- \n");

  std::vector<ptree> vector_cs_returned;
  error = stats->get_all(ret_table_id, vector_cs_returned);
  EXPECT_EQ(ErrorCode::OK, error);
  EXPECT_EQ(column_statistics.size(), vector_cs_returned.size());

  UTUtils::print("-- get column statistics by get_all start --");

  for (ObjectIdType column_number = 1;
       static_cast<size_t>(column_number) <= vector_cs_returned.size();
       column_number++) {
    ptree c_cs_returned = vector_cs_returned[column_number - 1];

    auto optional_column_statistic =
        c_cs_returned.get_child_optional(Statistics::COLUMN_STATISTIC);
    EXPECT_TRUE(optional_column_statistic);

    std::string s_cs_returned =
        UTUtils::get_tree_string(optional_column_statistic.get());
    std::string s_cs_expected =
        UTUtils::get_tree_string(column_statistics[column_number - 1]);

    EXPECT_EQ(s_cs_expected, s_cs_returned);

    auto optional_column_number =
        c_cs_returned.get_optional<std::int64_t>(Statistics::COLUMN_NUMBER);

    UTUtils::print(" column number: ", optional_column_number.get());
    UTUtils::print(" column statistic: " + s_cs_returned);
  }

  UTUtils::print("-- get column statistics by get_all end -- \n");

  /**
   * update column statistics
   * based on both existing table id and column number.
   */
  std::vector<ptree> column_statistics_to_update = std::get<2>(param);
  ColumnStatisticsHelper::add_column_statistics(ret_table_id,
                                                column_statistics_to_update);

  /**
   * check if results of column statistics are expected or not.
   */

  UTUtils::print(
      "-- After updating all column statistics, get column statistics by ",
      "get_by_column_number start --");

  for (ObjectIdType column_number = 1;
       static_cast<size_t>(column_number) <= column_statistics_to_update.size();
       column_number++) {
    ptree cs_returned;

    error =
        stats->get_by_column_number(ret_table_id, column_number, cs_returned);
    EXPECT_EQ(ErrorCode::OK, error);

    auto optional_column_statistic =
        cs_returned.get_child_optional(Statistics::COLUMN_STATISTIC);
    EXPECT_TRUE(optional_column_statistic);

    std::string s_cs_returned =
        UTUtils::get_tree_string(optional_column_statistic.get());
    std::string s_cs_expected = UTUtils::get_tree_string(
        column_statistics_to_update[column_number - 1]);

    EXPECT_EQ(s_cs_returned, s_cs_expected);

    auto optional_column_number =
        cs_returned.get_optional<std::int64_t>(Statistics::COLUMN_NUMBER);

    UTUtils::print(" column number: ", optional_column_number.get());
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

  for (ObjectIdType column_number = 1;
       static_cast<size_t>(column_number) <= vector_cs_updated_returned.size();
       column_number++) {
    ptree c_cs_returned = vector_cs_updated_returned[column_number - 1];

    auto optional_column_statistic =
        c_cs_returned.get_child_optional(Statistics::COLUMN_STATISTIC);
    EXPECT_TRUE(optional_column_statistic);

    std::string s_cs_returned =
        UTUtils::get_tree_string(optional_column_statistic.get());
    std::string s_cs_expected = UTUtils::get_tree_string(
        column_statistics_to_update[column_number - 1]);

    EXPECT_EQ(s_cs_expected, s_cs_returned);

    auto optional_column_number =
        c_cs_returned.get_optional<std::int64_t>(Statistics::COLUMN_NUMBER);

    UTUtils::print(" column number: ", optional_column_number.get());
    UTUtils::print(" column statistic: ", s_cs_returned);
  }

  UTUtils::print(
      "-- After updating all column statistics, get column statistics by ",
      "get_all end -- \n");

  /**
   * remove_by_column_number
   * based on both existing table id and column number.
   */
  ObjectIdType column_number_to_remove = std::get<3>(param);
  error = stats->remove_by_column_number(ret_table_id, column_number_to_remove);
  EXPECT_EQ(ErrorCode::OK, error);

  UTUtils::print("-- After removing column number=", column_number_to_remove,
                 " get column statistics by get_by_column_number start --");

  for (ObjectIdType column_number = 1;
       static_cast<size_t>(column_number) <= column_statistics_to_update.size();
       column_number++) {
    ptree cs_returned;

    error =
        stats->get_by_column_number(ret_table_id, column_number, cs_returned);

    if (column_number_to_remove == column_number) {
      EXPECT_EQ(ErrorCode::ID_NOT_FOUND, error);
    } else {
      EXPECT_EQ(ErrorCode::OK, error);

      auto optional_column_statistic =
          cs_returned.get_child_optional(Statistics::COLUMN_STATISTIC);
      EXPECT_TRUE(optional_column_statistic);

      std::string s_cs_returned =
          UTUtils::get_tree_string(optional_column_statistic.get());
      std::string s_cs_expected = UTUtils::get_tree_string(
          column_statistics_to_update[column_number - 1]);

      EXPECT_EQ(s_cs_returned, s_cs_expected);

      auto optional_column_number =
          cs_returned.get_optional<std::int64_t>(Statistics::COLUMN_NUMBER);

      UTUtils::print(" column number: ", optional_column_number.get());
      UTUtils::print(" column statistic: ", s_cs_returned);
    }
  }

  UTUtils::print("-- After removing column number=", column_number_to_remove,
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

  UTUtils::print("-- After removing column number=", column_number_to_remove,
                 " get column statistics by get_all start --");

  int column_number = 1;
  for (ptree statistic : vector_cs_removed_returned) {
    auto optional_column_statistic =
        statistic.get_child_optional(Statistics::COLUMN_STATISTIC);
    EXPECT_TRUE(optional_column_statistic);

    auto optional_column_number =
        statistic.get_optional<std::int64_t>(Statistics::COLUMN_NUMBER);
    EXPECT_NE(column_number_to_remove, optional_column_number.get());

    if (column_number_to_remove == column_number) {
      column_number++;
    }
    std::string s_cs_returned =
        UTUtils::get_tree_string(optional_column_statistic.get());
    std::string s_cs_expected = UTUtils::get_tree_string(
        column_statistics_to_update[column_number - 1]);
    column_number++;

    EXPECT_EQ(s_cs_expected, s_cs_returned);

    UTUtils::print(" column number: ", optional_column_number.get());
    UTUtils::print(" column statistic: ", s_cs_returned);
  }

  UTUtils::print("-- After removing column number=", column_number_to_remove,
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

  for (ObjectIdType column_number = 1;
       static_cast<size_t>(column_number) <= column_statistics_to_update.size();
       column_number++) {
    ptree cs_returned;

    error =
        stats->get_by_column_number(ret_table_id, column_number, cs_returned);
    EXPECT_EQ(ErrorCode::ID_NOT_FOUND, error);
  }

  // remove table metadata.
  TableMetadataHelper::remove_table(ret_table_id);
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
  TableMetadataHelper::add_table(table_name, &ret_table_id);

  /**
   * add new column statistics
   * based on both existing table id and column number.
   */
  std::vector<ptree> column_statistics = global->column_statistics;
  ColumnStatisticsHelper::add_column_statistics(ret_table_id,
                                                column_statistics);

  auto stats = std::make_unique<Statistics>(GlobalTestEnvironment::TEST_DB);

  ErrorCode error = stats->init();
  EXPECT_EQ(ErrorCode::OK, error);

  /**
   * check if results of column statistics are expected or not.
   */

  UTUtils::print("-- get column statistics by get_by_column_number start --");

  for (ObjectIdType column_number = 1;
       static_cast<size_t>(column_number) <= column_statistics.size();
       column_number++) {
    ptree cs_returned;

    error =
        stats->get_by_column_number(ret_table_id, column_number, cs_returned);
    EXPECT_EQ(ErrorCode::OK, error);

    auto optional_column_statistic =
        cs_returned.get_child_optional(Statistics::COLUMN_STATISTIC);
    EXPECT_TRUE(optional_column_statistic);

    std::string s_cs_returned =
        UTUtils::get_tree_string(optional_column_statistic.get());
    std::string s_cs_expected =
        UTUtils::get_tree_string(column_statistics[column_number - 1]);

    EXPECT_EQ(s_cs_returned, s_cs_expected);

    auto optional_column_number =
        cs_returned.get_optional<std::int64_t>(Statistics::COLUMN_NUMBER);

    UTUtils::print(" column number: ", optional_column_number.get());
    UTUtils::print(" column statistic: ", s_cs_returned);
  }

  UTUtils::print("-- get column statistics by get_by_column_number end -- \n");

  std::vector<ptree> vector_cs_returned;
  error = stats->get_all(ret_table_id, vector_cs_returned);
  EXPECT_EQ(ErrorCode::OK, error);
  EXPECT_EQ(column_statistics.size(), vector_cs_returned.size());

  UTUtils::print("-- get column statistics by get_all start --");

  for (ObjectIdType column_number = 1;
       static_cast<size_t>(column_number) <= vector_cs_returned.size();
       column_number++) {
    ptree c_cs_returned = vector_cs_returned[column_number - 1];

    auto optional_column_statistic =
        c_cs_returned.get_child_optional(Statistics::COLUMN_STATISTIC);
    EXPECT_TRUE(optional_column_statistic);

    std::string s_cs_returned =
        UTUtils::get_tree_string(optional_column_statistic.get());
    std::string s_cs_expected =
        UTUtils::get_tree_string(column_statistics[column_number - 1]);

    EXPECT_EQ(s_cs_expected, s_cs_returned);

    auto optional_column_number =
        c_cs_returned.get_optional<std::int64_t>(Statistics::COLUMN_NUMBER);

    UTUtils::print(" column number: ", optional_column_number.get());
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

  for (ObjectIdType column_number = 1;
       static_cast<size_t>(column_number) <= column_statistics.size();
       column_number++) {
    ptree cs_returned;

    error =
        stats->get_by_column_number(ret_table_id, column_number, cs_returned);
    EXPECT_EQ(ErrorCode::ID_NOT_FOUND, error);
  }

  // remove table metadata.
  TableMetadataHelper::remove_table(ret_table_id);
}

/**
 * @brief Exception path test for all API.
 * 1. add/get/remove one column statistic
 * based on non-existing table id or
 * non-existing column number.
 *
 * 2. get/remove all column statistics
 * based on non-existing table id.
 *
 * -
 * add/get_by_column_number/remove_by_column_number:
 *      - based on non-existing column number
 *                 and existing table id.
 *      - based on non-existing table id
 *                 and existing column number.
 *      - based on both non-existing table id and column number.
 * -  get_all/remove_by_table_id:
 *      - based on non-existing table id.
 */
TEST_P(ApiTestColumnStatisticsAllAPIException, all_api_exception) {
  auto param = GetParam();
  UTTableMetadata* testdata_table_metadata =
      global->testdata_table_metadata.get();
  std::string table_name = testdata_table_metadata->name + param;

  ObjectIdType ret_table_id;
  TableMetadataHelper::add_table(table_name, &ret_table_id);

  std::vector<ptree> column_statistics = global->column_statistics;
  ColumnStatisticsHelper::add_column_statistics(ret_table_id,
                                                column_statistics);

  auto stats = std::make_unique<Statistics>(GlobalTestEnvironment::TEST_DB);

  ErrorCode error = stats->init();
  EXPECT_EQ(ErrorCode::OK, error);

  for (ObjectIdType column_number = 1;
       static_cast<size_t>(column_number) <= column_statistics.size();
       column_number++) {
    ptree cs_returned;

    error =
        stats->get_by_column_number(ret_table_id, column_number, cs_returned);
    EXPECT_EQ(ErrorCode::OK, error);

    auto optional_column_statistic =
        cs_returned.get_child_optional(Statistics::COLUMN_STATISTIC);
    EXPECT_TRUE(optional_column_statistic);

    std::string s_cs_returned =
        UTUtils::get_tree_string(optional_column_statistic.get());
    std::string s_cs_expected =
        UTUtils::get_tree_string(column_statistics[column_number - 1]);

    EXPECT_EQ(s_cs_returned, s_cs_expected);
  }

  /**
   * add
   * based on non-existing column number
   * or non-existing table id.
   */
  for (ObjectIdType column_number : global->column_number_not_exists) {
    // column number only not exists
    {
      ptree statistic;
      // name
      std::string statistic_name = "ApiTestColumnStatisticsAllAPIException_" +
                                   std::to_string(ret_table_id) + "-" +
                                   std::to_string(column_number);
      statistic.put(Statistics::NAME, statistic_name);
      // table_id
      statistic.put(Statistics::TABLE_ID, ret_table_id);
      // column_number
      statistic.put(Statistics::COLUMN_NUMBER, column_number);
      // column_statistic
      statistic.add_child(Statistics::COLUMN_STATISTIC, column_statistics[0]);

      error = stats->add(statistic);
      EXPECT_EQ(ErrorCode::INVALID_PARAMETER, error);
    }

    // table id and column number not exists
    for (ObjectIdType table_id : global->table_id_not_exists) {
      ptree statistic;
      // name
      std::string statistic_name = "ApiTestColumnStatisticsAllAPIException_" +
                                   std::to_string(table_id) + "-" +
                                   std::to_string(column_number);
      statistic.put(Statistics::NAME, statistic_name);
      // table_id
      statistic.put(Statistics::TABLE_ID, table_id);
      // column_number
      statistic.put(Statistics::COLUMN_NUMBER, column_number);
      // column_statistic
      statistic.add_child(Statistics::COLUMN_STATISTIC, column_statistics[0]);

      error = stats->add(statistic);
      EXPECT_EQ(ErrorCode::INVALID_PARAMETER, error);
    }
  }

  // table id only not exists
  ObjectIdType column_number_exists = 1;
  for (ObjectIdType table_id : global->table_id_not_exists) {
    ptree statistic;
    // name
    std::string statistic_name = "ApiTestColumnStatisticsAllAPIException_" +
                                 std::to_string(table_id) + "-" +
                                 std::to_string(column_number_exists);
    statistic.put(Statistics::NAME, statistic_name);
    // table_id
    statistic.put(Statistics::TABLE_ID, table_id);
    // column_number
    statistic.put(Statistics::COLUMN_NUMBER, column_number_exists);
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
   * based on non-existing column number
   * or non-existing table id.
   */
  ptree cs_returned;
  for (std::int64_t column_number : global->column_number_not_exists) {
    // column number only not exists
    error =
        stats->get_by_column_number(ret_table_id, column_number, cs_returned);
    // if (column_number >= INT64_MAX) {
    //   EXPECT_EQ(ErrorCode::INVALID_PARAMETER, error);
    // } else {
    EXPECT_EQ(ErrorCode::ID_NOT_FOUND, error);
    // }

    for (ObjectIdType table_id : global->table_id_not_exists) {
      // table id and column number not exists
      error = stats->get_by_column_number(table_id, column_number, cs_returned);
      EXPECT_EQ(ErrorCode::ID_NOT_FOUND, error);
    }
  }

  for (ObjectIdType table_id : global->table_id_not_exists) {
    // table id only not exists
    error = stats->get_by_column_number(table_id, column_number_exists,
                                        cs_returned);
    EXPECT_EQ(ErrorCode::ID_NOT_FOUND, error);
  }

  /**
   * remove_by_column_number
   * based on non-existing column number
   * or non-existing table id.
   */
  for (ObjectIdType column_number : global->column_number_not_exists) {
    // column number only not exists
    error = stats->remove_by_column_number(ret_table_id, column_number);
    EXPECT_EQ(ErrorCode::ID_NOT_FOUND, error);

    for (ObjectIdType table_id : global->table_id_not_exists) {
      // table id and column number not exists
      error = stats->remove_by_column_number(table_id, column_number);
      EXPECT_EQ(ErrorCode::ID_NOT_FOUND, error);
    }
  }

  for (ObjectIdType table_id : global->table_id_not_exists) {
    // table id only not exists
    error = stats->remove_by_column_number(table_id, column_number_exists);
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
  TableMetadataHelper::remove_table(ret_table_id);
}

/**
 * @brief happy test for all API without init().
 * 1. add/get/remove one column statistic without init()
 * based on both existing table id and column number.
 *
 * 2. get/remove all column statistics without init()
 * based on existing table id.
 *
 * -
 * add/get_by_column_number/remove_by_column_number
 * : based on both existing table id and column number.
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
  TableMetadataHelper::add_table(table_name, &ret_table_id);

  /**
   * add without init()
   * based on both existing table id and column number.
   */
  auto stats_add = std::make_unique<Statistics>(GlobalTestEnvironment::TEST_DB);

  UTUtils::print("-- add column statistics by add start --");
  UTUtils::print(" id:", ret_table_id);

  ErrorCode error;
  std::vector<ptree> column_statistics = std::get<1>(param);
  for (std::int64_t column_number = 1;
       static_cast<std::size_t>(column_number) <= column_statistics.size();
       column_number++) {
    ptree statistic;
    // name
    std::string statistic_name = "ApiTestColumnStatisticsAllAPIException_" +
                                 std::to_string(ret_table_id) + "-" +
                                 std::to_string(column_number);
    statistic.put(Statistics::NAME, statistic_name);
    // table_id
    statistic.put(Statistics::TABLE_ID, ret_table_id);
    // column_number
    statistic.put(Statistics::COLUMN_NUMBER, column_number);
    // column_statistic
    statistic.add_child(Statistics::COLUMN_STATISTIC,
                        column_statistics[column_number - 1]);

    error = stats_add->add(statistic);

    EXPECT_EQ(ErrorCode::OK, error);

    UTUtils::print(" column number: ", column_number);
    UTUtils::print(
        " column statistics:",
        UTUtils::get_tree_string(column_statistics[column_number - 1]));
  }

  UTUtils::print("-- add column statistics by add end -- \n");

  /**
   * get_by_column_number without init()
   * based on both existing table id and column number.
   */
  auto stats_get_one_cs =
      std::make_unique<Statistics>(GlobalTestEnvironment::TEST_DB);

  UTUtils::print("-- get column statistics by get_by_column_number start --");

  for (ObjectIdType column_number = 1;
       static_cast<size_t>(column_number) <= column_statistics.size();
       column_number++) {
    ptree cs_returned;

    error = stats_get_one_cs->get_by_column_number(ret_table_id, column_number,
                                                   cs_returned);
    EXPECT_EQ(ErrorCode::OK, error);

    auto optional_column_statistic =
        cs_returned.get_child_optional(Statistics::COLUMN_STATISTIC);
    EXPECT_TRUE(optional_column_statistic);

    std::string s_cs_returned =
        UTUtils::get_tree_string(optional_column_statistic.get());
    std::string s_cs_expected =
        UTUtils::get_tree_string(column_statistics[column_number - 1]);

    EXPECT_EQ(s_cs_returned, s_cs_expected);

    auto optional_column_number =
        cs_returned.get_optional<std::int64_t>(Statistics::COLUMN_NUMBER);

    UTUtils::print(" column number: ", optional_column_number.get());
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

  for (ObjectIdType column_number = 1;
       static_cast<size_t>(column_number) <= vector_cs_returned.size();
       column_number++) {
    ptree c_cs_returned = vector_cs_returned[column_number - 1];

    auto optional_column_statistic =
        c_cs_returned.get_child_optional(Statistics::COLUMN_STATISTIC);
    EXPECT_TRUE(optional_column_statistic);

    std::string s_cs_returned =
        UTUtils::get_tree_string(optional_column_statistic.get());
    std::string s_cs_expected =
        UTUtils::get_tree_string(column_statistics[column_number - 1]);

    EXPECT_EQ(s_cs_expected, s_cs_returned);

    auto optional_column_number =
        c_cs_returned.get_optional<std::int64_t>(Statistics::COLUMN_NUMBER);

    UTUtils::print(" column number: ", optional_column_number.get());
    UTUtils::print(" column statistic: ", s_cs_returned);
  }

  UTUtils::print("-- get column statistics by get_all end -- \n");

  /**
   * remove_by_column_number without init()
   * based on both existing table id and column number.
   */
  auto stats_remove_one_cs =
      std::make_unique<Statistics>(GlobalTestEnvironment::TEST_DB);

  ObjectIdType column_number_to_remove = std::get<2>(param);
  error = stats_remove_one_cs->remove_by_column_number(ret_table_id,
                                                       column_number_to_remove);
  EXPECT_EQ(ErrorCode::OK, error);

  UTUtils::print("-- After removing column number=", column_number_to_remove,
                 " get column statistics by get_by_column_number start --");

  for (ObjectIdType column_number = 1;
       static_cast<size_t>(column_number) <= column_statistics.size();
       column_number++) {
    ptree cs_returned;

    error = stats_remove_one_cs->get_by_column_number(
        ret_table_id, column_number, cs_returned);

    if (column_number_to_remove == column_number) {
      EXPECT_EQ(ErrorCode::ID_NOT_FOUND, error);
    } else {
      EXPECT_EQ(ErrorCode::OK, error);

      auto optional_column_statistic =
          cs_returned.get_child_optional(Statistics::COLUMN_STATISTIC);
      EXPECT_TRUE(optional_column_statistic);

      std::string s_cs_returned =
          UTUtils::get_tree_string(optional_column_statistic.get());
      std::string s_cs_expected =
          UTUtils::get_tree_string(column_statistics[column_number - 1]);

      EXPECT_EQ(s_cs_returned, s_cs_expected);

      auto optional_column_number =
          cs_returned.get_optional<std::int64_t>(Statistics::COLUMN_NUMBER);

      UTUtils::print(" column number: ", optional_column_number.get());
      UTUtils::print(" column statistic: ", s_cs_returned);
    }
  }

  UTUtils::print("-- After removing column number=", column_number_to_remove,
                 " get column statistics by get_by_column_number end -- \n");

  std::vector<ptree> vector_cs_removed_returned;
  error =
      stats_remove_one_cs->get_all(ret_table_id, vector_cs_removed_returned);
  EXPECT_EQ(ErrorCode::OK, error);
  EXPECT_EQ(column_statistics.size() - 1, vector_cs_removed_returned.size());

  UTUtils::print("-- After removing column number=", column_number_to_remove,
                 " get column statistics by get_all start --");

  int column_number = 1;
  for (ptree statistic : vector_cs_removed_returned) {
    auto optional_column_statistic =
        statistic.get_child_optional(Statistics::COLUMN_STATISTIC);
    EXPECT_TRUE(optional_column_statistic);

    if (column_number_to_remove == column_number) {
      column_number++;
    }
    std::string s_cs_returned =
        UTUtils::get_tree_string(optional_column_statistic.get());
    std::string s_cs_expected =
        UTUtils::get_tree_string(column_statistics[column_number - 1]);
    column_number++;

    EXPECT_EQ(s_cs_expected, s_cs_returned);

    auto optional_column_number =
        statistic.get_optional<std::int64_t>(Statistics::COLUMN_NUMBER);
    EXPECT_NE(column_number_to_remove, optional_column_number.get());

    UTUtils::print(" column number: ", optional_column_number.get());
    UTUtils::print(" column statistic: ", s_cs_returned);
  }

  UTUtils::print("-- After removing column number=", column_number_to_remove,
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

  for (ObjectIdType column_number = 1;
       static_cast<size_t>(column_number) <= column_statistics.size();
       column_number++) {
    ptree cs_returned;

    error = stats_remove_all_cs->get_by_column_number(
        ret_table_id, column_number, cs_returned);
    EXPECT_EQ(ErrorCode::ID_NOT_FOUND, error);
  }

  // remove table metadata.
  TableMetadataHelper::remove_table(ret_table_id);
}

}  // namespace manager::metadata::testing
