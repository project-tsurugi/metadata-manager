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

#include <cmath>
#include <limits>
#include <memory>
#include <string>
#include <vector>

#include "manager/metadata/tables.h"
#include "test/global_test_environment.h"
#include "test/helper/table_metadata_helper.h"
#include "test/helper/table_statistics_helper.h"
#include "test/utility/ut_table_metadata.h"
#include "test/utility/ut_utils.h"

namespace manager::metadata::testing {

using boost::property_tree::ptree;

class ApiTestTableStatisticsByTableIdException
    : public ::testing::TestWithParam<ObjectIdType> {
  void SetUp() override { UTUtils::skip_if_connection_not_opened(); }
};
class ApiTestTableStatisticsByTableNameException
    : public ::testing::TestWithParam<std::string> {
  void SetUp() override { UTUtils::skip_if_connection_not_opened(); }
};

class ApiTestTableStatisticsByTableIdHappy
    : public ::testing::TestWithParam<
          TableStatisticsHelper::BasicTestParameter> {
  void SetUp() override { UTUtils::skip_if_connection_not_opened(); }
};
class ApiTestTableStatisticsByTableNameHappy
    : public ::testing::TestWithParam<
          TableStatisticsHelper::BasicTestParameter> {
  void SetUp() override { UTUtils::skip_if_connection_not_opened(); }
};

INSTANTIATE_TEST_CASE_P(
    ParameterizedTest, ApiTestTableStatisticsByTableIdException,
    ::testing::Values(-1, 0, INT64_MAX - 1, INT64_MAX,
                      std::numeric_limits<ObjectIdType>::infinity(),
                      -std::numeric_limits<ObjectIdType>::infinity(),
                      std::numeric_limits<ObjectIdType>::quiet_NaN()));

INSTANTIATE_TEST_CASE_P(ParameterizedTest,
                        ApiTestTableStatisticsByTableNameException,
                        ::testing::Values("table_name_not_exists", ""));

INSTANTIATE_TEST_CASE_P(
    ParameterizedTest, ApiTestTableStatisticsByTableIdHappy,
    ::testing::ValuesIn(
        TableStatisticsHelper::make_test_patterns_for_basic_tests("1")));

INSTANTIATE_TEST_CASE_P(
    ParameterizedTest, ApiTestTableStatisticsByTableNameHappy,
    ::testing::ValuesIn(
        TableStatisticsHelper::make_test_patterns_for_basic_tests("2")));

/**
 * @brief Exception path test for add_table_statistic
 * based on non-existing table id.
 */
TEST_P(ApiTestTableStatisticsByTableIdException,
       add_table_statistics_by_non_existing_table_id) {
  auto tables = std::make_unique<Tables>(GlobalTestEnvironment::TEST_DB);

  ErrorCode error = tables->init();
  EXPECT_EQ(ErrorCode::OK, error);

  auto table_id_not_exists = GetParam();
  float reltuples = 1000;

  // set table metadata.
  ptree table_meta;
  table_meta.put(Tables::ID, table_id_not_exists);
  table_meta.put(Tables::TUPLES, reltuples);

  error = tables->set_statistic(table_meta);
  EXPECT_EQ(ErrorCode::ID_NOT_FOUND, error);
}

/**
 * @brief Exception path test for add_table_statistic
 * based on non-existing table name.
 */
TEST_P(ApiTestTableStatisticsByTableNameException,
       add_table_statistics_by_non_existing_table_name) {
  auto tables = std::make_unique<Tables>(GlobalTestEnvironment::TEST_DB);

  ErrorCode error = tables->init();
  EXPECT_EQ(ErrorCode::OK, error);

  std::string table_name_not_exists = GetParam();
  float reltuples = 1000;

  // set table statistic.
  ptree table_meta;
  table_meta.put(Tables::NAME, table_name_not_exists);
  table_meta.put(Tables::TUPLES, reltuples);

  error = tables->set_statistic(table_meta);
  EXPECT_EQ(ErrorCode::NAME_NOT_FOUND, error);
}

/**
 * @brief Exception path test for get_table_statistic
 * based on non-existing table id.
 */
TEST_P(ApiTestTableStatisticsByTableIdException,
       get_table_statistics_by_non_existing_table_id) {
  auto tables = std::make_unique<Tables>(GlobalTestEnvironment::TEST_DB);

  ErrorCode error = tables->init();
  EXPECT_EQ(ErrorCode::OK, error);

  ptree table_stats;
  auto table_id_not_exists = GetParam();

  error = tables->get_statistic(table_id_not_exists, table_stats);
  EXPECT_EQ(ErrorCode::ID_NOT_FOUND, error);

  TableMetadataHelper::print_table_statistics(table_stats);
}

/**
 * @brief Exception path test for get_table_statistic
 * based on non-existing table name.
 */
TEST_P(ApiTestTableStatisticsByTableNameException,
       get_table_statistics_by_non_existing_table_name) {
  auto tables = std::make_unique<Tables>(GlobalTestEnvironment::TEST_DB);

  ErrorCode error = tables->init();
  EXPECT_EQ(ErrorCode::OK, error);

  ptree table_stats;
  std::string table_name_not_exists = GetParam();

  error = tables->get_statistic(table_name_not_exists, table_stats);
  EXPECT_EQ(ErrorCode::NAME_NOT_FOUND, error);
}

/**
 * @brief happy test for add_table_statistic/get_table_statistic
 * based on existing table id.
 */
TEST_P(ApiTestTableStatisticsByTableIdHappy,
       add_and_get_table_statistics_by_table_id) {
  // prepare test data for adding table metadata.
  UTTableMetadata* testdata_table_metadata =
      global->testdata_table_metadata.get();
  auto param = GetParam();
  std::string table_name = testdata_table_metadata->name + std::get<0>(param);

  // add table metadata.
  ObjectIdType ret_table_id = -1;
  TableMetadataHelper::add_table(table_name, &ret_table_id);

  auto tables = std::make_unique<Tables>(GlobalTestEnvironment::TEST_DB);

  ErrorCode error = tables->init();
  EXPECT_EQ(ErrorCode::OK, error);

  // The number of rows is NULL in the table metadata table.
  // So, add the number of rows to the table metadata table.
  float reltuples_to_add = std::get<1>(param);

  ptree table_statistic;
  table_statistic.put(Tables::ID, ret_table_id);
  table_statistic.put(Tables::TUPLES, reltuples_to_add);

  error = tables->set_statistic(table_statistic);

  auto optional_tuples_add =
      table_statistic.get_optional<float>(Tables::TUPLES);
  if (optional_tuples_add) {
    EXPECT_EQ(ErrorCode::OK, error);
  } else {
    EXPECT_EQ(ErrorCode::INVALID_PARAMETER, error);
  }

  // get table statistic.
  ptree table_stats_added;
  error = tables->get_statistic(ret_table_id, table_stats_added);
  EXPECT_EQ(ErrorCode::OK, error);

  auto add_metadata_id =
      table_stats_added.get_optional<ObjectIdType>(Tables::ID);
  auto add_metadata_name =
      table_stats_added.get_optional<std::string>(Tables::NAME);
  auto add_metadata_namespace =
      table_stats_added.get_optional<std::string>(Tables::NAMESPACE);
  auto add_metadata_tuples =
      table_stats_added.get_optional<float>(Tables::TUPLES);

  // verifies that the returned table statistic is expected one.
  EXPECT_EQ(ret_table_id, add_metadata_id.get());
  EXPECT_EQ(table_name, add_metadata_name.get());
  EXPECT_EQ(testdata_table_metadata->namespace_name,
            add_metadata_namespace.get());
  if (!optional_tuples_add) {
    reltuples_to_add = 0;
  }
  EXPECT_FLOAT_EQ(reltuples_to_add, add_metadata_tuples.get());

  TableMetadataHelper::print_table_statistics(table_stats_added);

  // update the number of rows.
  float reltuples_to_update = std::get<2>(param);
  table_statistic.put(Tables::TUPLES, reltuples_to_update);

  error = tables->set_statistic(table_statistic);

  auto optional_tuples_upd =
      table_statistic.get_optional<float>(Tables::TUPLES);
  if (optional_tuples_upd) {
    EXPECT_EQ(ErrorCode::OK, error);
  } else {
    EXPECT_EQ(ErrorCode::INVALID_PARAMETER, error);
  }

  ptree table_stats_updated;
  error = tables->get_statistic(ret_table_id, table_stats_updated);
  EXPECT_EQ(ErrorCode::OK, error);

  auto upd_metadata_id =
      table_stats_updated.get_optional<ObjectIdType>(Tables::ID);
  auto upd_metadata_name =
      table_stats_updated.get_optional<std::string>(Tables::NAME);
  auto upd_metadata_namespace =
      table_stats_updated.get_optional<std::string>(Tables::NAMESPACE);
  auto upd_metadata_tuples =
      table_stats_updated.get_optional<float>(Tables::TUPLES);

  // verifies that the returned table statistic is expected one.
  EXPECT_EQ(ret_table_id, upd_metadata_id.get());
  EXPECT_EQ(table_name, upd_metadata_name.get());
  EXPECT_EQ(testdata_table_metadata->namespace_name,
            upd_metadata_namespace.get());
  if (optional_tuples_upd) {
    EXPECT_FLOAT_EQ(reltuples_to_update, upd_metadata_tuples.get());
  } else {
    EXPECT_FLOAT_EQ(reltuples_to_add, upd_metadata_tuples.get());
  }

  TableMetadataHelper::print_table_statistics(table_stats_updated);

  // remove table metadata by table name.
  error = tables->remove(ret_table_id);
  EXPECT_EQ(ErrorCode::OK, error);
}

/**
 * @brief happy test for add_table_statistic/get_table_statistic
 * based on existing table name.
 */
TEST_P(ApiTestTableStatisticsByTableNameHappy,
       add_and_get_table_statistics_by_table_name) {
  // prepare test data for adding table metadata.
  UTTableMetadata* testdata_table_metadata =
      global->testdata_table_metadata.get();
  auto param = GetParam();
  std::string table_name = testdata_table_metadata->name + std::get<0>(param);

  // add table metadata.
  ObjectIdType ret_table_id;
  TableMetadataHelper::add_table(table_name, &ret_table_id);

  // add table statistic.
  auto tables = std::make_unique<Tables>(GlobalTestEnvironment::TEST_DB);

  ErrorCode error = tables->init();
  EXPECT_EQ(ErrorCode::OK, error);

  // The number of rows is NULL in the table metadata table.
  // So, add the number of rows to the table metadata table.
  float reltuples_to_add = std::get<1>(param);

  ptree table_statistic;
  table_statistic.put(Tables::NAME, table_name);
  table_statistic.put(Tables::TUPLES, reltuples_to_add);

  error = tables->set_statistic(table_statistic);

  auto optional_tuples_add =
      table_statistic.get_optional<float>(Tables::TUPLES);
  if (optional_tuples_add) {
    EXPECT_EQ(ErrorCode::OK, error);
  } else {
    EXPECT_EQ(ErrorCode::INVALID_PARAMETER, error);
  }

  // get table statistic.
  ptree table_stats_added;
  error = tables->get_statistic(table_name, table_stats_added);
  EXPECT_EQ(ErrorCode::OK, error);

  auto add_metadata_id =
      table_stats_added.get_optional<ObjectIdType>(Tables::ID);
  auto add_metadata_name =
      table_stats_added.get_optional<std::string>(Tables::NAME);
  auto add_metadata_namespace =
      table_stats_added.get_optional<std::string>(Tables::NAMESPACE);
  auto add_metadata_tuples =
      table_stats_added.get_optional<float>(Tables::TUPLES);

  // verifies that the returned table statistic is expected one.
  EXPECT_EQ(ret_table_id, add_metadata_id.get());
  EXPECT_EQ(table_name, add_metadata_name.get());
  EXPECT_EQ(testdata_table_metadata->namespace_name,
            add_metadata_namespace.get());
  if (!optional_tuples_add) {
    reltuples_to_add = 0;
  }
  EXPECT_FLOAT_EQ(reltuples_to_add, add_metadata_tuples.get());

  TableMetadataHelper::print_table_statistics(table_stats_added);

  // update the number of rows.
  float reltuples_to_update = std::get<2>(param);
  table_statistic.put(Tables::TUPLES, reltuples_to_update);

  error = tables->set_statistic(table_statistic);

  auto optional_tuples_upd =
      table_statistic.get_optional<float>(Tables::TUPLES);
  if (optional_tuples_upd) {
    EXPECT_EQ(ErrorCode::OK, error);
  } else {
    EXPECT_EQ(ErrorCode::INVALID_PARAMETER, error);
  }

  // get table statistic.
  ptree table_stats_updated;
  error = tables->get_statistic(table_name, table_stats_updated);

  auto upd_metadata_id =
      table_stats_updated.get_optional<ObjectIdType>(Tables::ID);
  auto upd_metadata_name =
      table_stats_updated.get_optional<std::string>(Tables::NAME);
  auto upd_metadata_namespace =
      table_stats_updated.get_optional<std::string>(Tables::NAMESPACE);
  auto upd_metadata_tuples =
      table_stats_updated.get_optional<float>(Tables::TUPLES);

  // verifies that the returned table statistic is expected one.
  EXPECT_EQ(ret_table_id, upd_metadata_id.get());
  EXPECT_EQ(table_name, upd_metadata_name.get());
  EXPECT_EQ(testdata_table_metadata->namespace_name,
            upd_metadata_namespace.get());
  if (optional_tuples_upd) {
    EXPECT_FLOAT_EQ(reltuples_to_update, upd_metadata_tuples.get());
  } else {
    EXPECT_FLOAT_EQ(reltuples_to_add, upd_metadata_tuples.get());
  }

  TableMetadataHelper::print_table_statistics(table_stats_updated);

  // remove table metadata by table name.
  error = tables->remove(ret_table_id);
  EXPECT_EQ(ErrorCode::OK, error);
}

}  // namespace manager::metadata::testing
