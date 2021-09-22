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
#include "test/api_test_table_statistics.h"

#include <gtest/gtest.h>
#include <cmath>
#include <limits>
#include <memory>
#include <string>
#include <vector>

#include "manager/metadata/tables.h"
#include "test/api_test_table_metadata.h"
#include "test/global_test_environment.h"
#include "test/utility/ut_table_metadata.h"
#include "test/utility/ut_utils.h"

using namespace manager::metadata;
using namespace boost::property_tree;

namespace manager::metadata::testing {

class ApiTestTableStatisticsByTableIdException
    : public ::testing::TestWithParam<ObjectIdType> {
  void SetUp() override { UTUtils::skip_if_connection_not_opened(); }
};
class ApiTestTableStatisticsByTableNameException
    : public ::testing::TestWithParam<std::string> {
  void SetUp() override { UTUtils::skip_if_connection_not_opened(); }
};

class ApiTestTableStatisticsByTableIdHappy
    : public ::testing::TestWithParam<TupleApiTestTableStatistics> {
  void SetUp() override { UTUtils::skip_if_connection_not_opened(); }
};
class ApiTestTableStatisticsByTableNameHappy
    : public ::testing::TestWithParam<TupleApiTestTableStatistics> {
  void SetUp() override { UTUtils::skip_if_connection_not_opened(); }
};

INSTANTIATE_TEST_CASE_P(
    ParamtererizedTest, ApiTestTableStatisticsByTableIdException,
    ::testing::Values(-1, 0, INT64_MAX - 1, INT64_MAX,
                      std::numeric_limits<ObjectIdType>::infinity(),
                      -std::numeric_limits<ObjectIdType>::infinity(),
                      std::numeric_limits<ObjectIdType>::quiet_NaN()));

INSTANTIATE_TEST_CASE_P(ParamtererizedTest,
                        ApiTestTableStatisticsByTableNameException,
                        ::testing::Values("table_name_not_exists", ""));

std::vector<float> reltuples_list = {-1,
                                     0,
                                     1,
                                     100000000,
                                     FLT_MAX,
                                     std::numeric_limits<float>::infinity(),
                                     -std::numeric_limits<float>::infinity(),
                                     std::numeric_limits<float>::quiet_NaN(),
                                     static_cast<float>(DBL_MAX),
                                     static_cast<float>(DBL_MIN)};

std::vector<TupleApiTestTableStatistics>
ApiTestTableStatistics::make_tuple_table_statistics(
    const std::string& test_number) {
  std::vector<TupleApiTestTableStatistics> v;
  int next;
  for (int i = 0; static_cast<size_t>(i) < reltuples_list.size(); i++) {
    next = (i + 1) % reltuples_list.size();
    std::string table_name =
        "_TableStatistic_" + test_number + "_" + std::to_string(i);
    v.emplace_back(table_name, reltuples_list[i], reltuples_list[next]);
  }
  return v;
}

INSTANTIATE_TEST_CASE_P(
    ParamtererizedTest, ApiTestTableStatisticsByTableIdHappy,
    ::testing::ValuesIn(
        ApiTestTableStatistics::make_tuple_table_statistics("1")));

INSTANTIATE_TEST_CASE_P(
    ParamtererizedTest, ApiTestTableStatisticsByTableNameHappy,
    ::testing::ValuesIn(
        ApiTestTableStatistics::make_tuple_table_statistics("2")));

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

  UTUtils::print_table_statistics(table_stats);
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
  ApiTestTableMetadata::add_table(table_name, &ret_table_id);

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

  boost::optional<float> optional_tuples_add =
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

  boost::optional<ObjectIdType> add_metadata_id =
      table_stats_added.get_optional<ObjectIdType>(Tables::ID);
  boost::optional<std::string> add_metadata_name =
      table_stats_added.get_optional<std::string>(Tables::NAME);
  boost::optional<std::string> add_metadata_namespace =
      table_stats_added.get_optional<std::string>(Tables::NAMESPACE);
  boost::optional<float> add_metadata_tuples =
      table_stats_added.get_optional<float>(Tables::TUPLES);

  // verifies that the returned table statistic is expected one.
  EXPECT_EQ(ret_table_id, add_metadata_id.get());
  EXPECT_EQ(table_name, add_metadata_name.get());
  EXPECT_EQ(testdata_table_metadata->namespace_name,
            add_metadata_namespace.get());
  if (add_metadata_tuples) {
    if (std::isnan(add_metadata_tuples.get())) {
      EXPECT_TRUE(std::isnan(add_metadata_tuples.get()));
    } else {
      EXPECT_FLOAT_EQ(reltuples_to_add, add_metadata_tuples.get());
    }
  }
  
  UTUtils::print_table_statistics(table_stats_added);

  // update the number of rows.
  float reltuples_to_update = std::get<2>(param);
  table_statistic.put(Tables::TUPLES, reltuples_to_update);

  error = tables->set_statistic(table_statistic);

  boost::optional<float> optional_tuples_upd =
      table_statistic.get_optional<float>(Tables::TUPLES);
  if (optional_tuples_upd) {
    EXPECT_EQ(ErrorCode::OK, error);
  } else {
    EXPECT_EQ(ErrorCode::INVALID_PARAMETER, error);
  }

  ptree table_stats_updated;
  error = tables->get_statistic(ret_table_id, table_stats_updated);
  EXPECT_EQ(ErrorCode::OK, error);

  boost::optional<ObjectIdType> upd_metadata_id =
      table_stats_updated.get_optional<ObjectIdType>(Tables::ID);
  boost::optional<std::string> upd_metadata_name =
      table_stats_updated.get_optional<std::string>(Tables::NAME);
  boost::optional<std::string> upd_metadata_namespace =
      table_stats_updated.get_optional<std::string>(Tables::NAMESPACE);
  boost::optional<float> upd_metadata_tuples =
      table_stats_updated.get_optional<float>(Tables::TUPLES);

  // verifies that the returned table statistic is expected one.
  EXPECT_EQ(ret_table_id, upd_metadata_id.get());
  EXPECT_EQ(table_name, upd_metadata_name.get());
  EXPECT_EQ(testdata_table_metadata->namespace_name,
            upd_metadata_namespace.get());
  if (upd_metadata_tuples) {
    if (std::isnan(upd_metadata_tuples.get())) {
      EXPECT_TRUE(std::isnan(upd_metadata_tuples.get()));
    } else {
      EXPECT_FLOAT_EQ(reltuples_to_update, upd_metadata_tuples.get());
    }
  }

  UTUtils::print_table_statistics(table_stats_updated);
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
  ApiTestTableMetadata::add_table(table_name, &ret_table_id);

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

  boost::optional<float> optional_tuples_add =
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

  boost::optional<ObjectIdType> add_metadata_id =
      table_stats_added.get_optional<ObjectIdType>(Tables::ID);
  boost::optional<std::string> add_metadata_name =
      table_stats_added.get_optional<std::string>(Tables::NAME);
  boost::optional<std::string> add_metadata_namespace =
      table_stats_added.get_optional<std::string>(Tables::NAMESPACE);
  boost::optional<float> add_metadata_tuples =
      table_stats_added.get_optional<float>(Tables::TUPLES);

  // verifies that the returned table statistic is expected one.
  EXPECT_EQ(ret_table_id, add_metadata_id.get());
  EXPECT_EQ(table_name, add_metadata_name.get());
  EXPECT_EQ(testdata_table_metadata->namespace_name,
            add_metadata_namespace.get());
  if (add_metadata_tuples) {
    if (std::isnan(add_metadata_tuples.get())) {
      EXPECT_TRUE(std::isnan(add_metadata_tuples.get()));
    } else {
      EXPECT_FLOAT_EQ(reltuples_to_add, add_metadata_tuples.get());
    }
  }

  UTUtils::print_table_statistics(table_stats_added);

  // update the number of rows.
  float reltuples_to_update = std::get<2>(param);
  table_statistic.put(Tables::TUPLES, reltuples_to_update);

  error = tables->set_statistic(table_statistic);

  boost::optional<float> optional_tuples_upd =
      table_statistic.get_optional<float>(Tables::TUPLES);
  if (optional_tuples_upd) {
    EXPECT_EQ(ErrorCode::OK, error);
  } else {
    EXPECT_EQ(ErrorCode::INVALID_PARAMETER, error);
  }

  // get table statistic.
  ptree table_stats_updated;
  error = tables->get_statistic(table_name, table_stats_updated);

  boost::optional<ObjectIdType> upd_metadata_id =
      table_stats_updated.get_optional<ObjectIdType>(Tables::ID);
  boost::optional<std::string> upd_metadata_name =
      table_stats_updated.get_optional<std::string>(Tables::NAME);
  boost::optional<std::string> upd_metadata_namespace =
      table_stats_updated.get_optional<std::string>(Tables::NAMESPACE);
  boost::optional<float> upd_metadata_tuples =
      table_stats_updated.get_optional<float>(Tables::TUPLES);

  // verifies that the returned table statistic is expected one.
  EXPECT_EQ(ret_table_id, upd_metadata_id.get());
  EXPECT_EQ(table_name, upd_metadata_name.get());
  EXPECT_EQ(testdata_table_metadata->namespace_name,
            upd_metadata_namespace.get());
  if (upd_metadata_tuples) {
    if (std::isnan(upd_metadata_tuples.get())) {
      EXPECT_TRUE(std::isnan(upd_metadata_tuples.get()));
    } else {
      EXPECT_FLOAT_EQ(reltuples_to_update, upd_metadata_tuples.get());
    }
  }

  UTUtils::print_table_statistics(table_stats_updated);
}

}  // namespace manager::metadata::testing
