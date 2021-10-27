/*
 * Copyright 2021 tsurugi project.
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

#include <boost/format.hpp>
#include "manager/metadata/statistics.h"
#include "test/global_test_environment.h"
#include "test/utility/ut_utils.h"

namespace manager::metadata::testing {

using boost::property_tree::ptree;

class ApiTestColumnStatistics : public ::testing::Test {};

/**
 * @brief Unsupported test in JSON version.
 */
TEST_F(ApiTestColumnStatistics, add) {
  ErrorCode error = ErrorCode::UNKNOWN;

  auto statistics =
      std::make_unique<Statistics>(GlobalTestEnvironment::TEST_DB);
  error = statistics->init();
  EXPECT_EQ(ErrorCode::NOT_SUPPORTED, error);

  ptree column_statistic;
  ObjectIdType object_id;

  // name
  column_statistic.put(Statistics::NAME, "ApiTestColumnStatistics");
  // table_id
  column_statistic.put(Statistics::TABLE_ID, 99999);
  // ordinal_position
  column_statistic.put(Statistics::ORDINAL_POSITION, 1);

  UTUtils::print("-- add column statistics --");
  error = statistics->add(column_statistic);
  EXPECT_EQ(ErrorCode::NOT_SUPPORTED, error);

  UTUtils::print("-- add column statistics with object id --");
  error = statistics->add(column_statistic, &object_id);
  EXPECT_EQ(ErrorCode::NOT_SUPPORTED, error);
}

/**
 * @brief Unsupported test in JSON version.
 */
TEST_F(ApiTestColumnStatistics, get) {
  ErrorCode error = ErrorCode::UNKNOWN;

  auto statistics =
      std::make_unique<Statistics>(GlobalTestEnvironment::TEST_DB);
  error = statistics->init();
  EXPECT_EQ(ErrorCode::NOT_SUPPORTED, error);

  ptree column_statistic;

  UTUtils::print("-- get column statistics by column statistics id --");
  error = statistics->get(99999, column_statistic);
  EXPECT_EQ(ErrorCode::NOT_SUPPORTED, error);

  UTUtils::print("-- get column statistics by column statistics name --");
  error = statistics->get("object_name", column_statistic);
  EXPECT_EQ(ErrorCode::NOT_SUPPORTED, error);

  UTUtils::print("-- get column statistics by column id --");
  error = statistics->get_by_column_id(99999, column_statistic);
  EXPECT_EQ(ErrorCode::NOT_SUPPORTED, error);

  UTUtils::print(
      "-- get column statistics by table id and ordinal position --");
  error = statistics->get_by_column_number(99999, 1, column_statistic);
  EXPECT_EQ(ErrorCode::NOT_SUPPORTED, error);

  UTUtils::print("-- get column statistics by table id and culumn name --");
  error =
      statistics->get_by_column_name(99999, "column_name", column_statistic);
  EXPECT_EQ(ErrorCode::NOT_SUPPORTED, error);

  UTUtils::print("-- all gets column statistics --");
  std::vector<boost::property_tree::ptree> container;
  error = statistics->get_all(container);
  EXPECT_EQ(ErrorCode::NOT_SUPPORTED, error);

  UTUtils::print("-- all gets column statistics by table id --");
  error = statistics->get_all(99999, container);
}

/**
 * @brief Unsupported test in JSON version.
 */
TEST_F(ApiTestColumnStatistics, remove) {
  ErrorCode error = ErrorCode::UNKNOWN;

  auto statistics =
      std::make_unique<Statistics>(GlobalTestEnvironment::TEST_DB);
  error = statistics->init();
  EXPECT_EQ(ErrorCode::NOT_SUPPORTED, error);

  ObjectIdType object_id;

  UTUtils::print("-- remove column statistics by statistics id --");
  error = statistics->remove(99999);
  EXPECT_EQ(ErrorCode::NOT_SUPPORTED, error);

  UTUtils::print("-- remove column statistics by statistics name --");
  error = statistics->remove("object_name", nullptr);
  EXPECT_EQ(ErrorCode::NOT_SUPPORTED, error);

  UTUtils::print(
      "-- remove column statistics by statistics name with object id --");
  error = statistics->remove("object_name", &object_id);
  EXPECT_EQ(ErrorCode::NOT_SUPPORTED, error);

  UTUtils::print("-- remove column statistics by table id --");
  error = statistics->remove_by_table_id(99999);
  EXPECT_EQ(ErrorCode::NOT_SUPPORTED, error);

  UTUtils::print("-- remove column statistics by column id --");
  error = statistics->remove_by_column_id(99999);
  EXPECT_EQ(ErrorCode::NOT_SUPPORTED, error);

  UTUtils::print(
      "-- remove column statistics by table id and ordinal position --");
  error = statistics->remove_by_column_number(99999, 1);
  EXPECT_EQ(ErrorCode::NOT_SUPPORTED, error);

  UTUtils::print("-- remove column statistics by table id and culumn name --");
  error = statistics->remove_by_column_name(99999, "column_name");
  EXPECT_EQ(ErrorCode::NOT_SUPPORTED, error);
}

}  // namespace manager::metadata::testing
