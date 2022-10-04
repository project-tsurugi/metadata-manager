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

#include <limits>
#include <memory>
#include <string>
#include <tuple>
#include <vector>

#include <boost/property_tree/ptree.hpp>

#include "manager/metadata/datatypes.h"
#include "manager/metadata/roles.h"
#include "manager/metadata/statistics.h"
#include "manager/metadata/tables.h"
#include "test/global_test_environment.h"
#include "test/helper/column_statistics_helper.h"
#include "test/utility/ut_utils.h"

namespace manager::metadata::testing {

using boost::property_tree::ptree;

class ApiTestDBAccessFailure : public ::testing::Test {
  void SetUp() override { UTUtils::skip_if_connection_opened(); }
};

class ApiTestDBAccessFailureByTableId
    : public ::testing::TestWithParam<ObjectIdType> {
  void SetUp() override { UTUtils::skip_if_connection_opened(); }
};

class ApiTestDBAccessFailureByTableName
    : public ::testing::TestWithParam<std::string> {
  void SetUp() override { UTUtils::skip_if_connection_opened(); }
};

class ApiTestDBAccessFailureByTableIdReltuples
    : public ::testing::TestWithParam<std::tuple<ObjectIdType, float>> {
  void SetUp() override { UTUtils::skip_if_connection_opened(); }
};

class ApiTestDBAccessFailureByTableNameReltuples
    : public ::testing::TestWithParam<std::tuple<std::string, float>> {
  void SetUp() override { UTUtils::skip_if_connection_opened(); }
};

class ApiTestDBAccessFailureByTableIdOrdinalPosition
    : public ::testing::TestWithParam<std::tuple<ObjectIdType, ObjectIdType>> {
  void SetUp() override { UTUtils::skip_if_connection_opened(); }
};

class ApiTestDBAccessFailureByColumnStatistics
    : public ::testing::TestWithParam<
          std::tuple<ObjectIdType, ObjectIdType, ptree>> {
  void SetUp() override { UTUtils::skip_if_connection_opened(); }
};

std::vector<ObjectIdType> table_id_not_exists_dbaf = {
    -1,
    0,
    INT64_MAX - 1,
    INT64_MAX,
    std::numeric_limits<ObjectIdType>::infinity(),
    -std::numeric_limits<ObjectIdType>::infinity(),
    std::numeric_limits<ObjectIdType>::quiet_NaN()};

std::vector<ObjectIdType> ordinal_position_not_exists_dbaf = {
    -1,
    0,
    INT64_MAX - 1,
    INT64_MAX,
    4,
    std::numeric_limits<ObjectIdType>::infinity(),
    -std::numeric_limits<ObjectIdType>::infinity(),
    std::numeric_limits<ObjectIdType>::quiet_NaN()};

std::vector<float> reltuples_dbaf = {-1,
                                     0,
                                     1,
                                     100000000,
                                     FLT_MAX,
                                     std::numeric_limits<float>::infinity(),
                                     -std::numeric_limits<float>::infinity(),
                                     std::numeric_limits<float>::quiet_NaN(),
                                     static_cast<float>(DBL_MAX),
                                     static_cast<float>(DBL_MIN)};

ptree empty_column_stats_dbaf;
std::vector<ptree> ptree_dbaf = {
    empty_column_stats_dbaf,
    ColumnStatisticsHelper::generate_column_statistic()};
std::vector<std::string> table_name_dbaf = {"table_name_not_exists", ""};

INSTANTIATE_TEST_CASE_P(ParameterizedTest, ApiTestDBAccessFailureByTableId,
                        ::testing::ValuesIn(table_id_not_exists_dbaf));

INSTANTIATE_TEST_CASE_P(ParameterizedTest, ApiTestDBAccessFailureByTableName,
                        ::testing::ValuesIn(table_name_dbaf));

INSTANTIATE_TEST_CASE_P(
    ParameterizedTest, ApiTestDBAccessFailureByTableIdReltuples,
    ::testing::Combine(::testing::ValuesIn(table_id_not_exists_dbaf),
                       ::testing::ValuesIn(reltuples_dbaf)));

INSTANTIATE_TEST_CASE_P(
    ParameterizedTest, ApiTestDBAccessFailureByTableNameReltuples,
    ::testing::Combine(::testing::ValuesIn(table_name_dbaf),
                       ::testing::ValuesIn(reltuples_dbaf)));

INSTANTIATE_TEST_CASE_P(
    ParameterizedTest, ApiTestDBAccessFailureByTableIdOrdinalPosition,
    ::testing::Combine(::testing::ValuesIn(table_id_not_exists_dbaf),
                       ::testing::ValuesIn(ordinal_position_not_exists_dbaf)));

INSTANTIATE_TEST_CASE_P(
    ParameterizedTest, ApiTestDBAccessFailureByColumnStatistics,
    ::testing::Combine(::testing::ValuesIn(table_id_not_exists_dbaf),
                       ::testing::ValuesIn(ordinal_position_not_exists_dbaf),
                       ::testing::ValuesIn(ptree_dbaf)));

/**
 * @brief API to add table metadata
 *   return ErrorCode::DATABASE_ACCESS_FAILURE
 */
TEST_F(ApiTestDBAccessFailure, add_table_metadata) {
  UTTableMetadata* testdata_table_metadata =
      global->testdata_table_metadata.get();
  ptree new_table = testdata_table_metadata->tables;

  std::string table_name =
      testdata_table_metadata->name + "ApiTestDBAccessFailure_add_table";
  new_table.put(Tables::NAME, table_name);

  auto tables = std::make_unique<Tables>(GlobalTestEnvironment::TEST_DB);

  ErrorCode error = tables->init();
  EXPECT_EQ(ErrorCode::DATABASE_ACCESS_FAILURE, error);

  ObjectIdType ret_table_id = -1;
  error = tables->add(new_table, &ret_table_id);
  EXPECT_EQ(ErrorCode::DATABASE_ACCESS_FAILURE, error);
  EXPECT_EQ(ret_table_id, -1);
}

/**
 * @brief API to get table metadata based on table id
 *   return ErrorCode::DATABASE_ACCESS_FAILURE
 */
TEST_F(ApiTestDBAccessFailure, get_table_metadata_by_table_id) {
  ObjectIdType table_id = 1;

  auto tables = std::make_unique<Tables>(GlobalTestEnvironment::TEST_DB);

  ErrorCode error = tables->init();
  EXPECT_EQ(ErrorCode::DATABASE_ACCESS_FAILURE, error);

  ptree table_metadata_inserted;
  error = tables->get(table_id, table_metadata_inserted);
  EXPECT_EQ(ErrorCode::DATABASE_ACCESS_FAILURE, error);

  ptree empty_ptree;
  EXPECT_EQ(UTUtils::get_tree_string(empty_ptree),
            UTUtils::get_tree_string(table_metadata_inserted));
}

/**
 * @brief API to get table metadata based on table name
 *   return ErrorCode::DATABASE_ACCESS_FAILURE
 */
TEST_F(ApiTestDBAccessFailure, get_table_metadata_by_table_name) {
  UTTableMetadata testdata_table_metadata =
      *(global->testdata_table_metadata.get());

  auto tables = std::make_unique<Tables>(GlobalTestEnvironment::TEST_DB);

  ErrorCode error = tables->init();
  EXPECT_EQ(ErrorCode::DATABASE_ACCESS_FAILURE, error);

  ptree table_metadata_inserted;
  std::string table_name = "table_name";
  error = tables->get(table_name, table_metadata_inserted);
  EXPECT_EQ(ErrorCode::DATABASE_ACCESS_FAILURE, error);

  ptree empty_ptree;
  EXPECT_EQ(UTUtils::get_tree_string(empty_ptree),
            UTUtils::get_tree_string(table_metadata_inserted));
}

/**
 * @brief API to update table metadata
 *   return ErrorCode::DATABASE_ACCESS_FAILURE
 */
TEST_F(ApiTestDBAccessFailure, update_table_metadata) {
  UTTableMetadata* testdata_table_metadata =
      global->testdata_table_metadata.get();
  ptree table_metadata = testdata_table_metadata->tables;

  std::string table_name =
      testdata_table_metadata->name + "ApiTestDBAccessFailure_update_table";
  table_metadata.put(Tables::NAME, table_name);

  auto tables = std::make_unique<Tables>(GlobalTestEnvironment::TEST_DB);

  ErrorCode error = tables->init();
  EXPECT_EQ(ErrorCode::DATABASE_ACCESS_FAILURE, error);

  ObjectIdType dummy_table_id = 1;
  error = tables->update(dummy_table_id, table_metadata);
  EXPECT_EQ(ErrorCode::DATABASE_ACCESS_FAILURE, error);
}

/**
 * @brief API to remove table metadata based on table id
 *   return ErrorCode::DATABASE_ACCESS_FAILURE
 */
TEST_F(ApiTestDBAccessFailure, remove_table_metadata_by_table_id) {
  auto tables = std::make_unique<Tables>(GlobalTestEnvironment::TEST_DB);

  ErrorCode error = tables->init();
  EXPECT_EQ(ErrorCode::DATABASE_ACCESS_FAILURE, error);

  error = tables->remove(1);
  EXPECT_EQ(ErrorCode::DATABASE_ACCESS_FAILURE, error);
}

/**
 * @brief API to remove table metadata based on table name
 *   return ErrorCode::DATABASE_ACCESS_FAILURE
 */
TEST_F(ApiTestDBAccessFailure, remove_table_metadata_by_table_name) {
  auto tables = std::make_unique<Tables>(GlobalTestEnvironment::TEST_DB);

  ErrorCode error = tables->init();
  EXPECT_EQ(ErrorCode::DATABASE_ACCESS_FAILURE, error);

  ObjectIdType ret_table_id = -1;
  std::string table_name = "table_name";
  error = tables->remove(table_name.c_str(), &ret_table_id);
  EXPECT_EQ(ErrorCode::DATABASE_ACCESS_FAILURE, error);
  EXPECT_EQ(ret_table_id, -1);
}

/**
 * @brief API to get data type metadata based on data type name
 *  return ErrorCode::DATABASE_ACCESS_FAILURE
 */
TEST_F(ApiTestDBAccessFailure, get_datatypes_by_name) {
  auto datatypes = std::make_unique<DataTypes>(GlobalTestEnvironment::TEST_DB);

  ErrorCode error = datatypes->init();
  EXPECT_EQ(ErrorCode::DATABASE_ACCESS_FAILURE, error);

  std::string table_name = "table_name";
  ptree datatype;
  error = datatypes->get(table_name, datatype);
  EXPECT_EQ(ErrorCode::DATABASE_ACCESS_FAILURE, error);

  ptree empty_ptree;
  EXPECT_EQ(UTUtils::get_tree_string(empty_ptree),
            UTUtils::get_tree_string(datatype));
}

/**
 * @brief API to get data type metadata based on key/value
 *   return ErrorCode::DATABASE_ACCESS_FAILURE
 */
TEST_F(ApiTestDBAccessFailure, get_datatypes_by_key_value) {
  auto datatypes = std::make_unique<DataTypes>(GlobalTestEnvironment::TEST_DB);

  ErrorCode error = datatypes->init();
  EXPECT_EQ(ErrorCode::DATABASE_ACCESS_FAILURE, error);

  std::string key = "key";
  std::string value = "value";
  ptree datatype;

  error = datatypes->get(key.c_str(), value, datatype);
  EXPECT_EQ(ErrorCode::DATABASE_ACCESS_FAILURE, error);

  ptree empty_ptree;
  EXPECT_EQ(UTUtils::get_tree_string(empty_ptree),
            UTUtils::get_tree_string(datatype));
}

/**
 * @brief API to get role metadata based on role id
 *   return ErrorCode::DATABASE_ACCESS_FAILURE
 */
TEST_F(ApiTestDBAccessFailure, get_roles_by_id) {
  auto roles = std::make_unique<Roles>(GlobalTestEnvironment::TEST_DB);

  ErrorCode error = roles->init();
  EXPECT_EQ(ErrorCode::DATABASE_ACCESS_FAILURE, error);

  ptree role_metadata;
  error = roles->get(9999, role_metadata);
  EXPECT_EQ(ErrorCode::DATABASE_ACCESS_FAILURE, error);

  ptree empty_ptree;
  EXPECT_EQ(UTUtils::get_tree_string(empty_ptree),
            UTUtils::get_tree_string(role_metadata));
}

/**
 * @brief API to get role metadata based on role name
 *   return ErrorCode::DATABASE_ACCESS_FAILURE
 */
TEST_F(ApiTestDBAccessFailure, get_roles_by_name) {
  auto roles = std::make_unique<Roles>(GlobalTestEnvironment::TEST_DB);

  ErrorCode error = roles->init();
  EXPECT_EQ(ErrorCode::DATABASE_ACCESS_FAILURE, error);

  ptree role_metadata;
  error = roles->get("role_name", role_metadata);
  EXPECT_EQ(ErrorCode::DATABASE_ACCESS_FAILURE, error);

  ptree empty_ptree;
  EXPECT_EQ(UTUtils::get_tree_string(empty_ptree),
            UTUtils::get_tree_string(role_metadata));
}

/**
 * @brief API to add table statistics based on table id
 * return ErrorCode::DATABASE_ACCESS_FAILURE
 */
TEST_P(ApiTestDBAccessFailureByTableIdReltuples,
       add_table_statistic_by_table_id) {
  auto tables = std::make_unique<Tables>(GlobalTestEnvironment::TEST_DB);

  ErrorCode error = tables->init();
  EXPECT_EQ(ErrorCode::DATABASE_ACCESS_FAILURE, error);

  auto params = GetParam();
  ObjectIdType table_id = std::get<0>(params);
  float reltuples = std::get<1>(params);

  // set table metadata.
  ptree table_meta;
  table_meta.put(Tables::ID, table_id);
  table_meta.put(Tables::TUPLES, reltuples);

  error = tables->set_statistic(table_meta);

  auto optional_tuples = table_meta.get_optional<float>(Tables::TUPLES);
  if (optional_tuples) {
    EXPECT_EQ(ErrorCode::DATABASE_ACCESS_FAILURE, error);
  } else {
    EXPECT_EQ(ErrorCode::INVALID_PARAMETER, error);
  }
}

/**
 * @brief API to add table statistics based on table name
 * return ErrorCode::DATABASE_ACCESS_FAILURE
 */
TEST_P(ApiTestDBAccessFailureByTableNameReltuples,
       add_table_statistic_by_table_name) {
  auto tables = std::make_unique<Tables>(GlobalTestEnvironment::TEST_DB);

  ErrorCode error = tables->init();
  EXPECT_EQ(ErrorCode::DATABASE_ACCESS_FAILURE, error);

  auto params = GetParam();
  std::string table_name = std::get<0>(params);
  float reltuples = std::get<1>(params);

  // set table metadata.
  ptree table_meta;
  table_meta.put(Tables::NAME, table_name);
  table_meta.put(Tables::TUPLES, reltuples);

  error = tables->set_statistic(table_meta);

  auto optional_tuples = table_meta.get_optional<float>(Tables::TUPLES);
  if (optional_tuples) {
    EXPECT_EQ(ErrorCode::DATABASE_ACCESS_FAILURE, error);
  } else {
    EXPECT_EQ(ErrorCode::INVALID_PARAMETER, error);
  }
}

/**
 * @brief API to get table statistics based on table id
 * return ErrorCode::DATABASE_ACCESS_FAILURE
 */
TEST_P(ApiTestDBAccessFailureByTableId, get_table_statistic_by_table_id) {
  auto tables = std::make_unique<Tables>(GlobalTestEnvironment::TEST_DB);

  ErrorCode error = tables->init();
  EXPECT_EQ(ErrorCode::DATABASE_ACCESS_FAILURE, error);

  ObjectIdType table_id = GetParam();

  ptree table_stats;
  error = tables->get_statistic(table_id, table_stats);

  if (table_id <= 0) {
    EXPECT_EQ(ErrorCode::ID_NOT_FOUND, error);
  } else {
    EXPECT_EQ(ErrorCode::DATABASE_ACCESS_FAILURE, error);
  }
  EXPECT_TRUE(table_stats.empty());
}

/**
 * @brief API to get table statistics based on table name
 * return ErrorCode::DATABASE_ACCESS_FAILURE
 */
TEST_P(ApiTestDBAccessFailureByTableName, get_table_statistics_by_table_name) {
  auto tables = std::make_unique<Tables>(GlobalTestEnvironment::TEST_DB);

  ErrorCode error = tables->init();
  EXPECT_EQ(ErrorCode::DATABASE_ACCESS_FAILURE, error);

  std::string table_name = GetParam();

  ptree table_stats;
  error = tables->get_statistic(table_name, table_stats);

  if (table_name.empty()) {
    EXPECT_EQ(ErrorCode::NAME_NOT_FOUND, error);
  } else {
    EXPECT_EQ(ErrorCode::DATABASE_ACCESS_FAILURE, error);
  }
  EXPECT_TRUE(table_stats.empty());
}

/**
 * @brief API to add one column statistic
 * return ErrorCode::DATABASE_ACCESS_FAILURE
 */
TEST_P(ApiTestDBAccessFailureByColumnStatistics, add_one_column_statistic) {
  auto stats = std::make_unique<Statistics>(GlobalTestEnvironment::TEST_DB);

  ErrorCode error = stats->init();
  EXPECT_EQ(ErrorCode::DATABASE_ACCESS_FAILURE, error);

  auto params = GetParam();
  ObjectIdType table_id = std::get<0>(params);
  ObjectIdType ordinal_position = std::get<1>(params);
  ptree column_stats = std::get<2>(params);

  ptree statistic;
  // name
  std::string statistic_name = "ApiTestDBAccessFailureByColumnStatistics_" +
                               std::to_string(table_id) + "-" +
                               std::to_string(ordinal_position);
  statistic.put(Statistics::NAME, statistic_name);
  // table_id
  statistic.put(Statistics::TABLE_ID, table_id);
  // ordinal_position
  statistic.put(Statistics::ORDINAL_POSITION, ordinal_position);
  // column_statistic
  statistic.add_child(Statistics::COLUMN_STATISTIC, column_stats);

  error = stats->add(statistic);

  if ((table_id <= 0) || (ordinal_position <= 0)) {
    EXPECT_EQ(ErrorCode::INVALID_PARAMETER, error);
  } else {
    EXPECT_EQ(ErrorCode::DATABASE_ACCESS_FAILURE, error);
  }
}

/**
 * @brief API to get one column statistic
 * return ErrorCode::DATABASE_ACCESS_FAILURE
 */
TEST_P(ApiTestDBAccessFailureByTableIdOrdinalPosition,
       get_one_column_statistic) {
  auto stats = std::make_unique<Statistics>(GlobalTestEnvironment::TEST_DB);

  ErrorCode error = stats->init();
  EXPECT_EQ(ErrorCode::DATABASE_ACCESS_FAILURE, error);

  auto params = GetParam();
  ObjectIdType table_id = std::get<0>(params);
  ObjectIdType ordinal_position = std::get<1>(params);

  ptree column_stats;
  error = stats->get_by_column_number(table_id, ordinal_position, column_stats);
  if ((table_id <= 0) || (ordinal_position <= 0)) {
    EXPECT_EQ(ErrorCode::ID_NOT_FOUND, error);
  } else {
    EXPECT_EQ(ErrorCode::DATABASE_ACCESS_FAILURE, error);
  }
  EXPECT_TRUE(column_stats.empty());
}

/**
 * @brief API to get all column statistic
 * return ErrorCode::DATABASE_ACCESS_FAILURE
 */
TEST_P(ApiTestDBAccessFailureByTableId, get_all_column_statistics) {
  auto stats = std::make_unique<Statistics>(GlobalTestEnvironment::TEST_DB);

  ErrorCode error = stats->init();
  EXPECT_EQ(ErrorCode::DATABASE_ACCESS_FAILURE, error);

  ObjectIdType table_id = GetParam();
  std::vector<ptree> column_stats;

  error = stats->get_all(table_id, column_stats);
  if (table_id <= 0) {
    EXPECT_EQ(ErrorCode::ID_NOT_FOUND, error);
  } else {
    EXPECT_EQ(ErrorCode::DATABASE_ACCESS_FAILURE, error);
  }
  EXPECT_EQ(0, column_stats.size());
}

/**
 * @brief API to remove one column statistic
 * return ErrorCode::DATABASE_ACCESS_FAILURE
 */
TEST_P(ApiTestDBAccessFailureByTableIdOrdinalPosition,
       remove_one_column_statistic) {
  auto stats = std::make_unique<Statistics>(GlobalTestEnvironment::TEST_DB);

  ErrorCode error = stats->init();
  EXPECT_EQ(ErrorCode::DATABASE_ACCESS_FAILURE, error);

  auto params = GetParam();
  ObjectIdType table_id = std::get<0>(params);
  ObjectIdType ordinal_position = std::get<1>(params);

  error = stats->remove_by_column_number(table_id, ordinal_position);
  if ((table_id <= 0) || (ordinal_position <= 0)) {
    EXPECT_EQ(ErrorCode::ID_NOT_FOUND, error);
  } else {
    EXPECT_EQ(ErrorCode::DATABASE_ACCESS_FAILURE, error);
  }
}

/**
 * @brief API to remove all column statistic
 * return ErrorCode::DATABASE_ACCESS_FAILURE
 */
TEST_P(ApiTestDBAccessFailureByTableId, remove_all_column_statistics) {
  auto stats = std::make_unique<Statistics>(GlobalTestEnvironment::TEST_DB);

  ErrorCode error = stats->init();
  EXPECT_EQ(ErrorCode::DATABASE_ACCESS_FAILURE, error);

  ObjectIdType table_id = GetParam();
  error = stats->remove_by_table_id(table_id);
  if (table_id <= 0) {
    EXPECT_EQ(ErrorCode::ID_NOT_FOUND, error);
  } else {
    EXPECT_EQ(ErrorCode::DATABASE_ACCESS_FAILURE, error);
  }
}

}  // namespace manager::metadata::testing
