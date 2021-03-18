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

#include <gtest/gtest.h>

#include <boost/property_tree/ptree.hpp>
#include <limits>
#include <memory>
#include <string>
#include <tuple>
#include <unordered_map>
#include <vector>

#include "manager/metadata/datatypes.h"
#include "manager/metadata/error_code.h"
#include "manager/metadata/statistics.h"
#include "manager/metadata/tables.h"

#include "test/api_test_environment.h"
#include "test/utility/ut_utils.h"

using namespace manager::metadata;
using namespace boost::property_tree;

namespace manager::metadata::testing {

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
std::vector<ptree> ptrees_dbaf = {empty_column_stats_dbaf,
                                  UTUtils::generate_column_statistic()};
std::vector<std::string> table_name_dbaf = {"table_name_not_exists", ""};

INSTANTIATE_TEST_CASE_P(ParamtererizedTest, ApiTestDBAccessFailureByTableId,
                        ::testing::ValuesIn(table_id_not_exists_dbaf));

INSTANTIATE_TEST_CASE_P(ParamtererizedTest, ApiTestDBAccessFailureByTableName,
                        ::testing::ValuesIn(table_name_dbaf));

INSTANTIATE_TEST_CASE_P(
    ParamtererizedTest, ApiTestDBAccessFailureByTableIdReltuples,
    ::testing::Combine(::testing::ValuesIn(table_id_not_exists_dbaf),
                       ::testing::ValuesIn(reltuples_dbaf)));

INSTANTIATE_TEST_CASE_P(
    ParamtererizedTest, ApiTestDBAccessFailureByTableNameReltuples,
    ::testing::Combine(::testing::ValuesIn(table_name_dbaf),
                       ::testing::ValuesIn(reltuples_dbaf)));

INSTANTIATE_TEST_CASE_P(
    ParamtererizedTest, ApiTestDBAccessFailureByTableIdOrdinalPosition,
    ::testing::Combine(::testing::ValuesIn(table_id_not_exists_dbaf),
                       ::testing::ValuesIn(ordinal_position_not_exists_dbaf)));

INSTANTIATE_TEST_CASE_P(
    ParamtererizedTest, ApiTestDBAccessFailureByColumnStatistics,
    ::testing::Combine(::testing::ValuesIn(table_id_not_exists_dbaf),
                       ::testing::ValuesIn(ordinal_position_not_exists_dbaf),
                       ::testing::ValuesIn(ptrees_dbaf)));

TEST_F(ApiTestDBAccessFailure, add_table_metadata) {
    UTTableMetadata *testdata_table_metadata =
        api_test_env->testdata_table_metadata.get();
    ptree new_table = testdata_table_metadata->tables;

    std::string table_name =
        testdata_table_metadata->name + "ApiTestDBAccessFailure_add_table";
    new_table.put(Tables::NAME, table_name);

    auto tables = std::make_unique<Tables>(ApiTestEnvironment::TEST_DB);

    ErrorCode error = tables->init();
    EXPECT_EQ(ErrorCode::DATABASE_ACCESS_FAILURE, error);

    ObjectIdType ret_table_id = -1;
    error = tables->add(new_table, &ret_table_id);
    EXPECT_EQ(ErrorCode::DATABASE_ACCESS_FAILURE, error);
    EXPECT_EQ(ret_table_id, -1);
}

TEST_F(ApiTestDBAccessFailure, get_table_metadata_by_table_id) {
    ObjectIdType table_id = 1;

    auto tables = std::make_unique<Tables>(ApiTestEnvironment::TEST_DB);

    ErrorCode error = tables->init();
    EXPECT_EQ(ErrorCode::DATABASE_ACCESS_FAILURE, error);

    ptree table_metadata_inserted;
    error = tables->get(table_id, table_metadata_inserted);
    EXPECT_EQ(ErrorCode::DATABASE_ACCESS_FAILURE, error);

    ptree empty_ptree;
    EXPECT_EQ(UTUtils::get_tree_string(empty_ptree),
              UTUtils::get_tree_string(table_metadata_inserted));
}

TEST_F(ApiTestDBAccessFailure, get_table_metadata_by_table_name) {
    UTTableMetadata testdata_table_metadata =
        *(api_test_env->testdata_table_metadata.get());

    auto tables = std::make_unique<Tables>(ApiTestEnvironment::TEST_DB);

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

TEST_F(ApiTestDBAccessFailure, remove_table_metadata_by_table_id) {
    auto tables = std::make_unique<Tables>(ApiTestEnvironment::TEST_DB);

    ErrorCode error = tables->init();
    EXPECT_EQ(ErrorCode::DATABASE_ACCESS_FAILURE, error);

    error = tables->remove(1);
    EXPECT_EQ(ErrorCode::DATABASE_ACCESS_FAILURE, error);
}

TEST_F(ApiTestDBAccessFailure, remove_table_metadata_by_table_name) {
    auto tables = std::make_unique<Tables>(ApiTestEnvironment::TEST_DB);

    ErrorCode error = tables->init();
    EXPECT_EQ(ErrorCode::DATABASE_ACCESS_FAILURE, error);

    ObjectIdType ret_table_id = -1;
    std::string table_name = "table_name";
    error = tables->remove(table_name.c_str(), &ret_table_id);
    EXPECT_EQ(ErrorCode::DATABASE_ACCESS_FAILURE, error);
    EXPECT_EQ(ret_table_id, -1);
}

TEST_F(ApiTestDBAccessFailure, get_datatypes_by_name) {
    auto datatypes = std::make_unique<DataTypes>(ApiTestEnvironment::TEST_DB);

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

TEST_F(ApiTestDBAccessFailure, get_datatypes_by_key_value) {
    auto datatypes = std::make_unique<DataTypes>(ApiTestEnvironment::TEST_DB);

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

TEST_P(ApiTestDBAccessFailureByTableIdReltuples,
       add_table_statistics_by_table_id) {
    auto stats = std::make_unique<Statistics>(ApiTestEnvironment::TEST_DB);

    ErrorCode error = stats->init();
    EXPECT_EQ(ErrorCode::DATABASE_ACCESS_FAILURE, error);

    auto params = GetParam();
    ObjectIdType table_id = std::get<0>(params);
    float reltuples = std::get<1>(params);
    error = stats->add_table_statistic(table_id, reltuples);
    EXPECT_EQ(ErrorCode::DATABASE_ACCESS_FAILURE, error);
}

TEST_P(ApiTestDBAccessFailureByTableNameReltuples,
       add_table_statistics_by_table_name) {
    auto stats = std::make_unique<Statistics>(ApiTestEnvironment::TEST_DB);

    ErrorCode error = stats->init();
    EXPECT_EQ(ErrorCode::DATABASE_ACCESS_FAILURE, error);

    ObjectIdType retval_table_id = -1;
    auto params = GetParam();
    std::string table_name = std::get<0>(params);
    float reltuples = std::get<1>(params);
    error = stats->add_table_statistic(table_name, reltuples, &retval_table_id);

    EXPECT_EQ(ErrorCode::DATABASE_ACCESS_FAILURE, error);
    EXPECT_EQ(retval_table_id, -1);
}

TEST_P(ApiTestDBAccessFailureByTableId, get_table_statistics_by_table_id) {
    auto stats = std::make_unique<Statistics>(ApiTestEnvironment::TEST_DB);

    ErrorCode error = stats->init();
    EXPECT_EQ(ErrorCode::DATABASE_ACCESS_FAILURE, error);

    ObjectIdType table_id = GetParam();
    TableStatistic table_stats;
    table_stats.id = -1;
    table_stats.reltuples = -1;

    error = stats->get_table_statistic(table_id, table_stats);
    EXPECT_EQ(ErrorCode::DATABASE_ACCESS_FAILURE, error);

    EXPECT_EQ(-1, table_stats.id);
    EXPECT_EQ(-1, table_stats.reltuples);
    EXPECT_EQ("", table_stats.name);
    EXPECT_EQ("", table_stats.namespace_name);
}

TEST_P(ApiTestDBAccessFailureByTableName, get_table_statistics_by_table_name) {
    auto stats = std::make_unique<Statistics>(ApiTestEnvironment::TEST_DB);

    ErrorCode error = stats->init();
    EXPECT_EQ(ErrorCode::DATABASE_ACCESS_FAILURE, error);

    std::string table_name = GetParam();

    TableStatistic table_stats;
    table_stats.id = -1;
    table_stats.reltuples = -1;

    error = stats->get_table_statistic(table_name, table_stats);
    EXPECT_EQ(ErrorCode::DATABASE_ACCESS_FAILURE, error);

    EXPECT_EQ(-1, table_stats.id);
    EXPECT_EQ(-1, table_stats.reltuples);
    EXPECT_EQ("", table_stats.name);
    EXPECT_EQ("", table_stats.namespace_name);
}

TEST_P(ApiTestDBAccessFailureByColumnStatistics, add_one_column_statistic) {
    auto stats = std::make_unique<Statistics>(ApiTestEnvironment::TEST_DB);

    ErrorCode error = stats->init();
    EXPECT_EQ(ErrorCode::DATABASE_ACCESS_FAILURE, error);

    auto params = GetParam();
    ObjectIdType table_id = std::get<0>(params);
    ObjectIdType ordinal_position = std::get<1>(params);
    ptree column_stats = std::get<2>(params);

    error = stats->add_one_column_statistic(table_id, ordinal_position,
                                            column_stats);
    EXPECT_EQ(ErrorCode::DATABASE_ACCESS_FAILURE, error);
}

TEST_P(ApiTestDBAccessFailureByTableIdOrdinalPosition,
       get_one_column_statistic) {
    auto stats = std::make_unique<Statistics>(ApiTestEnvironment::TEST_DB);

    ErrorCode error = stats->init();
    EXPECT_EQ(ErrorCode::DATABASE_ACCESS_FAILURE, error);

    auto params = GetParam();
    ObjectIdType table_id = std::get<0>(params);
    ObjectIdType ordinal_position = std::get<1>(params);

    ColumnStatistic column_stats;
    column_stats.table_id = -1;
    column_stats.ordinal_position = -1;

    error = stats->get_one_column_statistic(table_id, ordinal_position,
                                            column_stats);
    EXPECT_EQ(ErrorCode::DATABASE_ACCESS_FAILURE, error);
    EXPECT_EQ(-1, column_stats.table_id);
    EXPECT_EQ(-1, column_stats.ordinal_position);

    ptree empty_ptree;
    EXPECT_EQ(UTUtils::get_tree_string(empty_ptree),
              UTUtils::get_tree_string(column_stats.column_statistic));
}

TEST_P(ApiTestDBAccessFailureByTableId, get_all_column_statistics) {
    auto stats = std::make_unique<Statistics>(ApiTestEnvironment::TEST_DB);

    ErrorCode error = stats->init();
    EXPECT_EQ(ErrorCode::DATABASE_ACCESS_FAILURE, error);

    ObjectIdType table_id = GetParam();
    std::unordered_map<ObjectIdType, ColumnStatistic> column_stats;

    error = stats->get_all_column_statistics(table_id, column_stats);
    EXPECT_EQ(ErrorCode::DATABASE_ACCESS_FAILURE, error);
    EXPECT_EQ(0, column_stats.size());
}

TEST_P(ApiTestDBAccessFailureByTableIdOrdinalPosition,
       remove_one_column_statistic) {
    auto stats = std::make_unique<Statistics>(ApiTestEnvironment::TEST_DB);

    ErrorCode error = stats->init();
    EXPECT_EQ(ErrorCode::DATABASE_ACCESS_FAILURE, error);

    auto params = GetParam();
    ObjectIdType table_id = std::get<0>(params);
    ObjectIdType ordinal_position = std::get<1>(params);

    error = stats->remove_one_column_statistic(table_id, ordinal_position);
    EXPECT_EQ(ErrorCode::DATABASE_ACCESS_FAILURE, error);
}

TEST_P(ApiTestDBAccessFailureByTableId, remove_all_column_statistics) {
    auto stats = std::make_unique<Statistics>(ApiTestEnvironment::TEST_DB);

    ErrorCode error = stats->init();
    EXPECT_EQ(ErrorCode::DATABASE_ACCESS_FAILURE, error);

    ObjectIdType table_id = GetParam();
    error = stats->remove_all_column_statistics(table_id);
    EXPECT_EQ(ErrorCode::DATABASE_ACCESS_FAILURE, error);
}

}  // namespace manager::metadata::testing
