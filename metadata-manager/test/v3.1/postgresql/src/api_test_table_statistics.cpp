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

#include "manager/metadata/error_code.h"
#include "manager/metadata/statistics.h"

#include "test/api_test_table_metadatas.h"
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
class ApiTestTableStatisticsBySameTableNameException
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
                                     static_cast<float>(DBL_MIN)
                                };

std::vector<TupleApiTestTableStatistics>
ApiTestTableStatistics::make_tuple_table_statistics(
    const std::string &test_number) {
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

INSTANTIATE_TEST_CASE_P(ParamtererizedTest,
                        ApiTestTableStatisticsBySameTableNameException,
                        ::testing::Values("_TableStatistic_3"));

/**
 * @brief Exception path test for add_table_statistic
 * based on non-existing table id.
 */
TEST_P(ApiTestTableStatisticsByTableIdException,
       add_table_statistics_by_non_existing_table_id) {
    auto stats = std::make_unique<Statistics>(GlobalTestEnvironment::TEST_DB);

    ErrorCode error = stats->init();
    EXPECT_EQ(ErrorCode::OK, error);

    auto table_id_not_exists = GetParam();
    float reltuples = 1000;

    error = stats->add_table_statistic(table_id_not_exists, reltuples);
    EXPECT_EQ(ErrorCode::INVALID_PARAMETER, error);
}

/**
 * @brief Exception path test for add_table_statistic
 * based on non-existing table name.
 */
TEST_P(ApiTestTableStatisticsByTableNameException,
       add_table_statistics_by_non_existing_table_name) {
    auto stats = std::make_unique<Statistics>(GlobalTestEnvironment::TEST_DB);

    ErrorCode error = stats->init();
    EXPECT_EQ(ErrorCode::OK, error);

    std::string table_name_not_exists = GetParam();
    float reltuples = 1000;
    ObjectIdType retval_table_id = -1;

    error = stats->add_table_statistic(table_name_not_exists, reltuples,
                                       &retval_table_id);
    EXPECT_EQ(ErrorCode::INVALID_PARAMETER, error);
    EXPECT_EQ(retval_table_id, -1);
}

/**
 * @brief Exception path test for get_table_statistic
 * based on non-existing table id.
 */
TEST_P(ApiTestTableStatisticsByTableIdException,
       get_table_statistics_by_non_existing_table_id) {
    auto stats = std::make_unique<Statistics>(GlobalTestEnvironment::TEST_DB);

    ErrorCode error = stats->init();
    EXPECT_EQ(ErrorCode::OK, error);

    TableStatistic table_stats;
    auto table_id_not_exists = GetParam();

    error = stats->get_table_statistic(table_id_not_exists, table_stats);
    EXPECT_EQ(ErrorCode::INVALID_PARAMETER, error);

    UTUtils::print_table_statistics(table_stats);
}

/**
 * @brief Exception path test for get_table_statistic
 * based on non-existing table name.
 */
TEST_P(ApiTestTableStatisticsByTableNameException,
       get_table_statistics_by_non_existing_table_name) {
    auto stats = std::make_unique<Statistics>(GlobalTestEnvironment::TEST_DB);

    ErrorCode error = stats->init();
    EXPECT_EQ(ErrorCode::OK, error);

    TableStatistic table_stats;
    std::string table_name_not_exists = GetParam();

    error = stats->get_table_statistic(table_name_not_exists, table_stats);
    EXPECT_EQ(ErrorCode::INVALID_PARAMETER, error);
}

/**
 * @brief happy test for add_table_statistic/get_table_statistic
 * based on existing table id.
 */
TEST_P(ApiTestTableStatisticsByTableIdHappy,
       add_and_get_table_statistics_by_table_id) {
    // prepare test data for adding table metadata.
    UTTableMetadata *testdata_table_metadata =
        global->testdata_table_metadata.get();
    auto param = GetParam();
    std::string table_name = testdata_table_metadata->name + std::get<0>(param);

    // add table metadata.
    ObjectIdType ret_table_id = -1;
    ApiTestTableMetadata::add_table(table_name, &ret_table_id);

    auto stats = std::make_unique<Statistics>(GlobalTestEnvironment::TEST_DB);

    ErrorCode error = stats->init();
    EXPECT_EQ(ErrorCode::OK, error);

    // The number of rows is NULL in the table metadata table.
    // So, add the number of rows to the table metadata table.
    float reltuples_to_add = std::get<1>(param);
    error = stats->add_table_statistic(ret_table_id, reltuples_to_add);
    EXPECT_EQ(ErrorCode::OK, error);

    // get table statistic.
    TableStatistic table_stats_added;
    error = stats->get_table_statistic(ret_table_id, table_stats_added);
    EXPECT_EQ(ErrorCode::OK, error);

    // verifies that the returned table statistic is expected one.
    EXPECT_EQ(ret_table_id, table_stats_added.id);
    EXPECT_EQ(table_name, table_stats_added.name);
    EXPECT_EQ(testdata_table_metadata->namespace_name,
              table_stats_added.namespace_name);

    if (std::isnan(table_stats_added.reltuples)) {
        EXPECT_TRUE(std::isnan(table_stats_added.reltuples));
    } else {
        EXPECT_FLOAT_EQ(reltuples_to_add, table_stats_added.reltuples);
    }

    UTUtils::print_table_statistics(table_stats_added);

    // update the number of rows.
    float reltuples_to_update = std::get<2>(param);
    error = stats->add_table_statistic(ret_table_id, reltuples_to_update);
    EXPECT_EQ(ErrorCode::OK, error);

    TableStatistic table_stats_updated;
    error = stats->get_table_statistic(ret_table_id, table_stats_updated);
    EXPECT_EQ(ErrorCode::OK, error);

    // verifies that the returned table statistic is expected one.
    EXPECT_EQ(ret_table_id, table_stats_updated.id);
    EXPECT_EQ(table_name, table_stats_updated.name);
    EXPECT_EQ(testdata_table_metadata->namespace_name,
              table_stats_updated.namespace_name);
    if (std::isnan(table_stats_updated.reltuples)) {
        EXPECT_TRUE(std::isnan(table_stats_updated.reltuples));
    } else {
        EXPECT_FLOAT_EQ(reltuples_to_update, table_stats_updated.reltuples);
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
    UTTableMetadata *testdata_table_metadata =
        global->testdata_table_metadata.get();
    auto param = GetParam();
    std::string table_name = testdata_table_metadata->name + std::get<0>(param);

    // add table metadata.
    ObjectIdType ret_table_id;
    ApiTestTableMetadata::add_table(table_name, &ret_table_id);

    // add table statistic.
    auto stats = std::make_unique<Statistics>(GlobalTestEnvironment::TEST_DB);

    ErrorCode error = stats->init();
    EXPECT_EQ(ErrorCode::OK, error);

    // The number of rows is NULL in the table metadata table.
    // So, add the number of rows to the table metadata table.
    float reltuples_to_add = std::get<1>(param);
    ObjectIdType ret_table_id_ts_add;
    error = stats->add_table_statistic(table_name, reltuples_to_add,
                                       &ret_table_id_ts_add);
    EXPECT_EQ(ErrorCode::OK, error);
    EXPECT_EQ(ret_table_id_ts_add, ret_table_id);

    // get table statistic.
    TableStatistic table_stats_added;
    error = stats->get_table_statistic(ret_table_id_ts_add, table_stats_added);
    EXPECT_EQ(ErrorCode::OK, error);

    // verifies that the returned table statistic is expected one.
    EXPECT_EQ(ret_table_id_ts_add, table_stats_added.id);
    EXPECT_EQ(table_name, table_stats_added.name);
    EXPECT_EQ(testdata_table_metadata->namespace_name,
              table_stats_added.namespace_name);
    if (std::isnan(table_stats_added.reltuples)) {
        EXPECT_TRUE(std::isnan(table_stats_added.reltuples));
    } else {
        EXPECT_FLOAT_EQ(reltuples_to_add, table_stats_added.reltuples);
    }

    UTUtils::print_table_statistics(table_stats_added);

    // update the number of rows.
    float reltuples_to_update = std::get<2>(param);
    ObjectIdType ret_table_id_ts_update;
    error = stats->add_table_statistic(table_name, reltuples_to_update,
                                       &ret_table_id_ts_update);
    EXPECT_EQ(ErrorCode::OK, error);
    EXPECT_EQ(ret_table_id_ts_update, ret_table_id);

    // get table statistic.
    TableStatistic table_stats_updated;
    error =
        stats->get_table_statistic(ret_table_id_ts_update, table_stats_updated);
    EXPECT_EQ(ErrorCode::OK, error);

    // verifies that the returned table statistic is expected one.
    EXPECT_EQ(ret_table_id_ts_update, table_stats_updated.id);
    EXPECT_EQ(table_name, table_stats_updated.name);
    EXPECT_EQ(testdata_table_metadata->namespace_name,
              table_stats_updated.namespace_name);
    if (std::isnan(table_stats_updated.reltuples)) {
        EXPECT_TRUE(std::isnan(table_stats_updated.reltuples));
    } else {
        EXPECT_FLOAT_EQ(reltuples_to_update, table_stats_updated.reltuples);
    }

    UTUtils::print_table_statistics(table_stats_updated);
}

/**
 * @brief On the presupposition that two same table name exists in the metadata
 * repository, exception path test for get_table_statistic based on table name.
 */
TEST_P(ApiTestTableStatisticsBySameTableNameException,
       add_same_two_table_name_and_get_table_statistics_by_table_name) {
    // prepare test data for adding table metadata.
    UTTableMetadata *testdata_table_metadata =
        global->testdata_table_metadata.get();
    std::string table_name = testdata_table_metadata->name + GetParam();

    // add same two table metadata.
    ObjectIdType ret_table_id;
    ApiTestTableMetadata::add_table(table_name, &ret_table_id);
    ApiTestTableMetadata::add_table(table_name, &ret_table_id);

    // add table statistic.
    auto stats = std::make_unique<Statistics>(GlobalTestEnvironment::TEST_DB);

    ErrorCode error = stats->init();
    EXPECT_EQ(ErrorCode::OK, error);

    // add the number of rows to the table metadata table.
    float reltuples_to_add = 100;
    ObjectIdType ret_table_id_ts_add = -1;
    error = stats->add_table_statistic(table_name, reltuples_to_add,
                                       &ret_table_id_ts_add);
    // but, returned error code is not ok
    // because same two table name exists in the metadata repository.
    EXPECT_EQ(ErrorCode::INVALID_PARAMETER, error);
    EXPECT_EQ(-1, ret_table_id_ts_add);

    // get table statistic.
    TableStatistic table_stats_added;
    table_stats_added.id = -1;
    table_stats_added.reltuples = -1;
    error = stats->get_table_statistic(table_name, table_stats_added);
    // but, returned error code is not ok
    // because same two table name exists in the metadata repository.
    EXPECT_EQ(ErrorCode::INVALID_PARAMETER, error);

    // verifies that the returned table statistic is expected one.
    EXPECT_EQ(-1, table_stats_added.id);
    EXPECT_EQ("", table_stats_added.name);
    EXPECT_EQ("", table_stats_added.namespace_name);
    EXPECT_FLOAT_EQ(-1, table_stats_added.reltuples);

    UTUtils::print_table_statistics(table_stats_added);
}

}  // namespace manager::metadata::testing
