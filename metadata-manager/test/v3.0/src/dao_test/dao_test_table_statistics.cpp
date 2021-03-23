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
#include <cmath>
#include <limits>
#include <memory>
#include <string>
#include <tuple>

#include "manager/metadata/dao/db_session_manager.h"
#include "manager/metadata/dao/generic_dao.h"
#include "manager/metadata/dao/tables_dao.h"
#include "manager/metadata/error_code.h"

#include "test/api_test_table_statistics.h"
#include "test/dao_test/dao_test_table_metadatas.h"
#include "test/global_test_environment.h"
#include "test/utility/ut_utils.h"

namespace manager::metadata::testing {

using namespace boost::property_tree;

using namespace manager::metadata::db;

typedef std::tuple<std::string, float, float> TupleApiTestTableStatistics;

class DaoTestTableStatisticsByTableIdException
    : public ::testing::TestWithParam<ObjectIdType> {
    void SetUp() override { UTUtils::skip_if_connection_not_opened(); }
};
class DaoTestTableStatisticsByTableNameException
    : public ::testing::TestWithParam<std::string> {
    void SetUp() override { UTUtils::skip_if_connection_not_opened(); }
};

class DaoTestTableStatisticsByTableIdHappy
    : public ::testing::TestWithParam<TupleApiTestTableStatistics> {
    void SetUp() override { UTUtils::skip_if_connection_not_opened(); }
};
class DaoTestTableStatisticsByTableNameHappy
    : public ::testing::TestWithParam<TupleApiTestTableStatistics> {
    void SetUp() override { UTUtils::skip_if_connection_not_opened(); }
};

INSTANTIATE_TEST_CASE_P(
    ParamtererizedTest, DaoTestTableStatisticsByTableIdException,
    ::testing::Values(-1, 0, INT64_MAX - 1, INT64_MAX,
                      std::numeric_limits<ObjectIdType>::infinity(),
                      -std::numeric_limits<ObjectIdType>::infinity(),
                      std::numeric_limits<ObjectIdType>::quiet_NaN()));

INSTANTIATE_TEST_CASE_P(ParamtererizedTest,
                        DaoTestTableStatisticsByTableNameException,
                        ::testing::Values("table_name_not_exists", ""));

INSTANTIATE_TEST_CASE_P(
    ParamtererizedTest, DaoTestTableStatisticsByTableIdHappy,
    ::testing::ValuesIn(
        ApiTestTableStatistics::make_tuple_table_statistics("3")));

INSTANTIATE_TEST_CASE_P(
    ParamtererizedTest, DaoTestTableStatisticsByTableNameHappy,
    ::testing::ValuesIn(
        ApiTestTableStatistics::make_tuple_table_statistics("4")));

/**
 * @brief Exception paths test for add_table_statistic
 * based on non-existing table id.
 */
TEST_P(DaoTestTableStatisticsByTableIdException,
       add_table_statistics_by_table_id_if_not_exists) {
    std::shared_ptr<GenericDAO> t_gdao = nullptr;
    DBSessionManager db_session_manager;

    ErrorCode error =
        db_session_manager.get_dao(GenericDAO::TableName::TABLES, t_gdao);

    EXPECT_EQ(ErrorCode::OK, error);

    std::shared_ptr<TablesDAO> tdao;
    tdao = std::static_pointer_cast<TablesDAO>(t_gdao);

    error = db_session_manager.start_transaction();

    EXPECT_EQ(ErrorCode::OK, error);

    float reltuples = 1000;
    auto table_id_not_exists = GetParam();
    error = tdao->update_reltuples_by_table_id(reltuples, table_id_not_exists);

    EXPECT_EQ(ErrorCode::INVALID_PARAMETER, error);

    error = db_session_manager.rollback();

    EXPECT_EQ(ErrorCode::OK, error);
}

/**
 * @brief Exception path test for add_table_statistic
 * based on non-existing table name.
 */
TEST_P(DaoTestTableStatisticsByTableNameException,
       add_table_statistics_by_table_name_if_not_exists) {
    std::shared_ptr<GenericDAO> t_gdao = nullptr;
    DBSessionManager db_session_manager;

    ErrorCode error =
        db_session_manager.get_dao(GenericDAO::TableName::TABLES, t_gdao);

    EXPECT_EQ(ErrorCode::OK, error);

    std::shared_ptr<TablesDAO> tdao;
    tdao = std::static_pointer_cast<TablesDAO>(t_gdao);

    error = db_session_manager.start_transaction();

    EXPECT_EQ(ErrorCode::OK, error);

    float reltuples = 1000;
    std::string table_name_not_exists = GetParam();
    ObjectIdType retval_table_id = -1;

    error = tdao->update_reltuples_by_table_name(
        reltuples, table_name_not_exists, retval_table_id);

    EXPECT_EQ(ErrorCode::INVALID_PARAMETER, error);
    EXPECT_EQ(retval_table_id, -1);

    error = db_session_manager.rollback();

    EXPECT_EQ(ErrorCode::OK, error);
}

/**
 * @brief Exception path test for get_table_statistic
 * based on non-existing table id.
 */
TEST_P(DaoTestTableStatisticsByTableIdException,
       get_table_statistics_by_table_id_if_not_exists) {
    std::shared_ptr<GenericDAO> t_gdao = nullptr;
    DBSessionManager db_session_manager;

    ErrorCode error =
        db_session_manager.get_dao(GenericDAO::TableName::TABLES, t_gdao);

    EXPECT_EQ(ErrorCode::OK, error);

    std::shared_ptr<TablesDAO> tdao;
    tdao = std::static_pointer_cast<TablesDAO>(t_gdao);

    auto table_id_not_exists = GetParam();
    TableStatistic table_stats;
    error = tdao->select_table_statistic_by_table_id(table_id_not_exists,
                                                     table_stats);

    EXPECT_EQ(ErrorCode::INVALID_PARAMETER, error);
    UTUtils::print_table_statistics(table_stats);
}

/**
 * @brief Exception path test for get_table_statistic
 * based on non-existing table name.
 */
TEST_P(DaoTestTableStatisticsByTableNameException,
       get_table_statistics_by_table_name_if_not_exists) {
    std::shared_ptr<GenericDAO> t_gdao = nullptr;
    DBSessionManager db_session_manager;

    ErrorCode error =
        db_session_manager.get_dao(GenericDAO::TableName::TABLES, t_gdao);

    EXPECT_EQ(ErrorCode::OK, error);

    std::shared_ptr<TablesDAO> tdao;
    tdao = std::static_pointer_cast<TablesDAO>(t_gdao);

    std::string table_name_not_exists = GetParam();
    TableStatistic table_stats;
    error = tdao->select_table_statistic_by_table_name(table_name_not_exists,
                                                       table_stats);

    EXPECT_EQ(ErrorCode::INVALID_PARAMETER, error);
}

/**
 * @brief happy test for add_table_statistic/get_table_statistic
 * based on existing table id.
 */
TEST_P(DaoTestTableStatisticsByTableIdHappy,
       add_and_get_table_statistics_by_table_id) {
    UTTableMetadata *testdata_table_metadata =
        global->testdata_table_metadata.get();

    auto param = GetParam();
    std::string table_name = testdata_table_metadata->name + std::get<0>(param);
    ObjectIdType ret_table_id;

    DaoTestTableMetadata::add_table(table_name, &ret_table_id);

    std::shared_ptr<GenericDAO> t_gdao = nullptr;
    DBSessionManager db_session_manager;

    ErrorCode error =
        db_session_manager.get_dao(GenericDAO::TableName::TABLES, t_gdao);
    EXPECT_EQ(ErrorCode::OK, error);

    std::shared_ptr<TablesDAO> tdao;
    tdao = std::static_pointer_cast<TablesDAO>(t_gdao);

    // The number of rows is NULL in the table metadata table.
    // So, adds the number of rows to the table metadata table.
    error = db_session_manager.start_transaction();
    EXPECT_EQ(ErrorCode::OK, error);

    float reltuples_to_add = std::get<1>(param);
    error = tdao->update_reltuples_by_table_id(reltuples_to_add, ret_table_id);
    EXPECT_EQ(ErrorCode::OK, error);

    error = db_session_manager.commit();
    EXPECT_EQ(ErrorCode::OK, error);

    TableStatistic table_stats_added;
    error = tdao->select_table_statistic_by_table_id(ret_table_id,
                                                     table_stats_added);
    EXPECT_EQ(ErrorCode::OK, error);

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

    // updates the number of rows.
    error = db_session_manager.start_transaction();
    EXPECT_EQ(ErrorCode::OK, error);

    float reltuples_to_update = std::get<2>(param);
    error =
        tdao->update_reltuples_by_table_id(reltuples_to_update, ret_table_id);
    EXPECT_EQ(ErrorCode::OK, error);

    error = db_session_manager.commit();
    EXPECT_EQ(ErrorCode::OK, error);

    TableStatistic table_stats_updated;
    error = tdao->select_table_statistic_by_table_id(ret_table_id,
                                                     table_stats_updated);
    EXPECT_EQ(ErrorCode::OK, error);

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
TEST_P(DaoTestTableStatisticsByTableNameHappy,
       add_and_get_table_statistics_by_table_name) {
    UTTableMetadata *testdata_table_metadata =
        global->testdata_table_metadata.get();

    auto param = GetParam();
    std::string table_name = testdata_table_metadata->name + std::get<0>(param);

    ObjectIdType ret_table_id;
    DaoTestTableMetadata::add_table(table_name, &ret_table_id);

    std::shared_ptr<GenericDAO> t_gdao = nullptr;
    DBSessionManager db_session_manager;

    ErrorCode error =
        db_session_manager.get_dao(GenericDAO::TableName::TABLES, t_gdao);
    EXPECT_EQ(ErrorCode::OK, error);

    std::shared_ptr<TablesDAO> tdao;
    tdao = std::static_pointer_cast<TablesDAO>(t_gdao);

    // The number of rows is NULL in the table metadata table.
    // So, adds the number of rows to the table metadata table.
    error = db_session_manager.start_transaction();
    EXPECT_EQ(ErrorCode::OK, error);

    float reltuples_to_add = std::get<1>(param);
    ObjectIdType ret_table_id_ts_add;
    error = tdao->update_reltuples_by_table_name(reltuples_to_add, table_name,
                                                 ret_table_id_ts_add);
    EXPECT_EQ(ErrorCode::OK, error);
    EXPECT_EQ(ret_table_id_ts_add, ret_table_id);

    error = db_session_manager.commit();
    EXPECT_EQ(ErrorCode::OK, error);

    TableStatistic table_stats_added;
    error = tdao->select_table_statistic_by_table_name(table_name,
                                                       table_stats_added);
    EXPECT_EQ(ErrorCode::OK, error);

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

    // updates the number of rows.
    error = db_session_manager.start_transaction();
    EXPECT_EQ(ErrorCode::OK, error);

    float reltuples_to_update = std::get<2>(param);
    ObjectIdType ret_table_id_ts_update;
    error = tdao->update_reltuples_by_table_name(
        reltuples_to_update, table_name, ret_table_id_ts_update);
    EXPECT_EQ(ErrorCode::OK, error);
    EXPECT_EQ(ret_table_id_ts_update, ret_table_id);

    error = db_session_manager.commit();
    EXPECT_EQ(ErrorCode::OK, error);

    TableStatistic table_stats_updated;
    error = tdao->select_table_statistic_by_table_name(table_name,
                                                       table_stats_updated);
    EXPECT_EQ(ErrorCode::OK, error);

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

}  // namespace manager::metadata::testing
