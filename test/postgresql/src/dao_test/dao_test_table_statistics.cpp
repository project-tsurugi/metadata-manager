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
#include <tuple>

#include "manager/metadata/dao/generic_dao.h"
#include "manager/metadata/dao/postgresql/db_session_manager_pg.h"
#include "manager/metadata/dao/tables_dao.h"
#include "manager/metadata/error_code.h"
#include "manager/metadata/tables.h"
#include "test/global_test_environment.h"
#include "test/helper/table_metadata_helper.h"
#include "test/helper/table_statistics_helper.h"
#include "test/utility/ut_utils.h"

namespace manager::metadata::testing {

using boost::property_tree::ptree;
using db::postgresql::DBSessionManager;

class DaoTestTableStatisticsByTableIdException
    : public ::testing::TestWithParam<ObjectIdType> {
  void SetUp() override { UTUtils::skip_if_connection_not_opened(); }
};  // class DaoTestTableStatisticsByTableIdException

class DaoTestTableStatisticsByTableNameException
    : public ::testing::TestWithParam<std::string> {
  void SetUp() override { UTUtils::skip_if_connection_not_opened(); }
};  // class DaoTestTableStatisticsByTableNameException

class DaoTestTableStatisticsByTableIdHappy
    : public ::testing::TestWithParam<
          TableStatisticsHelper::BasicTestParameter> {
  void SetUp() override { UTUtils::skip_if_connection_not_opened(); }
};  // class DaoTestTableStatisticsByTableIdHappy

class DaoTestTableStatisticsByTableNameHappy
    : public ::testing::TestWithParam<
          TableStatisticsHelper::BasicTestParameter> {
  void SetUp() override { UTUtils::skip_if_connection_not_opened(); }
};  // class DaoTestTableStatisticsByTableNameHappy

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
        TableStatisticsHelper::make_test_patterns_for_basic_tests("3")));

INSTANTIATE_TEST_CASE_P(
    ParamtererizedTest, DaoTestTableStatisticsByTableNameHappy,
    ::testing::ValuesIn(
        TableStatisticsHelper::make_test_patterns_for_basic_tests("4")));

/**
 * @brief Exception paths test for add_table_statistic
 * based on non-existing table id.
 */
TEST_P(DaoTestTableStatisticsByTableIdException,
       add_table_statistics_by_table_id_if_not_exists) {
  std::shared_ptr<db::GenericDAO> t_gdao = nullptr;
  DBSessionManager db_session_manager;

  ErrorCode error =
      db_session_manager.get_dao(db::GenericDAO::TableName::TABLES, t_gdao);

  EXPECT_EQ(ErrorCode::OK, error);

  std::shared_ptr<db::TablesDAO> tdao;
  tdao = std::static_pointer_cast<db::TablesDAO>(t_gdao);

  error = db_session_manager.start_transaction();

  EXPECT_EQ(ErrorCode::OK, error);

  float reltuples = 1000;
  auto table_id_not_exists = GetParam();
  ObjectIdType retval_table_id = -1;
  error = tdao->update_reltuples(reltuples, Tables::ID,
                                 std::to_string(table_id_not_exists),
                                 retval_table_id);
  EXPECT_EQ(ErrorCode::ID_NOT_FOUND, error);
  EXPECT_EQ(-1, retval_table_id);

  error = db_session_manager.rollback();

  EXPECT_EQ(ErrorCode::OK, error);
}

/**
 * @brief Exception path test for add_table_statistic
 * based on non-existing table name.
 */
TEST_P(DaoTestTableStatisticsByTableNameException,
       add_table_statistics_by_table_name_if_not_exists) {
  std::shared_ptr<db::GenericDAO> t_gdao = nullptr;
  DBSessionManager db_session_manager;

  ErrorCode error =
      db_session_manager.get_dao(db::GenericDAO::TableName::TABLES, t_gdao);

  EXPECT_EQ(ErrorCode::OK, error);

  std::shared_ptr<db::TablesDAO> tdao;
  tdao = std::static_pointer_cast<db::TablesDAO>(t_gdao);

  error = db_session_manager.start_transaction();

  EXPECT_EQ(ErrorCode::OK, error);

  float reltuples = 1000;
  std::string table_name_not_exists = GetParam();
  ObjectIdType retval_table_id = -1;
  error = tdao->update_reltuples(reltuples, Tables::NAME, table_name_not_exists,
                                 retval_table_id);
  EXPECT_EQ(ErrorCode::NAME_NOT_FOUND, error);
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
  std::shared_ptr<db::GenericDAO> t_gdao = nullptr;
  DBSessionManager db_session_manager;

  ErrorCode error =
      db_session_manager.get_dao(db::GenericDAO::TableName::TABLES, t_gdao);

  EXPECT_EQ(ErrorCode::OK, error);

  std::shared_ptr<db::TablesDAO> tdao;
  tdao = std::static_pointer_cast<db::TablesDAO>(t_gdao);

  auto table_id_not_exists = GetParam();
  ptree table_stats;
  error = tdao->select_table_metadata(
      Tables::ID, std::to_string(table_id_not_exists), table_stats);

  EXPECT_EQ(ErrorCode::ID_NOT_FOUND, error);
  TableMetadataHelper::print_table_statistics(table_stats);
}

/**
 * @brief Exception path test for get_table_statistic
 * based on non-existing table name.
 */
TEST_P(DaoTestTableStatisticsByTableNameException,
       get_table_statistics_by_table_name_if_not_exists) {
  std::shared_ptr<db::GenericDAO> t_gdao = nullptr;
  DBSessionManager db_session_manager;

  ErrorCode error =
      db_session_manager.get_dao(db::GenericDAO::TableName::TABLES, t_gdao);

  EXPECT_EQ(ErrorCode::OK, error);

  std::shared_ptr<db::TablesDAO> tdao;
  tdao = std::static_pointer_cast<db::TablesDAO>(t_gdao);

  std::string table_name_not_exists = GetParam();
  ptree table_stats;
  error = tdao->select_table_metadata(Tables::NAME, table_name_not_exists,
                                      table_stats);

  EXPECT_EQ(ErrorCode::NAME_NOT_FOUND, error);
}

/**
 * @brief happy test for add_table_statistic/get_table_statistic
 * based on existing table id.
 */
TEST_P(DaoTestTableStatisticsByTableIdHappy,
       add_and_get_table_statistics_by_table_id) {
  UTTableMetadata* testdata_table_metadata =
      global->testdata_table_metadata.get();

  auto param = GetParam();
  std::string table_name = testdata_table_metadata->name + std::get<0>(param);

  ObjectIdType ret_table_id;
  TableMetadataHelper::add_table(table_name, &ret_table_id);

  std::shared_ptr<db::GenericDAO> t_gdao = nullptr;
  DBSessionManager db_session_manager;

  ErrorCode error =
      db_session_manager.get_dao(db::GenericDAO::TableName::TABLES, t_gdao);
  EXPECT_EQ(ErrorCode::OK, error);

  std::shared_ptr<db::TablesDAO> tdao;
  tdao = std::static_pointer_cast<db::TablesDAO>(t_gdao);

  // The number of rows is NULL in the table metadata table.
  // So, adds the number of rows to the table metadata table.
  error = db_session_manager.start_transaction();
  EXPECT_EQ(ErrorCode::OK, error);

  float reltuples_to_add = std::get<1>(param);
  ObjectIdType retval_table_id = -1;
  error = tdao->update_reltuples(reltuples_to_add, Tables::ID,
                                 std::to_string(ret_table_id), retval_table_id);
  EXPECT_EQ(ErrorCode::OK, error);
  EXPECT_NE(-1, retval_table_id);

  error = db_session_manager.commit();
  EXPECT_EQ(ErrorCode::OK, error);

  ptree table_stats_added;
  error = tdao->select_table_metadata(Tables::ID, std::to_string(ret_table_id),
                                      table_stats_added);
  EXPECT_EQ(ErrorCode::OK, error);

  boost::optional<ObjectIdType> add_metadata_id =
      table_stats_added.get_optional<ObjectIdType>(Tables::ID);
  boost::optional<std::string> add_metadata_name =
      table_stats_added.get_optional<std::string>(Tables::NAME);
  boost::optional<std::string> add_metadata_namespace =
      table_stats_added.get_optional<std::string>(Tables::NAMESPACE);
  boost::optional<float> add_metadata_tuples =
      table_stats_added.get_optional<float>(Tables::TUPLES);

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

  TableMetadataHelper::print_table_statistics(table_stats_added);

  // updates the number of rows.
  error = db_session_manager.start_transaction();
  EXPECT_EQ(ErrorCode::OK, error);

  float reltuples_to_update = std::get<2>(param);
  retval_table_id = -1;
  error = tdao->update_reltuples(reltuples_to_update, Tables::ID,
                                 std::to_string(ret_table_id), retval_table_id);
  EXPECT_EQ(ErrorCode::OK, error);
  EXPECT_NE(-1, retval_table_id);

  error = db_session_manager.commit();
  EXPECT_EQ(ErrorCode::OK, error);

  ptree table_stats_updated;
  error = tdao->select_table_metadata(Tables::ID, std::to_string(ret_table_id),
                                      table_stats_updated);
  EXPECT_EQ(ErrorCode::OK, error);

  boost::optional<ObjectIdType> upd_metadata_id =
      table_stats_updated.get_optional<ObjectIdType>(Tables::ID);
  boost::optional<std::string> upd_metadata_name =
      table_stats_updated.get_optional<std::string>(Tables::NAME);
  boost::optional<std::string> upd_metadata_namespace =
      table_stats_updated.get_optional<std::string>(Tables::NAMESPACE);
  boost::optional<float> upd_metadata_tuples =
      table_stats_updated.get_optional<float>(Tables::TUPLES);

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

  TableMetadataHelper::print_table_statistics(table_stats_updated);

  // remove table metadata.
  TableMetadataHelper::remove_table(ret_table_id);
}

/**
 * @brief happy test for add_table_statistic/get_table_statistic
 * based on existing table name.
 */
TEST_P(DaoTestTableStatisticsByTableNameHappy,
       add_and_get_table_statistics_by_table_name) {
  UTTableMetadata* testdata_table_metadata =
      global->testdata_table_metadata.get();

  auto param = GetParam();
  std::string table_name = testdata_table_metadata->name + std::get<0>(param);

  ObjectIdType ret_table_id;
  TableMetadataHelper::add_table(table_name, &ret_table_id);

  std::shared_ptr<db::GenericDAO> t_gdao = nullptr;
  DBSessionManager db_session_manager;

  ErrorCode error =
      db_session_manager.get_dao(db::GenericDAO::TableName::TABLES, t_gdao);
  EXPECT_EQ(ErrorCode::OK, error);

  std::shared_ptr<db::TablesDAO> tdao;
  tdao = std::static_pointer_cast<db::TablesDAO>(t_gdao);

  // The number of rows is NULL in the table metadata table.
  // So, adds the number of rows to the table metadata table.
  error = db_session_manager.start_transaction();
  EXPECT_EQ(ErrorCode::OK, error);

  float reltuples_to_add = std::get<1>(param);
  ObjectIdType ret_table_id_ts_add = -1;
  error = tdao->update_reltuples(reltuples_to_add, Tables::NAME, table_name,
                                 ret_table_id_ts_add);
  EXPECT_EQ(ErrorCode::OK, error);
  EXPECT_EQ(ret_table_id_ts_add, ret_table_id);

  error = db_session_manager.commit();
  EXPECT_EQ(ErrorCode::OK, error);

  ptree table_stats_added;
  error =
      tdao->select_table_metadata(Tables::NAME, table_name, table_stats_added);
  EXPECT_EQ(ErrorCode::OK, error);

  boost::optional<ObjectIdType> add_metadata_id =
      table_stats_added.get_optional<ObjectIdType>(Tables::ID);
  boost::optional<std::string> add_metadata_name =
      table_stats_added.get_optional<std::string>(Tables::NAME);
  boost::optional<std::string> add_metadata_namespace =
      table_stats_added.get_optional<std::string>(Tables::NAMESPACE);
  boost::optional<float> add_metadata_tuples =
      table_stats_added.get_optional<float>(Tables::TUPLES);

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

  TableMetadataHelper::print_table_statistics(table_stats_added);

  // updates the number of rows.
  error = db_session_manager.start_transaction();
  EXPECT_EQ(ErrorCode::OK, error);

  float reltuples_to_update = std::get<2>(param);
  ObjectIdType ret_table_id_ts_update = -1;
  error = tdao->update_reltuples(reltuples_to_update, Tables::NAME, table_name,
                                 ret_table_id_ts_update);
  EXPECT_EQ(ErrorCode::OK, error);
  EXPECT_EQ(ret_table_id_ts_update, ret_table_id);

  error = db_session_manager.commit();
  EXPECT_EQ(ErrorCode::OK, error);

  ptree table_stats_updated;
  error = tdao->select_table_metadata(Tables::NAME, table_name,
                                      table_stats_updated);
  EXPECT_EQ(ErrorCode::OK, error);

  boost::optional<ObjectIdType> upd_metadata_id =
      table_stats_updated.get_optional<ObjectIdType>(Tables::ID);
  boost::optional<std::string> upd_metadata_name =
      table_stats_updated.get_optional<std::string>(Tables::NAME);
  boost::optional<std::string> upd_metadata_namespace =
      table_stats_updated.get_optional<std::string>(Tables::NAMESPACE);
  boost::optional<float> upd_metadata_tuples =
      table_stats_updated.get_optional<float>(Tables::TUPLES);

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

  TableMetadataHelper::print_table_statistics(table_stats_updated);

  // remove table metadata.
  TableMetadataHelper::remove_table(ret_table_id);
}

}  // namespace manager::metadata::testing
