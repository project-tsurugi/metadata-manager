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

#include "manager/metadata/dao/postgresql/db_session_manager_pg.h"
#include "manager/metadata/dao/postgresql/tables_dao_pg.h"
#include "manager/metadata/error_code.h"
#include "test/common/global_test_environment.h"
#include "test/common/ut_utils.h"
#include "test/helper/postgresql/table_statistics_helper_pg.h"
#include "test/helper/table_metadata_helper.h"

namespace manager::metadata::testing {

using boost::property_tree::ptree;

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
    ParameterizedTest, DaoTestTableStatisticsByTableIdException,
    ::testing::Values(-1, 0, INT64_MAX - 1, INT64_MAX,
                      std::numeric_limits<ObjectIdType>::infinity(),
                      -std::numeric_limits<ObjectIdType>::infinity(),
                      std::numeric_limits<ObjectIdType>::quiet_NaN()));

INSTANTIATE_TEST_CASE_P(ParameterizedTest,
                        DaoTestTableStatisticsByTableNameException,
                        ::testing::Values("table_name_not_exists", ""));

INSTANTIATE_TEST_CASE_P(
    ParameterizedTest, DaoTestTableStatisticsByTableIdHappy,
    ::testing::ValuesIn(
        TableStatisticsHelper::make_test_patterns_for_basic_tests("3")));

INSTANTIATE_TEST_CASE_P(
    ParameterizedTest, DaoTestTableStatisticsByTableNameHappy,
    ::testing::ValuesIn(
        TableStatisticsHelper::make_test_patterns_for_basic_tests("4")));

/**
 * @brief Exception paths test for add_table_statistic
 * based on non-existing table id.
 */
TEST_P(DaoTestTableStatisticsByTableIdException,
       add_table_statistics_by_table_id_if_not_exists) {
  ErrorCode error = ErrorCode::UNKNOWN;
  db::DbSessionManagerPg db_session_manager;

  // Get TablesDAO.
  auto tables_dao = db_session_manager.get_tables_dao();
  ASSERT_NE(nullptr, tables_dao);
  error = tables_dao->prepare();
  ASSERT_EQ(ErrorCode::OK, error);

  // Run the API under test.
  error = db_session_manager.start_transaction();
  EXPECT_EQ(ErrorCode::OK, error);

  int64_t reltuples        = 1000;
  auto table_id_not_exists = GetParam();

  ptree object;
  object.put(Table::NUMBER_OF_TUPLES, reltuples);

  // Run the API under test.
  error = tables_dao->update(Tables::ID, {std::to_string(table_id_not_exists)},
                             object);
  EXPECT_EQ(ErrorCode::ID_NOT_FOUND, error);

  // Run the API under test.
  error = db_session_manager.rollback();
  EXPECT_EQ(ErrorCode::OK, error);
}

/**
 * @brief Exception path test for add_table_statistic
 * based on non-existing table name.
 */
TEST_P(DaoTestTableStatisticsByTableNameException,
       add_table_statistics_by_table_name_if_not_exists) {
  ErrorCode error = ErrorCode::UNKNOWN;
  db::DbSessionManagerPg db_session_manager;

  // Get TablesDAO.
  auto tables_dao = db_session_manager.get_tables_dao();
  ASSERT_NE(nullptr, tables_dao);
  error = tables_dao->prepare();
  ASSERT_EQ(ErrorCode::OK, error);

  // Run the API under test.
  error = db_session_manager.start_transaction();
  EXPECT_EQ(ErrorCode::OK, error);

  int64_t reltuples                 = 1000;
  std::string table_name_not_exists = GetParam();

  ptree object;
  object.put(Table::NUMBER_OF_TUPLES, reltuples);

  // Run the API under test.
  error = tables_dao->update(Tables::NAME, {table_name_not_exists}, object);
  EXPECT_EQ(ErrorCode::NAME_NOT_FOUND, error);

  // Run the API under test.
  error = db_session_manager.rollback();
  EXPECT_EQ(ErrorCode::OK, error);
}

/**
 * @brief Exception path test for get_table_statistic
 * based on non-existing table id.
 */
TEST_P(DaoTestTableStatisticsByTableIdException,
       get_table_statistics_by_table_id_if_not_exists) {
  ErrorCode error = ErrorCode::UNKNOWN;
  db::DbSessionManagerPg db_session_manager;

  // Get TablesDAO.
  auto tables_dao = db_session_manager.get_tables_dao();
  ASSERT_NE(nullptr, tables_dao);
  error = tables_dao->prepare();
  ASSERT_EQ(ErrorCode::OK, error);

  auto table_id_not_exists = GetParam();
  ptree table_stats;
  // Run the API under test.
  error = tables_dao->select(Tables::ID, {std::to_string(table_id_not_exists)},
                             table_stats);
  EXPECT_EQ(ErrorCode::ID_NOT_FOUND, error);

  TableMetadataHelper::print_table_statistics(table_stats);
}

/**
 * @brief Exception path test for get_table_statistic
 * based on non-existing table name.
 */
TEST_P(DaoTestTableStatisticsByTableNameException,
       get_table_statistics_by_table_name_if_not_exists) {
  ErrorCode error = ErrorCode::UNKNOWN;
  db::DbSessionManagerPg db_session_manager;

  // Get TablesDAO.
  auto tables_dao = db_session_manager.get_tables_dao();
  ASSERT_NE(nullptr, tables_dao);
  error = tables_dao->prepare();
  ASSERT_EQ(ErrorCode::OK, error);

  std::string table_name_not_exists = GetParam();
  ptree table_stats;
  // Run the API under test.
  error =
      tables_dao->select(Tables::NAME, {table_name_not_exists}, table_stats);
  EXPECT_EQ(ErrorCode::NAME_NOT_FOUND, error);
}

/**
 * @brief happy test for add_table_statistic/get_table_statistic
 * based on existing table id.
 */
TEST_P(DaoTestTableStatisticsByTableIdHappy,
       add_and_get_table_statistics_by_table_id) {
  ErrorCode error = ErrorCode::UNKNOWN;
  db::DbSessionManagerPg db_session_manager;

  auto param = GetParam();

  std::string table_name = TableMetadataHelper::make_table_name(
      "DaoTestTableStatistics", std::get<0>(param), __LINE__);

  // Generate test metadata.
  UtTableMetadata testdata_table_metadata(table_name);
  auto testdata_table = testdata_table_metadata.get_metadata_struct();

  ObjectIdType ret_table_id;
  TableMetadataHelper::add_table(table_name, &ret_table_id);

  // Get TablesDAO.
  auto tables_dao = db_session_manager.get_tables_dao();
  ASSERT_NE(nullptr, tables_dao);
  error = tables_dao->prepare();
  ASSERT_EQ(ErrorCode::OK, error);

  // Get table metadata.
  ptree table_object;
  error = tables_dao->select(Tables::ID, {std::to_string(ret_table_id)},
                             table_object);

  // The number of rows is NULL in the table metadata table.
  // So, adds the number of rows to the table metadata table.
  error = db_session_manager.start_transaction();
  EXPECT_EQ(ErrorCode::OK, error);

  int64_t reltuples_to_add = std::get<1>(param);
  table_object.put(Table::NUMBER_OF_TUPLES, reltuples_to_add);

  // Run the API under test.
  error = tables_dao->update(Tables::ID, {std::to_string(ret_table_id)},
                             table_object);
  EXPECT_EQ(ErrorCode::OK, error);

  // Run the API under test.
  error = db_session_manager.commit();
  EXPECT_EQ(ErrorCode::OK, error);

  ptree table_stats_added;
  // Run the API under test.
  error = tables_dao->select(Tables::ID, {std::to_string(ret_table_id)},
                             table_stats_added);
  EXPECT_EQ(ErrorCode::OK, error);

  auto add_metadata_id =
      table_stats_added.get_optional<ObjectIdType>(Table::ID);
  auto add_metadata_name =
      table_stats_added.get_optional<std::string>(Table::NAME);
  auto add_metadata_namespace =
      table_stats_added.get_optional<std::string>(Table::NAMESPACE);
  auto add_metadata_tuples =
      table_stats_added.get_optional<int64_t>(Table::NUMBER_OF_TUPLES);

  EXPECT_EQ(ret_table_id, add_metadata_id.get());
  EXPECT_EQ(table_name, add_metadata_name.get());
  EXPECT_EQ(testdata_table->namespace_name, add_metadata_namespace.get());
  if (add_metadata_tuples) {
    if (std::isnan(add_metadata_tuples.get())) {
      EXPECT_TRUE(std::isnan(add_metadata_tuples.get()));
    } else {
      EXPECT_EQ(reltuples_to_add, add_metadata_tuples.get());
    }
  }

  TableMetadataHelper::print_table_statistics(table_stats_added);

  // updates the number of rows.
  error = db_session_manager.start_transaction();
  EXPECT_EQ(ErrorCode::OK, error);

  int64_t tuples_to_update = std::get<2>(param);
  table_object.put(Table::NUMBER_OF_TUPLES, tuples_to_update);

  // Run the API under test.
  error = tables_dao->update(Tables::ID, {std::to_string(ret_table_id)},
                             table_object);
  EXPECT_EQ(ErrorCode::OK, error);

  // Run the API under test.
  error = db_session_manager.commit();
  EXPECT_EQ(ErrorCode::OK, error);

  ptree table_stats_updated;
  // Run the API under test.
  error = tables_dao->select(Tables::ID, {std::to_string(ret_table_id)},
                             table_stats_updated);
  EXPECT_EQ(ErrorCode::OK, error);

  auto upd_metadata_id =
      table_stats_updated.get_optional<ObjectIdType>(Table::ID);
  auto upd_metadata_name =
      table_stats_updated.get_optional<std::string>(Table::NAME);
  auto upd_metadata_namespace =
      table_stats_updated.get_optional<std::string>(Table::NAMESPACE);
  auto upd_metadata_tuples =
      table_stats_updated.get_optional<int64_t>(Table::NUMBER_OF_TUPLES);

  EXPECT_EQ(ret_table_id, upd_metadata_id.get());
  EXPECT_EQ(table_name, upd_metadata_name.get());
  EXPECT_EQ(testdata_table->namespace_name, upd_metadata_namespace.get());
  if (upd_metadata_tuples) {
    if (std::isnan(upd_metadata_tuples.get())) {
      EXPECT_TRUE(std::isnan(upd_metadata_tuples.get()));
    } else {
      EXPECT_EQ(tuples_to_update, upd_metadata_tuples.get());
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
  ErrorCode error = ErrorCode::UNKNOWN;
  db::DbSessionManagerPg db_session_manager;

  auto param = GetParam();

  std::string table_name = TableMetadataHelper::make_table_name(
      "DaoTestTableStatistics", std::get<0>(param), __LINE__);

  // Generate test metadata.
  UtTableMetadata testdata_table_metadata(table_name);
  auto testdata_table = testdata_table_metadata.get_metadata_struct();

  ObjectIdType ret_table_id;
  TableMetadataHelper::add_table(table_name, &ret_table_id);

  // Get TablesDAO.
  auto tables_dao = db_session_manager.get_tables_dao();
  ASSERT_NE(nullptr, tables_dao);
  error = tables_dao->prepare();
  ASSERT_EQ(ErrorCode::OK, error);

  // Get table metadata.
  ptree table_object;
  error = tables_dao->select(Tables::ID, {std::to_string(ret_table_id)},
                             table_object);

  // The number of rows is NULL in the table metadata table.
  // So, adds the number of rows to the table metadata table.
  error = db_session_manager.start_transaction();
  EXPECT_EQ(ErrorCode::OK, error);

  int64_t reltuples_to_add = std::get<1>(param);
  table_object.put(Table::NUMBER_OF_TUPLES, reltuples_to_add);

  // Run the API under test.
  error = tables_dao->update(Tables::NAME, {table_name}, table_object);
  EXPECT_EQ(ErrorCode::OK, error);

  // Run the API under test.
  error = db_session_manager.commit();
  EXPECT_EQ(ErrorCode::OK, error);

  ptree table_stats_added;
  // Run the API under test.
  error = tables_dao->select(Tables::NAME, {table_name}, table_stats_added);
  EXPECT_EQ(ErrorCode::OK, error);

  auto add_metadata_id =
      table_stats_added.get_optional<ObjectIdType>(Table::ID);
  auto add_metadata_name =
      table_stats_added.get_optional<std::string>(Table::NAME);
  auto add_metadata_namespace =
      table_stats_added.get_optional<std::string>(Table::NAMESPACE);
  auto add_metadata_tuples =
      table_stats_added.get_optional<int64_t>(Table::NUMBER_OF_TUPLES);

  EXPECT_EQ(ret_table_id, add_metadata_id.get());
  EXPECT_EQ(table_name, add_metadata_name.get());
  EXPECT_EQ(testdata_table->namespace_name, add_metadata_namespace.get());

  if (add_metadata_tuples) {
    if (std::isnan(add_metadata_tuples.get())) {
      EXPECT_TRUE(std::isnan(add_metadata_tuples.get()));
    } else {
      EXPECT_EQ(reltuples_to_add, add_metadata_tuples.get());
    }
  }

  TableMetadataHelper::print_table_statistics(table_stats_added);

  // updates the number of rows.
  error = db_session_manager.start_transaction();
  EXPECT_EQ(ErrorCode::OK, error);

  int64_t tuples_to_update = std::get<2>(param);
  table_object.put(Table::NUMBER_OF_TUPLES, tuples_to_update);

  // Run the API under test.
  error = tables_dao->update(Table::NAME, {table_name}, table_object);
  EXPECT_EQ(ErrorCode::OK, error);

  // Run the API under test.
  error = db_session_manager.commit();
  EXPECT_EQ(ErrorCode::OK, error);

  ptree table_stats_updated;
  // Run the API under test.
  error = tables_dao->select(Tables::NAME, {table_name}, table_stats_updated);
  EXPECT_EQ(ErrorCode::OK, error);

  auto upd_metadata_id =
      table_stats_updated.get_optional<ObjectIdType>(Table::ID);
  auto upd_metadata_name =
      table_stats_updated.get_optional<std::string>(Table::NAME);
  auto upd_metadata_namespace =
      table_stats_updated.get_optional<std::string>(Table::NAMESPACE);
  auto upd_metadata_tuples =
      table_stats_updated.get_optional<int64_t>(Table::NUMBER_OF_TUPLES);

  EXPECT_EQ(ret_table_id, upd_metadata_id.get());
  EXPECT_EQ(table_name, upd_metadata_name.get());
  EXPECT_EQ(testdata_table->namespace_name, upd_metadata_namespace.get());
  if (upd_metadata_tuples) {
    if (std::isnan(upd_metadata_tuples.get())) {
      EXPECT_TRUE(std::isnan(upd_metadata_tuples.get()));
    } else {
      EXPECT_EQ(tuples_to_update, upd_metadata_tuples.get());
    }
  }

  TableMetadataHelper::print_table_statistics(table_stats_updated);

  // remove table metadata.
  TableMetadataHelper::remove_table(ret_table_id);
}

}  // namespace manager::metadata::testing
