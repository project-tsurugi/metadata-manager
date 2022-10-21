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

#include <boost/property_tree/json_parser.hpp>

#include "manager/metadata/common/message.h"
#include "manager/metadata/dao/postgresql/db_session_manager_pg.h"
#include "manager/metadata/dao/statistics_dao.h"
#include "test/common/global_test_environment.h"
#include "test/common/ut_utils.h"
#include "test/helper/column_statistics_helper.h"
#include "test/helper/table_metadata_helper.h"

namespace manager::metadata::testing {

namespace json_parser = boost::property_tree::json_parser;

using boost::property_tree::ptree;
using db::postgresql::DBSessionManager;

class DaoTestColumnStatistics : public ::testing::Test {
 public:
  static void add_column_statistics(
      ObjectIdType table_id,
      std::vector<boost::property_tree::ptree> column_statistics);
  static ErrorCode add_one_column_statistic(
      ObjectIdType table_id, ObjectIdType ordinal_position,
      boost::property_tree::ptree& column_statistic);

  static ErrorCode get_one_column_statistic(
      ObjectIdType table_id, ObjectIdType ordinal_position,
      const boost::property_tree::ptree& expected_column_statistic);
  static ErrorCode get_all_column_statistics(
      ObjectIdType table_id,
      std::vector<boost::property_tree::ptree> column_statistics_expected);
  static ErrorCode get_all_column_statistics(
      ObjectIdType table_id,
      std::vector<boost::property_tree::ptree> column_statistics_expected,
      ObjectIdType ordinal_position_removed);
  static ErrorCode remove_one_column_statistic(ObjectIdType table_id,
                                               ObjectIdType ordinal_position);
  static ErrorCode remove_all_column_statistics(ObjectIdType table_id);
};  // class DaoTestColumnStatistics

class DaoTestColumnStatisticsAllAPIHappy
    : public ::testing::TestWithParam<
          ColumnStatisticsHelper::BasicTestParameter> {
  void SetUp() override { UTUtils::skip_if_connection_not_opened(); }
};  // class DaoTestColumnStatisticsAllAPIHappy

class DaoTestColumnStatisticsUpdateHappy
    : public ::testing::TestWithParam<
          ColumnStatisticsHelper::UpdateTestParameter> {
  void SetUp() override { UTUtils::skip_if_connection_not_opened(); }
};  // class DaoTestColumnStatisticsUpdateHappy

class DaoTestColumnStatisticsRemoveAllHappy
    : public ::testing::TestWithParam<std::string> {
  void SetUp() override { UTUtils::skip_if_connection_not_opened(); }
};  // class DaoTestColumnStatisticsRemoveAllHappy

class DaoTestColumnStatisticsAllAPIException
    : public ::testing::TestWithParam<std::string> {
  void SetUp() override { UTUtils::skip_if_connection_not_opened(); }
};  // class DaoTestColumnStatisticsAllAPIException

INSTANTIATE_TEST_CASE_P(
    ParameterizedTest, DaoTestColumnStatisticsAllAPIHappy,
    ::testing::ValuesIn(
        ColumnStatisticsHelper::make_test_patterns_for_basic_tests("3")));
INSTANTIATE_TEST_CASE_P(
    ParameterizedTest, DaoTestColumnStatisticsUpdateHappy,
    ::testing::ValuesIn(
        ColumnStatisticsHelper::make_test_patterns_for_update_tests("4")));
INSTANTIATE_TEST_CASE_P(ParameterizedTest,
                        DaoTestColumnStatisticsRemoveAllHappy,
                        ::testing::Values("_ColumnStatistic_5"));
INSTANTIATE_TEST_CASE_P(ParameterizedTest,
                        DaoTestColumnStatisticsAllAPIException,
                        ::testing::Values("_ColumnStatistic_6"));

/**
 * @brief Add column statistics based on the given table id and
 *  the given ptree type column statistics.
 * @param (table_id)              [in]  table id.
 * @param (column_statistics)     [in]  ptree type column statistics.
 * @return none.
 */
void DaoTestColumnStatistics::add_column_statistics(
    ObjectIdType table_id, std::vector<ptree> column_statistics) {
  UTUtils::print(
      "-- add column statistics by add_one_column_statistic start --");
  UTUtils::print("id:", table_id);

  ErrorCode error;
  for (ObjectIdType ordinal_position = 1;
       static_cast<size_t>(ordinal_position) <= column_statistics.size();
       ordinal_position++) {
    error = DaoTestColumnStatistics::add_one_column_statistic(
        table_id, ordinal_position, column_statistics[ordinal_position - 1]);
    EXPECT_EQ(ErrorCode::OK, error);
  }

  UTUtils::print(
      "-- add column statistics by add_one_column_statistic end -- \n");
}

/**
 * @brief Adds or updates one column statistic
 *  to the column statistics table
 *  based on the given table id and the given column ordinal position.
 *  Adds one column statistic if it not exists in the metadata repository.
 *  Updates one column statistic if it already exists.
 * @param (table_id)          [in]  table id.
 * @param (ordinal_position)  [in]  column ordinal position.
 * @param (column_statistic)  [in]  one column statistic to add or update.
 * @return ErrorCode::OK if success, otherwise an error code.
 */
ErrorCode DaoTestColumnStatistics::add_one_column_statistic(
    ObjectIdType table_id, std::int64_t ordinal_position,
    boost::property_tree::ptree& column_statistic) {
  ErrorCode error = ErrorCode::INTERNAL_ERROR;

  std::shared_ptr<db::GenericDAO> s_gdao = nullptr;
  DBSessionManager db_session_manager;

  error =
      db_session_manager.get_dao(db::GenericDAO::TableName::STATISTICS, s_gdao);
  EXPECT_EQ(ErrorCode::OK, error);

  std::shared_ptr<db::StatisticsDAO> sdao;
  sdao = std::static_pointer_cast<db::StatisticsDAO>(s_gdao);

  std::string statistic_name = "statistic-name";

  std::string s_column_statistic;
  if (!column_statistic.empty()) {
    std::stringstream ss;
    try {
      json_parser::write_json(ss, column_statistic, false);
    } catch (boost::property_tree::json_parser_error& e) {
      std::cerr << Message::WRITE_JSON_FAILURE << e.what() << std::endl;
      return ErrorCode::INTERNAL_ERROR;
    } catch (...) {
      std::cerr << Message::WRITE_JSON_FAILURE << std::endl;
      return ErrorCode::INTERNAL_ERROR;
    }

    s_column_statistic = ss.str();
  }

  error = db_session_manager.start_transaction();
  EXPECT_EQ(ErrorCode::OK, error);

  ObjectIdType ret_statistic_id;
  error = sdao->upsert_column_statistic(
      table_id, Statistics::COLUMN_NUMBER, std::to_string(ordinal_position),
      &statistic_name, column_statistic, ret_statistic_id);

  if (error == ErrorCode::OK) {
    ErrorCode commit_error = db_session_manager.commit();
    EXPECT_EQ(ErrorCode::OK, commit_error);
    EXPECT_GT(ret_statistic_id, 0);

    UTUtils::print(" statistic id: ", ret_statistic_id);
    UTUtils::print(" ordinal position: ", ordinal_position);
    UTUtils::print(" column statistics: " + s_column_statistic);

  } else {
    ErrorCode rollback_error = db_session_manager.rollback();
    EXPECT_EQ(ErrorCode::OK, rollback_error);

    if (rollback_error != ErrorCode::OK) {
      error = rollback_error;
    }
  }

  return error;
}

/**
 * @brief Gets one column statistic from the column statistics table
 *  based on the given table id and the given column ordinal position.
 * @param (table_id)                  [in]  table id.
 * @param (ordinal_position)          [in]  column ordinal position.
 * @param (expected_column_statistic) [in]  expected column statistic with the
 * specified table id and column ordinal position.
 * @return ErrorCode::OK if success, otherwise an error code.
 */
ErrorCode DaoTestColumnStatistics::get_one_column_statistic(
    ObjectIdType table_id, ObjectIdType ordinal_position,
    const ptree& expected_column_statistic) {
  ErrorCode error = ErrorCode::INTERNAL_ERROR;

  std::shared_ptr<db::GenericDAO> s_gdao = nullptr;
  DBSessionManager db_session_manager;

  error =
      db_session_manager.get_dao(db::GenericDAO::TableName::STATISTICS, s_gdao);
  EXPECT_EQ(ErrorCode::OK, error);

  std::shared_ptr<db::StatisticsDAO> sdao;
  sdao = std::static_pointer_cast<db::StatisticsDAO>(s_gdao);

  ptree column_statistic;
  error = sdao->select_column_statistic(table_id, Statistics::COLUMN_NUMBER,
                                        std::to_string(ordinal_position),
                                        column_statistic);

  if (error == ErrorCode::OK) {
    auto optional_ordinal_position =
        column_statistic.get_optional<std::int64_t>(Statistics::COLUMN_NUMBER);
    EXPECT_TRUE(optional_ordinal_position);

    auto optional_column_statistic =
        column_statistic.get_child_optional(Statistics::COLUMN_STATISTIC);
    EXPECT_TRUE(optional_column_statistic);

    std::string s_cs_returned =
        UTUtils::get_tree_string(optional_column_statistic.get());
    std::string s_cs_expected =
        UTUtils::get_tree_string(expected_column_statistic);

    EXPECT_EQ(s_cs_returned, s_cs_expected);

    UTUtils::print(" ordinal position: ", optional_ordinal_position.get());
    UTUtils::print(" column statistics: " + s_cs_returned);
  }

  return error;
}

/**
 * @brief Gets all column statistics from the column statistics table
 *  based on the given table id.
 * @param (table_id)                    [in] table id.
 * @param (column_statistics_expected)  [in] all column statistics
 *  expected with the specified table id.
 * @return ErrorCode::OK if success, otherwise an error code.
 */
ErrorCode DaoTestColumnStatistics::get_all_column_statistics(
    ObjectIdType table_id, std::vector<ptree> column_statistics_expected) {
  ErrorCode error = ErrorCode::INTERNAL_ERROR;

  std::shared_ptr<db::GenericDAO> s_gdao = nullptr;
  DBSessionManager db_session_manager;

  error =
      db_session_manager.get_dao(db::GenericDAO::TableName::STATISTICS, s_gdao);
  EXPECT_EQ(ErrorCode::OK, error);

  std::shared_ptr<db::StatisticsDAO> sdao;
  sdao = std::static_pointer_cast<db::StatisticsDAO>(s_gdao);

  std::vector<ptree> column_statistics;
  error = sdao->select_column_statistic(table_id, column_statistics);

  if (error == ErrorCode::OK) {
    UTUtils::print(
        "-- get column statistics by get_all_column_statistics start --");

    EXPECT_EQ(column_statistics_expected.size(), column_statistics.size());

    for (ObjectIdType ordinal_position = 1;
         static_cast<size_t>(ordinal_position) <= column_statistics.size();
         ordinal_position++) {
      ptree c_cs_returned = column_statistics[ordinal_position - 1];

      auto optional_ordinal_position =
          c_cs_returned.get_optional<std::int64_t>(Statistics::COLUMN_NUMBER);
      EXPECT_TRUE(optional_ordinal_position);

      auto optional_column_statistic =
          c_cs_returned.get_child_optional(Statistics::COLUMN_STATISTIC);
      EXPECT_TRUE(optional_column_statistic);

      std::string s_cs_returned =
          UTUtils::get_tree_string(optional_column_statistic.get());
      std::string s_cs_expected = UTUtils::get_tree_string(
          column_statistics_expected[ordinal_position - 1]);

      EXPECT_EQ(s_cs_expected, s_cs_returned);

      UTUtils::print(" ordinal position: ", optional_ordinal_position.get());
      UTUtils::print(" column statistic: " + s_cs_returned);
    }

    UTUtils::print(
        "-- get column statistics by get_all_column_statistics end -- \n");
  } else {
    EXPECT_EQ(column_statistics.size(), 0);
  }

  return error;
}

/**
 * @brief Gets all column statistics from the column statistics table
 *   based on the given table id.
 * @param (table_id)                    [in] table id.
 * @param (column_statistics_expected)  [in] all column statistics
 *   expected with the specified table id.
 * @param (ordinal_position_removed)    [in] ordinal position of
 *   column statistics removed.
 * @return ErrorCode::OK if success, otherwise an error code.
 */
ErrorCode DaoTestColumnStatistics::get_all_column_statistics(
    ObjectIdType table_id, std::vector<ptree> column_statistics_expected,
    ObjectIdType ordinal_position_removed) {
  ErrorCode error = ErrorCode::INTERNAL_ERROR;

  std::shared_ptr<db::GenericDAO> s_gdao = nullptr;
  DBSessionManager db_session_manager;

  error =
      db_session_manager.get_dao(db::GenericDAO::TableName::STATISTICS, s_gdao);
  EXPECT_EQ(ErrorCode::OK, error);

  std::shared_ptr<db::StatisticsDAO> sdao;
  sdao = std::static_pointer_cast<db::StatisticsDAO>(s_gdao);

  std::vector<ptree> column_statistics;
  error = sdao->select_column_statistic(table_id, column_statistics);

  if (error == ErrorCode::OK) {
    UTUtils::print(
        "-- After removing ordinal position=", ordinal_position_removed,
        " get column statistics by get_all_column_statistics start --");

    int ordinal_position = 1;
    for (ptree statistic : column_statistics) {
      boost::optional<ptree&> optional_column_statistic =
          statistic.get_child_optional(Statistics::COLUMN_STATISTIC);
      EXPECT_TRUE(optional_column_statistic);

      boost::optional<std::int64_t> optional_ordinal_position =
          statistic.get_optional<std::int64_t>(Statistics::COLUMN_NUMBER);
      EXPECT_NE(ordinal_position_removed, optional_ordinal_position.get());

      if (ordinal_position_removed == ordinal_position) {
        ordinal_position++;
      }

      std::string s_cs_returned =
          UTUtils::get_tree_string(optional_column_statistic.get());
      std::string s_cs_expected = UTUtils::get_tree_string(
          column_statistics_expected[ordinal_position - 1]);
      ordinal_position++;

      EXPECT_EQ(s_cs_expected, s_cs_returned);

      UTUtils::print(" ordinal position: ", optional_ordinal_position.get());
      UTUtils::print(" column statistic: " + s_cs_returned);
    }

    EXPECT_EQ(column_statistics_expected.size() - 1, column_statistics.size());

    UTUtils::print(
        "-- After removing ordinal position=", ordinal_position_removed,
        " get column statistics by get_all_column_statistics end --");
  } else {
    EXPECT_EQ(column_statistics.size(), 0);
  }

  return error;
}

/**
 * @brief Removes one column statistic from the column statistics table
 *  based on the given table id and the given column ordinal position.
 * @param (table_id)          [in]  table id.
 * @param (ordinal_position)  [in]  column ordinal position.
 * @return ErrorCode::OK if success, otherwise an error code.
 */
ErrorCode DaoTestColumnStatistics::remove_one_column_statistic(
    ObjectIdType table_id, ObjectIdType ordinal_position) {
  ErrorCode error = ErrorCode::INTERNAL_ERROR;

  std::shared_ptr<db::GenericDAO> s_gdao = nullptr;
  DBSessionManager db_session_manager;

  error =
      db_session_manager.get_dao(db::GenericDAO::TableName::STATISTICS, s_gdao);
  EXPECT_EQ(ErrorCode::OK, error);

  std::shared_ptr<db::StatisticsDAO> sdao;
  sdao = std::static_pointer_cast<db::StatisticsDAO>(s_gdao);

  error = db_session_manager.start_transaction();
  EXPECT_EQ(ErrorCode::OK, error);

  ObjectIdType ret_statistic_id;
  error = sdao->delete_column_statistic(table_id, Statistics::COLUMN_NUMBER,
                                        std::to_string(ordinal_position),
                                        ret_statistic_id);

  if (error == ErrorCode::OK) {
    ErrorCode commit_error = db_session_manager.commit();

    EXPECT_EQ(ErrorCode::OK, commit_error);
    EXPECT_GT(ret_statistic_id, 0);
  } else {
    ErrorCode rollback_error = db_session_manager.rollback();
    EXPECT_EQ(ErrorCode::OK, rollback_error);

    if (rollback_error != ErrorCode::OK) {
      error = rollback_error;
    }
  }

  return error;
}

/**
 * @brief Removes all column statistics
 *  from the column statistics table
 *  based on the given table id.
 * @param (table_id)          [in]  table id.
 * @return ErrorCode::OK if success, otherwise an error code.
 */
ErrorCode DaoTestColumnStatistics::remove_all_column_statistics(
    ObjectIdType table_id) {
  ErrorCode error = ErrorCode::INTERNAL_ERROR;

  std::shared_ptr<db::GenericDAO> s_gdao = nullptr;
  DBSessionManager db_session_manager;

  error =
      db_session_manager.get_dao(db::GenericDAO::TableName::STATISTICS, s_gdao);
  EXPECT_EQ(ErrorCode::OK, error);

  std::shared_ptr<db::StatisticsDAO> sdao;
  sdao = std::static_pointer_cast<db::StatisticsDAO>(s_gdao);

  error = db_session_manager.start_transaction();
  EXPECT_EQ(ErrorCode::OK, error);

  error = sdao->delete_column_statistic(table_id);

  if (error == ErrorCode::OK) {
    ErrorCode commit_error = db_session_manager.commit();
    EXPECT_EQ(ErrorCode::OK, commit_error);

  } else {
    ErrorCode rollback_error = db_session_manager.rollback();
    EXPECT_EQ(ErrorCode::OK, rollback_error);

    if (rollback_error != ErrorCode::OK) {
      error = rollback_error;
    }
  }

  return error;
}

/**
 * @brief happy test for all API.
 * 1. add/get/remove one column statistic
 * based on both existing table id and column ordinal position.
 *
 * 2. get/remove all column statistics
 * based on existing table id.
 *
 * -
 * add_one_column_statistic/get_one_column_statistic/remove_one_column_statistic
 * : based on both existing table id and column ordinal position.
 * - get_all_column_statistics/remove_all_column_statistics :
 *      based on existing table id.
 */
TEST_P(DaoTestColumnStatisticsAllAPIHappy, All_API_happy) {
  auto param = GetParam();

  UTTableMetadata* testdata_table_metadata =
      global->testdata_table_metadata.get();
  std::string table_name = testdata_table_metadata->name + std::get<0>(param);

  ObjectIdType ret_table_id;
  TableMetadataHelper::add_table(table_name, &ret_table_id);

  /**
   * add_one_column_statistic
   * based on both existing table id and column ordinal position.
   */
  std::vector<ptree> column_statistics = std::get<1>(param);
  DaoTestColumnStatistics::add_column_statistics(ret_table_id,
                                                 column_statistics);
  /**
   * get_one_column_statistic
   * based on both existing table id and column ordinal position.
   */
  UTUtils::print(
      "-- get column statistics by get_one_column_statistic start --");

  ErrorCode error;
  for (ObjectIdType ordinal_position = 1;
       static_cast<size_t>(ordinal_position) <= column_statistics.size();
       ordinal_position++) {
    error = DaoTestColumnStatistics::get_one_column_statistic(
        ret_table_id, ordinal_position,
        column_statistics[ordinal_position - 1]);
    EXPECT_EQ(ErrorCode::OK, error);
  }

  UTUtils::print(
      "-- get column statistics by get_one_column_statistic end -- \n");

  /**
   * get_all_column_statistics
   * based on existing table id.
   */
  error = DaoTestColumnStatistics::get_all_column_statistics(ret_table_id,
                                                             column_statistics);
  EXPECT_EQ(ErrorCode::OK, error);

  /**
   * remove_one_column_statistic
   * based on both existing table id and column ordinal position.
   */
  ObjectIdType ordinal_position_to_remove = std::get<2>(param);
  error = DaoTestColumnStatistics::remove_one_column_statistic(
      ret_table_id, ordinal_position_to_remove);
  EXPECT_EQ(ErrorCode::OK, error);

  for (ObjectIdType ordinal_position = 1;
       static_cast<size_t>(ordinal_position) <= column_statistics.size();
       ordinal_position++) {
    error = DaoTestColumnStatistics::get_one_column_statistic(
        ret_table_id, ordinal_position,
        column_statistics[ordinal_position - 1]);

    if (ordinal_position_to_remove == ordinal_position) {
      EXPECT_EQ(ErrorCode::ID_NOT_FOUND, error);
    } else {
      EXPECT_EQ(ErrorCode::OK, error);
    }
  }

  error = DaoTestColumnStatistics::get_all_column_statistics(
      ret_table_id, column_statistics, ordinal_position_to_remove);
  EXPECT_EQ(ErrorCode::OK, error);

  /**
   * remove_all_column_statistics
   * based on existing table.
   */
  error = DaoTestColumnStatistics::remove_all_column_statistics(ret_table_id);
  EXPECT_EQ(ErrorCode::OK, error);

  error = DaoTestColumnStatistics::get_all_column_statistics(ret_table_id,
                                                             column_statistics);
  EXPECT_EQ(ErrorCode::ID_NOT_FOUND, error);

  for (ObjectIdType ordinal_position = 1;
       static_cast<size_t>(ordinal_position) <= column_statistics.size();
       ordinal_position++) {
    error = DaoTestColumnStatistics::get_one_column_statistic(
        ret_table_id, ordinal_position,
        column_statistics[ordinal_position - 1]);
    EXPECT_EQ(ErrorCode::ID_NOT_FOUND, error);
  }

  // remove table metadata.
  TableMetadataHelper::remove_table(ret_table_id);
}

/**
 * @brief happy test to update column statistics
 * based on both existing table id and column ordinal position.
 *
 * - add_one_column_statistic:
 *      update column statistics
 *      based on both existing table id and column ordinal position.
 */
TEST_P(DaoTestColumnStatisticsUpdateHappy, update_column_statistics) {
  auto param = GetParam();

  UTTableMetadata* testdata_table_metadata =
      global->testdata_table_metadata.get();
  std::string table_name = testdata_table_metadata->name + std::get<0>(param);

  ObjectIdType ret_table_id;
  TableMetadataHelper::add_table(table_name, &ret_table_id);

  /**
   * add new column statistics
   * based on both existing table id and column ordinal position.
   */
  std::vector<ptree> column_statistics = std::get<1>(param);
  DaoTestColumnStatistics::add_column_statistics(ret_table_id,
                                                 column_statistics);

  /**
   * check if results of column statistics are expected or not.
   */

  UTUtils::print(
      "-- get column statistics by get_one_column_statistic start --");

  ErrorCode error;
  for (ObjectIdType ordinal_position = 1;
       static_cast<size_t>(ordinal_position) <= column_statistics.size();
       ordinal_position++) {
    error = DaoTestColumnStatistics::get_one_column_statistic(
        ret_table_id, ordinal_position,
        column_statistics[ordinal_position - 1]);
    EXPECT_EQ(ErrorCode::OK, error);
  }

  UTUtils::print(
      "-- get column statistics by get_one_column_statistic end -- \n");

  error = DaoTestColumnStatistics::get_all_column_statistics(ret_table_id,
                                                             column_statistics);
  EXPECT_EQ(ErrorCode::OK, error);

  /**
   * update column statistics
   * based on both existing table id and column ordinal position.
   */
  std::vector<ptree> column_statistics_to_update = std::get<2>(param);
  DaoTestColumnStatistics::add_column_statistics(ret_table_id,
                                                 column_statistics_to_update);

  /**
   * check if results of column statistics are expected or not.
   */
  UTUtils::print(
      "-- After updating all column statistics, get column statistics by ",
      "get_one_column_statistic start --");

  for (ObjectIdType ordinal_position = 1;
       static_cast<size_t>(ordinal_position) <=
       column_statistics_to_update.size();
       ordinal_position++) {
    error = DaoTestColumnStatistics::get_one_column_statistic(
        ret_table_id, ordinal_position,
        column_statistics_to_update[ordinal_position - 1]);
    EXPECT_EQ(ErrorCode::OK, error);
  }

  UTUtils::print(
      "-- After updating all column statistics, get column statistics by ",
      "get_one_column_statistic end -- \n");

  UTUtils::print(
      "-- After updating all column statistics, get column statistics by ",
      "get_all_column_statistics start --");

  error = DaoTestColumnStatistics::get_all_column_statistics(
      ret_table_id, column_statistics_to_update);
  EXPECT_EQ(ErrorCode::OK, error);

  UTUtils::print(
      "-- After updating all column statistics, get column statistics by ",
      "get_all_column_statistics end -- \n");

  /**
   * remove_one_column_statistic
   * based on both existing table id and column ordinal position.
   */
  ObjectIdType ordinal_position_to_remove = std::get<3>(param);
  error = DaoTestColumnStatistics::remove_one_column_statistic(
      ret_table_id, ordinal_position_to_remove);
  EXPECT_EQ(ErrorCode::OK, error);

  UTUtils::print(
      "-- After removing ordinal position=", ordinal_position_to_remove,
      " get column statistics by get_one_column_statistic start --");

  for (ObjectIdType ordinal_position = 1;
       static_cast<size_t>(ordinal_position) <=
       column_statistics_to_update.size();
       ordinal_position++) {
    error = DaoTestColumnStatistics::get_one_column_statistic(
        ret_table_id, ordinal_position,
        column_statistics_to_update[ordinal_position - 1]);

    if (ordinal_position_to_remove == ordinal_position) {
      EXPECT_EQ(ErrorCode::ID_NOT_FOUND, error);
    } else {
      EXPECT_EQ(ErrorCode::OK, error);
    }
  }

  UTUtils::print(
      "-- After removing ordinal position=", ordinal_position_to_remove,
      " get column statistics by get_one_column_statistic end --");

  error = DaoTestColumnStatistics::get_all_column_statistics(
      ret_table_id, column_statistics_to_update, ordinal_position_to_remove);

  if (column_statistics_to_update.size() == 1) {
    EXPECT_EQ(ErrorCode::ID_NOT_FOUND, error);
  } else {
    EXPECT_EQ(ErrorCode::OK, error);
  }

  /**
   * remove_all_column_statistics
   * based on existing table id.
   */
  error = DaoTestColumnStatistics::remove_all_column_statistics(ret_table_id);

  if (column_statistics_to_update.size() == 1) {
    EXPECT_EQ(ErrorCode::ID_NOT_FOUND, error);
  } else {
    EXPECT_EQ(ErrorCode::OK, error);
  }

  error = DaoTestColumnStatistics::get_all_column_statistics(ret_table_id,
                                                             column_statistics);

  for (ObjectIdType ordinal_position = 1;
       static_cast<size_t>(ordinal_position) <=
       column_statistics_to_update.size();
       ordinal_position++) {
    error = DaoTestColumnStatistics::get_one_column_statistic(
        ret_table_id, ordinal_position,
        column_statistics_to_update[ordinal_position - 1]);
    EXPECT_EQ(ErrorCode::ID_NOT_FOUND, error);
  }

  // remove table metadata.
  TableMetadataHelper::remove_table(ret_table_id);
}

/**
 * @brief happy test to remove all column statistics
 * based on both existing table id.
 *
 * - add_one_column_statistic:
 *      remove all column statistics
 *      based on both existing table id.
 */
TEST_P(DaoTestColumnStatisticsRemoveAllHappy, remove_all_column_statistics) {
  auto param = GetParam();

  UTTableMetadata* testdata_table_metadata =
      global->testdata_table_metadata.get();
  std::string table_name = testdata_table_metadata->name + param;

  ObjectIdType ret_table_id;
  TableMetadataHelper::add_table(table_name, &ret_table_id);

  /**
   * add new column statistics
   * based on both existing table id and column ordinal position.
   */
  std::vector<ptree> column_statistics = global->column_statistics;
  DaoTestColumnStatistics::add_column_statistics(ret_table_id,
                                                 column_statistics);
  /**
   * check if results of column statistics are expected or not.
   */

  UTUtils::print(
      "-- get column statistics by get_one_column_statistic start --");

  ErrorCode error;
  for (ObjectIdType ordinal_position = 1;
       static_cast<size_t>(ordinal_position) <= column_statistics.size();
       ordinal_position++) {
    error = DaoTestColumnStatistics::get_one_column_statistic(
        ret_table_id, ordinal_position,
        column_statistics[ordinal_position - 1]);
    EXPECT_EQ(ErrorCode::OK, error);
  }

  error = DaoTestColumnStatistics::get_all_column_statistics(ret_table_id,
                                                             column_statistics);
  EXPECT_EQ(ErrorCode::OK, error);

  /**
   * remove_all_column_statistics
   * based on existing table id.
   */
  error = DaoTestColumnStatistics::remove_all_column_statistics(ret_table_id);
  EXPECT_EQ(ErrorCode::OK, error);

  error = DaoTestColumnStatistics::get_all_column_statistics(ret_table_id,
                                                             column_statistics);
  EXPECT_EQ(ErrorCode::ID_NOT_FOUND, error);

  for (ObjectIdType ordinal_position = 1;
       static_cast<size_t>(ordinal_position) <= column_statistics.size();
       ordinal_position++) {
    error = DaoTestColumnStatistics::get_one_column_statistic(
        ret_table_id, ordinal_position,
        column_statistics[ordinal_position - 1]);
    EXPECT_EQ(ErrorCode::ID_NOT_FOUND, error);
  }

  // remove table metadata.
  TableMetadataHelper::remove_table(ret_table_id);
}

/**
 * @brief Exception path test for all API.
 * 1. add/get/remove one column statistic
 * based on non-existing table id or
 * non-existing column ordinal position.
 *
 * 2. get/remove all column statistics
 * based on non-existing table id.
 *
 * -
 * add_one_column_statistic/get_one_column_statistic/remove_one_column_statistic:
 *      - based on non-existing column ordinal position
 *                 and existing table id.
 *      - based on non-existing table id
 *                 and existing column ordinal position.
 *      - based on both non-existing table id and column ordinal position.
 * -  get_all_column_statistics/remove_all_column_statistics:
 *      - based on non-existing table id.
 */
TEST_P(DaoTestColumnStatisticsAllAPIException, all_api_exception) {
  auto param = GetParam();
  UTTableMetadata* testdata_table_metadata =
      global->testdata_table_metadata.get();
  std::string table_name = testdata_table_metadata->name + param;

  ObjectIdType ret_table_id;
  TableMetadataHelper::add_table(table_name, &ret_table_id);

  std::vector<ptree> column_statistics = global->column_statistics;
  DaoTestColumnStatistics::add_column_statistics(ret_table_id,
                                                 column_statistics);

  ErrorCode error;
  for (ObjectIdType ordinal_position = 1;
       static_cast<size_t>(ordinal_position) <= column_statistics.size();
       ordinal_position++) {
    error = DaoTestColumnStatistics::get_one_column_statistic(
        ret_table_id, ordinal_position,
        column_statistics[ordinal_position - 1]);
    EXPECT_EQ(ErrorCode::OK, error);
  }

  /**
   * add_one_column_statistic
   * based on non-existing column ordinal position
   * or non-existing table id.
   */
  for (ObjectIdType ordinal_position : global->column_number_not_exists) {
    // ordinal position only not exists
    error = DaoTestColumnStatistics::add_one_column_statistic(
        ret_table_id, ordinal_position, column_statistics[0]);
    EXPECT_EQ(ErrorCode::INVALID_PARAMETER, error);

    for (ObjectIdType table_id : global->table_id_not_exists) {
      // table id and ordinal position not exists
      error = DaoTestColumnStatistics::add_one_column_statistic(
          table_id, ordinal_position, column_statistics[0]);
      EXPECT_EQ(ErrorCode::INVALID_PARAMETER, error);
    }
  }

  ObjectIdType ordinal_position_exists = 1;
  for (ObjectIdType table_id : global->table_id_not_exists) {
    // table id only not exists
    error = DaoTestColumnStatistics::add_one_column_statistic(
        table_id, ordinal_position_exists, column_statistics[0]);
    EXPECT_EQ(ErrorCode::INVALID_PARAMETER, error);
  }

  /**
   * get_all_column_statistics
   * based on non-existing table id.
   */
  std::vector<ptree> empty_column_statistics;
  for (ObjectIdType table_id : global->table_id_not_exists) {
    // table id only not exists
    error = DaoTestColumnStatistics::get_all_column_statistics(
        table_id, empty_column_statistics);
    EXPECT_EQ(ErrorCode::ID_NOT_FOUND, error);
  }

  /**
   * get_one_column_statistic
   * based on non-existing column ordinal position
   * or non-existing table id.
   */
  ptree empty_column_statistic;
  for (ObjectIdType ordinal_position : global->column_number_not_exists) {
    // ordinal position only not exists
    error = DaoTestColumnStatistics::get_one_column_statistic(
        ret_table_id, ordinal_position, empty_column_statistic);
    EXPECT_EQ(ErrorCode::ID_NOT_FOUND, error);

    for (ObjectIdType table_id : global->table_id_not_exists) {
      // table id and ordinal position not exists
      error = DaoTestColumnStatistics::get_one_column_statistic(
          table_id, ordinal_position, empty_column_statistic);
      EXPECT_EQ(ErrorCode::ID_NOT_FOUND, error);
    }
  }

  for (ObjectIdType table_id : global->table_id_not_exists) {
    // table id only not exists
    error = DaoTestColumnStatistics::get_one_column_statistic(
        table_id, ordinal_position_exists, empty_column_statistic);
    EXPECT_EQ(ErrorCode::ID_NOT_FOUND, error);
  }

  /**
   * remove_one_column_statistic
   * based on non-existing column ordinal position
   * or non-existing table id.
   */
  for (ObjectIdType ordinal_position : global->column_number_not_exists) {
    // ordinal position only not exists
    error = DaoTestColumnStatistics::remove_one_column_statistic(
        ret_table_id, ordinal_position);
    EXPECT_EQ(ErrorCode::ID_NOT_FOUND, error);

    for (ObjectIdType table_id : global->table_id_not_exists) {
      // table id and ordinal position not exists
      error = DaoTestColumnStatistics::remove_one_column_statistic(
          table_id, ordinal_position);
      EXPECT_EQ(ErrorCode::ID_NOT_FOUND, error);
    }
  }

  for (ObjectIdType table_id : global->table_id_not_exists) {
    // table id only not exists
    error = DaoTestColumnStatistics::remove_one_column_statistic(
        table_id, ordinal_position_exists);
    EXPECT_EQ(ErrorCode::ID_NOT_FOUND, error);
  }

  /**
   * remove_all_column_statistics
   * based on non-existing table id.
   */
  for (ObjectIdType table_id : global->table_id_not_exists) {
    // table id not exists
    error = DaoTestColumnStatistics::remove_all_column_statistics(table_id);
    EXPECT_EQ(ErrorCode::ID_NOT_FOUND, error);
  }

  // remove table metadata.
  TableMetadataHelper::remove_table(ret_table_id);
}

TEST_F(DaoTestColumnStatisticsAllAPIException,
       upsert_one_column_statistics_in_nullptr) {
  ErrorCode error = ErrorCode::INTERNAL_ERROR;

  UTTableMetadata* testdata_table_metadata =
      global->testdata_table_metadata.get();
  std::string table_name     = testdata_table_metadata->name + "_empty";
  std::string statistic_name = "statistic-name";

  ObjectIdType ret_table_id;
  TableMetadataHelper::add_table(table_name, &ret_table_id);

  std::shared_ptr<db::GenericDAO> s_gdao = nullptr;
  DBSessionManager db_session_manager;

  error =
      db_session_manager.get_dao(db::GenericDAO::TableName::STATISTICS, s_gdao);
  EXPECT_EQ(ErrorCode::OK, error);

  std::shared_ptr<db::StatisticsDAO> sdao;
  sdao = std::static_pointer_cast<db::StatisticsDAO>(s_gdao);

  error = db_session_manager.start_transaction();
  EXPECT_EQ(ErrorCode::OK, error);

  ptree column_statistic;
  std::int64_t ordinal_position = 1;
  ObjectIdType ret_statistic_id;

  error = sdao->upsert_column_statistic(
      ret_table_id, Statistics::COLUMN_NUMBER, std::to_string(ordinal_position),
      &statistic_name, column_statistic, ret_statistic_id);

  EXPECT_EQ(ErrorCode::OK, error);
  EXPECT_GT(ret_statistic_id, 0);

  UTUtils::print(" statistic id: ", ret_statistic_id);
  UTUtils::print(" ordinal position: ", ordinal_position);
  UTUtils::print(" column statistics: null");

  ErrorCode rollback_error = db_session_manager.rollback();
  EXPECT_EQ(ErrorCode::OK, rollback_error);

  // remove table metadata.
  TableMetadataHelper::remove_table(ret_table_id);
}

}  // namespace manager::metadata::testing
