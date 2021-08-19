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

#include "test/dao_test/dao_test_column_statistics.h"

#include <gtest/gtest.h>
#include <boost/property_tree/json_parser.hpp>
#include <memory>
#include <string>
#include <unordered_map>
#include <vector>

#include "manager/metadata/dao/common/message.h"
#include "manager/metadata/dao/generic_dao.h"
#include "manager/metadata/dao/postgresql/db_session_manager.h"
#include "manager/metadata/dao/statistics_dao.h"
#include "manager/metadata/entity/column_statistic.h"
#include "manager/metadata/error_code.h"

#include "test/api_test_column_statistics.h"
#include "test/dao_test/dao_test_table_metadatas.h"
#include "test/global_test_environment.h"
#include "test/utility/ut_utils.h"

namespace manager::metadata::testing {

namespace storage = manager::metadata::db::postgresql;
using namespace boost::property_tree;
using namespace manager::metadata;
using namespace manager::metadata::db;

class DaoTestColumnStatisticsAllAPIHappy
    : public ::testing::TestWithParam<TupleApiTestColumnStatisticsAllAPI> {
  void SetUp() override { UTUtils::skip_if_connection_not_opened(); }
};
class DaoTestColumnStatisticsUpdateHappy
    : public ::testing::TestWithParam<TupleApiTestColumnStatisticsUpdate> {
  void SetUp() override { UTUtils::skip_if_connection_not_opened(); }
};
class DaoTestColumnStatisticsRemoveAllHappy
    : public ::testing::TestWithParam<std::string> {
  void SetUp() override { UTUtils::skip_if_connection_not_opened(); }
};
class DaoTestColumnStatisticsAllAPIException
    : public ::testing::TestWithParam<std::string> {
  void SetUp() override { UTUtils::skip_if_connection_not_opened(); }
};

INSTANTIATE_TEST_CASE_P(
    ParamtererizedTest, DaoTestColumnStatisticsAllAPIHappy,
    ::testing::ValuesIn(
        ApiTestColumnStatistics::
            make_tuple_for_api_test_column_statistics_all_api_happy("3")));
INSTANTIATE_TEST_CASE_P(
    ParamtererizedTest, DaoTestColumnStatisticsUpdateHappy,
    ::testing::ValuesIn(
        ApiTestColumnStatistics::
            make_tuple_for_api_test_column_statistics_update_happy("4")));
INSTANTIATE_TEST_CASE_P(ParamtererizedTest,
                        DaoTestColumnStatisticsRemoveAllHappy,
                        ::testing::Values("_ColumnStatistic_5"));
INSTANTIATE_TEST_CASE_P(ParamtererizedTest,
                        DaoTestColumnStatisticsAllAPIException,
                        ::testing::Values("_ColumnStatistic_6"));

/**
 *  @brief  Add column statistics based on the given table id and
 *  the given ptree type column statistics.
 *  @param  (table_id)              [in]  table id.
 *  @param  (column_statistics)     [in]  ptree type column statistics.
 *  @return none.
 */
void DaoTestColumnStatistics::add_column_statistics(
    ObjectIdType table_id, std::vector<ptree> column_statistics) {
  UTUtils::print(
      " -- add column statistics by add_one_column_statistic start --");
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
      " -- add column statistics by add_one_column_statistic end -- \n");
}

/**
 *  @brief  Adds or updates one column statistic
 *  to the column statistics table
 *  based on the given table id and the given column ordinal position.
 *  Adds one column statistic if it not exists in the metadata repository.
 *  Updates one column statistic if it already exists.
 *  @param  (table_id)          [in]  table id.
 *  @param  (ordinal_position)  [in]  column ordinal position.
 *  @param  (column_statistic)  [in]  one column statistic to add or update.
 *  @return ErrorCode::OK if success, otherwise an error code.
 */
ErrorCode DaoTestColumnStatistics::add_one_column_statistic(
    ObjectIdType table_id, ObjectIdType ordinal_position,
    boost::property_tree::ptree& column_statistic) {
  ErrorCode error = ErrorCode::INTERNAL_ERROR;

  std::shared_ptr<GenericDAO> s_gdao = nullptr;
  storage::DBSessionManager db_session_manager;

  error = db_session_manager.get_dao(GenericDAO::TableName::STATISTICS, s_gdao);
  EXPECT_EQ(ErrorCode::OK, error);

  std::shared_ptr<StatisticsDAO> sdao;
  sdao = std::static_pointer_cast<StatisticsDAO>(s_gdao);

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

  error = sdao->upsert_one_column_statistic_by_table_id_column_ordinal_position(
      table_id, ordinal_position, s_column_statistic);

  if (error == ErrorCode::OK) {
    ErrorCode commit_error = db_session_manager.commit();
    EXPECT_EQ(ErrorCode::OK, commit_error);

    UTUtils::print("ordinal position:", ordinal_position);
    UTUtils::print("column statistics:" + s_column_statistic);

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
 *  @brief  Gets one column statistic from the column statistics table
 *  based on the given table id and the given column ordinal position.
 *  @param  (table_id)                  [in]  table id.
 *  @param  (ordinal_position)          [in]  column ordinal position.
 *  @param  (expected_column_statistic) [in]  expected column statistic with the
 * specified table id and column ordinal position.
 *  @return ErrorCode::OK if success, otherwise an error code.
 */
ErrorCode DaoTestColumnStatistics::get_one_column_statistic(
    ObjectIdType table_id, ObjectIdType ordinal_position,
    const ptree& expected_column_statistic) {
  ErrorCode error = ErrorCode::INTERNAL_ERROR;

  std::shared_ptr<GenericDAO> s_gdao = nullptr;
  storage::DBSessionManager db_session_manager;

  error = db_session_manager.get_dao(GenericDAO::TableName::STATISTICS, s_gdao);
  EXPECT_EQ(ErrorCode::OK, error);

  std::shared_ptr<StatisticsDAO> sdao;
  sdao = std::static_pointer_cast<StatisticsDAO>(s_gdao);

  ColumnStatistic column_statistic;
  error = sdao->select_one_column_statistic_by_table_id_column_ordinal_position(
      table_id, ordinal_position, column_statistic);

  if (error == ErrorCode::OK) {
    std::string s_cs_returned =
        UTUtils::get_tree_string(column_statistic.column_statistic);
    std::string s_cs_expected =
        UTUtils::get_tree_string(expected_column_statistic);

    EXPECT_EQ(s_cs_returned, s_cs_expected);

    UTUtils::print("ordinal position:", column_statistic.ordinal_position);
    UTUtils::print("column statistic:" + s_cs_returned);
  }

  return error;
}

/**
 *  @brief  Gets all column statistics from the column statistics table
 *  based on the given table id.
 *  @param  (table_id)                    [in] table id.
 *  @param  (column_statistics_expected)  [in] all column statistics
 *  expected with the specified table id.
 *  @return ErrorCode::OK if success, otherwise an error code.
 */
ErrorCode DaoTestColumnStatistics::get_all_column_statistics(
    ObjectIdType table_id, std::vector<ptree> column_statistics_expected) {
  ErrorCode error = ErrorCode::INTERNAL_ERROR;

  std::shared_ptr<GenericDAO> s_gdao = nullptr;
  storage::DBSessionManager db_session_manager;

  error = db_session_manager.get_dao(GenericDAO::TableName::STATISTICS, s_gdao);
  EXPECT_EQ(ErrorCode::OK, error);

  std::shared_ptr<StatisticsDAO> sdao;
  sdao = std::static_pointer_cast<StatisticsDAO>(s_gdao);

  std::unordered_map<ObjectIdType, ColumnStatistic> column_statistics;
  error = sdao->select_all_column_statistic_by_table_id(table_id,
                                                        column_statistics);

  if (error == ErrorCode::OK) {
    UTUtils::print(
        " -- get column statistics by get_all_column_statistics start --");

    for (ObjectIdType ordinal_position = 1;
         static_cast<size_t>(ordinal_position) <= column_statistics.size();
         ordinal_position++) {
      ColumnStatistic c_cs_returned = column_statistics[ordinal_position];

      std::string s_cs_returned =
          UTUtils::get_tree_string(c_cs_returned.column_statistic);
      std::string s_cs_expected = UTUtils::get_tree_string(
          column_statistics_expected[ordinal_position - 1]);

      EXPECT_EQ(s_cs_expected, s_cs_returned);

      UTUtils::print("ordinal position:", c_cs_returned.ordinal_position);
      UTUtils::print("column statistic:" + s_cs_returned);
    }

    UTUtils::print(
        " -- get column statistics by get_all_column_statistics end -- \n");
  } else {
    EXPECT_EQ(column_statistics.size(), 0);
  }

  return error;
}

/**
 *  @brief  Gets all column statistics from the column statistics table
 *  based on the given table id.
 *  @param  (table_id)                    [in] table id.
 *  @param  (column_statistics_expected)  [in] all column statistics
 *  expected with the specified table id.
 *  @param  (ordinal_position_removed)    [in] ordinal position of
 *  column statistics removed.
 *  @return ErrorCode::OK if success, otherwise an error code.
 */
ErrorCode DaoTestColumnStatistics::get_all_column_statistics(
    ObjectIdType table_id, std::vector<ptree> column_statistics_expected,
    ObjectIdType ordinal_position_removed) {
  ErrorCode error = ErrorCode::INTERNAL_ERROR;

  std::shared_ptr<GenericDAO> s_gdao = nullptr;
  storage::DBSessionManager db_session_manager;

  error = db_session_manager.get_dao(GenericDAO::TableName::STATISTICS, s_gdao);
  EXPECT_EQ(ErrorCode::OK, error);

  std::shared_ptr<StatisticsDAO> sdao;
  sdao = std::static_pointer_cast<StatisticsDAO>(s_gdao);

  std::unordered_map<ObjectIdType, ColumnStatistic> column_statistics;
  error = sdao->select_all_column_statistic_by_table_id(table_id,
                                                        column_statistics);

  if (error == ErrorCode::OK) {
    UTUtils::print(
        " -- After removing ordinal position=", ordinal_position_removed,
        " get column statistics by get_all_column_statistics start --");

    for (ObjectIdType ordinal_position = 1;
         static_cast<size_t>(ordinal_position) <= column_statistics.size();
         ordinal_position++) {
      auto got = column_statistics.find(ordinal_position);

      if (got == column_statistics.end()) {
        EXPECT_TRUE(ordinal_position_removed == ordinal_position);
      } else {
        ColumnStatistic c_cs_returned = got->second;
        std::string s_cs_returned =
            UTUtils::get_tree_string(c_cs_returned.column_statistic);
        std::string s_cs_expected = UTUtils::get_tree_string(
            column_statistics_expected[ordinal_position - 1]);

        EXPECT_EQ(s_cs_expected, s_cs_returned);

        UTUtils::print("ordinal position:", c_cs_returned.ordinal_position);
        UTUtils::print("column statistic:" + s_cs_returned);
      }
    }

    EXPECT_EQ(column_statistics_expected.size() - 1, column_statistics.size());

    UTUtils::print(
        " -- After removing ordinal position=", ordinal_position_removed,
        " get column statistics by get_all_column_statistics end --");
  } else {
    EXPECT_EQ(column_statistics.size(), 0);
  }

  return error;
}

/**
 *  @brief  Removes one column statistic from the column statistics table
 *  based on the given table id and the given column ordinal position.
 *  @param  (table_id)          [in]  table id.
 *  @param  (ordinal_position)  [in]  column ordinal position.
 *  @return ErrorCode::OK if success, otherwise an error code.
 */
ErrorCode DaoTestColumnStatistics::remove_one_column_statistic(
    ObjectIdType table_id, ObjectIdType ordinal_position) {
  ErrorCode error = ErrorCode::INTERNAL_ERROR;

  std::shared_ptr<GenericDAO> s_gdao = nullptr;
  storage::DBSessionManager db_session_manager;

  error = db_session_manager.get_dao(GenericDAO::TableName::STATISTICS, s_gdao);
  EXPECT_EQ(ErrorCode::OK, error);

  std::shared_ptr<StatisticsDAO> sdao;
  sdao = std::static_pointer_cast<StatisticsDAO>(s_gdao);

  error = db_session_manager.start_transaction();
  EXPECT_EQ(ErrorCode::OK, error);

  error = sdao->delete_one_column_statistic_by_table_id_column_ordinal_position(
      table_id, ordinal_position);

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
 *  @brief  Removes all column statistics
 *  from the column statistics table
 *  based on the given table id.
 *  @param  (table_id)          [in]  table id.
 *  @return ErrorCode::OK if success, otherwise an error code.
 */
ErrorCode DaoTestColumnStatistics::remove_all_column_statistics(
    ObjectIdType table_id) {
  ErrorCode error = ErrorCode::INTERNAL_ERROR;

  std::shared_ptr<GenericDAO> s_gdao = nullptr;
  storage::DBSessionManager db_session_manager;

  error = db_session_manager.get_dao(GenericDAO::TableName::STATISTICS, s_gdao);
  EXPECT_EQ(ErrorCode::OK, error);

  std::shared_ptr<StatisticsDAO> sdao;
  sdao = std::static_pointer_cast<StatisticsDAO>(s_gdao);

  error = db_session_manager.start_transaction();
  EXPECT_EQ(ErrorCode::OK, error);

  error = sdao->delete_all_column_statistic_by_table_id(table_id);

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
  DaoTestTableMetadata::add_table(table_name, &ret_table_id);

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
      " -- get column statistics by get_one_column_statistic start --");

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
      " -- get column statistics by get_one_column_statistic end -- \n");

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
      EXPECT_EQ(ErrorCode::NOT_FOUND, error);
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
  EXPECT_EQ(ErrorCode::NOT_FOUND, error);

  for (ObjectIdType ordinal_position = 1;
       static_cast<size_t>(ordinal_position) <= column_statistics.size();
       ordinal_position++) {
    error = DaoTestColumnStatistics::get_one_column_statistic(
        ret_table_id, ordinal_position,
        column_statistics[ordinal_position - 1]);
    EXPECT_EQ(ErrorCode::NOT_FOUND, error);
  }
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
  DaoTestTableMetadata::add_table(table_name, &ret_table_id);

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
      " -- get column statistics by get_one_column_statistic start --");

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
      " -- get column statistics by get_one_column_statistic end -- \n");

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
      " -- After updating all column statistics, get column statistics by ",
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
      " -- After updating all column statistics, get column statistics by ",
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
      " -- After removing ordinal position=", ordinal_position_to_remove,
      " get column statistics by get_one_column_statistic start --");

  for (ObjectIdType ordinal_position = 1;
       static_cast<size_t>(ordinal_position) <=
       column_statistics_to_update.size();
       ordinal_position++) {
    error = DaoTestColumnStatistics::get_one_column_statistic(
        ret_table_id, ordinal_position,
        column_statistics_to_update[ordinal_position - 1]);

    if (ordinal_position_to_remove == ordinal_position) {
      EXPECT_EQ(ErrorCode::NOT_FOUND, error);
    } else {
      EXPECT_EQ(ErrorCode::OK, error);
    }
  }

  UTUtils::print(
      " -- After removing ordinal position=", ordinal_position_to_remove,
      " get column statistics by get_one_column_statistic end --");

  error = DaoTestColumnStatistics::get_all_column_statistics(
      ret_table_id, column_statistics_to_update, ordinal_position_to_remove);

  if (column_statistics_to_update.size() == 1) {
    EXPECT_EQ(ErrorCode::NOT_FOUND, error);
  } else {
    EXPECT_EQ(ErrorCode::OK, error);
  }

  /**
   * remove_all_column_statistics
   * based on existing table id.
   */
  error = DaoTestColumnStatistics::remove_all_column_statistics(ret_table_id);

  if (column_statistics_to_update.size() == 1) {
    EXPECT_EQ(ErrorCode::NOT_FOUND, error);
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
    EXPECT_EQ(ErrorCode::NOT_FOUND, error);
  }
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
  DaoTestTableMetadata::add_table(table_name, &ret_table_id);

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
      " -- get column statistics by get_one_column_statistic start --");

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
  EXPECT_EQ(ErrorCode::NOT_FOUND, error);

  for (ObjectIdType ordinal_position = 1;
       static_cast<size_t>(ordinal_position) <= column_statistics.size();
       ordinal_position++) {
    error = DaoTestColumnStatistics::get_one_column_statistic(
        ret_table_id, ordinal_position,
        column_statistics[ordinal_position - 1]);
    EXPECT_EQ(ErrorCode::NOT_FOUND, error);
  }
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
TEST_P(DaoTestColumnStatisticsAllAPIException, All_API_exception) {
  auto param = GetParam();
  UTTableMetadata* testdata_table_metadata =
      global->testdata_table_metadata.get();
  std::string table_name = testdata_table_metadata->name + param;

  ObjectIdType ret_table_id;
  DaoTestTableMetadata::add_table(table_name, &ret_table_id);

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
  for (ObjectIdType ordinal_position : global->ordinal_position_not_exists) {
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
    EXPECT_EQ(ErrorCode::NOT_FOUND, error);
  }

  /**
   * get_one_column_statistic
   * based on non-existing column ordinal position
   * or non-existing table id.
   */
  ptree empty_column_statistic;
  for (ObjectIdType ordinal_position : global->ordinal_position_not_exists) {
    // ordinal position only not exists
    error = DaoTestColumnStatistics::get_one_column_statistic(
        ret_table_id, ordinal_position, empty_column_statistic);
    EXPECT_EQ(ErrorCode::NOT_FOUND, error);

    for (ObjectIdType table_id : global->table_id_not_exists) {
      // table id and ordinal position not exists
      error = DaoTestColumnStatistics::get_one_column_statistic(
          table_id, ordinal_position, empty_column_statistic);
      EXPECT_EQ(ErrorCode::NOT_FOUND, error);
    }
  }

  for (ObjectIdType table_id : global->table_id_not_exists) {
    // table id only not exists
    error = DaoTestColumnStatistics::get_one_column_statistic(
        table_id, ordinal_position_exists, empty_column_statistic);
    EXPECT_EQ(ErrorCode::NOT_FOUND, error);
  }

  /**
   * remove_one_column_statistic
   * based on non-existing column ordinal position
   * or non-existing table id.
   */
  for (ObjectIdType ordinal_position : global->ordinal_position_not_exists) {
    // ordinal position only not exists
    error = DaoTestColumnStatistics::remove_one_column_statistic(
        ret_table_id, ordinal_position);
    EXPECT_EQ(ErrorCode::NOT_FOUND, error);

    for (ObjectIdType table_id : global->table_id_not_exists) {
      // table id and ordinal position not exists
      error = DaoTestColumnStatistics::remove_one_column_statistic(
          table_id, ordinal_position);
      EXPECT_EQ(ErrorCode::NOT_FOUND, error);
    }
  }

  for (ObjectIdType table_id : global->table_id_not_exists) {
    // table id only not exists
    error = DaoTestColumnStatistics::remove_one_column_statistic(
        table_id, ordinal_position_exists);
    EXPECT_EQ(ErrorCode::NOT_FOUND, error);
  }

  /**
   * remove_all_column_statistics
   * based on non-existing table id.
   */
  for (ObjectIdType table_id : global->table_id_not_exists) {
    // table id not exists
    error = DaoTestColumnStatistics::remove_all_column_statistics(table_id);
    EXPECT_EQ(ErrorCode::NOT_FOUND, error);
  }
}

TEST_F(DaoTestColumnStatisticsAllAPIException,
       upsert_one_column_statistics_in_non_json_format) {
  ErrorCode error = ErrorCode::INTERNAL_ERROR;

  UTTableMetadata* testdata_table_metadata =
      global->testdata_table_metadata.get();
  std::string s_column_statistic = "{not_json";
  std::string table_name = testdata_table_metadata->name + s_column_statistic;

  ObjectIdType ret_table_id;
  DaoTestTableMetadata::add_table(table_name, &ret_table_id);

  std::shared_ptr<GenericDAO> s_gdao = nullptr;
  storage::DBSessionManager db_session_manager;
  error = db_session_manager.get_dao(GenericDAO::TableName::STATISTICS, s_gdao);
  EXPECT_EQ(ErrorCode::OK, error);

  std::shared_ptr<StatisticsDAO> sdao;
  sdao = std::static_pointer_cast<StatisticsDAO>(s_gdao);

  error = db_session_manager.start_transaction();
  EXPECT_EQ(ErrorCode::OK, error);

  ObjectIdType ordinal_position = 1;
  error = sdao->upsert_one_column_statistic_by_table_id_column_ordinal_position(
      ret_table_id, ordinal_position, s_column_statistic);

  EXPECT_EQ(ErrorCode::INVALID_PARAMETER, error);

  UTUtils::print("ordinal position:", ordinal_position);
  UTUtils::print("column statistics:" + s_column_statistic);

  ErrorCode rollback_error = db_session_manager.rollback();
  EXPECT_EQ(ErrorCode::OK, rollback_error);
}

}  // namespace manager::metadata::testing
