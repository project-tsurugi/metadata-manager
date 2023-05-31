/*
 * Copyright 2020-2022 tsurugi project.
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

#include "manager/metadata/metadata_factory.h"
#include "test/common/global_test_environment.h"
#include "test/common/ut_utils.h"
#include "test/helper/column_statistics_helper.h"
#include "test/metadata/ut_column_statistics.h"

namespace manager::metadata::testing {

using boost::property_tree::ptree;

class ApiTestDBAccessFailure : public ::testing::Test {
  void SetUp() override {
    UTUtils::skip_if_json();
    UTUtils::skip_if_connection_opened();
  }
};

class ApiTestDBAccessFailureByTableId
    : public ::testing::TestWithParam<ObjectIdType> {
  void SetUp() override {
    UTUtils::skip_if_json();
    UTUtils::skip_if_connection_opened();
  }
};

class ApiTestDBAccessFailureByTableName
    : public ::testing::TestWithParam<std::string> {
  void SetUp() override {
    UTUtils::skip_if_json();
    UTUtils::skip_if_connection_opened();
  }
};

class ApiTestDBAccessFailureByTableIdReltuples
    : public ::testing::TestWithParam<std::tuple<ObjectIdType, int64_t>> {
  void SetUp() override {
    UTUtils::skip_if_json();
    UTUtils::skip_if_connection_opened();
  }
};

class ApiTestDBAccessFailureByTableNameReltuples
    : public ::testing::TestWithParam<std::tuple<std::string, int64_t>> {
  void SetUp() override {
    UTUtils::skip_if_json();
    UTUtils::skip_if_connection_opened();
  }
};

class ApiTestDBAccessFailureByTableIdColumnNumber
    : public ::testing::TestWithParam<std::tuple<ObjectIdType, ObjectIdType>> {
  void SetUp() override {
    UTUtils::skip_if_json();
    UTUtils::skip_if_connection_opened();
  }
};

class ApiTestDBAccessFailureByColumnStatistics
    : public ::testing::TestWithParam<std::tuple<ObjectIdType, ObjectIdType>> {
  void SetUp() override {
    UTUtils::skip_if_json();
    UTUtils::skip_if_connection_opened();
  }
};

std::vector<ObjectIdType> table_id_not_exists_dbaf = {
    -1,
    0,
    INT64_MAX - 1,
    INT64_MAX,
    std::numeric_limits<ObjectIdType>::infinity(),
    -std::numeric_limits<ObjectIdType>::infinity(),
    std::numeric_limits<ObjectIdType>::quiet_NaN()};

std::vector<ObjectIdType> column_number_not_exists_dbaf = {
    -1,
    0,
    INT64_MAX - 1,
    INT64_MAX,
    4,
    std::numeric_limits<ObjectIdType>::infinity(),
    -std::numeric_limits<ObjectIdType>::infinity(),
    std::numeric_limits<ObjectIdType>::quiet_NaN()};

std::vector<int64_t> reltuples_dbaf = {
    -1,
    0,
    1,
    100000000,
    INT64_MAX,
    std::numeric_limits<int64_t>::infinity(),
    -std::numeric_limits<int64_t>::infinity(),
    std::numeric_limits<int64_t>::quiet_NaN(),
    static_cast<int64_t>(INT64_MAX),
    static_cast<int64_t>(INT64_MIN)};

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
    ParameterizedTest, ApiTestDBAccessFailureByTableIdColumnNumber,
    ::testing::Combine(::testing::ValuesIn(table_id_not_exists_dbaf),
                       ::testing::ValuesIn(column_number_not_exists_dbaf)));

INSTANTIATE_TEST_CASE_P(
    ParameterizedTest, ApiTestDBAccessFailureByColumnStatistics,
    ::testing::Combine(::testing::ValuesIn(table_id_not_exists_dbaf),
                       ::testing::ValuesIn(column_number_not_exists_dbaf)));

/**
 * @brief API to add table metadata
 *   return ErrorCode::DATABASE_ACCESS_FAILURE
 */
TEST_F(ApiTestDBAccessFailure, add_table_metadata) {
  auto tables = get_tables_ptr(GlobalTestEnvironment::TEST_DB);

  ErrorCode error = tables->init();
  EXPECT_EQ(ErrorCode::DATABASE_ACCESS_FAILURE, error);

  ptree table_metadata;
  ptree column_metadata;
  table_metadata.put(Table::NAME, "dummy_name");
  table_metadata.add_child(Table::COLUMNS_NODE, column_metadata);

  ObjectIdType ret_table_id = -1;
  // Execute the test.
  error = tables->add(table_metadata, &ret_table_id);
  EXPECT_EQ(ErrorCode::DATABASE_ACCESS_FAILURE, error);
  EXPECT_EQ(ret_table_id, -1);
}

/**
 * @brief API to get table metadata based on table id
 *   return ErrorCode::DATABASE_ACCESS_FAILURE
 */
TEST_F(ApiTestDBAccessFailure, get_table_metadata_by_table_id) {
  ObjectIdType table_id = 1;

  auto tables = get_tables_ptr(GlobalTestEnvironment::TEST_DB);

  ErrorCode error = tables->init();
  EXPECT_EQ(ErrorCode::DATABASE_ACCESS_FAILURE, error);

  ptree table_metadata;
  // Execute the test.
  error = tables->get(table_id, table_metadata);
  EXPECT_EQ(ErrorCode::DATABASE_ACCESS_FAILURE, error);

  ptree empty_ptree;
  EXPECT_EQ(UTUtils::get_tree_string(empty_ptree),
            UTUtils::get_tree_string(table_metadata));
}

/**
 * @brief API to get table metadata based on table name
 *   return ErrorCode::DATABASE_ACCESS_FAILURE
 */
TEST_F(ApiTestDBAccessFailure, get_table_metadata_by_table_name) {
  auto tables = get_tables_ptr(GlobalTestEnvironment::TEST_DB);

  ErrorCode error = tables->init();
  EXPECT_EQ(ErrorCode::DATABASE_ACCESS_FAILURE, error);

  ptree table_metadata;
  std::string table_name = "table_name";
  // Execute the test.
  error = tables->get(table_name, table_metadata);
  EXPECT_EQ(ErrorCode::DATABASE_ACCESS_FAILURE, error);

  ptree empty_ptree;
  EXPECT_EQ(UTUtils::get_tree_string(empty_ptree),
            UTUtils::get_tree_string(table_metadata));
}

/**
 * @brief API to update table metadata
 *   return ErrorCode::DATABASE_ACCESS_FAILURE
 */
TEST_F(ApiTestDBAccessFailure, update_table_metadata) {
  auto tables = get_tables_ptr(GlobalTestEnvironment::TEST_DB);

  ErrorCode error = tables->init();
  EXPECT_EQ(ErrorCode::DATABASE_ACCESS_FAILURE, error);

  ptree table_metadata;
  ptree column_metadata;
  table_metadata.put(Table::NAME, "dummy_name");
  table_metadata.add_child(Table::COLUMNS_NODE, column_metadata);

  ObjectIdType dummy_table_id = 1;
  // Execute the test.
  error = tables->update(dummy_table_id, table_metadata);
  EXPECT_EQ(ErrorCode::DATABASE_ACCESS_FAILURE, error);
}

/**
 * @brief API to remove table metadata based on table id
 *   return ErrorCode::DATABASE_ACCESS_FAILURE
 */
TEST_F(ApiTestDBAccessFailure, remove_table_metadata_by_table_id) {
  auto tables = get_tables_ptr(GlobalTestEnvironment::TEST_DB);

  ErrorCode error = tables->init();
  EXPECT_EQ(ErrorCode::DATABASE_ACCESS_FAILURE, error);

  // Execute the test.
  error = tables->remove(1);
  EXPECT_EQ(ErrorCode::DATABASE_ACCESS_FAILURE, error);
}

/**
 * @brief API to remove table metadata based on table name
 *   return ErrorCode::DATABASE_ACCESS_FAILURE
 */
TEST_F(ApiTestDBAccessFailure, remove_table_metadata_by_table_name) {
  auto tables = get_tables_ptr(GlobalTestEnvironment::TEST_DB);

  ErrorCode error = tables->init();
  EXPECT_EQ(ErrorCode::DATABASE_ACCESS_FAILURE, error);

  ObjectIdType ret_table_id = -1;
  std::string table_name    = "table_name";
  // Execute the test.
  error = tables->remove(table_name.c_str(), &ret_table_id);
  EXPECT_EQ(ErrorCode::DATABASE_ACCESS_FAILURE, error);
  EXPECT_EQ(ret_table_id, -1);
}

/**
 * @brief API to get data type metadata based on data type name
 *  return ErrorCode::DATABASE_ACCESS_FAILURE
 */
TEST_F(ApiTestDBAccessFailure, get_datatypes_by_name) {
  auto datatypes = get_datatypes_ptr(GlobalTestEnvironment::TEST_DB);

  ErrorCode error = datatypes->init();
  EXPECT_EQ(ErrorCode::DATABASE_ACCESS_FAILURE, error);

  std::string table_name = "table_name";
  ptree datatype;
  // Execute the test.
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
  // TODO(future): Change when changing Metadata class.
  auto datatypes_tmp = get_datatypes_ptr(GlobalTestEnvironment::TEST_DB);
  auto datatypes = static_cast<DataTypes*>(datatypes_tmp.get());

  ErrorCode error = datatypes->init();
  EXPECT_EQ(ErrorCode::DATABASE_ACCESS_FAILURE, error);

  std::string key   = "key";
  std::string value = "value";
  ptree datatype;
  // Execute the test.
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
  auto roles = get_roles_ptr(GlobalTestEnvironment::TEST_DB);

  ErrorCode error = roles->init();
  EXPECT_EQ(ErrorCode::DATABASE_ACCESS_FAILURE, error);

  ptree role_metadata;
  // Execute the test.
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
  auto roles = get_roles_ptr(GlobalTestEnvironment::TEST_DB);

  ErrorCode error = roles->init();
  EXPECT_EQ(ErrorCode::DATABASE_ACCESS_FAILURE, error);

  ptree role_metadata;
  // Execute the test.
  error = roles->get("role_name", role_metadata);
  EXPECT_EQ(ErrorCode::DATABASE_ACCESS_FAILURE, error);

  ptree empty_ptree;
  EXPECT_EQ(UTUtils::get_tree_string(empty_ptree),
            UTUtils::get_tree_string(role_metadata));
}

/**
 * @brief API to add constraint metadata
 *   return ErrorCode::DATABASE_ACCESS_FAILURE
 */
TEST_F(ApiTestDBAccessFailure, add_constraint_metadata) {
  // generate metadata.
  ptree new_constraints;
  new_constraints.put<ObjectId>(Constraint::TABLE_ID, 1);

  auto constraints =
      get_constraints_ptr(GlobalTestEnvironment::TEST_DB);

  ErrorCode error = constraints->init();
  EXPECT_EQ(ErrorCode::DATABASE_ACCESS_FAILURE, error);

  ObjectIdType ret_constraint_id = -1;
  // Execute the test.
  error = constraints->add(new_constraints, &ret_constraint_id);
  EXPECT_EQ(ErrorCode::DATABASE_ACCESS_FAILURE, error);
  EXPECT_EQ(ret_constraint_id, -1);
}

/**
 * @brief API to get constraint metadata based on constraint id
 *   return ErrorCode::DATABASE_ACCESS_FAILURE
 */
TEST_F(ApiTestDBAccessFailure, get_constraint_metadata) {
  ObjectIdType constraint_id = 1;

  auto constraints =
      get_constraints_ptr(GlobalTestEnvironment::TEST_DB);

  ErrorCode error = constraints->init();
  EXPECT_EQ(ErrorCode::DATABASE_ACCESS_FAILURE, error);

  ptree constraint_metadata;
  // Execute the test.
  error = constraints->get(constraint_id, constraint_metadata);
  EXPECT_EQ(ErrorCode::DATABASE_ACCESS_FAILURE, error);

  ptree empty_ptree;
  EXPECT_EQ(UTUtils::get_tree_string(empty_ptree),
            UTUtils::get_tree_string(constraint_metadata));
}

/**
 * @brief API to remove constraint metadata based on constraint id
 *   return ErrorCode::DATABASE_ACCESS_FAILURE
 */
TEST_F(ApiTestDBAccessFailure, remove_constraint_metadata) {
  auto constraints =
      get_constraints_ptr(GlobalTestEnvironment::TEST_DB);

  ErrorCode error = constraints->init();
  EXPECT_EQ(ErrorCode::DATABASE_ACCESS_FAILURE, error);

  // Execute the test.
  error = constraints->remove(1);
  EXPECT_EQ(ErrorCode::DATABASE_ACCESS_FAILURE, error);
}

/**
 * @brief API to add table statistics based on table id
 * return ErrorCode::DATABASE_ACCESS_FAILURE
 */
TEST_P(ApiTestDBAccessFailureByTableIdReltuples,
       add_table_statistic_by_table_id) {
  // TODO(future): Change when changing Metadata class.
  auto tables_tmp = get_tables_ptr(GlobalTestEnvironment::TEST_DB);
  auto tables = static_cast<Tables*>(tables_tmp.get());

  ErrorCode error = tables->init();
  EXPECT_EQ(ErrorCode::DATABASE_ACCESS_FAILURE, error);

  auto params           = GetParam();
  ObjectIdType table_id = std::get<0>(params);
  int64_t reltuples     = std::get<1>(params);

  // set table metadata.
  ptree table_meta;
  table_meta.put(Tables::ID, table_id);
  table_meta.put(Table::NUMBER_OF_TUPLES, reltuples);

  // Execute the test.
  error = tables->set_statistic(table_meta);

  auto optional_tuples =
      table_meta.get_optional<int64_t>(Table::NUMBER_OF_TUPLES);
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
  // TODO(future): Change when changing Metadata class.
  auto tables_tmp = get_tables_ptr(GlobalTestEnvironment::TEST_DB);
  auto tables = static_cast<Tables*>(tables_tmp.get());

  ErrorCode error = tables->init();
  EXPECT_EQ(ErrorCode::DATABASE_ACCESS_FAILURE, error);

  auto params            = GetParam();
  std::string table_name = std::get<0>(params);
  int64_t reltuples      = std::get<1>(params);

  // set table metadata.
  ptree table_meta;
  table_meta.put(Table::NAME, table_name);
  table_meta.put(Table::NUMBER_OF_TUPLES, reltuples);

  // Execute the test.
  error = tables->set_statistic(table_meta);

  auto optional_tuples =
      table_meta.get_optional<int64_t>(Table::NUMBER_OF_TUPLES);
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
  // TODO(future): Change when changing Metadata class.
  auto tables_tmp = get_tables_ptr(GlobalTestEnvironment::TEST_DB);
  auto tables = static_cast<Tables*>(tables_tmp.get());

  ErrorCode error = tables->init();
  EXPECT_EQ(ErrorCode::DATABASE_ACCESS_FAILURE, error);

  ObjectIdType table_id = GetParam();

  ptree table_stats;
  // Execute the test.
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
  // TODO(future): Change when changing Metadata class.
  auto tables_tmp = get_tables_ptr(GlobalTestEnvironment::TEST_DB);
  auto tables = static_cast<Tables*>(tables_tmp.get());

  ErrorCode error = tables->init();
  EXPECT_EQ(ErrorCode::DATABASE_ACCESS_FAILURE, error);

  std::string table_name = GetParam();

  ptree table_stats;
  // Execute the test.
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
  auto stats = get_statistics_ptr(GlobalTestEnvironment::TEST_DB);

  ErrorCode error = stats->init();
  EXPECT_EQ(ErrorCode::DATABASE_ACCESS_FAILURE, error);

  auto params                = GetParam();
  ObjectIdType table_id      = std::get<0>(params);
  ObjectIdType column_number = std::get<1>(params);

  std::string statistic_name = "ApiTestDBAccessFailureByColumnStatistics_" +
                               std::to_string(table_id) + "-" +
                               std::to_string(column_number);
  UtColumnStatistics ut_statistics(table_id, column_number, statistic_name);
  ptree statistic = ut_statistics.get_metadata_ptree();

  // Execute the test.
  error = stats->add(statistic);

  if ((table_id <= 0) || (column_number <= 0)) {
    EXPECT_EQ(ErrorCode::INVALID_PARAMETER, error);
  } else {
    EXPECT_EQ(ErrorCode::DATABASE_ACCESS_FAILURE, error);
  }
}

/**
 * @brief API to get one column statistic
 * return ErrorCode::DATABASE_ACCESS_FAILURE
 */
TEST_P(ApiTestDBAccessFailureByTableIdColumnNumber, get_one_column_statistic) {
  // TODO(future): Change when changing Metadata class.
  auto stats_tmp = get_statistics_ptr(GlobalTestEnvironment::TEST_DB);
  auto stats = static_cast<Statistics*>(stats_tmp.get());

  ErrorCode error = stats->init();
  EXPECT_EQ(ErrorCode::DATABASE_ACCESS_FAILURE, error);

  auto params                = GetParam();
  ObjectIdType table_id      = std::get<0>(params);
  ObjectIdType column_number = std::get<1>(params);

  ptree column_stats;
  // Execute the test.
  error = stats->get_by_column_number(table_id, column_number, column_stats);
  if ((table_id <= 0) || (column_number <= 0)) {
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
  // TODO(future): Change when changing Metadata class.
  auto stats_tmp = get_statistics_ptr(GlobalTestEnvironment::TEST_DB);
  auto stats = static_cast<Statistics*>(stats_tmp.get());

  ErrorCode error = stats->init();
  EXPECT_EQ(ErrorCode::DATABASE_ACCESS_FAILURE, error);

  ObjectIdType table_id = GetParam();
  std::vector<ptree> column_stats;

  // Execute the test.
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
TEST_P(ApiTestDBAccessFailureByTableIdColumnNumber,
       remove_one_column_statistic) {
  // TODO(future): Change when changing Metadata class.
  auto stats_tmp = get_statistics_ptr(GlobalTestEnvironment::TEST_DB);
  auto stats = static_cast<Statistics*>(stats_tmp.get());

  ErrorCode error = stats->init();
  EXPECT_EQ(ErrorCode::DATABASE_ACCESS_FAILURE, error);

  auto params                = GetParam();
  ObjectIdType table_id      = std::get<0>(params);
  ObjectIdType column_number = std::get<1>(params);

  // Execute the test.
  error = stats->remove_by_column_number(table_id, column_number);
  if ((table_id <= 0) || (column_number <= 0)) {
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
  // TODO(future): Change when changing Metadata class.
  auto stats_tmp = get_statistics_ptr(GlobalTestEnvironment::TEST_DB);
  auto stats = static_cast<Statistics*>(stats_tmp.get());

  ErrorCode error = stats->init();
  EXPECT_EQ(ErrorCode::DATABASE_ACCESS_FAILURE, error);

  ObjectIdType table_id = GetParam();
  // Execute the test.
  error = stats->remove_by_table_id(table_id);
  if (table_id <= 0) {
    EXPECT_EQ(ErrorCode::ID_NOT_FOUND, error);
  } else {
    EXPECT_EQ(ErrorCode::DATABASE_ACCESS_FAILURE, error);
  }
}

}  // namespace manager::metadata::testing
