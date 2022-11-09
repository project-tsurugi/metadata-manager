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

#include "boost/foreach.hpp"
#include "boost/property_tree/ptree.hpp"

#include "manager/metadata/statistics.h"
#include "test/common/dummy_object.h"
#include "test/helper/column_statistics_helper.h"
#include "test/helper/table_metadata_helper.h"
#include "test/metadata/ut_column_statistics.h"
#include "test/test/api_test_facade.h"

namespace {

using StatisticsTestData =
    std::tuple<manager::metadata::ObjectId,
               std::vector<boost::property_tree::ptree>,
               std::vector<manager::metadata::ObjectId>,
               std::vector<manager::metadata::testing::UtColumnStatistics>>;

std::vector<std::string> invalid_names = {"", "undefined_name"};

}  // namespace

namespace manager::metadata::testing {

using boost::property_tree::ptree;

class ApiTestColumnStatisticsPg
    : public ApiTestFacade<DummyObject, ColumnStatisticsHelper> {
 public:
  manager::metadata::ObjectId table_id_;

  ApiTestColumnStatisticsPg()
      : ApiTestFacade(
            std::make_unique<Statistics>(GlobalTestEnvironment::TEST_DB)),
        table_id_(0) {}

  void SetUp() override {
    UTUtils::skip_if_json();
    UTUtils::skip_if_connection_not_opened();

    UTUtils::print(">> gtest::SetUp()");

    // Change to a unique table name.
    std::string table_name =
        "ApiTestColumnStatistic_" + UTUtils::generate_narrow_uid();

    // Add table metadata.
    TableMetadataHelper::add_table(table_name, &table_id_);
  }

  void TearDown() override {
    UTUtils::skip_if_json();

    if (global->is_open()) {
      UTUtils::print(">> gtest::TearDown()");

      // Remove table metadata.
      TableMetadataHelper::remove_table(table_id_);
    }
  }

  /**
   * @brief Create a test data object
   * @return std::vector<StatisticsTestData> - [tableID, columns[],
   *   statisticIDs[], UtColumnStatistics[]]
   */
  std::vector<StatisticsTestData> create_test_data() {
    CALL_TRACE;

    UTUtils::print(">> Create test data.");
    std::vector<StatisticsTestData> v{};
    std::vector<ptree> columns{};
    std::vector<ObjectId> statistic_ids{};
    std::vector<UtColumnStatistics> ut_statistics{};

    // Add table metadata.
    for (int32_t i = 1; i <= kMakeTableCount; i++) {
      ObjectId table_id;
      if (i == 1) {
        table_id = table_id_;
      } else {
        std::string table_name = "ApiTestColumnStatistic_" +
                                 UTUtils::generate_narrow_uid() + "_" +
                                 std::to_string(i);
        TableMetadataHelper::add_table(table_name, &table_id);
      }
      ptree retrieved_metadata = TableMetadataHelper::get_table(table_id);

      columns.clear();
      BOOST_FOREACH (ptree::value_type& column_node,
                     retrieved_metadata.get_child(Table::COLUMNS_NODE)) {
        columns.push_back(column_node.second);
      }

      statistic_ids.clear();
      ut_statistics.clear();
      // Add column statistics of the table metadata.
      for (int32_t n = 1; n <= kMakeStatisticCount; n++) {
        UtColumnStatistics test_data(table_id, n);
        ObjectId statistic_id = INVALID_OBJECT_ID;
        // Add column statistics.
        auto statistic = test_data.get_metadata_ptree();
        statistic_id   = this->test_add(nullptr, statistic, ErrorCode::OK);

        statistic_ids.push_back(statistic_id);
        ut_statistics.push_back(test_data);
      }
      v.push_back(
          std::make_tuple(table_id, columns, statistic_ids, ut_statistics));
    }
    UTUtils::print("<< Create test data.");

    return v;
  }

  void cleanup_test_data(std::vector<StatisticsTestData> test_data) {
    // Generate columns statistics manager.
    auto managers =
        std::make_unique<Statistics>(GlobalTestEnvironment::TEST_DB);

    // Add table metadata.
    for (auto& tables : test_data) {
      auto table_id = std::get<0>(tables);
      for (auto& statistic_id : std::get<2>(tables)) {
        managers->remove(statistic_id);
      }
      if (table_id != table_id_) {
        TableMetadataHelper::remove_table(table_id);
      }
    }
  }

 private:
  const int32_t kMakeTableCount     = 2;
  const int32_t kMakeStatisticCount = 2;
};

class ApiTestColumnStatisticsJson
    : public ApiTestFacade<DummyObject, ColumnStatisticsHelper> {
 public:
  ApiTestColumnStatisticsJson()
      : ApiTestFacade(
            std::make_unique<Statistics>(GlobalTestEnvironment::TEST_DB)) {}

  void SetUp() override { UTUtils::skip_if_postgresql(); }
  void TearDown() override { UTUtils::skip_if_postgresql(); }
};

/**
 * @brief Test to add new statistics and get it in ptree type
 *   with object ID as key.
 */
TEST_F(ApiTestColumnStatisticsPg, test_get_by_id_with_ptree) {
  CALL_TRACE;

  // Execute the test.
  this->test_flow_get_by_id(UtColumnStatistics(table_id_, 1));
}

/**
 * @brief Test to add new statistics and get it in ptree type
 *   with object name as key.
 */
TEST_F(ApiTestColumnStatisticsPg, test_get_by_name_with_ptree) {
  CALL_TRACE;

  // Execute the test.
  this->test_flow_get_by_name(UtColumnStatistics(table_id_, 1));
}

/**
 * @brief Test to add new statistics and get/remove it by columns ID.
 */
TEST_F(ApiTestColumnStatisticsPg, test_get_by_column_id) {
  // Generate columns statistics manager.
  auto managers = std::make_unique<Statistics>(GlobalTestEnvironment::TEST_DB);

  // Create test data for column statistics.
  auto test_data = create_test_data();

  auto& columns       = std::get<1>(test_data[0]);
  auto& ut_statistics = std::get<3>(test_data[0]);

  // Get column statistics by column ID.
  ErrorCode error;
  ptree retrieved_ptree;
  const auto& column_id = columns[0].get<ObjectId>(Column::ID);

  // Get by column ID.
  error = managers->get_by_column_id(column_id, retrieved_ptree);
  EXPECT_EQ(ErrorCode::OK, error);
  ut_statistics[0].check_metadata_expected(retrieved_ptree, __FILE__, __LINE__);

  // Remove by column ID.
  error = managers->remove_by_column_id(column_id);
  EXPECT_EQ(ErrorCode::OK, error);

  // Check for data availability.
  error = managers->remove_by_column_id(column_id);
  EXPECT_EQ(ErrorCode::ID_NOT_FOUND, error);

  // Check for the presence of other data.
  error = managers->get_by_column_id(columns[1].get<ObjectId>(Column::ID),
                                     retrieved_ptree);
  EXPECT_EQ(ErrorCode::OK, error);
  ut_statistics[1].check_metadata_expected(retrieved_ptree, __FILE__, __LINE__);

  // Cleanup of test data.
  cleanup_test_data(test_data);
}

/**
 * @brief Test to add new statistics and get/remove it by columns name.
 */
TEST_F(ApiTestColumnStatisticsPg, test_get_by_column_name) {
  // Generate columns statistics manager.
  auto managers = std::make_unique<Statistics>(GlobalTestEnvironment::TEST_DB);

  // Create test data for column statistics.
  auto test_data = create_test_data();

  auto [table_id, columns, statistic_ids, ut_statistics] = test_data[0];

  // Get column statistics by column name.
  ErrorCode error;
  ptree retrieved_ptree;
  const auto& column_name = columns[0].get<std::string>(Column::NAME);

  // Get by column name.
  error = managers->get_by_column_name(table_id, column_name, retrieved_ptree);
  EXPECT_EQ(ErrorCode::OK, error);
  ut_statistics[0].check_metadata_expected(retrieved_ptree, __FILE__, __LINE__);

  // Remove by column name.
  error = managers->remove_by_column_name(table_id, column_name);
  EXPECT_EQ(ErrorCode::OK, error);

  // Check for data availability.
  error = managers->remove_by_column_name(table_id, column_name);
  EXPECT_EQ(ErrorCode::NAME_NOT_FOUND, error);

  // Check for the presence of other data.
  error = managers->get_by_column_name(
      table_id, columns[1].get<std::string>(Column::NAME), retrieved_ptree);
  EXPECT_EQ(ErrorCode::OK, error);
  ut_statistics[1].check_metadata_expected(retrieved_ptree, __FILE__, __LINE__);

  // Cleanup of test data.
  cleanup_test_data(test_data);
}

/**
 * @brief Test to add new statistics and get/remove it by columns number.
 */
TEST_F(ApiTestColumnStatisticsPg, test_get_by_column_number) {
  // Generate columns statistics manager.
  auto managers = std::make_unique<Statistics>(GlobalTestEnvironment::TEST_DB);

  // Create test data for column statistics.
  auto test_data = create_test_data();

  auto& table_id      = std::get<0>(test_data[0]);
  auto& ut_statistics = std::get<3>(test_data[0]);

  // Get column statistics by column number.
  ErrorCode error;
  ptree retrieved_ptree;

  // Get by column number.
  error = managers->get_by_column_number(table_id, 1, retrieved_ptree);
  EXPECT_EQ(ErrorCode::OK, error);
  ut_statistics[0].check_metadata_expected(retrieved_ptree, __FILE__, __LINE__);

  // Remove by column number.
  error = managers->remove_by_column_number(table_id, 1);
  EXPECT_EQ(ErrorCode::OK, error);

  // Check for data availability.
  error = managers->remove_by_column_number(table_id, 1);
  EXPECT_EQ(ErrorCode::ID_NOT_FOUND, error);

  // Check for the presence of other data.
  error = managers->get_by_column_number(table_id, 2, retrieved_ptree);
  EXPECT_EQ(ErrorCode::OK, error);
  ut_statistics[1].check_metadata_expected(retrieved_ptree, __FILE__, __LINE__);

  // Cleanup of test data.
  cleanup_test_data(test_data);
}

/**
 * @brief Test to add new statistics and remove it by table ID.
 */
TEST_F(ApiTestColumnStatisticsPg, test_remove_by_table_id) {
  // Generate columns statistics manager.
  auto managers = std::make_unique<Statistics>(GlobalTestEnvironment::TEST_DB);

  // Create test data for column statistics.
  auto test_data = create_test_data();

  auto& table_id_1 = std::get<0>(test_data[0]);
  auto& table_id_2 = std::get<0>(test_data[1]);

  ErrorCode error;
  // Remove column statistics by table ID.
  {
    error = managers->remove_by_table_id(table_id_1);
    EXPECT_EQ(ErrorCode::OK, error);

    error = managers->remove_by_table_id(table_id_1);
    EXPECT_EQ(ErrorCode::ID_NOT_FOUND, error);

    std::vector<boost::property_tree::ptree> container;
    error = managers->get_all(table_id_1, container);
    EXPECT_EQ(ErrorCode::ID_NOT_FOUND, error);
    EXPECT_EQ(container.size(), 0);
  }

  // Remove column statistics by table ID.
  {
    error = managers->remove_by_table_id(table_id_2);
    EXPECT_EQ(ErrorCode::OK, error);

    error = managers->remove_by_table_id(table_id_2);
    EXPECT_EQ(ErrorCode::ID_NOT_FOUND, error);

    std::vector<boost::property_tree::ptree> container;
    error = managers->get_all(table_id_2, container);
    EXPECT_EQ(ErrorCode::ID_NOT_FOUND, error);
    EXPECT_EQ(container.size(), 0);
  }

  // Cleanup of test data.
  cleanup_test_data(test_data);
}

/**
 * @brief Test to add new statistics and get_all it in ptree type.
 */
TEST_F(ApiTestColumnStatisticsPg, test_getall_with_ptree) {
  CALL_TRACE;

  static constexpr const int32_t kTestColumnsCount = 2;
  this->test_flow_getall(
      UtColumnStatistics(table_id_),
      [](ptree& object, const int64_t unique_num) {
        std::string metadata_name = "metadata_name_" +
                                    UTUtils::generate_narrow_uid() + "_" +
                                    std::to_string(unique_num);
        object.put(Statistics::NAME, metadata_name);
        object.put(Statistics::COLUMN_NUMBER, unique_num);
      },
      kTestColumnsCount);
}

/**
 * @brief Test to add new metadata and update it in ptree type
 *   with object ID as key.
 */
TEST_F(ApiTestColumnStatisticsPg, test_update) {
  CALL_TRACE;

  ptree statistic;
  // Execute the test.
  this->test_update(nullptr, INT64_MAX, statistic, ErrorCode::UNKNOWN);
}

/**
 * @brief Test to update column statistics based on both existing
 *   table id and column number.
 */
TEST_F(ApiTestColumnStatisticsPg, test_add_exists) {
  // Create test data for column statistics.
  auto test_data = create_test_data();

  auto [table_id, columns, statistic_ids, ut_statistics] = test_data[0];

  // Generate columns statistics manager.
  auto managers = std::make_unique<Statistics>(GlobalTestEnvironment::TEST_DB);

  ErrorCode error;
  // Get the statistics of the columns before updating.
  std::vector<ptree> container_before;
  {
    error = managers->get_all(table_id, container_before);
    EXPECT_EQ(ErrorCode::OK, error);
  }

  std::string metadata_name =
      ut_statistics[0].get_metadata_struct()->name + "_update";
  UtColumnStatistics ut_statistic(table_id, 1, metadata_name);
  // Add (update) column statistics.
  {
    ptree updated_ptree = ut_statistic.get_metadata_ptree();

    ObjectId statistic_id =
        this->test_add(nullptr, updated_ptree, ErrorCode::OK);
    EXPECT_EQ(statistic_ids[0], statistic_id);
  }

  // Get the statistics of the columns after updating.
  std::vector<ptree> container_after;
  {
    error = managers->get_all(table_id, container_after);
    EXPECT_EQ(ErrorCode::OK, error);
  }
  EXPECT_EQ(container_after.size(), container_before.size());
  EXPECT_NE(UTUtils::get_tree_string(container_after[0]),
            UTUtils::get_tree_string(container_before[0]));
  EXPECT_EQ(UTUtils::get_tree_string(container_after[1]),
            UTUtils::get_tree_string(container_before[1]));

  CALL_TRACE;
  ut_statistic.check_metadata_expected(container_after[0], __FILE__, __LINE__);

  // Cleanup of test data.
  cleanup_test_data(test_data);
}

/**
 * @brief This is a test using an invalid ID.
 */
TEST_F(ApiTestColumnStatisticsPg, test_invalid_ids) {
  CALL_TRACE;
  ErrorCode error;

  // Generate columns statistics manager.
  auto managers = std::make_unique<Statistics>(GlobalTestEnvironment::TEST_DB);

  error = managers->init();
  EXPECT_EQ(ErrorCode::OK, error);

  // Test of add to a table ID that does not exist.
  for (auto invalid_id : global->invalid_ids) {
    UtColumnStatistics ut_statistic(invalid_id, 1);
    ptree statistic = ut_statistic.get_metadata_ptree();

    this->test_add(nullptr, statistic, ErrorCode::INVALID_PARAMETER);
  }

  // Test of get to a statistic ID that does not exist.
  for (auto invalid_id : global->invalid_ids) {
    ptree statistic;
    this->test_get(nullptr, invalid_id, ErrorCode::ID_NOT_FOUND, statistic);
  }

  // Test of get to a statistic name that does not exist.
  for (auto invalid_name : invalid_names) {
    ptree statistic;
    this->test_get(nullptr, invalid_name, ErrorCode::NAME_NOT_FOUND, statistic);
  }

  // Test of get to a column ID that does not exist.
  for (auto invalid_id : global->invalid_ids) {
    ptree statistic;
    error = managers->get_by_column_id(invalid_id, statistic);
    EXPECT_EQ(ErrorCode::ID_NOT_FOUND, error);
  }

  // Test of get to a column number that does not exist.
  for (auto invalid_id : global->invalid_ids) {
    ptree statistic;
    error = managers->get_by_column_number(table_id_, invalid_id, statistic);
    EXPECT_EQ(ErrorCode::ID_NOT_FOUND, error);
  }

  // Test of get to a column name that does not exist.
  for (auto invalid_name : invalid_names) {
    ptree statistic;
    error = managers->get_by_column_name(table_id_, invalid_name, statistic);
    EXPECT_EQ(ErrorCode::NAME_NOT_FOUND, error);
  }

  // Test of get_all to a table ID that does not exist.
  for (auto invalid_id : global->invalid_ids) {
    std::vector<ptree> container{};

    error = managers->get_all(invalid_id, container);
    EXPECT_EQ(ErrorCode::ID_NOT_FOUND, error);
    EXPECT_EQ(container.size(), 0);
  }

  // Test of remove to a statistic ID that does not exist.
  for (auto invalid_id : global->invalid_ids) {
    this->test_remove(nullptr, invalid_id, ErrorCode::ID_NOT_FOUND);
  }

  // Test of remove to a statistic name that does not exist.
  for (auto invalid_name : invalid_names) {
    this->test_remove(nullptr, invalid_name, ErrorCode::NAME_NOT_FOUND);
  }

  // Test of remove to a table ID that does not exist.
  for (auto invalid_id : global->invalid_ids) {
    error = managers->remove_by_table_id(invalid_id);
    EXPECT_EQ(ErrorCode::ID_NOT_FOUND, error);
  }

  // Test of remove to a column ID that does not exist.
  for (auto invalid_id : global->invalid_ids) {
    error = managers->remove_by_column_id(invalid_id);
    EXPECT_EQ(ErrorCode::ID_NOT_FOUND, error);
  }

  // Test of remove to a column number that does not exist.
  for (auto invalid_id : global->invalid_ids) {
    error = managers->remove_by_column_number(table_id_, invalid_id);
    EXPECT_EQ(ErrorCode::ID_NOT_FOUND, error);
  }

  // Test of remove to a column name that does not exist.
  for (auto invalid_name : invalid_names) {
    error = managers->remove_by_column_name(table_id_, invalid_name);
    EXPECT_EQ(ErrorCode::NAME_NOT_FOUND, error);
  }
}

/**
 * @brief happy test for adding, getting and removing
 *   one new table metadata without initialization of all api.
 */
TEST_F(ApiTestColumnStatisticsPg, test_without_initialized) {
  CALL_TRACE;

  // Create test data for column statistics.
  auto test_data = create_test_data();

  auto [table_id_1, columns_1, statistic_ids_1, ut_statistics_1] = test_data[0];
  auto [table_id_2, columns_2, statistic_ids_2, ut_statistics_2] = test_data[1];

  {
    // Generate columns statistics manager.
    auto managers =
        std::make_unique<Statistics>(GlobalTestEnvironment::TEST_DB);

    UtColumnStatistics ut_statistic_1(table_id_1, 3);
    ptree statistic = ut_statistic_1.get_metadata_ptree();

    CALL_TRACE;
    this->test_add(managers.get(), statistic, ErrorCode::OK);

    UtColumnStatistics ut_statistic_2(table_id_2, 3);
    statistic = ut_statistic_2.get_metadata_ptree();

    CALL_TRACE;
    this->test_add(managers.get(), statistic, ErrorCode::OK);
  }

  {
    // Generate columns statistics manager.
    auto managers =
        std::make_unique<Statistics>(GlobalTestEnvironment::TEST_DB);

    ptree retrieved;
    auto object_id = statistic_ids_1[0];

    CALL_TRACE;
    this->test_get(managers.get(), object_id, ErrorCode::OK, retrieved);
  }

  {
    // Generate columns statistics manager.
    auto managers =
        std::make_unique<Statistics>(GlobalTestEnvironment::TEST_DB);

    ptree retrieved;
    auto object_name = ut_statistics_1[0].get_metadata_struct()->name;

    CALL_TRACE;
    this->test_get(managers.get(), object_name, ErrorCode::OK, retrieved);
  }

  {
    // Generate columns statistics manager.
    auto managers =
        std::make_unique<Statistics>(GlobalTestEnvironment::TEST_DB);

    ptree statistic;
    auto object_id = columns_1[0].get<ObjectId>(Column::ID);

    ErrorCode error = managers->get_by_column_id(object_id, statistic);
    EXPECT_EQ(ErrorCode::OK, error);
  }

  {
    // Generate columns statistics manager.
    auto managers =
        std::make_unique<Statistics>(GlobalTestEnvironment::TEST_DB);

    ptree statistic;

    ErrorCode error = managers->get_by_column_number(table_id_1, 1, statistic);
    EXPECT_EQ(ErrorCode::OK, error);
  }

  {
    // Generate columns statistics manager.
    auto managers =
        std::make_unique<Statistics>(GlobalTestEnvironment::TEST_DB);

    ptree statistic;
    auto object_name = columns_1[0].get<std::string>(Column::NAME);

    ErrorCode error =
        managers->get_by_column_name(table_id_1, object_name, statistic);
    EXPECT_EQ(ErrorCode::OK, error);
  }

  {
    // Generate columns statistics manager.
    auto managers =
        std::make_unique<Statistics>(GlobalTestEnvironment::TEST_DB);

    std::vector<ptree> container = {};

    CALL_TRACE;
    this->test_getall(managers.get(), ErrorCode::OK, container);
  }

  {
    // Generate columns statistics manager.
    auto managers =
        std::make_unique<Statistics>(GlobalTestEnvironment::TEST_DB);

    auto object_id = statistic_ids_1[0];

    CALL_TRACE;
    this->test_remove(managers.get(), object_id, ErrorCode::OK);
  }

  {
    // Generate columns statistics manager.
    auto managers =
        std::make_unique<Statistics>(GlobalTestEnvironment::TEST_DB);

    auto object_name = ut_statistics_1[1].get_metadata_struct()->name;

    CALL_TRACE;
    this->test_remove(managers.get(), object_name, ErrorCode::OK);
  }

  {
    // Generate columns statistics manager.
    auto managers =
        std::make_unique<Statistics>(GlobalTestEnvironment::TEST_DB);

    ErrorCode error = managers->remove_by_table_id(table_id_1);
    EXPECT_EQ(ErrorCode::OK, error);
  }

  {
    // Generate columns statistics manager.
    auto managers =
        std::make_unique<Statistics>(GlobalTestEnvironment::TEST_DB);

    auto object_id = columns_2[0].get<ObjectId>(Column::ID);

    ErrorCode error = managers->remove_by_column_id(object_id);
    EXPECT_EQ(ErrorCode::OK, error);
  }

  {
    // Generate columns statistics manager.
    auto managers =
        std::make_unique<Statistics>(GlobalTestEnvironment::TEST_DB);

    ErrorCode error = managers->remove_by_column_number(table_id_2, 3);
    EXPECT_EQ(ErrorCode::OK, error);
  }

  {
    // Generate columns statistics manager.
    auto managers =
        std::make_unique<Statistics>(GlobalTestEnvironment::TEST_DB);

    auto object_name = columns_2[1].get<std::string>(Column::NAME);

    ErrorCode error = managers->remove_by_column_name(table_id_2, object_name);
    EXPECT_EQ(ErrorCode::OK, error);
  }

  // Cleanup of test data.
  cleanup_test_data(test_data);
}

/**
 * @brief api test for add statistic metadata.
 */
TEST_F(ApiTestColumnStatisticsJson, test_add) {
  CALL_TRACE;

  // Generate statistics metadata manager.
  auto managers = std::make_unique<Statistics>(GlobalTestEnvironment::TEST_DB);

  // Test to initialize the manager.
  this->test_init(managers.get(), ErrorCode::NOT_SUPPORTED);

  UtColumnStatistics ut_statistic(INT32_MAX, 1);
  ptree inserted_metadata = ut_statistic.get_metadata_ptree();

  // Test to add the manager.
  this->test_add(managers.get(), inserted_metadata, ErrorCode::NOT_SUPPORTED);
}

/**
 * @brief Unsupported test in JSON version.
 */
TEST_F(ApiTestColumnStatisticsJson, test_get) {
  CALL_TRACE;

  // Generate statistics metadata manager.
  auto managers = std::make_unique<Statistics>(GlobalTestEnvironment::TEST_DB);

  // Test to initialize the manager.
  this->test_init(managers.get(), ErrorCode::NOT_SUPPORTED);

  ptree retrieve_metadata;

  // Test to get the manager by statistic id.
  this->test_get(managers.get(), INT32_MAX, ErrorCode::NOT_SUPPORTED,
                 retrieve_metadata);

  // Test to get the manager by statistic name.
  this->test_get(managers.get(), "statistics_name", ErrorCode::NOT_SUPPORTED,
                 retrieve_metadata);
}

/**
 * @brief api test for get_all statistic metadata.
 */
TEST_F(ApiTestColumnStatisticsJson, test_getall) {
  CALL_TRACE;

  // Generate statistics metadata manager.
  auto managers = std::make_unique<Statistics>(GlobalTestEnvironment::TEST_DB);

  // Test to initialize the manager.
  this->test_init(managers.get(), ErrorCode::NOT_SUPPORTED);

  std::vector<boost::property_tree::ptree> container = {};

  // Test to gte all the manager.
  this->test_getall(managers.get(), ErrorCode::NOT_SUPPORTED, container);
  EXPECT_TRUE(container.empty());
}

/**
 * @brief api test for remove statistic metadata.
 */
TEST_F(ApiTestColumnStatisticsJson, remove_statistic_metadata) {
  CALL_TRACE;

  // Generate statistics metadata manager.
  auto managers = std::make_unique<Statistics>(GlobalTestEnvironment::TEST_DB);

  // Test to initialize the manager.
  this->test_init(managers.get(), ErrorCode::NOT_SUPPORTED);

  std::vector<boost::property_tree::ptree> container = {};

  // Test to remove the manager by statistic id.
  this->test_remove(managers.get(), INT32_MAX, ErrorCode::NOT_SUPPORTED);

  // Test to remove the manager by statistic name.
  this->test_remove(managers.get(), "statistic_name", ErrorCode::NOT_SUPPORTED);
}

}  // namespace manager::metadata::testing
