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

#include <boost/foreach.hpp>

#include "manager/metadata/tables.h"
#include "test/common/global_test_environment.h"
#include "test/common/ut_utils.h"
#include "test/helper/table_metadata_helper.h"

namespace manager::metadata::testing {

using boost::property_tree::ptree;

class ApiTestAddTableMetadataException
    : public ::testing::TestWithParam<boost::property_tree::ptree> {
 public:
  void SetUp() override {
    UTUtils::skip_if_connection_not_opened();

    invalid_table_metadata_ = make_invalid_table_metadata();
  }

 private:
  /**
   * @brief Make invalid table metadata used as test data.
   */
  static std::vector<boost::property_tree::ptree>
  make_invalid_table_metadata() {
    std::vector<ptree> invalid_table_metadata;

    // empty ptree
    ptree empty_table;
    invalid_table_metadata.emplace_back(empty_table);

    // Generate test metadata.
    UTTableMetadata testdata_table_metadata;
    auto new_table = testdata_table_metadata.get_metadata_ptree();

    new_table.erase(Table::NAME);
    invalid_table_metadata.emplace_back(new_table);

    // remove all column name
    new_table = testdata_table_metadata.get_metadata_ptree();
    BOOST_FOREACH (ptree::value_type& node,
                   new_table.get_child(Table::COLUMNS_NODE)) {
      ptree& column = node.second;
      column.erase(Column::NAME);
    }
    invalid_table_metadata.emplace_back(new_table);

    // remove all column number
    new_table = testdata_table_metadata.get_metadata_ptree();
    BOOST_FOREACH (ptree::value_type& node,
                   new_table.get_child(Table::COLUMNS_NODE)) {
      ptree& column = node.second;
      column.erase(Column::COLUMN_NUMBER);
    }
    invalid_table_metadata.emplace_back(new_table);

    // remove all data type id
    new_table = testdata_table_metadata.get_metadata_ptree();
    BOOST_FOREACH (ptree::value_type& node,
                   new_table.get_child(Table::COLUMNS_NODE)) {
      ptree& column = node.second;
      column.erase(Column::DATA_TYPE_ID);
    }
    invalid_table_metadata.emplace_back(new_table);

    // add invalid data type id
    BOOST_FOREACH (ptree::value_type& node,
                   new_table.get_child(Table::COLUMNS_NODE)) {
      ptree& column            = node.second;
      int invalid_data_type_id = -1;
      column.put(Column::DATA_TYPE_ID, invalid_data_type_id);
    }
    invalid_table_metadata.emplace_back(new_table);

    // remove all not null constraint
    new_table = testdata_table_metadata.get_metadata_ptree();
    BOOST_FOREACH (ptree::value_type& node,
                   new_table.get_child(Table::COLUMNS_NODE)) {
      ptree& column = node.second;
      column.erase(Column::IS_NOT_NULL);
    }
    invalid_table_metadata.emplace_back(new_table);

    return invalid_table_metadata;
  }

 protected:
  /**
   * @brief invalid table metadata used as test data.
   */
  std::vector<boost::property_tree::ptree> invalid_table_metadata_;
};

class ApiTestTableMetadataByTableIdException
    : public ::testing::TestWithParam<ObjectIdType> {
  void SetUp() override { UTUtils::skip_if_connection_not_opened(); }
};

class ApiTestTableMetadataByTableNameException
    : public ::testing::TestWithParam<std::string> {
  void SetUp() override { UTUtils::skip_if_connection_not_opened(); }
};

INSTANTIATE_TEST_CASE_P(
    ParameterizedTest, ApiTestTableMetadataByTableIdException,
    ::testing::Values(-1, 0, INT64_MAX - 1, INT64_MAX,
                      std::numeric_limits<ObjectIdType>::infinity(),
                      -std::numeric_limits<ObjectIdType>::infinity(),
                      std::numeric_limits<ObjectIdType>::quiet_NaN()));

INSTANTIATE_TEST_CASE_P(ParameterizedTest,
                        ApiTestTableMetadataByTableNameException,
                        ::testing::Values("table_name_not_exists", ""));

/**
 * @brief Add invalid table metadata.
 */
TEST_F(ApiTestAddTableMetadataException, add_table_metadata) {
  auto tables = std::make_unique<Tables>(GlobalTestEnvironment::TEST_DB);

  ErrorCode error = tables->init();
  EXPECT_EQ(ErrorCode::OK, error);

  for (auto invalid_table : invalid_table_metadata_) {
    UTUtils::print("-- add invalid table metadata --");
    UTUtils::print(UTUtils::get_tree_string(invalid_table));

    ObjectIdType ret_table_id = -1;
    error                     = tables->add(invalid_table, &ret_table_id);
    EXPECT_EQ(ErrorCode::INVALID_PARAMETER, error);
    EXPECT_EQ(ret_table_id, -1);
  }
}

/**
 * @brief Exception path test for getting table metadata based on
 *   non-existing table id.
 */
TEST_P(ApiTestTableMetadataByTableIdException,
       get_table_metadata_by_non_existing_table_id) {
  auto tables = std::make_unique<Tables>(GlobalTestEnvironment::TEST_DB);

  ErrorCode error = tables->init();
  EXPECT_EQ(ErrorCode::OK, error);

  ptree table;
  error = tables->get(GetParam(), table);
  EXPECT_EQ(ErrorCode::ID_NOT_FOUND, error);
}

/**
 * @brief Exception path test for getting table metadata based on
 *   non-existing table name.
 */
TEST_P(ApiTestTableMetadataByTableNameException,
       get_table_metadata_by_non_existing_table_name) {
  auto tables = std::make_unique<Tables>(GlobalTestEnvironment::TEST_DB);

  ErrorCode error = tables->init();
  EXPECT_EQ(ErrorCode::OK, error);

  ptree table;
  error = tables->get(GetParam(), table);
  EXPECT_EQ(ErrorCode::NAME_NOT_FOUND, error);
}

/**
 * @brief Update invalid table metadata.
 */
TEST_F(ApiTestAddTableMetadataException, update_table_metadata) {
  auto tables = std::make_unique<Tables>(GlobalTestEnvironment::TEST_DB);

  ErrorCode error = tables->init();
  EXPECT_EQ(ErrorCode::OK, error);

  for (auto invalid_table : invalid_table_metadata_) {
    UTUtils::print("-- update invalid table metadata --");
    UTUtils::print(UTUtils::get_tree_string(invalid_table));

    ObjectIdType dummy_table_id = 1;
    error                       = tables->update(dummy_table_id, invalid_table);
    EXPECT_EQ(ErrorCode::INVALID_PARAMETER, error);
  }
}

/**
 * @brief Exception path test for update table metadata based on
 *   non-existing table id.
 */
TEST_P(ApiTestTableMetadataByTableIdException,
       update_table_metadata_by_non_existing_table_id) {
  // Generate test metadata.
  UTTableMetadata testdata_table_metadata;

  UTUtils::print(
      UTUtils::get_tree_string(testdata_table_metadata.get_metadata_ptree()));

  auto tables = std::make_unique<Tables>(GlobalTestEnvironment::TEST_DB);

  ErrorCode error = tables->init();
  EXPECT_EQ(ErrorCode::OK, error);

  error =
      tables->update(GetParam(), testdata_table_metadata.get_metadata_ptree());
  EXPECT_EQ(ErrorCode::ID_NOT_FOUND, error);
}

/**
 * @brief Exception path test for removing table metadata based on
 *   non-existing table id.
 */
TEST_P(ApiTestTableMetadataByTableIdException,
       remove_table_metadata_by_non_existing_table_id) {
  auto tables = std::make_unique<Tables>(GlobalTestEnvironment::TEST_DB);

  ErrorCode error = tables->init();
  EXPECT_EQ(ErrorCode::OK, error);

  error = tables->remove(GetParam());
  EXPECT_EQ(ErrorCode::ID_NOT_FOUND, error);
}

/**
 * @brief Exception path test for removing table metadata based on
 *   non-existing table name.
 */
TEST_P(ApiTestTableMetadataByTableNameException,
       remove_table_metadata_by_non_existing_table_name) {
  auto tables = std::make_unique<Tables>(GlobalTestEnvironment::TEST_DB);

  ErrorCode error = tables->init();
  EXPECT_EQ(ErrorCode::OK, error);

  ObjectIdType ret_table_id = -1;
  error                     = tables->remove(GetParam().c_str(), &ret_table_id);
  EXPECT_EQ(ErrorCode::NAME_NOT_FOUND, error);
  EXPECT_EQ(-1, ret_table_id);
}

}  // namespace manager::metadata::testing
