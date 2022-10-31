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
#include <boost/property_tree/json_parser.hpp>

#include "manager/metadata/tables.h"
#include "test/common/global_test_environment.h"
#include "test/common/ut_utils.h"
#include "test/helper/table_metadata_helper.h"

namespace manager::metadata::testing {

using boost::property_tree::ptree;

class ApiTestTableMetadataExtra
    : public ::testing::TestWithParam<boost::property_tree::ptree> {
 public:
  void SetUp() override {
    UTUtils::skip_if_connection_not_opened();

    // If metadata repository is opened,
    // make valid table metadata used as test data.
    table_metadata_ = TableMetadataHelper::make_valid_table_metadata();

    // If valid test data could not be made, skip this test.
    if (table_metadata_.empty()) {
      GTEST_SKIP_("could not read a json file with table metadata.");
    }
  }

 protected:
  std::vector<UTTableMetadata> table_metadata_;
};

/**
 * @brief Add, get, remove valid table metadata based on table name.
 */
TEST_F(ApiTestTableMetadataExtra, add_get_remove_table_metadata_by_table_name) {
  // variable "table_metadata" is test data set.
  for (auto table_metadata : table_metadata_) {
    auto table_expected = table_metadata.get_metadata_ptree();

    // add valid table metadata.
    ObjectIdType ret_table_id = -1;
    TableMetadataHelper::add_table(table_expected, &ret_table_id);

    // get valid table metadata by table name.
    auto tables     = std::make_unique<Tables>(GlobalTestEnvironment::TEST_DB);
    ErrorCode error = tables->init();
    EXPECT_EQ(ErrorCode::OK, error);

    ptree table_metadata_inserted;
    std::string table_name = table_expected.get<std::string>(Table::NAME);

    error = tables->get(table_name, table_metadata_inserted);
    EXPECT_EQ(ErrorCode::OK, error);

    UTUtils::print("-- get valid table metadata --");
    UTUtils::print(UTUtils::get_tree_string(table_metadata_inserted));

    // verifies that the returned table metadata is expected one.
    table_expected.put(Table::ID, ret_table_id);
    table_metadata.CHECK_METADATA_EXPECTED(table_expected,
                                           table_metadata_inserted);

    // remove valid table metadata by table name.
    ObjectIdType table_id_removed;
    error = tables->remove(table_name.c_str(), &table_id_removed);
    EXPECT_EQ(ErrorCode::OK, error);
    EXPECT_EQ(ret_table_id, table_id_removed);

    // verifies that table metadata does not exist.
    ptree table_metadata_got;
    error = tables->get(ret_table_id, table_metadata_got);
    EXPECT_EQ(ErrorCode::ID_NOT_FOUND, error);

    UTUtils::print("-- verifies that table metadata does not exist. --");
    UTUtils::print(UTUtils::get_tree_string(table_metadata_got));
  }
}

/**
 * @brief Add, get, update, remove valid table metadata based on table id.
 */
TEST_F(ApiTestTableMetadataExtra,
       add_get_update_remove_table_metadata_by_table_id) {
  // variable "table_metadata" is test data set.
  for (auto table_metadata : table_metadata_) {
    auto table_expected = table_metadata.get_metadata_ptree();

    // add valid table metadata.
    ObjectIdType ret_table_id = -1;
    TableMetadataHelper::add_table(table_expected, &ret_table_id);

    // get valid table metadata by table id.
    auto tables     = std::make_unique<Tables>(GlobalTestEnvironment::TEST_DB);
    ErrorCode error = tables->init();
    EXPECT_EQ(ErrorCode::OK, error);

    ptree table_metadata_inserted;
    error = tables->get(ret_table_id, table_metadata_inserted);
    EXPECT_EQ(ErrorCode::OK, error);

    UTUtils::print("-- get valid table metadata after add --");
    UTUtils::print(UTUtils::get_tree_string(table_metadata_inserted));

    // verifies that the returned table metadata is expected one.
    table_expected.put(Table::ID, ret_table_id);
    table_metadata.CHECK_METADATA_EXPECTED(table_expected,
                                           table_metadata_inserted);

    // update valid table metadata.
    table_expected = table_metadata_inserted;
    std::string table_name =
        table_metadata_inserted.get_optional<std::string>(Table::NAME).value() +
        "-update";
    table_expected.put(Table::NAME, table_name);
    error = tables->update(ret_table_id, table_expected);

    // get valid table metadata by table id.
    ptree table_metadata_updated;
    error = tables->get(ret_table_id, table_metadata_updated);
    EXPECT_EQ(ErrorCode::OK, error);

    UTUtils::print("-- get valid table metadata after update --");
    UTUtils::print(UTUtils::get_tree_string(table_metadata_updated));

    // verifies that the returned table metadata is expected one.
    table_metadata.CHECK_METADATA_EXPECTED(table_expected,
                                           table_metadata_updated);

    // remove valid table metadata by table id.
    error = tables->remove(ret_table_id);
    EXPECT_EQ(ErrorCode::OK, error);

    // verifies that table metadata does not exist.
    ptree table_metadata_got;
    error = tables->get(ret_table_id, table_metadata_got);
    EXPECT_EQ(ErrorCode::ID_NOT_FOUND, error);

    UTUtils::print("-- verifies that table metadata does not exist. --");
    UTUtils::print(UTUtils::get_tree_string(table_metadata_got));
  }
}

}  // namespace manager::metadata::testing
