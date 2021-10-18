/*
 * Copyright 2021 tsurugi project.
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
#include "test/api_test_table_metadata_extra.h"

#include <boost/foreach.hpp>
#include <boost/property_tree/json_parser.hpp>
#include <iostream>
#include <memory>
#include <string>

#include "manager/metadata/error_code.h"
#include "manager/metadata/metadata.h"
#include "manager/metadata/tables.h"
#include "test/api_test_table_metadata.h"
#include "test/global_test_environment.h"
#include "test/utility/ut_utils.h"

using namespace manager::metadata;
using namespace boost::property_tree;

namespace manager::metadata::testing {

void ApiTestTableMetadataExtra::SetUp() {
  if (!global->is_open()) {
    GTEST_SKIP_("metadata repository is not started.");
  } else {
    // If metadata repository is opened,
    // make valid table metadata used as test data.
    table_metadata = make_valid_table_metadata();

    // If valid test data could not be made, skip this test.
    if (table_metadata.empty()) {
      GTEST_SKIP_("could not read a json file with table metadata.");
    }
  }
}

/**
 * @brief Make valid table metadata used as test data,
 * by reading a json file with table metadata.
 */
std::vector<ptree> ApiTestTableMetadataExtra::make_valid_table_metadata() {
  std::vector<ptree> test_data_table_metadata;

  ptree pt;
  try {
    // read a json file with table metadata used as test data.
    read_json(global->get_json_schema_file_name(), pt);
  } catch (boost::property_tree::json_parser_error& e) {
    UTUtils::print("could not read a json file with table metadata.", e.what());
    return test_data_table_metadata;
  } catch (...) {
    UTUtils::print("could not read a json file with table metadata.");
    return test_data_table_metadata;
  }

  // Make valid table metadata used as test data.
  boost::optional<ptree&> o_tables = pt.get_child_optional("tables");
  if (o_tables) {
    ptree& tables = o_tables.value();
    BOOST_FOREACH (const ptree::value_type& node, tables) {
      ptree table = node.second;
      test_data_table_metadata.emplace_back(table);
    }
  }

  return test_data_table_metadata;
}

/**
 * @brief Add, get, remove valid table metadata based on table name.
 */
TEST_F(ApiTestTableMetadataExtra, add_get_remove_table_metadata_by_table_name) {
  // variable "table_metadata" is test data set.
  for (auto table_metadata_expected : table_metadata) {
    // add valid table metadata.
    ObjectIdType ret_table_id = -1;
    ApiTestTableMetadata::add_table(table_metadata_expected, &ret_table_id);

    // get valid table metadata by table name.
    auto tables = std::make_unique<Tables>(GlobalTestEnvironment::TEST_DB);
    ErrorCode error = tables->init();
    EXPECT_EQ(ErrorCode::OK, error);

    ptree table_metadata_inserted;
    std::string table_name =
        table_metadata_expected.get<std::string>(Tables::NAME);

    error = tables->get(table_name, table_metadata_inserted);
    EXPECT_EQ(ErrorCode::OK, error);

    UTUtils::print("-- get valid table metadata --");
    UTUtils::print(UTUtils::get_tree_string(table_metadata_inserted));

    // verifies that the returned table metadata is expected one.
    table_metadata_expected.put(Tables::ID, ret_table_id);
    ApiTestTableMetadata::check_table_metadata_expected(
        table_metadata_expected, table_metadata_inserted);

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
 * @brief Add, get, remove valid table metadata based on table id.
 */
TEST_F(ApiTestTableMetadataExtra, add_get_remove_table_metadata_by_table_id) {
  // variable "table_metadata" is test data set.
  for (auto table_metadata_expected : table_metadata) {
    // add valid table metadata.
    ObjectIdType ret_table_id = -1;
    ApiTestTableMetadata::add_table(table_metadata_expected, &ret_table_id);

    // get valid table metadata by table id.
    auto tables = std::make_unique<Tables>(GlobalTestEnvironment::TEST_DB);
    ErrorCode error = tables->init();
    EXPECT_EQ(ErrorCode::OK, error);

    ptree table_metadata_inserted;
    error = tables->get(ret_table_id, table_metadata_inserted);
    EXPECT_EQ(ErrorCode::OK, error);

    UTUtils::print("-- get valid table metadata --");
    UTUtils::print(UTUtils::get_tree_string(table_metadata_inserted));

    // verifies that the returned table metadata is expected one.
    table_metadata_expected.put(Tables::ID, ret_table_id);
    ApiTestTableMetadata::check_table_metadata_expected(
        table_metadata_expected, table_metadata_inserted);

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
