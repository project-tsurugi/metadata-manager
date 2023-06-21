/*
 * Copyright 2021-2023 tsurugi project.
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
#include "test/helper/table_metadata_helper.h"

#include <gtest/gtest.h>

#include <string>
#include <vector>

#include <boost/foreach.hpp>
#include <boost/property_tree/json_parser.hpp>

#include "manager/metadata/helper/ptree_helper.h"
#include "manager/metadata/metadata_factory.h"
#include "manager/metadata/table.h"
#include "test/common/global_test_environment.h"
#include "test/common/ut_utils.h"

namespace manager::metadata::testing {

namespace json_parser = boost::property_tree::json_parser;

using boost::property_tree::json_parser_error;
using boost::property_tree::ptree;

/**
 * @brief Generate unique table names.
 * @param prefix prefix string.
 * @param identifier identifier string or empty string.
 * @param line_num line number.
 * @return std::string - table name.
 */
std::string TableMetadataHelper::make_table_name(std::string_view prefix,
                                                 std::string_view identifier,
                                                 int32_t line_num) {
  return std::string(prefix) + UTUtils::generate_narrow_uid() + "_" +
         (identifier.empty() ? "" : std::string(identifier) + "_") +
         std::to_string(line_num);
}

/**
 * @brief Make valid table metadata used as test data,
 * by reading a json file with table metadata.
 */
std::vector<UtTableMetadata> TableMetadataHelper::make_valid_table_metadata() {
  std::vector<UtTableMetadata> testdata_table_metadata_list;

  ptree pt;
  try {
    // read a json file with table metadata used as test data.
    json_parser::read_json(g_environment_->get_json_schema_file_name(), pt);
  } catch (boost::property_tree::json_parser_error& e) {
    UTUtils::print("could not read a json file with table metadata.", e.what());
    return testdata_table_metadata_list;
  } catch (...) {
    UTUtils::print("could not read a json file with table metadata.");
    return testdata_table_metadata_list;
  }

  // Make valid table metadata used as test data.
  boost::optional<ptree&> o_tables = pt.get_child_optional("tables");
  if (o_tables) {
    ptree& tables = o_tables.value();
    BOOST_FOREACH (const ptree::value_type& node, tables) {
      ptree table = node.second;
      UtTableMetadata testdata_table_metadata(table);
      testdata_table_metadata_list.emplace_back(testdata_table_metadata);
    }
  }

  return testdata_table_metadata_list;
}

/**
 * @brief Add one new table metadata to table metadata table.
 * @param (table_name)    [in]  table name of new table metadata.
 * @param (ret_table_id)  [out] (optional) table id returned from the api to add
 *   new table metadata.
 * @return none.
 */
void TableMetadataHelper::add_table(std::string_view table_name,
                                    ObjectId* ret_table_id) {
  // Generate test metadata.
  UtTableMetadata ut_metadata(table_name);

  // add table metadata.
  add_table(ut_metadata.get_metadata_ptree(), ret_table_id);
}

/**
 * @brief Add one new table metadata to table metadata table.
 * @param (new_table)  [in]  new table metadata.
 * @param (table_id)   [out] (optional) table id returned from the api to add
 *   new table metadata.
 * @return none.
 */
void TableMetadataHelper::add_table(
    const boost::property_tree::ptree& new_table, ObjectId* table_id) {
  UTUtils::print("-- add table metadata --");
  UTUtils::print(" " + UTUtils::get_tree_string(new_table));

  auto tables = get_tables_ptr(GlobalTestEnvironment::TEST_DB);

  ErrorCode error = tables->init();
  ASSERT_EQ(ErrorCode::OK, error);

  // add table metadata.
  ObjectId ret_table_id = INVALID_VALUE;
  error                 = tables->add(new_table, &ret_table_id);
  ASSERT_EQ(ErrorCode::OK, error);
  ASSERT_GT(ret_table_id, 0);

  UTUtils::print(" >> new table_id: ", ret_table_id);

  if (table_id != nullptr) {
    *table_id = ret_table_id;
  }
}

/**
 * @brief Add one new table metadata to table metadata table.
 * @param (new_table)  [in]  new table metadata.
 * @param (table_id)   [out] (optional) table id returned from the api to add
 *   new table metadata.
 * @return none.
 */
void TableMetadataHelper::add_table(const manager::metadata::Table& new_table,
                                    ObjectId* table_id) {
  UTUtils::print("-- add table metadata --");
  ptree pt_table = new_table.convert_to_ptree();
  UTUtils::print(" " + UTUtils::get_tree_string(pt_table));

  auto tables = get_tables_ptr(GlobalTestEnvironment::TEST_DB);

  ErrorCode error = tables->init();
  ASSERT_EQ(ErrorCode::OK, error);

  // add table metadata.
  ObjectId ret_table_id = INVALID_VALUE;
  error                 = tables->add(new_table, &ret_table_id);
  ASSERT_EQ(ErrorCode::OK, error);
  ASSERT_GT(ret_table_id, 0);

  UTUtils::print(" >> new table_id: ", ret_table_id);

  if (table_id != nullptr) {
    *table_id = ret_table_id;
  }
}

/**
 * @brief Get metadata from table metadata table.
 * @param table_id table id.
 * @return boost::property_tree::ptree - table metadata.
 */
boost::property_tree::ptree TableMetadataHelper::get_table(
    const ObjectId table_id) {
  UTUtils::print("-- get table metadata --");
  UTUtils::print(" >> table_id:" + std::to_string(table_id));

  auto tables = get_tables_ptr(GlobalTestEnvironment::TEST_DB);

  ErrorCode error = tables->init();
  EXPECT_EQ(ErrorCode::OK, error);

  // add table metadata.
  ptree retrievd_metadata;
  error = tables->get(table_id, retrievd_metadata);
  EXPECT_EQ(ErrorCode::OK, error);

  UTUtils::print(" " + UTUtils::get_tree_string(retrievd_metadata));

  return retrievd_metadata;
}

/**
 * @brief Remove one table metadata to table metadata table.
 * @param (table_id)  [in]   table id of remove table metadata.
 * @return none.
 */
void TableMetadataHelper::remove_table(const ObjectId table_id) {
  UTUtils::print("-- remove table metadata --");
  UTUtils::print(" >> table_id: ", table_id);

  auto tables = get_tables_ptr(GlobalTestEnvironment::TEST_DB);

  ErrorCode error = tables->init();
  ASSERT_EQ(ErrorCode::OK, error);

  // remove table metadata.
  error = tables->remove(table_id);
  ASSERT_EQ(ErrorCode::OK, error);
}

/**
 * @brief Remove one table metadata to table metadata table.
 * @param (table_name)  [in]  table name of remove table metadata.
 * @return none.
 */
void TableMetadataHelper::remove_table(std::string_view table_name) {
  UTUtils::print("-- remove table metadata --");
  UTUtils::print(" >> table_name: ", table_name);

  auto tables = get_tables_ptr(GlobalTestEnvironment::TEST_DB);

  ErrorCode error = tables->init();
  ASSERT_EQ(ErrorCode::OK, error);

  // remove table metadata.
  error = tables->remove(table_name, nullptr);
  ASSERT_EQ(ErrorCode::OK, error);
}

/**
 * @brief Print column metadata fields used as test data.
 * @param (column_metadata)    [in] column metadata used as test data.
 */
void TableMetadataHelper::print_column_metadata(
    const UTColumnMetadata& column_metadata) {
  std::string data_length_string;
  for (auto value : column_metadata.data_length) {
    data_length_string = data_length_string +
                         (data_length_string.empty() ? "" : ",") +
                         std::to_string(value);
  }

  UTUtils::print(" id: ", column_metadata.id);
  UTUtils::print(" tableId: ", column_metadata.table_id);
  UTUtils::print(" name: ", column_metadata.name);
  UTUtils::print(" ordinalPosition: ", column_metadata.column_number);
  UTUtils::print(" dataTypeId: ", column_metadata.data_type_id);
  UTUtils::print(" dataLength: [", data_length_string, "]");
  UTUtils::print(" varying: ", column_metadata.varying);
  UTUtils::print(" nullable: ", column_metadata.is_not_null);
  UTUtils::print(" defaultExpr: ", column_metadata.default_expr);
}

/**
 * @brief Print table statistic fields.
 * @param (table_statistics)    [in] table statistics used as test data.
 */
void TableMetadataHelper::print_table_statistics(
    const boost::property_tree::ptree& table_statistics) {
  auto metadata_id   = table_statistics.get_optional<ObjectId>(Table::ID);
  auto metadata_name = table_statistics.get_optional<std::string>(Table::NAME);
  auto metadata_namespace =
      table_statistics.get_optional<std::string>(Table::NAMESPACE);
  auto metadata_tuples =
      table_statistics.get_optional<int64_t>(Table::NUMBER_OF_TUPLES);

  UTUtils::print(" id: ", metadata_id.get_value_or(0));
  UTUtils::print(" name: ", metadata_name.get_value_or("<NULL>"));
  UTUtils::print(" namespace: ", metadata_namespace.get_value_or("<NULL>"));
  UTUtils::print(" tuples: ", metadata_tuples.get_value_or(0));
}

/**
 * @brief Verifies that the actual table metadata equals expected one.
 * @param (expected)   [in]  expected table metadata.
 * @param (actual)     [in]  actual table metadata.
 * @return none.
 */
void TableMetadataHelper::check_table_acls_expected(
    const std::map<std::string_view, std::string_view>& expected,
    const boost::property_tree::ptree& actual) {
  auto expected_check = expected;

  auto o_acls_actual = actual.get_child_optional(Table::TABLE_ACL_NODE);
  if (o_acls_actual) {
    std::vector<ptree> p_columns_expected;
    std::vector<ptree> p_columns_actual;

    BOOST_FOREACH (const ptree::value_type& node, o_acls_actual.value()) {
      auto actual_table_name   = node.first;
      std::string actual_value = actual_table_name + "|" + node.second.data();

      auto expected_item = expected.find(actual_table_name);
      if (expected_item != expected.end()) {
        std::string expected_value = std::string(expected_item->first) + "|" +
                                     std::string(expected_item->second);
        EXPECT_EQ(expected_value, actual_value);

        expected_check.erase(actual_table_name);
      }
    }

    for (auto& check : expected_check) {
      auto table_name = check.first;
      auto acl_value  = check.second;

      std::string actual_value   = "";
      std::string expected_value = "";
      if (acl_value.empty()) {
        // Normal due to no authority.
        EXPECT_TRUE(true);
      } else {
        // Abnormal because the expected authorization cannot be obtained.
        actual_value   = "";
        expected_value = std::string(table_name) + "|" + std::string(acl_value);
        EXPECT_EQ(expected_value, actual_value);
      }
    }
  } else {
    FAIL();
  }
}

}  // namespace manager::metadata::testing
