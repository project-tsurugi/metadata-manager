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
#include <iostream>
#include <string>
#include <string_view>

#include <boost/foreach.hpp>
#include <boost/format.hpp>
#include <boost/property_tree/ptree.hpp>

#include "manager/metadata/dao/postgresql/common_pg.h"
#include "manager/metadata/tables.h"

namespace {

using boost::property_tree::ptree;
using manager::metadata::ErrorCode;
using manager::metadata::FormatVersionType;
using manager::metadata::GenerationType;
using manager::metadata::ObjectIdType;
using manager::metadata::Tables;
using manager::metadata::db::postgresql::ConnectionSPtr;

static constexpr const char* const TEST_DB = "test";
static constexpr const char* const ROLE_NAME = "tsurugi_ut_role_user_1";

ConnectionSPtr connection;
bool test_succeed = true;

#define EXPECT_EQ(expected, actual) \
  func_expect_eq(expected, actual, __FILE__, __LINE__)
#define EXPECT_GT(actual, value) \
  func_expect_gt(actual, value, __FILE__, __LINE__)
#define EXPECT_TRUE(actual) func_expect_bool(actual, true, __FILE__, __LINE__)

bool func_expect_eq(ErrorCode expected, ErrorCode actual, std::string_view file,
                    std::int32_t line) {
  if (expected != actual) {
    std::cout << file << ": " << line << ": Failure" << std::endl
              << "  Expecting it to be equal to "
              << static_cast<int32_t>(expected) << "." << std::endl
              << "  Actual value: " << static_cast<int32_t>(actual)
              << std::endl;
    test_succeed = false;
    return false;
  }
  return true;
}

template <typename T>
bool func_expect_eq(T expected, T actual, std::string_view file,
                    std::int32_t line) {
  if (expected != actual) {
    std::cout << file << ": " << line << ": Failure" << std::endl
              << "  Expecting it to be equal to " << expected << "."
              << std::endl
              << "  Actual value: " << actual << std::endl;
    test_succeed = false;
    return false;
  }
  return true;
}

template <typename T1, typename T2>
bool func_expect_gt(T1 actual, T2 value, std::string_view file,
                    std::int32_t line) {
  if (actual <= static_cast<T1>(value)) {
    std::cout << file << ": " << line << ": Failure" << std::endl
              << "  Expecting it to be greater than " << value << "."
              << std::endl
              << "  Actual value: " << actual << std::endl;
    test_succeed = false;
    return false;
  }
  return true;
}

bool func_expect_bool(bool expected, bool actual, std::string_view file,
                      std::int32_t line) {
  if (expected != actual) {
    std::cout << file << ": " << line << ": Failure" << std::endl
              << "  Expecting it to be equal to " << std::boolalpha << expected
              << "." << std::endl
              << "  Actual: " << std::boolalpha << actual << std::endl;
    test_succeed = false;
    return false;
  }
  return true;
}

/**
 */
std::string indent(int level) {
  std::string s;
  for (int i = 0; i < level; i++) s += "  ";
  return s;
}

/**
 * @brief internal function used in get_tree_string, print_tree.
 * get string converted from ptree.
 * @param (pt)                   [in]  ptree to be converted to string.
 * @param (level)                [in]  indent level.
 * @param (output_string)        [out] string converted from ptree.
 * @param (print_tree_enabled)   [in]  enable/disable to print output_string.
 */
void get_tree_string_internal(const boost::property_tree::ptree& pt, int level,
                              std::string& output_string,
                              bool print_tree_enabled) {
  if (pt.empty()) {
    output_string.append("\"");
    output_string.append(pt.data());
    output_string.append("\"");

    if (print_tree_enabled) std::cerr << "\"" << pt.data() << "\"";
  } else {
    if (level && print_tree_enabled) std::cerr << std::endl;

    if (print_tree_enabled) std::cerr << indent(level) << "{" << std::endl;
    output_string.append("{");

    for (auto pos = pt.begin(); pos != pt.end();) {
      if (print_tree_enabled)
        std::cerr << indent(level + 1) << "\"" << pos->first << "\": ";
      output_string.append("\"");
      output_string.append(pos->first);
      output_string.append("\": ");

      get_tree_string_internal(pos->second, level + 1, output_string,
                               print_tree_enabled);
      ++pos;
      if (pos != pt.end()) {
        if (print_tree_enabled) std::cerr << ",";
        output_string.append(",");
      }
      if (print_tree_enabled) std::cerr << std::endl;
    }

    if (print_tree_enabled) std::cerr << indent(level) << " }";
    output_string.append(" }");
  }

  return;
}

/**
 * @brief Get string converted from ptree. (not print string)
 * @param (pt)                   [in]  ptree to be converted to string.
 */
std::string get_tree_string(const boost::property_tree::ptree& pt) {
  std::string output_string;
  int level = 0;
  get_tree_string_internal(pt, level, output_string, false);
  return output_string;
}

}  // namespace

namespace helper {

namespace db = manager::metadata::db;

/**
 * @brief Add one new table metadata to table metadata table.
 * @param (new_table)     [in]   new table-metadata.
 * @param (ret_table_id)  [out]  (optional) table ID of the new table-metadata.
 * @return none.
 */
void add_table(const boost::property_tree::ptree& new_table,
               ObjectIdType* ret_table_id = nullptr) {
  std::cout << "-- add table metadata --" << std::endl;

  auto tables = std::make_unique<Tables>(TEST_DB);

  ErrorCode result = tables->init();
  EXPECT_EQ(ErrorCode::OK, result);

  // add table metadata.
  ObjectIdType retval_table_id = 0;
  result = tables->add(new_table, &retval_table_id);
  EXPECT_EQ(ErrorCode::OK, result);
  EXPECT_GT(retval_table_id, 0);

  std::cout << "> new table_id: " << retval_table_id << std::endl;
  std::cout << "  " << get_tree_string(new_table) << std::endl;

  if (ret_table_id != nullptr) {
    *ret_table_id = retval_table_id;
  }
}

/**
 * @brief Remove one new table metadata to table metadata table.
 * @param (table_name)  [in]   table name of remove table metadata.
 * @return none.
 */
void remove_table(std::string_view table_name) {
  std::cout << "-- remove table metadata --" << std::endl;

  auto tables = std::make_unique<Tables>(TEST_DB);

  ErrorCode result = tables->init();
  EXPECT_EQ(ErrorCode::OK, result);

  ObjectIdType table_id = 0;
  // remove table metadata.
  result = tables->remove(table_name, &table_id);
  EXPECT_EQ(ErrorCode::OK, result);

  std::cout << "> table_id: " << table_id << std::endl;
}

/**
 * @brief Verifies that the actual metadata equals expected one.
 * @param (expected)   [in]  expected metadata.
 * @param (actual)     [in]  actual metadata.
 * @param (meta_name)  [in]  column name of column metadata table.
 * @return none.
 */
void check_metadata_expected(const ptree& expected, const ptree& actual,
                             const char* meta_name) {
  auto o_expected = expected.get_child_optional(meta_name);
  auto o_actual = actual.get_child_optional(meta_name);

  if (o_expected && o_actual) {
    auto& p_expected = o_expected.value();
    auto& p_actual = o_actual.value();
    EXPECT_EQ(get_tree_string(p_expected), get_tree_string(p_actual));
  } else {
    bool actual = (!o_expected && !o_actual) ||
                  (o_expected && o_expected.value().empty() && !o_actual) ||
                  (o_actual && o_actual.value().empty() && !o_expected);
    EXPECT_TRUE(actual);
  }
}

/**
 * @brief Verifies that the actual column metadata equals expected one.
 * @param (expected)   [in]  expected column metadata.
 * @param (actual)     [in]  actual column metadata.
 * @param (meta_name)  [in]  column name of column metadata table.
 * @return none.
 */
template <typename T>
void check_column_metadata_expected(const ptree& expected, const ptree& actual,
                                    const char* meta_name) {
  auto value_expected = expected.get_optional<T>(meta_name);
  auto value_actual = actual.get_optional<T>(meta_name);

  if (value_expected && value_actual) {
    EXPECT_EQ(value_expected.value(), value_actual.value());
  } else {
    bool actual = !value_expected && !value_actual;
    EXPECT_TRUE(actual);
  }
}

/**
 * @brief Verifies that the actual table metadata equals expected one.
 * @param (expected)   [in]  expected table metadata.
 * @param (actual)     [in]  actual table metadata.
 * @return none.
 */
void check_table_metadata_expected(const ptree& expected, const ptree& actual) {
  // format version
  EXPECT_EQ(Tables::format_version(),
            actual.get<FormatVersionType>(Tables::FORMAT_VERSION));

  // generation
  EXPECT_EQ(Tables::generation(),
            actual.get<GenerationType>(Tables::GENERATION));

  // table name
  EXPECT_EQ(expected.get<std::string>(Tables::NAME),
            actual.get<std::string>(Tables::NAME));

  // table id
  ObjectIdType table_id_expected = expected.get<ObjectIdType>(Tables::ID);
  EXPECT_EQ(table_id_expected, actual.get<ObjectIdType>(Tables::ID));

  // namespace
  boost::optional<std::string> o_namespace_expected =
      expected.get_optional<std::string>(Tables::NAMESPACE);
  boost::optional<std::string> o_namespace_actual =
      actual.get_optional<std::string>(Tables::NAMESPACE);
  bool namespace_empty_expected =
      (o_namespace_expected ? o_namespace_expected.value().empty() : true);
  bool namespace_empty_actual =
      (o_namespace_actual ? o_namespace_actual.value().empty() : true);
  if (!namespace_empty_expected && !namespace_empty_actual) {
    std::string& s_namespace_expected = o_namespace_expected.value();
    std::string& s_namespace_actual = o_namespace_actual.value();
    EXPECT_EQ(s_namespace_expected, s_namespace_actual);
  } else {
    bool actual = namespace_empty_expected && namespace_empty_actual;
    EXPECT_TRUE(actual);
  }

  // primary keys
  check_metadata_expected(expected, actual, Tables::PRIMARY_KEY_NODE);

  // tuples
  auto o_tuples_expected = expected.get_optional<float>(Tables::TUPLES);
  auto o_tuples_actual = expected.get_optional<float>(Tables::TUPLES);
  if (o_tuples_expected && o_tuples_actual) {
    EXPECT_EQ(o_tuples_expected.value(), o_tuples_actual.value());
  } else {
    EXPECT_TRUE(!o_tuples_expected && !o_tuples_actual);
  }

  // column metadata
  auto o_columns_expected = expected.get_child_optional(Tables::COLUMNS_NODE);
  auto o_columns_actual = actual.get_child_optional(Tables::COLUMNS_NODE);

  if (o_columns_expected && o_columns_actual) {
    std::vector<ptree> p_columns_expected;
    std::vector<ptree> p_columns_actual;
    BOOST_FOREACH (const ptree::value_type& node, o_columns_expected.value()) {
      ptree column = node.second;
      p_columns_expected.emplace_back(column);
    }
    BOOST_FOREACH (const ptree::value_type& node, o_columns_actual.value()) {
      ptree column = node.second;
      p_columns_actual.emplace_back(column);
    }

    // Verifies that the number of column metadata is expected number.
    EXPECT_EQ(p_columns_expected.size(), p_columns_actual.size());

    for (std::size_t op = 0; op < p_columns_expected.size(); op += 1) {
      ptree column_expected = p_columns_expected[op];
      ptree column_actual = p_columns_actual[op];

      // column metadata id
      boost::optional<ObjectIdType> id_actual =
          column_actual.get<ObjectIdType>(Tables::Column::ID);
      EXPECT_GT(id_actual.value(), static_cast<ObjectIdType>(0));
      // column metadata table id
      boost::optional<ObjectIdType> table_id_actual =
          column_actual.get<ObjectIdType>(Tables::Column::TABLE_ID);
      EXPECT_EQ(table_id_expected, table_id_actual.value());
      // column name
      check_column_metadata_expected<std::string>(
          column_expected, column_actual, Tables::Column::NAME);
      // column ordinal position
      check_column_metadata_expected<ObjectIdType>(
          column_expected, column_actual, Tables::Column::ORDINAL_POSITION);
      // column data type id
      check_column_metadata_expected<ObjectIdType>(
          column_expected, column_actual, Tables::Column::DATA_TYPE_ID);
      // column data length
      check_metadata_expected(column_expected, column_actual,
                              Tables::Column::DATA_LENGTH);
      // column varying
      check_column_metadata_expected<bool>(column_expected, column_actual,
                                           Tables::Column::VARYING);
      // nullable
      check_column_metadata_expected<bool>(column_expected, column_actual,
                                           Tables::Column::NULLABLE);
      // default
      check_column_metadata_expected<std::string>(
          column_expected, column_actual, Tables::Column::DEFAULT);
      // direction
      check_column_metadata_expected<ObjectIdType>(
          column_expected, column_actual, Tables::Column::DIRECTION);
    }
  } else {
    bool actual = !o_columns_expected && !o_columns_actual;
    EXPECT_TRUE(actual);
  }
}

}  // namespace helper

namespace test {

/**
 * @brief Test for Tables class object.
 */
ErrorCode tables_test() {
  ErrorCode result = ErrorCode::UNKNOWN;

  const std::string table_name =
      "UTex_test_table_name_" + std::to_string(__LINE__);

  // create dummy metadata for Tables.
  ptree new_table;
  new_table.put(Tables::NAME, table_name);
  new_table.put(Tables::NAMESPACE, "namespace");
  new_table.put(Tables::TUPLES, 1.5);

  // Set the value of the primary_keys column to ptree.
  ptree primary_keys;
  ptree primary_keys_value;
  primary_keys_value.put("", "1");
  primary_keys.push_back(std::make_pair("", primary_keys_value));
  new_table.add_child(Tables::PRIMARY_KEY_NODE, primary_keys);

  // Set the value of the columns to ptree.
  ptree columns;
  ptree column;
  column.put(Tables::Column::NAME, "col-1");
  column.put(Tables::Column::ORDINAL_POSITION, 1);
  column.put(Tables::Column::DATA_TYPE_ID, 6);
  column.put(Tables::Column::NULLABLE, "true");
  columns.push_back(std::make_pair("", column));

  column.put(Tables::Column::NAME, "col-2");
  column.put(Tables::Column::ORDINAL_POSITION, 2);
  column.put(Tables::Column::NULLABLE, "false");
  column.put(Tables::Column::DATA_TYPE_ID, 14);
  column.put(Tables::Column::VARYING, "true");
  column.put(Tables::Column::DATA_LENGTH, 100);
  column.put(Tables::Column::DEFAULT, "default-text");
  columns.push_back(std::make_pair("", column));
  new_table.add_child(Tables::COLUMNS_NODE, columns);

  // add table metadata.
  ObjectIdType ret_table_id = -1;
  helper::add_table(new_table, &ret_table_id);
  new_table.put(Tables::ID, ret_table_id);

  auto tables = std::make_unique<Tables>(TEST_DB);
  result = tables->init();
  EXPECT_EQ(ErrorCode::OK, result);

  ptree table_metadata;

  // get table metadata by table id.
  result = tables->get(ret_table_id, table_metadata);
  EXPECT_EQ(ErrorCode::OK, result);

  std::cout << "-- get table metadata by table id --" << std::endl;
  std::cout << "  " << get_tree_string(table_metadata) << std::endl;

  // verifies that the returned table metadata is expected one.
  helper::check_table_metadata_expected(new_table, table_metadata);

  // clear property_tree.
  table_metadata.clear();

  // get table metadata by table name.
  result = tables->get(table_name, table_metadata);
  EXPECT_EQ(ErrorCode::OK, result);

  std::cout << "-- get table metadata by table name --" << std::endl;
  std::cout << "  " << get_tree_string(table_metadata) << std::endl;

  // verifies that the returned table metadata is expected one.
  helper::check_table_metadata_expected(new_table, table_metadata);

  std::cout << std::endl << std::string(30, '-') << std::endl;
  std::cout << "-- update table metadata --" << std::endl;
  ptree update_table;
  update_table.put(Tables::ID, ret_table_id);
  update_table.put(Tables::NAME, table_name + "-update");
  update_table.put(Tables::NAMESPACE, "namespace-update");
  update_table.put(Tables::TUPLES, 3.1);

  auto columns_node = table_metadata.get_child(Tables::COLUMNS_NODE);
  auto it = columns_node.begin();

  ptree update_columns;
  ptree update_column;

  // 1 item skip.
  // 2 item update.
  update_column = (++it)->second;
  update_column.put(
      Tables::Column::ID,
      it->second.get_optional<ObjectIdType>(Tables::Column::ID).value());
  update_column.put(Tables::Column::NAME,
                    it->second.get_optional<std::string>(Tables::Column::NAME)
                            .value_or("unknown-1") +
                        "-update");
  update_column.put(Tables::Column::ORDINAL_POSITION, 1);
  update_column.put(Tables::Column::DATA_TYPE_ID, 6);
  update_column.erase(Tables::Column::DATA_LENGTH);
  update_column.put<bool>(Tables::Column::VARYING, false);
  update_column.put<bool>(Tables::Column::NULLABLE, true);
  update_column.put(Tables::Column::DEFAULT, -1);
  update_column.put(Tables::Column::DIRECTION,
                    static_cast<int>(Tables::Column::Direction::ASCENDANT));
  update_columns.push_back(std::make_pair("", update_column));

  // 3 item add.
  update_column.clear();
  update_column.put(Tables::Column::NAME, "new-col-3");
  update_column.put(Tables::Column::ORDINAL_POSITION, 2);
  update_column.put(Tables::Column::DATA_TYPE_ID, 14);
  update_column.put<bool>(Tables::Column::VARYING, false);
  update_column.put<bool>(Tables::Column::NULLABLE, true);
  update_column.put(Tables::Column::DATA_LENGTH, 200);
  update_column.put(Tables::Column::DEFAULT, "default-text-2");
  update_columns.push_back(std::make_pair("", update_column));

  update_table.add_child(Tables::COLUMNS_NODE, update_columns);

  // update table metadata.
  result = tables->update(ret_table_id, update_table);
  EXPECT_EQ(ErrorCode::OK, result);

  // get table metadata by table id.
  table_metadata.clear();
  result = tables->get(ret_table_id, table_metadata);
  EXPECT_EQ(ErrorCode::OK, result);

  std::cout << "-- get table metadata by table id --" << std::endl;
  std::cout << "  " << get_tree_string(table_metadata) << std::endl;

  // verifies that the returned table metadata is expected one.
  helper::check_table_metadata_expected(update_table, table_metadata);

  std::cout << std::endl << std::string(30, '-') << std::endl;

  // remove table metadata.
  auto remove_table_name = table_metadata.get<std::string>(Tables::NAME);
  helper::remove_table(remove_table_name);

  result = ErrorCode::OK;

  return result;
}

}  // namespace test

/**
 * @brief main function.
 */
int main(void) {
  std::cout << "*** TablesMetadata test start. ***" << std::endl << std::endl;

  std::cout << "=== class object test start. ===" << std::endl;
  test::tables_test();
  std::cout << "=== class object test done. ===" << std::endl;
  std::cout << std::endl;

  std::cout << "TablesMetadata test : ";
  if (test_succeed) {
    std::cout << "Success" << std::endl;
  } else {
    std::cout << "*** Failure ***" << std::endl;
  }

  std::cout << std::endl;

  std::cout << "*** TablesMetadata test completed. ***" << std::endl;

  return EXIT_SUCCESS;
}
