/*
 * Copyright 2020-2023 tsurugi project.
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

#include "manager/metadata/dao/postgresql/pg_common.h"
#include "manager/metadata/metadata_factory.h"

namespace {

using boost::property_tree::ptree;
using manager::metadata::Column;
using manager::metadata::Constraint;
using manager::metadata::DataTypes;
using manager::metadata::ErrorCode;
using manager::metadata::FormatVersionType;
using manager::metadata::GenerationType;
using manager::metadata::ObjectIdType;
using manager::metadata::Table;
using manager::metadata::Tables;

static constexpr const char* const TEST_DB   = "test";
static constexpr const char* const ROLE_NAME = "tsurugi_ut_role_user_1";

manager::metadata::db::PgConnectionPtr connection;
bool test_succeed = true;

#define EXPECT_EQ(expected, actual) \
  func_expect_eq(expected, actual, "", __FILE__, __LINE__)
#define EXPECT_GT(actual, value) \
  func_expect_gt(actual, value, "", __FILE__, __LINE__)
#define EXPECT_TRUE(actual) \
  func_expect_bool(actual, true, "", __FILE__, __LINE__)
#define EXPECT_EQ_T(expected, actual, text) \
  func_expect_eq(expected, actual, text, __FILE__, __LINE__)
#define EXPECT_GT_T(actual, value, text) \
  func_expect_gt(actual, value, text, __FILE__, __LINE__)
#define EXPECT_TRUE_T(actual, text) \
  func_expect_bool(actual, true, text, __FILE__, __LINE__)

std::ostream& operator<<(std::ostream& os, const ErrorCode& ec) {
  os << static_cast<std::int32_t>(ec);
  return os;
}

template <typename T>
bool func_expect_eq(T expected, T actual, std::string_view text,
                    std::string_view file, std::int32_t line) {
  if (expected != actual) {
    std::cout << file << ": " << line << ": Failure" << std::endl
              << "  Expecting it to be equal to " << expected << ".";
    if (!text.empty()) {
      std::cout << " [" << text << "]";
    }
    std::cout << std::endl << "  Actual value: " << actual << std::endl;
    test_succeed = false;
    return false;
  }
  return true;
}

template <typename T1, typename T2>
bool func_expect_gt(T1 actual, T2 value, std::string_view text,
                    std::string_view file, std::int32_t line) {
  if (actual <= static_cast<T1>(value)) {
    std::cout << file << ": " << line << ": Failure" << std::endl
              << "  Expecting it to be greater than " << value << ".";
    if (!text.empty()) {
      std::cout << " [" << text << "]";
    }
    std::cout << std::endl << "  Actual value: " << actual << std::endl;
    test_succeed = false;
    return false;
  }
  return true;
}

bool func_expect_bool(bool expected, bool actual, std::string_view text,
                      std::string_view file, std::int32_t line) {
  if (expected != actual) {
    std::cout << file << ": " << line << ": Failure" << std::endl
              << "  Expecting it to be equal to " << std::boolalpha << expected
              << ".";
    if (!text.empty()) {
      std::cout << " [" << text << "]";
    }
    std::cout << "  Actual: " << std::boolalpha << actual << std::endl;
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

  auto tables = manager::metadata::get_tables_ptr(TEST_DB);

  ErrorCode result = tables->init();
  EXPECT_EQ(ErrorCode::OK, result);

  ObjectIdType retval_table_id = 0;
  // add table metadata.
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

  auto tables = manager::metadata::get_tables_ptr(TEST_DB);

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
void check_child_expected(const ptree& expected, const ptree& actual,
                          const char* meta_name) {
  auto o_expected = expected.get_child_optional(meta_name);
  auto o_actual   = actual.get_child_optional(meta_name);

  if (o_expected && o_actual) {
    const auto& p_expected = o_expected.value();
    const auto& p_actual   = o_actual.value();
    EXPECT_EQ_T(get_tree_string(p_expected), get_tree_string(p_actual),
                meta_name);
  } else if (o_expected) {
    EXPECT_EQ_T(o_expected.value().empty(), !o_actual.is_initialized(),
                meta_name);
  } else if (o_actual) {
    EXPECT_EQ_T(!o_expected.is_initialized(), o_actual.value().empty(),
                meta_name);
  } else {
    EXPECT_EQ_T(!o_expected.is_initialized(), !o_actual.is_initialized(),
                meta_name);
  }
}

/**
 * @brief Verifies that the actual metadata equals expected one.
 * @param (expected)   [in]  expected metadata.
 * @param (actual)     [in]  actual metadata.
 * @param (meta_name)  [in]  name of metadata table.
 * @return none.
 */
template <typename T>
void check_expected(const ptree& expected, const ptree& actual,
                    const char* meta_name) {
  auto value_expected = expected.get_optional<T>(meta_name);
  auto value_actual   = actual.get_optional<T>(meta_name);

  if (value_expected && value_actual) {
    EXPECT_EQ_T(value_expected.value(), value_actual.value(), meta_name);
  } else {
    if (value_expected) {
      const auto& value_expected = expected.get<std::string>(meta_name);
      EXPECT_EQ_T(value_expected.empty(), !value_actual.is_initialized(),
                  meta_name);
    } else if (value_actual) {
      const auto& value_actual = actual.get<std::string>(meta_name);
      EXPECT_EQ_T(!value_expected.is_initialized(), value_actual.empty(),
                  meta_name);
    } else {
      EXPECT_EQ_T(!value_expected.is_initialized(),
                  !value_actual.is_initialized(), meta_name);
    }
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
            actual.get<FormatVersionType>(Table::FORMAT_VERSION));

  // generation
  EXPECT_EQ(Tables::generation(),
            actual.get<GenerationType>(Table::GENERATION));

  // table name
  check_expected<std::string>(expected, actual, Table::NAME);

  // table id
  ObjectIdType table_id_expected = expected.get<ObjectIdType>(Table::ID);
  EXPECT_EQ(table_id_expected, actual.get<ObjectIdType>(Table::ID));

  // namespace
  check_expected<std::string>(expected, actual, Table::NAMESPACE);

  // tuples
  auto o_tuples_expected =
      expected.get_optional<int64_t>(Table::NUMBER_OF_TUPLES);
  auto o_tuples_actual =
      expected.get_optional<int64_t>(Table::NUMBER_OF_TUPLES);
  if (o_tuples_expected && o_tuples_actual) {
    EXPECT_EQ(o_tuples_expected.value(), o_tuples_actual.value());
  } else {
    EXPECT_TRUE(!o_tuples_expected && !o_tuples_actual);
  }

  // column metadata
  {
    auto o_columns_expected = expected.get_child_optional(Table::COLUMNS_NODE);
    auto o_columns_actual   = actual.get_child_optional(Table::COLUMNS_NODE);

    if (o_columns_expected && o_columns_actual) {
      std::vector<ptree> p_columns_expected;
      std::vector<ptree> p_columns_actual;
      BOOST_FOREACH (const ptree::value_type& node,
                     o_columns_expected.value()) {
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
        ptree columns_expected = p_columns_expected[op];
        ptree columns_actual   = p_columns_actual[op];

        // column metadata id
        boost::optional<ObjectIdType> id_actual =
            columns_actual.get<ObjectIdType>(Column::ID);
        EXPECT_GT(id_actual.value(), static_cast<ObjectIdType>(0));
        // column metadata table id
        boost::optional<ObjectIdType> table_id_actual =
            columns_actual.get<ObjectIdType>(Column::TABLE_ID);
        EXPECT_EQ(table_id_expected, table_id_actual.value());
        // column name
        check_expected<std::string>(columns_expected, columns_actual,
                                    Column::NAME);
        // column ordinal position
        check_expected<ObjectIdType>(columns_expected, columns_actual,
                                     Column::COLUMN_NUMBER);
        // column data type id
        check_expected<ObjectIdType>(columns_expected, columns_actual,
                                     Column::DATA_TYPE_ID);
        // column data length
        check_child_expected(columns_expected, columns_actual,
                             Column::DATA_LENGTH);
        // column varying
        check_expected<bool>(columns_expected, columns_actual, Column::VARYING);
        // nullable
        check_expected<bool>(columns_expected, columns_actual,
                             Column::IS_NOT_NULL);
        // default
        check_expected<std::string>(columns_expected, columns_actual,
                                    Column::DEFAULT_EXPR);
        // is_funcexpr
        check_expected<bool>(columns_expected, columns_actual,
                             Column::IS_FUNCEXPR);
      }
    } else {
      EXPECT_EQ(o_columns_expected.is_initialized(),
                o_columns_actual.is_initialized());
    }
  }

  // constraint metadata
  {
    auto o_constraints_expected =
        expected.get_child_optional(Table::CONSTRAINTS_NODE);
    auto o_constraints_actual =
        actual.get_child_optional(Table::CONSTRAINTS_NODE);

    if (o_constraints_expected && o_constraints_actual) {
      std::vector<ptree> p_constraints_expected;
      std::vector<ptree> p_constraints_actual;
      BOOST_FOREACH (const ptree::value_type& node,
                     o_constraints_expected.value()) {
        ptree constraint = node.second;
        p_constraints_expected.emplace_back(constraint);
      }
      BOOST_FOREACH (const ptree::value_type& node,
                     o_constraints_actual.value()) {
        ptree constraint = node.second;
        p_constraints_actual.emplace_back(constraint);
      }

      // Verifies that the number of constraint metadata is expected number.
      EXPECT_EQ(p_constraints_expected.size(), p_constraints_actual.size());

      for (std::size_t op = 0; op < p_constraints_expected.size(); op += 1) {
        ptree constraints_expected = p_constraints_expected[op];
        ptree constraints_actual   = p_constraints_actual[op];

        // constraint metadata id
        boost::optional<ObjectIdType> id_actual =
            constraints_actual.get<ObjectIdType>(Constraint::ID);
        EXPECT_GT(id_actual.value(), static_cast<ObjectIdType>(0));
        // constraint metadata table id
        boost::optional<ObjectIdType> table_id_actual =
            constraints_actual.get<ObjectIdType>(Constraint::TABLE_ID);
        EXPECT_EQ(table_id_expected, table_id_actual.value());

        // constraint name
        check_expected<std::string>(constraints_expected, constraints_actual,
                                    Constraint::NAME);
        // constraint type
        check_expected<ObjectIdType>(constraints_expected, constraints_actual,
                                     Constraint::TYPE);
        // constraint column numbers
        check_child_expected(constraints_expected, constraints_actual,
                             Constraint::COLUMNS);
        // constraint column IDs
        check_child_expected(constraints_expected, constraints_actual,
                             Constraint::COLUMNS_ID);
        // constraint index id
        check_expected<ObjectIdType>(constraints_expected, constraints_actual,
                                     Constraint::INDEX_ID);
        // constraint expression
        check_expected<std::string>(constraints_expected, constraints_actual,
                                    Constraint::EXPRESSION);
      }
    } else {
      if (o_constraints_expected) {
        EXPECT_EQ(o_constraints_expected.value().size() == 0,
                  !o_constraints_actual.is_initialized());
      } else if (o_constraints_actual) {
        EXPECT_EQ(!o_constraints_expected.is_initialized(),
                  o_constraints_actual.value().size() == 0);
      } else {
        EXPECT_EQ(!o_constraints_expected.is_initialized(),
                  !o_constraints_actual.is_initialized());
      }
    }
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
  new_table.put(Table::NAME, table_name);
  new_table.put(Table::NAMESPACE, "namespace");
  new_table.put(Table::NUMBER_OF_TUPLES, 15);

  // Set the value of the columns to ptree.
  ptree columns;
  ptree column;
  column.put(Column::NAME, "col-1");
  column.put(Column::COLUMN_NUMBER, 1);
  column.put(Column::DATA_TYPE_ID,
             static_cast<int64_t>(DataTypes::DataTypesId::INT64));
  column.put(Column::IS_NOT_NULL, "true");
  column.put(Column::VARYING, "false");
  column.put(Column::IS_FUNCEXPR, "false");
  columns.push_back(std::make_pair("", column));

  column.clear();
  column.put(Column::NAME, "col-2");
  column.put(Column::COLUMN_NUMBER, 2);
  column.put(Column::IS_NOT_NULL, "false");
  column.put(Column::DATA_TYPE_ID,
             static_cast<int64_t>(DataTypes::DataTypesId::VARCHAR));
  column.put(Column::VARYING, "true");
  {
    ptree elements;
    ptree element;
    element.put("", 100);
    elements.push_back(std::make_pair("", element));
    column.add_child(Column::DATA_LENGTH, elements);
  }
  column.put(Column::DEFAULT_EXPR, "default-text");
  columns.push_back(std::make_pair("", column));
  new_table.add_child(Table::COLUMNS_NODE, columns);

  // Set the value of the constraints to ptree.
  ptree constraints;
  ptree constraint;
  ptree columns_num;
  ptree columns_num_value;
  ptree columns_id;
  ptree columns_id_value;

  constraint.put(Constraint::TYPE,
                 static_cast<int32_t>(Constraint::ConstraintType::UNIQUE));
  // constraints
  constraints.push_back(std::make_pair("", constraint));

  constraint.clear();
  columns_num.clear();
  columns_num_value.clear();
  columns_id.clear();
  columns_id_value.clear();
  // type
  constraint.put(Constraint::TYPE,
                 static_cast<int32_t>(Constraint::ConstraintType::CHECK));
  // columns
  columns_num_value.put("", 1);
  columns_num.push_back(std::make_pair("", columns_num_value));
  columns_num_value.put("", 2);
  columns_num.push_back(std::make_pair("", columns_num_value));
  constraint.add_child(Constraint::COLUMNS, columns_num);
  // columns id
  columns_id_value.put("", 1234);
  columns_id.push_back(std::make_pair("", columns_id_value));
  columns_id_value.put("", 5678);
  columns_id.push_back(std::make_pair("", columns_id_value));
  constraint.add_child(Constraint::COLUMNS_ID, columns_id);
  // expression
  constraint.put(Constraint::EXPRESSION, "expression text");
  // constraints
  constraints.push_back(std::make_pair("", constraint));

  new_table.add_child(Table::CONSTRAINTS_NODE, constraints);

  // add table metadata.
  ObjectIdType ret_table_id = -1;
  helper::add_table(new_table, &ret_table_id);
  new_table.put(Table::ID, ret_table_id);

  auto tables = manager::metadata::get_tables_ptr(TEST_DB);
  result      = tables->init();
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
  update_table.put(Table::ID, ret_table_id);
  update_table.put(Table::NAME, table_name + "-update");
  update_table.put(Table::NAMESPACE, "namespace-update");
  update_table.put(Table::NUMBER_OF_TUPLES, 31);

  // columns metadata.
  {
    auto columns_node = table_metadata.get_child(Table::COLUMNS_NODE);
    ptree update_columns;
    ptree update_column;

    auto it = columns_node.begin();
    // 1 item skip.
    // 2 item update.
    update_column = (++it)->second;
    update_column.put(
        Column::ID, it->second.get_optional<ObjectIdType>(Column::ID).value());
    update_column.put(Column::NAME,
                      it->second.get_optional<std::string>(Column::NAME)
                              .value_or("unknown-1") +
                          "-update");
    update_column.put(Column::COLUMN_NUMBER, 1);
    update_column.put(Column::DATA_TYPE_ID,
                      static_cast<int64_t>(DataTypes::DataTypesId::INT64));
    update_column.erase(Column::DATA_LENGTH);
    update_column.put<bool>(Column::VARYING, false);
    update_column.put<bool>(Column::IS_NOT_NULL, true);
    update_column.put(Column::DEFAULT_EXPR, -1);
    update_column.put<bool>(Column::IS_FUNCEXPR, false);
    update_columns.push_back(std::make_pair("", update_column));

    // 3 item add.
    update_column.clear();
    update_column.put(Column::NAME, "new-col-3");
    update_column.put(Column::COLUMN_NUMBER, 2);
    update_column.put(Column::DATA_TYPE_ID,
                      static_cast<int64_t>(DataTypes::DataTypesId::VARCHAR));
    update_column.put<bool>(Column::VARYING, false);
    update_column.put<bool>(Column::IS_NOT_NULL, true);
    {
      ptree elements;
      ptree element;
      element.put("", 200);
      elements.push_back(std::make_pair("", element));
      update_column.add_child(Column::DATA_LENGTH, elements);
    }
    update_column.put(Column::DEFAULT_EXPR, "default-text-2");
    update_columns.push_back(std::make_pair("", update_column));
    update_column.put<bool>(Column::IS_FUNCEXPR, true);

    update_table.add_child(Table::COLUMNS_NODE, update_columns);
  }

  // constraint metadata.
  {
    auto constraints_node = table_metadata.get_child(Table::CONSTRAINTS_NODE);
    ptree update_constraints;
    ptree update_constraint;

    auto it = constraints_node.begin();
    // 1 item update.
    update_constraint = it->second;
    update_constraint.put(Constraint::NAME,
                          it->second.get_optional<std::string>(Constraint::NAME)
                                  .value_or("unknown-1") +
                              "-update");
    update_constraints.push_back(std::make_pair("", update_constraint));
    update_table.add_child(Table::CONSTRAINTS_NODE, update_constraints);
  }

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
  auto remove_table_name = table_metadata.get<std::string>(Table::NAME);
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
