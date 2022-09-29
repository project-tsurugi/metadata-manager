/*
 * Copyright 2021-2022 tsurugi project.
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
#include <libpq-fe.h>

#include <memory>
#include <string>
#include <vector>

#include <boost/foreach.hpp>
#include <boost/format.hpp>

#include "manager/metadata/common/config.h"
#include "manager/metadata/dao/postgresql/tables_dao_pg.h"
#include "manager/metadata/datatypes.h"
#include "test/global_test_environment.h"
#include "test/utility/ut_table_metadata.h"
#include "test/utility/ut_utils.h"

namespace manager::metadata::testing {

#define EXPECT_EQ_T(expected, actual, text)                 \
  if (expected != actual) std::cout << "[" << text << "] "; \
  EXPECT_EQ(expected, actual)

namespace storage = manager::metadata::db::postgresql;

using boost::property_tree::ptree;
using manager::metadata::db::postgresql::TablesDAO;

/**
 * @brief Get the number of records in the current table metadata.
 * @return Current number of records.
 */
std::int64_t TableMetadataHelper::get_record_count() {
  PGconn* connection = PQconnectdb(Config::get_connection_string().c_str());

  boost::format statement =
      boost::format("SELECT COUNT(*) FROM %s.%s") % storage::SCHEMA_NAME % TablesDAO::kTableName;
  PGresult* res = PQexec(connection, statement.str().c_str());

  std::int64_t res_val;
  storage::DbcUtils::str_to_integral(PQgetvalue(res, 0, 0), res_val);

  PQclear(res);
  PQfinish(connection);

  return res_val;
}

/**
 * @brief Generate table metadata.
 * @param (testdata_table_metadata)    [out] table metadata used as test data.
 */
void TableMetadataHelper::generate_table_metadata(
    std::unique_ptr<UTTableMetadata>& testdata_table_metadata) {
  // generate unique table name.
  int s                   = time(NULL);
  std::string table_name  = "table_name" + std::to_string(s);
  testdata_table_metadata = std::make_unique<UTTableMetadata>(table_name);

  // generate namespace.
  testdata_table_metadata->namespace_name = "namespace";

  // generate primary keys.
  std::vector<ObjectIdType> ordinal_positions = {1, 2, 3};
  testdata_table_metadata->primary_keys.push_back(ordinal_positions[0]);
  testdata_table_metadata->primary_keys.push_back(ordinal_positions[1]);

  // generate tuples.
  testdata_table_metadata->tuples = 0;

  // generate three column metadata.
  {
    std::vector<std::string> col_names = {"col1", "col2", "col3"};

    // first column metadata
    bool is_null = true;
    UTColumnMetadata column1{col_names[0], ordinal_positions[0],
                             static_cast<ObjectIdType>(DataTypes::DataTypesId::FLOAT32), !is_null};
    column1.direction = static_cast<ObjectIdType>(Tables::Column::Direction::ASCENDANT);

    // second column metadata
    UTColumnMetadata column2{col_names[1], ordinal_positions[1],
                             static_cast<ObjectIdType>(DataTypes::DataTypesId::VARCHAR), !is_null};
    column2.direction = static_cast<ObjectIdType>(Tables::Column::Direction::DEFAULT);
    ptree data_length;
    data_length.put("", 8);
    column2.p_data_lengths.push_back(std::make_pair("", data_length));
    data_length.put("", 2);
    column2.p_data_lengths.push_back(std::make_pair("", data_length));

    column2.varying = true;

    // third column metadata
    UTColumnMetadata column3{col_names[2], ordinal_positions[2],
                             static_cast<ObjectIdType>(DataTypes::DataTypesId::CHAR), is_null};
    column3.default_expr = "default";
    column3.data_length  = 1;
    column3.varying      = false;

    // set table metadata to three column metadata
    testdata_table_metadata->columns.emplace_back(column1);
    testdata_table_metadata->columns.emplace_back(column2);
    testdata_table_metadata->columns.emplace_back(column3);
  }

  // generate three constraint metadata.
  {
    std::vector<Constraint::ConstraintType> constraint_types = {Constraint::ConstraintType::UNIQUE,
                                                                Constraint::ConstraintType::CHECK};

    ptree columns;
    ptree columns_id;

    // first constraint metadata
    UTConstraintMetadata constraint1{"constraint1", Constraint::ConstraintType::UNIQUE};
    columns.put("", 1);
    constraint1.p_columns.push_back(std::make_pair("", columns));
    constraint1.columns_list.emplace_back(1);
    columns_id.put("", 1234);
    constraint1.p_columns.push_back(std::make_pair("", columns_id));
    constraint1.columns_id_list.emplace_back(1234);
    constraint1.index_id = 1L;

    // second column metadata
    // first constraint metadata
    UTConstraintMetadata constraint2{"constraint2", Constraint::ConstraintType::CHECK};
    columns.put("", 2);
    constraint2.p_columns.push_back(std::make_pair("", columns));
    constraint2.columns_list.emplace_back(5678);
    columns_id.put("", 5678);
    constraint2.p_columns_id.push_back(std::make_pair("", columns_id));
    constraint2.columns_id_list.emplace_back(5678);
    constraint2.expression = "expression-text";

    // set table metadata to two constraints metadata
    testdata_table_metadata->constraints.emplace_back(constraint1);
    testdata_table_metadata->constraints.emplace_back(constraint2);
  }

  // generate ptree from UTTableMetadata fields.
  testdata_table_metadata->generate_ptree();
}

/**
 * @brief Add one new table metadata to table metadata table.
 * @param (table_name)    [in]  table name of new table metadata.
 * @param (ret_table_id)  [out] (optional) table id returned from the api to add new
 *   table metadata.
 * @return none.
 */
void TableMetadataHelper::add_table(std::string_view table_name, ObjectIdType* ret_table_id) {
  // prepare test data for adding table metadata.
  UTTableMetadata* testdata_table_metadata = global->testdata_table_metadata.get();

  ptree new_table = testdata_table_metadata->tables;
  new_table.put(Tables::NAME, table_name);

  // add table metadata.
  add_table(new_table, ret_table_id);
}

/**
 * @brief Add one new table metadata to table metadata table.
 * @param (new_table)  [in]  new table metadata.
 * @param (table_id)   [out] (optional) table id returned from the api to add new table metadata.
 * @return none.
 */
void TableMetadataHelper::add_table(const boost::property_tree::ptree& new_table,
                                    ObjectIdType* table_id) {
  UTUtils::print("-- add table metadata --");
  UTUtils::print(" " + UTUtils::get_tree_string(new_table));

  auto tables = std::make_unique<Tables>(GlobalTestEnvironment::TEST_DB);

  ErrorCode error = tables->init();
  ASSERT_EQ(ErrorCode::OK, error);

  // add table metadata.
  ObjectIdType ret_table_id = INVALID_VALUE;
  error                     = tables->add(new_table, &ret_table_id);
  ASSERT_EQ(ErrorCode::OK, error);
  ASSERT_GT(ret_table_id, 0);

  UTUtils::print(" >> new table_id: ", ret_table_id);

  if (table_id != nullptr) {
    *table_id = ret_table_id;
  }
}

/**
 * @brief Remove one new table metadata to table metadata table.
 * @param (table_id)  [in]   table id of remove table metadata.
 * @return none.
 */
void TableMetadataHelper::remove_table(const ObjectIdType table_id) {
  UTUtils::print("-- remove table metadata --");
  UTUtils::print(" table_id: ", table_id);

  auto tables = std::make_unique<Tables>(GlobalTestEnvironment::TEST_DB);

  ErrorCode error = tables->init();
  ASSERT_EQ(ErrorCode::OK, error);

  // remove table metadata.
  error = tables->remove(table_id);
  ASSERT_EQ(ErrorCode::OK, error);
}

/**
 * @brief Remove one new table metadata to table metadata table.
 * @param (table_name)  [in]   table name of remove table metadata.
 * @return none.
 */
void TableMetadataHelper::remove_table(std::string_view table_name) {
  UTUtils::print("-- remove table metadata --");
  UTUtils::print(" table_name: ", table_name);

  auto tables = std::make_unique<Tables>(GlobalTestEnvironment::TEST_DB);

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
void TableMetadataHelper::print_column_metadata(const UTColumnMetadata& column_metadata) {
  UTUtils::print(" id: ", column_metadata.id);
  UTUtils::print(" tableId: ", column_metadata.table_id);
  UTUtils::print(" name: ", column_metadata.name);
  UTUtils::print(" ordinalPosition: ", column_metadata.ordinal_position);
  UTUtils::print(" dataTypeId: ", column_metadata.data_type_id);
  UTUtils::print(" dataLength: ", column_metadata.data_length);
  UTUtils::print(" varying: ", column_metadata.varying);
  UTUtils::print(" nullable: ", column_metadata.nullable);
  UTUtils::print(" defaultExpr: ", column_metadata.default_expr);
  UTUtils::print(" direction: ", column_metadata.direction);
}

/**
 * @brief Print table statistic fields.
 * @param (table_statistics)    [in] table statistics used as test data.
 */
void TableMetadataHelper::print_table_statistics(
    const boost::property_tree::ptree& table_statistics) {
  boost::optional<ObjectIdType> metadata_id =
      table_statistics.get_optional<ObjectIdType>(Tables::ID);
  boost::optional<std::string> metadata_name =
      table_statistics.get_optional<std::string>(Tables::NAME);
  boost::optional<std::string> metadata_namespace =
      table_statistics.get_optional<std::string>(Tables::NAMESPACE);
  boost::optional<float> metadata_tuples = table_statistics.get_optional<float>(Tables::TUPLES);

  UTUtils::print(" id: ", (metadata_id ? metadata_id.get() : 0));
  UTUtils::print(" name: ", (metadata_name ? metadata_name.get() : "NULL"));
  UTUtils::print(" namespace: ", (metadata_namespace ? metadata_namespace.get() : "NULL"));
  UTUtils::print(" reltuples: ", (metadata_tuples ? metadata_tuples.get() : 0));
}

/**
 * @brief Verifies that the actual table metadata equals expected one.
 * @param (expected)   [in]  expected table metadata.
 * @param (actual)     [in]  actual table metadata.
 * @return none.
 */
void TableMetadataHelper::check_table_metadata_expected(const boost::property_tree::ptree& expected,
                                                        const boost::property_tree::ptree& actual) {
  // format version
  EXPECT_EQ(Tables::format_version(), actual.get<FormatVersionType>(Tables::FORMAT_VERSION));

  // generation
  EXPECT_EQ(Tables::generation(), actual.get<GenerationType>(Tables::GENERATION));

  // table name
  ASSERT_EQ(expected.get<std::string>(Tables::NAME), actual.get<std::string>(Tables::NAME));

  // table id
  ObjectIdType table_id_expected = expected.get<ObjectIdType>(Tables::ID);
  ASSERT_EQ(table_id_expected, actual.get<ObjectIdType>(Tables::ID));

  // namespace
  boost::optional<std::string> o_namespace_expected =
      expected.get_optional<std::string>(Tables::NAMESPACE);
  boost::optional<std::string> o_namespace_actual =
      actual.get_optional<std::string>(Tables::NAMESPACE);
  bool namespace_empty_expected =
      (o_namespace_expected ? o_namespace_expected.value().empty() : true);
  bool namespace_empty_actual = (o_namespace_actual ? o_namespace_actual.value().empty() : true);
  if (!namespace_empty_expected && !namespace_empty_actual) {
    std::string& s_namespace_expected = o_namespace_expected.value();
    std::string& s_namespace_actual   = o_namespace_actual.value();
    EXPECT_EQ(s_namespace_expected, s_namespace_actual);
  } else if (namespace_empty_actual && namespace_empty_actual) {
    ASSERT_TRUE(true);
  } else {
    ASSERT_TRUE(false);
  }

  // primary keys
  check_child_expected(expected, actual, Tables::PRIMARY_KEY_NODE);

  // tuples
  auto o_tuples_expected = expected.get_optional<float>(Tables::TUPLES);
  auto o_tuples_actual   = expected.get_optional<float>(Tables::TUPLES);
  if (o_tuples_expected && o_tuples_actual) {
    EXPECT_EQ(o_tuples_expected.value(), o_tuples_actual.value());
  } else if (!o_tuples_expected && !o_tuples_actual) {
    ASSERT_TRUE(true);
  } else {
    ASSERT_TRUE(false);
  }

  // column metadata
  {
    auto o_columns_expected = expected.get_child_optional(Tables::COLUMNS_NODE);
    auto o_columns_actual   = actual.get_child_optional(Tables::COLUMNS_NODE);

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
        ptree column_actual   = p_columns_actual[op];

        // column metadata id
        boost::optional<ObjectIdType> id_actual =
            column_actual.get<ObjectIdType>(Tables::Column::ID);
        EXPECT_GT(id_actual.value(), static_cast<ObjectIdType>(0));
        // column metadata table id
        boost::optional<ObjectIdType> table_id_actual =
            column_actual.get<ObjectIdType>(Tables::Column::TABLE_ID);
        EXPECT_EQ(table_id_expected, table_id_actual.value());
        // column name
        check_expected<std::string>(column_expected, column_actual, Tables::Column::NAME);
        // column ordinal position
        check_expected<ObjectIdType>(column_expected, column_actual,
                                     Tables::Column::ORDINAL_POSITION);
        // column data type id
        check_expected<ObjectIdType>(column_expected, column_actual, Tables::Column::DATA_TYPE_ID);
        // column data length
        check_child_expected(column_expected, column_actual, Tables::Column::DATA_LENGTH);
        // column varying
        check_expected<bool>(column_expected, column_actual, Tables::Column::VARYING);
        // nullable
        check_expected<bool>(column_expected, column_actual, Tables::Column::NULLABLE);
        // default
        check_expected<std::string>(column_expected, column_actual, Tables::Column::DEFAULT);
        // direction
        check_expected<ObjectIdType>(column_expected, column_actual, Tables::Column::DIRECTION);
      }
    } else {
      EXPECT_EQ(o_columns_expected.is_initialized(), o_columns_actual.is_initialized());
    }
  }

  // constraint metadata
  {
    auto o_constraints_expected = expected.get_child_optional(Tables::CONSTRAINTS_NODE);
    auto o_constraints_actual   = actual.get_child_optional(Tables::CONSTRAINTS_NODE);

    if (o_constraints_expected && o_constraints_actual) {
      std::vector<ptree> p_constraints_expected = {};
      std::vector<ptree> p_constraints_actual = {};

      BOOST_FOREACH (const ptree::value_type& node, o_constraints_expected.value()) {
        ptree constraint = node.second;
        p_constraints_expected.emplace_back(constraint);
      }
      BOOST_FOREACH (const ptree::value_type& node, o_constraints_actual.value()) {
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
        check_expected<std::string>(constraints_expected, constraints_actual, Constraint::NAME);
        // constraint type
        check_expected<ObjectIdType>(constraints_expected, constraints_actual, Constraint::TYPE);
        // constraint column numbers
        check_child_expected(constraints_expected, constraints_actual, Constraint::COLUMNS);
        // constraint column IDs
        check_child_expected(constraints_expected, constraints_actual, Constraint::COLUMNS_ID);
        // constraint index id
        check_expected<ObjectIdType>(constraints_expected, constraints_actual,
                                     Constraint::INDEX_ID);
        // constraint expression
        check_expected<std::string>(constraints_expected, constraints_actual,
                                    Constraint::EXPRESSION);
      }
    } else if (o_constraints_expected) {
      EXPECT_EQ(o_constraints_expected.value().empty(), !o_constraints_actual.is_initialized());
    } else if (o_constraints_actual) {
      EXPECT_EQ(!o_constraints_expected.is_initialized(), o_constraints_actual.value().empty());
    } else {
      EXPECT_EQ(o_constraints_expected.is_initialized(), o_constraints_actual.is_initialized());
    }
  }
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

  auto o_acls_actual = actual.get_child_optional(Tables::TABLE_ACL_NODE);
  if (o_acls_actual) {
    std::vector<ptree> p_columns_expected;
    std::vector<ptree> p_columns_actual;

    BOOST_FOREACH (const ptree::value_type& node, o_acls_actual.value()) {
      auto actual_table_name   = node.first;
      std::string actual_value = actual_table_name + "|" + node.second.data();

      auto expected_item = expected.find(actual_table_name);
      if (expected_item != expected.end()) {
        std::string expected_value =
            std::string(expected_item->first) + "|" + std::string(expected_item->second);
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
    EXPECT_TRUE(false);
  }
}

/**
 * @brief Verifies that the actual metadata equals expected one.
 * @param (expected)   [in]  expected metadata.
 * @param (actual)     [in]  actual metadata.
 * @param (meta_name)  [in]  name of metadata table.
 * @return none.
 */
void TableMetadataHelper::check_child_expected(const boost::property_tree::ptree& expected,
                                               const boost::property_tree::ptree& actual,
                                               const char* meta_name) {
  auto o_expected = expected.get_child_optional(meta_name);
  auto o_actual   = actual.get_child_optional(meta_name);

  if (o_expected && o_actual) {
    auto expected_value = UTUtils::get_tree_string(o_expected.value());
    auto actual_value = UTUtils::get_tree_string(o_actual.value());
    EXPECT_EQ_T(expected_value, actual_value, meta_name);
  } else if (o_expected) {
    EXPECT_EQ_T(o_expected.value().empty(), !o_actual.is_initialized(), meta_name);
  } else if (o_actual) {
    EXPECT_EQ_T(!o_expected.is_initialized(), o_actual.value().empty(), meta_name);
  } else {
    EXPECT_EQ_T(o_expected.is_initialized(), o_actual.is_initialized(), meta_name);
  }
}

/**
 * @brief Verifies that the actual metadata equals expected one.
 * @param (expected)   [in]  expected column metadata.
 * @param (actual)     [in]  actual column metadata.
 * @param (meta_name)  [in]  name of column metadata table.
 * @return none.
 */
template <typename T>
void TableMetadataHelper::check_expected(const boost::property_tree::ptree& expected,
                                         const boost::property_tree::ptree& actual,
                                         const char* meta_name) {
  auto value_expected = expected.get_optional<T>(meta_name);
  auto value_actual   = actual.get_optional<T>(meta_name);

  if (value_expected && value_actual) {
    EXPECT_EQ(value_expected.value(), value_actual.value());
  } else {
    if (value_expected) {
      const auto& value_expected = expected.get<std::string>(meta_name);
      EXPECT_EQ_T(value_expected.empty(), !value_actual.is_initialized(), meta_name);
    } else if (value_actual) {
      const auto& value_actual = actual.get<std::string>(meta_name);
      EXPECT_EQ_T(!value_expected.is_initialized(), value_actual.empty(), meta_name);
    } else {
      EXPECT_EQ_T(value_expected.is_initialized(), value_actual.is_initialized(), meta_name);
    }
  }
}

}  // namespace manager::metadata::testing
