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
#include "test/helper/table_metadata_helper.h"

#include <gtest/gtest.h>
#include <libpq-fe.h>

#include <boost/foreach.hpp>
#include <boost/format.hpp>
#include <memory>
#include <string>
#include <vector>

#include "manager/metadata/dao/common/config.h"
#include "manager/metadata/dao/postgresql/tables_dao.h"
#include "manager/metadata/datatypes.h"
#include "test/global_test_environment.h"
#include "test/utility/ut_table_metadata.h"
#include "test/utility/ut_utils.h"

namespace manager::metadata::testing {

namespace storage = manager::metadata::db::postgresql;

using boost::property_tree::ptree;
using manager::metadata::db::postgresql::TablesDAO;

/**
 * @brief Get the number of records in the current table metadata.
 * @return Current number of records.
 */
std::int64_t TableMetadataHelper::get_record_count() {
  PGconn* connection = PQconnectdb(db::Config::get_connection_string().c_str());

  boost::format statement = boost::format("SELECT COUNT(*) FROM %s.%s") %
                            storage::SCHEMA_NAME % TablesDAO::kTableName;
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
  int s = time(NULL);
  std::string table_name = "table_name" + std::to_string(s);
  testdata_table_metadata = std::make_unique<UTTableMetadata>(table_name);

  // generate namespace.
  testdata_table_metadata->namespace_name = "namespace";

  // generate three column metadata.
  std::vector<ObjectIdType> ordinal_positions = {1, 2, 3};
  std::vector<std::string> col_names = {"col1", "col2", "col3"};

  // generate primary keys.
  testdata_table_metadata->primary_keys.push_back(ordinal_positions[0]);
  testdata_table_metadata->primary_keys.push_back(ordinal_positions[1]);

  // first column metadata
  bool is_null = true;
  UTColumnMetadata column1{
      col_names[0], ordinal_positions[0],
      static_cast<ObjectIdType>(DataTypes::DataTypesId::FLOAT32), !is_null};
  column1.direction =
      static_cast<ObjectIdType>(Tables::Column::Direction::ASCENDANT);

  // second column metadata
  UTColumnMetadata column2{
      col_names[1], ordinal_positions[1],
      static_cast<ObjectIdType>(DataTypes::DataTypesId::VARCHAR), !is_null};
  column2.direction =
      static_cast<ObjectIdType>(Tables::Column::Direction::DEFAULT);
  ptree data_length;
  data_length.put("", 8);
  column2.p_data_lengths.push_back(std::make_pair("", data_length));
  data_length.put("", 2);
  column2.p_data_lengths.push_back(std::make_pair("", data_length));

  column2.varying = true;

  // third column metadata
  UTColumnMetadata column3{
      col_names[2], ordinal_positions[2],
      static_cast<ObjectIdType>(DataTypes::DataTypesId::CHAR), is_null};
  column3.default_expr = "default";
  column3.data_length = 1;
  column3.varying = false;

  // set table metadata to three column metadata
  testdata_table_metadata->columns.emplace_back(column1);
  testdata_table_metadata->columns.emplace_back(column2);
  testdata_table_metadata->columns.emplace_back(column3);

  // generate ptree from UTTableMetadata fields.
  testdata_table_metadata->generate_ptree();
}

/**
 * @brief Add one new table metadata to table metadata table.
 * @param (table_name)    [in]  table name of new table metadata.
 * @param (ret_table_id)  [out]  (optional) table ID of the new table-metadata.
 * @return none.
 */
void TableMetadataHelper::add_table(std::string_view table_name,
                                    ObjectIdType* ret_table_id) {
  // prepare test data for adding table metadata.
  UTTableMetadata* testdata_table_metadata =
      global->testdata_table_metadata.get();

  ptree new_table = testdata_table_metadata->tables;
  new_table.put(Tables::NAME, table_name);

  // add table metadata.
  add_table(new_table, ret_table_id);
}

/**
 * @brief Add one new table metadata to table metadata table.
 * @param (new_table)     [in]   new table-metadata.
 * @param (ret_table_id)  [out]  (optional) table ID of the new table-metadata.
 * @return none.
 */
void TableMetadataHelper::add_table(
    const boost::property_tree::ptree& new_table, ObjectIdType* ret_table_id) {
  UTUtils::print("-- add table metadata --");

  auto tables = std::make_unique<Tables>(GlobalTestEnvironment::TEST_DB);

  ErrorCode error = tables->init();
  ASSERT_EQ(ErrorCode::OK, error);

  // add table metadata.
  ObjectIdType retval_table_id = 0;
  error = tables->add(new_table, &retval_table_id);
  ASSERT_EQ(ErrorCode::OK, error);
  ASSERT_GT(retval_table_id, 0);

  UTUtils::print(" new table_id: ", ret_table_id);
  UTUtils::print(" " + UTUtils::get_tree_string(new_table));

  if (ret_table_id != nullptr) {
    *ret_table_id = retval_table_id;
  }
}

/**
 * @brief Remove one new table metadata to table metadata table.
 * @param (table_id)  [in]   table id of remove table metadata.
 * @return none.
 */
void TableMetadataHelper::remove_table(const ObjectIdType table_id) {
  UTUtils::print("-- remove table metadata --");

  auto tables = std::make_unique<Tables>(GlobalTestEnvironment::TEST_DB);

  ErrorCode error = tables->init();
  ASSERT_EQ(ErrorCode::OK, error);

  // remove table metadata.
  error = tables->remove(table_id);
  ASSERT_EQ(ErrorCode::OK, error);

  UTUtils::print(" table_id: ", table_id);
}

/**
 * @brief Remove one new table metadata to table metadata table.
 * @param (table_name)  [in]   table name of remove table metadata.
 * @return none.
 */
void TableMetadataHelper::remove_table(std::string_view table_name) {
  UTUtils::print("-- remove table metadata --");

  auto tables = std::make_unique<Tables>(GlobalTestEnvironment::TEST_DB);

  ErrorCode error = tables->init();
  ASSERT_EQ(ErrorCode::OK, error);

  // remove table metadata.
  error = tables->remove(table_name, nullptr);
  ASSERT_EQ(ErrorCode::OK, error);

  UTUtils::print(" table_name: ", table_name);
}

/**
 * @brief Print column metadata fields used as test data.
 * @param (column_metadata)    [in] column metadata used as test data.
 */
void TableMetadataHelper::print_column_metadata(
    const UTColumnMetadata& column_metadata) {
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
  boost::optional<float> metadata_tuples =
      table_statistics.get_optional<float>(Tables::TUPLES);

  UTUtils::print(" id: ", (metadata_id ? metadata_id.get() : 0));
  UTUtils::print(" name: ", (metadata_name ? metadata_name.get() : "NULL"));
  UTUtils::print(" namespace: ",
                 (metadata_namespace ? metadata_namespace.get() : "NULL"));
  UTUtils::print(" reltuples: ", (metadata_tuples ? metadata_tuples.get() : 0));
}

/**
 * @brief Verifies that the actual table metadata equals expected one.
 * @param (expected)   [in]  expected table metadata.
 * @param (actual)     [in]  actual table metadata.
 * @return none.
 */
void TableMetadataHelper::check_table_metadata_expected(
    const boost::property_tree::ptree& expected,
    const boost::property_tree::ptree& actual) {
  // table name
  ASSERT_EQ(expected.get<std::string>(Tables::NAME),
            actual.get<std::string>(Tables::NAME));

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
  bool namespace_empty_actual =
      (o_namespace_actual ? o_namespace_actual.value().empty() : true);

  if (!namespace_empty_expected && !namespace_empty_actual) {
    std::string& s_namespace_expected = o_namespace_expected.value();
    std::string& s_namespace_actual = o_namespace_actual.value();
    EXPECT_EQ(s_namespace_expected, s_namespace_actual);
  } else if (namespace_empty_actual && namespace_empty_actual) {
    ASSERT_TRUE(true);
  } else {
    ASSERT_TRUE(false);
  }

  // primary keys
  check_metadata_expected(expected, actual, Tables::PRIMARY_KEY_NODE);

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

    for (int op = 0; static_cast<size_t>(op) < p_columns_expected.size();
         op++) {
      ptree column_expected = p_columns_expected[op];
      ptree column_actual = p_columns_actual[op];

      // column metadata id
      boost::optional<ObjectIdType> id_actual =
          column_actual.get<ObjectIdType>(Tables::Column::ID);
      EXPECT_GT(id_actual, static_cast<ObjectIdType>(0));

      // column metadata table id
      boost::optional<ObjectIdType> table_id_actual =
          column_actual.get<ObjectIdType>(Tables::Column::TABLE_ID);
      EXPECT_EQ(table_id_expected, table_id_actual);

      // column name
      check_column_metadata_expecetd<std::string>(
          column_expected, column_actual, Tables::Column::NAME);
      // column ordinal position
      check_column_metadata_expecetd<ObjectIdType>(
          column_expected, column_actual, Tables::Column::ORDINAL_POSITION);
      // column data type id
      check_column_metadata_expecetd<ObjectIdType>(
          column_expected, column_actual, Tables::Column::DATA_TYPE_ID);
      // column data length
      check_metadata_expected(column_expected, column_actual,
                              Tables::Column::DATA_LENGTH);
      // column varying
      check_column_metadata_expecetd<bool>(column_expected, column_actual,
                                           Tables::Column::VARYING);
      // nullable
      check_column_metadata_expecetd<bool>(column_expected, column_actual,
                                           Tables::Column::NULLABLE);
      // default
      check_column_metadata_expecetd<std::string>(
          column_expected, column_actual, Tables::Column::DEFAULT);
      // direction
      check_column_metadata_expecetd<ObjectIdType>(
          column_expected, column_actual, Tables::Column::DIRECTION);
    }
  } else if (!o_columns_expected && !o_columns_actual) {
    ASSERT_TRUE(true);
  } else {
    ASSERT_TRUE(false);
  }
}

/**
 * @brief Verifies that the actual metadata equals expected one.
 * @param (expected)   [in]  expected metadata.
 * @param (actual)     [in]  actual metadata.
 * @param (meta_name)  [in]  column name of column metadata table.
 * @return none.
 */
void TableMetadataHelper::check_metadata_expected(
    const boost::property_tree::ptree& expected,
    const boost::property_tree::ptree& actual, const char* meta_name) {
  auto o_expected = expected.get_child_optional(meta_name);
  auto o_actual = actual.get_child_optional(meta_name);

  if (o_expected && o_actual) {
    auto& p_expected = o_expected.value();
    auto& p_actual = o_actual.value();
    EXPECT_EQ(UTUtils::get_tree_string(p_expected),
              UTUtils::get_tree_string(p_actual));
  } else if ((!o_expected && !o_actual) ||
             (o_expected && o_expected.value().empty() && !o_actual) ||
             (o_actual && o_actual.value().empty() && !o_expected)) {
    ASSERT_TRUE(true);
  } else {
    ASSERT_TRUE(false);
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
void TableMetadataHelper::check_column_metadata_expecetd(
    const boost::property_tree::ptree& expected,
    const boost::property_tree::ptree& actual, const char* meta_name) {
  auto value_expected = expected.get_optional<T>(meta_name);
  auto value_actual = actual.get_optional<T>(meta_name);

  if (value_expected && value_actual) {
    EXPECT_EQ(value_expected.value(), value_actual.value());
  } else if (!value_expected && !value_actual) {
    ASSERT_TRUE(true);
  } else {
    ASSERT_TRUE(false);
  }
}

}  // namespace manager::metadata::testing
