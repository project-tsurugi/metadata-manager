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
#include <gtest/gtest.h>

#include <memory>
#include <string>

#include <boost/property_tree/ptree.hpp>

#include "manager/metadata/metadata.h"
#include "manager/metadata/metadata_factory.h"
#include "manager/metadata/tables.h"

#include "test/global_test_environment.h"
#include "test/helper/table_metadata_helper.h"
#include "test/utility/ut_table_metadata.h"
#include "test/utility/ut_utils.h"

namespace manager::metadata::testing {

using boost::property_tree::ptree;

class ApiTestTableMetadata : public ::testing::Test {};

/**
 * @brief happy test for adding one new table metadata
 *  and getting it by table name.
 */
TEST_F(ApiTestTableMetadata, add_get_table_metadata_by_table_name3) {
  // prepare test data for adding table metadata.

  UTTableMetadata testdata_table_metadata =
      *(global->testdata_table_metadata.get());
  manager::metadata::Table& new_table = testdata_table_metadata.table;
  std::string new_table_name =
      new_table.name + "_ApiTestTableMetadata" + std::to_string(__LINE__);
  new_table.name = new_table_name;

  // add table metadata.
  ObjectIdType ret_table_id = INVALID_OBJECT_ID;
  TableMetadataHelper::add_table(new_table, &ret_table_id);
  new_table.id = ret_table_id;

  // get table metadata by table name.
  auto tables     = std::make_unique<Tables>(GlobalTestEnvironment::TEST_DB);
  ErrorCode error = tables->init();
  EXPECT_EQ(ErrorCode::OK, error);

  // check if the specified object exists.
  bool exists = tables->exists(new_table.id);
  EXPECT_EQ(exists, true);

  // check if the specified object exists.
  exists = tables->exists(new_table_name);
  EXPECT_EQ(exists, true);

  // get table metadata by table name.
  ptree table_metadata_inserted;
  error = tables->get(new_table_name, table_metadata_inserted);
  EXPECT_EQ(ErrorCode::OK, error);

  UTUtils::print("-- get table metadata --");
  UTUtils::print(UTUtils::get_tree_string(table_metadata_inserted));

  // verifies that the returned table metadata is expected one.
  TableMetadataHelper::check_table_metadata_expected(new_table,
                                                     table_metadata_inserted);
  // cleanup
  tables->remove(new_table_name.c_str(), nullptr);
}

/**
 * @brief happy test for adding one new table metadata
 *  and getting it by table name.
 */
std::string get_new_table_name(ptree new_table, int64_t line_num) {
  return new_table.get<std::string>(Tables::NAME) + "_ApiTestTableMetadata" +
         std::to_string(line_num);
}

TEST_F(ApiTestTableMetadata, add_get_table_metadata_by_table_name2) {
  // prepare test data for adding table metadata.
  UTTableMetadata testdata_table_metadata =
      *(global->testdata_table_metadata.get());
  ptree new_table            = testdata_table_metadata.tables;
  std::string new_table_name = get_new_table_name(new_table, __LINE__);
  //      new_table.get<std::string>(Tables::NAME) + "_ApiTestTableMetadata" +
  //      std::to_string(__LINE__);
  new_table.put(Tables::NAME, new_table_name);

  // add table metadata.
  ObjectIdType ret_table_id = INVALID_OBJECT_ID;
  TableMetadataHelper::add_table(new_table, &ret_table_id);
  new_table.put(Tables::ID, ret_table_id);

  // get table metadata by table name.
  //  auto tables     =
  //  std::make_unique<Tables>(GlobalTestEnvironment::TEST_DB);
  auto tables     = get_table_metadata(GlobalTestEnvironment::TEST_DB);
  ErrorCode error = tables->init();
  EXPECT_EQ(ErrorCode::OK, error);

  // get table metadata by table name.
  metadata::Table table_metadata_inserted;
  error = tables->get(new_table_name, table_metadata_inserted);
  EXPECT_EQ(ErrorCode::OK, error);

  UTUtils::print("-- get table metadata --");
  ptree pt_table = table_metadata_inserted.convert_to_ptree();
  UTUtils::print(UTUtils::get_tree_string(pt_table));

  // verifies that the returned table metadata is expected one.
  TableMetadataHelper::check_table_metadata_expected(new_table,
                                                     table_metadata_inserted);
  // cleanup
  tables->remove(new_table_name.c_str(), nullptr);
}

/**
 * @brief happy test for adding one new table metadata
 *  and getting it by table name.
 */
TEST_F(ApiTestTableMetadata, add_get_table_metadata_by_table_name) {
  // prepare test data for adding table metadata.
  UTTableMetadata testdata_table_metadata =
      *(global->testdata_table_metadata.get());
  ptree new_table            = testdata_table_metadata.tables;
  std::string new_table_name = new_table.get<std::string>(Tables::NAME) +
                               "_ApiTestTableMetadata" +
                               std::to_string(__LINE__);
  new_table.put(Tables::NAME, new_table_name);

  // add table metadata.
  ObjectIdType ret_table_id = -1;
  TableMetadataHelper::add_table(new_table, &ret_table_id);
  new_table.put(Tables::ID, ret_table_id);

  // get table metadata by table name.
  auto tables     = std::make_unique<Tables>(GlobalTestEnvironment::TEST_DB);
  ErrorCode error = tables->init();
  EXPECT_EQ(ErrorCode::OK, error);

  // check if the specified object exists.
  bool exists = tables->exists(ret_table_id);
  EXPECT_EQ(exists, true);

  // check if the specified object exists.
  exists = tables->exists(new_table_name);
  EXPECT_EQ(exists, true);

  // get table metadata by table name.
  ptree table_metadata_inserted;
  error = tables->get(new_table_name, table_metadata_inserted);
  EXPECT_EQ(ErrorCode::OK, error);

  UTUtils::print("-- get table metadata --");
  UTUtils::print(UTUtils::get_tree_string(table_metadata_inserted));

  // verifies that the returned table metadata is expected one.
  TableMetadataHelper::check_table_metadata_expected(new_table,
                                                     table_metadata_inserted);
  // cleanup
  tables->remove(new_table_name.c_str(), nullptr);
}

/**
 * @brief happy test for adding one new table metadata without returned table
 * id and getting it by table name.
 */
TEST_F(ApiTestTableMetadata,
       add_without_returned_table_id_get_table_metadata_by_table_name) {
  // prepare test data for adding table metadata.
  UTTableMetadata testdata_table_metadata =
      *(global->testdata_table_metadata.get());
  ptree new_table            = testdata_table_metadata.tables;
  std::string new_table_name = new_table.get<std::string>(Tables::NAME) +
                               "_ApiTestTableMetadata" +
                               std::to_string(__LINE__);
  new_table.put(Tables::NAME, new_table_name);

  // add table metadata.
  auto tables = std::make_unique<Tables>(GlobalTestEnvironment::TEST_DB);

  ErrorCode error = tables->init();
  EXPECT_EQ(ErrorCode::OK, error);

  error = tables->add(new_table);
  EXPECT_EQ(ErrorCode::OK, error);

  UTUtils::print("-- add table metadata --");
  UTUtils::print(UTUtils::get_tree_string(new_table));

  // get table metadata by table name.
  ptree table_metadata_inserted;
  error = tables->get(new_table_name, table_metadata_inserted);
  EXPECT_EQ(ErrorCode::OK, error);

  UTUtils::print("-- get table metadata --");
  UTUtils::print(UTUtils::get_tree_string(table_metadata_inserted));

  // verifies that the returned table metadata is expected one.
  new_table.put(Tables::ID,
                table_metadata_inserted.get<ObjectIdType>(Tables::ID));
  TableMetadataHelper::check_table_metadata_expected(new_table,
                                                     table_metadata_inserted);

  // cleanup
  tables->remove(new_table_name.c_str(), nullptr);
}

/**
 * @brief happy test for adding two same table metadata
 *  and getting them by table name.
 */
TEST_F(ApiTestTableMetadata, get_two_table_metadata_by_table_name) {
  // prepare test data for adding table metadata.
  UTTableMetadata testdata_table_metadata =
      *(global->testdata_table_metadata.get());
  ptree new_table            = testdata_table_metadata.tables;
  std::string new_table_name = new_table.get<std::string>(Tables::NAME) +
                               "_ApiTestTableMetadata" +
                               std::to_string(__LINE__);
  new_table.put(Tables::NAME, new_table_name);

  auto tables = std::make_unique<Tables>(GlobalTestEnvironment::TEST_DB);

  ErrorCode error = tables->init();
  EXPECT_EQ(ErrorCode::OK, error);

  // prepare variables for returned value from add api.
  std::vector<ObjectIdType> ret_table_id;
  ret_table_id.emplace_back(INVALID_VALUE);
  ret_table_id.emplace_back(INVALID_VALUE);

  // add first table metadata.
  error = tables->add(new_table, &ret_table_id[0]);
  EXPECT_EQ(ErrorCode::OK, error);
  EXPECT_GT(ret_table_id[0], 0);

  // add second table metadata.
  error = tables->add(new_table, &ret_table_id[1]);

  EXPECT_EQ(ErrorCode::ALREADY_EXISTS, error);
  EXPECT_EQ(ret_table_id[1], INVALID_VALUE);

  UTUtils::print("-- add table metadata --");
  UTUtils::print(UTUtils::get_tree_string(new_table));

  // get table metadata by table name.
  ptree table_metadata_inserted;
  error = tables->get(new_table_name, table_metadata_inserted);
  EXPECT_EQ(ErrorCode::OK, error);

  UTUtils::print("-- get table metadata --");
  UTUtils::print(UTUtils::get_tree_string(table_metadata_inserted));

  new_table.put(Tables::ID, ret_table_id[0]);
  TableMetadataHelper::check_table_metadata_expected(new_table,
                                                     table_metadata_inserted);

  // cleanup
  tables->remove(new_table_name.c_str(), nullptr);
}

/**
 * @brief happy test for adding one new table metadata
 *  and getting it by table id.
 */
TEST_F(ApiTestTableMetadata, add_get_table_metadata_by_table_id) {
  // prepare test data for adding table metadata.
  UTTableMetadata testdata_table_metadata =
      *(global->testdata_table_metadata.get());
  ptree new_table            = testdata_table_metadata.tables;
  std::string new_table_name = new_table.get<std::string>(Tables::NAME) +
                               "_ApiTestTableMetadata" +
                               std::to_string(__LINE__);
  new_table.put(Tables::NAME, new_table_name);

  // add table metadata.
  ObjectIdType ret_table_id = -1;
  TableMetadataHelper::add_table(new_table, &ret_table_id);
  new_table.put(Tables::ID, ret_table_id);

  // get table metadata by table id.
  auto tables     = std::make_unique<Tables>(GlobalTestEnvironment::TEST_DB);
  ErrorCode error = tables->init();
  EXPECT_EQ(ErrorCode::OK, error);

  ptree table_metadata_inserted;
  error = tables->get(ret_table_id, table_metadata_inserted);
  EXPECT_EQ(ErrorCode::OK, error);

  UTUtils::print("-- get table metadata --");
  UTUtils::print(UTUtils::get_tree_string(table_metadata_inserted));

  // verifies that the returned table metadata is expected one.
  TableMetadataHelper::check_table_metadata_expected(new_table,
                                                     table_metadata_inserted);

  // cleanup
  tables->remove(new_table_name.c_str(), nullptr);
}

/**
 * @brief happy test for all table metadata getting.
 */
TEST_F(ApiTestTableMetadata, get_all_table_struct) {
  constexpr int test_table_count      = 5;
  std::string table_name_prefix       = "Table-ApiTestTableMetadata-GetAll-";
  std::vector<ObjectIdType> table_ids = {};

  // prepare test data for adding table metadata.
  UTTableMetadata testdata_table_metadata =
      *(global->testdata_table_metadata.get());
  ptree expected_table = testdata_table_metadata.tables;

  // add table metadata.
  for (int count = 1; count <= test_table_count; count++) {
    std::string table_name = table_name_prefix + std::to_string(count);
    ObjectIdType table_id;
    TableMetadataHelper::add_table(table_name, &table_id);
    table_ids.emplace_back(table_id);
  }

  // gets all table metadata.
  auto tables = manager::metadata::get_tables_ptr(GlobalTestEnvironment::TEST_DB);
//  auto tables = std::make_unique<manager::metadata::Tables>(GlobalTestEnvironment::TEST_DB);
  ErrorCode error = tables->init();
  EXPECT_EQ(ErrorCode::OK, error);

  int64_t actual_count = 0;
#if 0  
  ptree pt;
  while ((error = tables->next(pt)) == ErrorCode::OK) {
    UTUtils::print("-- get all table metadata --");
    UTUtils::print(UTUtils::get_tree_string(pt));

    std::string table_name = table_name_prefix + std::to_string(actual_count + 1);
    expected_table.put(Tables::ID, table_ids[actual_count]);
    expected_table.put(Tables::NAME, table_name);

    // verifies that the returned table metadata is expected one.
    TableMetadataHelper::check_table_metadata_expected(expected_table,
                                                       pt);
    ++actual_count;
  }
  ASSERT_EQ(test_table_count, actual_count);

  actual_count = 0;
#endif
  Table table;
  while ((error = tables->next(table)) == ErrorCode::OK) {
    UTUtils::print("-- get all table metadata --");
    UTUtils::print(UTUtils::get_tree_string(table.convert_to_ptree()));

    std::string table_name = table_name_prefix + std::to_string(actual_count + 1);
    expected_table.put(Tables::ID, table_ids[actual_count]);
    expected_table.put(Tables::NAME, table_name);

    // verifies that the returned table metadata is expected one.
    TableMetadataHelper::check_table_metadata_expected(expected_table,
                                                       table.convert_to_ptree());
    ++actual_count;
  }
  ASSERT_EQ(test_table_count, actual_count);

  // cleanup
  for (ObjectIdType table_id : table_ids) {
    error = tables->remove(table_id);
    EXPECT_EQ(ErrorCode::OK, error);
  }
}

/**
 * @brief happy test for all table metadata getting.
 */
TEST_F(ApiTestTableMetadata, get_all_table_metadata) {
  constexpr int test_table_count      = 5;
  std::string table_name_prefix       = "Table-ApiTestTableMetadata-GetAll-";
  std::vector<ObjectIdType> table_ids = {};

  // prepare test data for adding table metadata.
  UTTableMetadata testdata_table_metadata =
      *(global->testdata_table_metadata.get());
  ptree expected_table = testdata_table_metadata.tables;

  // add table metadata.
  for (int count = 1; count <= test_table_count; count++) {
    std::string table_name = table_name_prefix + std::to_string(count);
    ObjectIdType table_id;
    TableMetadataHelper::add_table(table_name, &table_id);
    table_ids.emplace_back(table_id);
  }

  // gets all table metadata.
  auto tables     = std::make_unique<Tables>(GlobalTestEnvironment::TEST_DB);
  ErrorCode error = tables->init();
  EXPECT_EQ(ErrorCode::OK, error);

  std::vector<boost::property_tree::ptree> container = {};
  error = tables->get_all(container);
  EXPECT_EQ(ErrorCode::OK, error);
  ASSERT_EQ(test_table_count, container.size());

  UTUtils::print("-- get all table metadata --");
  for (int count = 1; count <= test_table_count; count++) {
    ptree table_metadata = container[count - 1];
    UTUtils::print(UTUtils::get_tree_string(table_metadata));

    std::string table_name = table_name_prefix + std::to_string(count);
    expected_table.put(Tables::ID, table_ids[count - 1]);
    expected_table.put(Tables::NAME, table_name);

    // verifies that the returned table metadata is expected one.
    TableMetadataHelper::check_table_metadata_expected(expected_table,
                                                       table_metadata);
  }

  // cleanup
  for (ObjectIdType table_id : table_ids) {
    error = tables->remove(table_id);
    EXPECT_EQ(ErrorCode::OK, error);
  }
}

/**
 * @brief happy test for all table metadata getting.
 */
TEST_F(ApiTestTableMetadata, get_all_table_metadata_empty) {
  // gets all table metadata.
  auto tables     = std::make_unique<Tables>(GlobalTestEnvironment::TEST_DB);
  ErrorCode error = tables->init();
  EXPECT_EQ(ErrorCode::OK, error);

  std::vector<boost::property_tree::ptree> container = {};
  error = tables->get_all(container);
  EXPECT_EQ(ErrorCode::OK, error);
  EXPECT_EQ(0, container.size());
}

/**
 * @brief happy test for all table metadata update.
 */
TEST_F(ApiTestTableMetadata, update_table_metadata) {
  ErrorCode error = ErrorCode::UNKNOWN;

  // prepare test data for adding table metadata.
  std::string new_table_name =
      "ApiTestTableMetadata_" + std::to_string(__LINE__);

  // new table
  ptree new_table;
  new_table.put(Table::NAME, new_table_name);
  new_table.put(Table::NAMESPACE, "default");
  // new table columns.
  ptree columns_metadata;
  {
    ptree column;
    // column #1
    column.clear();
    column.put(Column::NAME, "col-1");
    column.put(Column::COLUMN_NUMBER, 1);
    column.put<ObjectIdType>(Column::DATA_TYPE_ID, 4);
    column.put<bool>(Column::IS_NOT_NULL, false);
    columns_metadata.push_back(std::make_pair("", column));

    // column #2
    column.clear();
    column.put(Column::NAME, "col-2");
    column.put(Column::COLUMN_NUMBER, 2);
    column.put<ObjectIdType>(Column::DATA_TYPE_ID, 6);
    column.put<bool>(Column::IS_NOT_NULL, true);
    columns_metadata.push_back(std::make_pair("", column));
  }
  new_table.add_child(Table::COLUMNS_NODE, columns_metadata);

  // new table constraint.
  ptree constraint_metadata;
  {
    ptree constraint;
    ptree columns_num;
    ptree columns_num_value;
    ptree columns_id;
    ptree columns_id_value;

    // constraint #1
    constraint.clear();
    columns_num.clear();
    columns_num_value.clear();
    columns_id.clear();
    columns_id_value.clear();
    // name
    constraint.put(Constraint::NAME, "unique constraint");
    // type
    constraint.put(Constraint::TYPE,
                   static_cast<int32_t>(Constraint::ConstraintType::UNIQUE));
    // columns
    constraint.add_child(Constraint::COLUMNS, columns_num);
    // columns id
    constraint.add_child(Constraint::COLUMNS_ID, columns_id);
    // index id
    constraint.put(Constraint::INDEX_ID, 1);
    // constraints
    constraint_metadata.push_back(std::make_pair("", constraint));

    // constraint #2
    constraint.clear();
    columns_num.clear();
    columns_num_value.clear();
    columns_id.clear();
    columns_id_value.clear();
    // name
    constraint.put(Constraint::NAME, "check constraint");
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
    constraint_metadata.push_back(std::make_pair("", constraint));
  }
  new_table.add_child(Table::CONSTRAINTS_NODE, constraint_metadata);

  // Generation of Tables object.
  auto tables = get_table_metadata(GlobalTestEnvironment::TEST_DB);
  error       = tables->init();
  if (error != ErrorCode::OK) {
    ASSERT_EQ(ErrorCode::OK, error);
  }

  // add table metadata.
  ObjectIdType ret_table_id = -1;
  error                     = tables->add(new_table, &ret_table_id);
  if (error != ErrorCode::OK) {
    ASSERT_EQ(ErrorCode::OK, error);
  }

  ptree table_metadata_inserted;
  error = tables->get(ret_table_id, table_metadata_inserted);
  ASSERT_EQ(ErrorCode::OK, error);

  UTUtils::print("-- get table metadata of the before updating --");
  UTUtils::print(UTUtils::get_tree_string(table_metadata_inserted));

  // update table metadata.
  ptree update_table = table_metadata_inserted;
  update_table.put(Table::NAME, "table_name-update");
  update_table.put(Table::NAMESPACE, "namespace-update");
  update_table.put(Table::NUMBER_OF_TUPLES, 567);

  // columns
  update_table.erase(Table::COLUMNS_NODE);
  ptree columns;
  {
    auto columns_node = table_metadata_inserted.get_child(Table::COLUMNS_NODE);
    auto it           = columns_node.begin();

    ptree column;
    // 1 item skip.
    // 2 item update.
    column = (++it)->second;
    column.put(Column::NAME, it->second.get_optional<std::string>(Column::NAME)
                                     .value_or("unknown-1") +
                                 "-update");
    column.put(Column::COLUMN_NUMBER, 1);
    columns.push_back(std::make_pair("", column));

    // new column
    column.clear();
    column.put(Column::NAME, "new-col");
    column.put(Column::COLUMN_NUMBER, 2);
    column.put<ObjectIdType>(Column::DATA_TYPE_ID, 13);
    column.put<bool>(Column::VARYING, false);
    column.put(Column::DATA_LENGTH, 32);
    column.put<bool>(Column::IS_NOT_NULL, false);
    column.put(Column::DEFAULT_EXPR, "default-value");
    columns.push_back(std::make_pair("", column));
  }
  update_table.add_child(Table::COLUMNS_NODE, columns);

  // constraint
  update_table.erase(Table::CONSTRAINTS_NODE);
  ptree constraints;
  {
    ptree constraint;
    ptree columns_num;
    ptree columns_num_value;
    ptree columns_id;
    ptree columns_id_value;

    auto constraints_node =
        table_metadata_inserted.get_child(Table::CONSTRAINTS_NODE);
    auto it = constraints_node.begin();

    // 1 item skip.
    // 2 item update.
    constraint = (++it)->second;
    constraint.put(Constraint::NAME,
                   it->second.get_optional<std::string>(Constraint::NAME)
                           .value_or("unknown-1") +
                       "-update");
    // columns
    columns_num_value.put("", 3);
    columns_num.push_back(std::make_pair("", columns_num_value));
    constraint.add_child(Constraint::COLUMNS, columns_num);
    // columns id
    columns_id_value.put("", 9876);
    columns_id.push_back(std::make_pair("", columns_id_value));
    constraint.add_child(Constraint::COLUMNS_ID, columns_id);

    // constraints
    constraints.push_back(std::make_pair("", constraint));

    // new constraint
    // name
    constraint.put(Constraint::NAME, "new unique constraint");
    // type
    constraint.put(Constraint::TYPE,
                   static_cast<int32_t>(Constraint::ConstraintType::UNIQUE));
    // columns
    constraint.add_child(Constraint::COLUMNS, columns_num);
    // columns id
    constraint.add_child(Constraint::COLUMNS_ID, columns_id);
    // index id
    constraint.put(Constraint::INDEX_ID, 9);

    // constraints
    constraints.push_back(std::make_pair("", constraint));
  }
  update_table.add_child(Table::CONSTRAINTS_NODE, constraint_metadata);

  // update table metadata.
  error = tables->update(ret_table_id, update_table);
  ASSERT_EQ(ErrorCode::OK, error);

  ptree table_metadata_updated;
  error = tables->get(ret_table_id, table_metadata_updated);
  ASSERT_EQ(ErrorCode::OK, error);

  UTUtils::print("-- get table metadata of the after updating --");
  UTUtils::print(UTUtils::get_tree_string(table_metadata_updated));

  // verifies that the returned table metadata is expected one.
  TableMetadataHelper::check_table_metadata_expected(update_table,
                                                     table_metadata_updated);

  // cleanup
  tables->remove(ret_table_id);
}

/**
 * @brief happy test for removing one new table metadata by table name.
 */
TEST_F(ApiTestTableMetadata, remove_table_metadata_by_table_name) {
  // prepare test data for adding table metadata.
  UTTableMetadata testdata_table_metadata =
      *(global->testdata_table_metadata.get());
  ptree new_table            = testdata_table_metadata.tables;
  std::string new_table_name = new_table.get<std::string>(Table::NAME) +
                               "_ApiTestTableMetadata" +
                               std::to_string(__LINE__);
  new_table.put(Table::NAME, new_table_name);

  // add table metadata.
  ObjectIdType ret_table_id = -1;
  TableMetadataHelper::add_table(new_table, &ret_table_id);

  // remove table metadata by table name.
  auto tables     = get_table_metadata(GlobalTestEnvironment::TEST_DB);
  ErrorCode error = tables->init();
  EXPECT_EQ(ErrorCode::OK, error);

  ObjectIdType table_id_to_remove = -1;
  error = tables->remove(new_table_name.c_str(), &table_id_to_remove);
  EXPECT_EQ(ErrorCode::OK, error);
  EXPECT_EQ(ret_table_id, table_id_to_remove);

  // verifies that table metadata does not exist.
  ptree table_metadata_got;
  error = tables->get(table_id_to_remove, table_metadata_got);
  EXPECT_EQ(ErrorCode::ID_NOT_FOUND, error);

  UTUtils::print("-- get table metadata --");
  UTUtils::print(UTUtils::get_tree_string(table_metadata_got));
}

/**
 * @brief happy test for removing one new table metadata by table id.
 */
TEST_F(ApiTestTableMetadata, remove_table_metadata_by_table_id) {
  // prepare test data for adding table metadata.
  UTTableMetadata testdata_table_metadata =
      *(global->testdata_table_metadata.get());
  ptree new_table            = testdata_table_metadata.tables;
  std::string new_table_name = new_table.get<std::string>(Table::NAME) +
                               "_ApiTestTableMetadata" +
                               std::to_string(__LINE__);
  new_table.put(Table::NAME, new_table_name);

  // add table metadata.
  ObjectIdType ret_table_id = -1;
  TableMetadataHelper::add_table(new_table, &ret_table_id);

  // remove table metadata by table id.
  auto tables     = get_table_metadata(GlobalTestEnvironment::TEST_DB);
  ErrorCode error = tables->init();
  EXPECT_EQ(ErrorCode::OK, error);

  error = tables->remove(ret_table_id);
  EXPECT_EQ(ErrorCode::OK, error);

  // verifies that table metadata does not exist.
  ptree table_metadata_got;
  error = tables->get(ret_table_id, table_metadata_got);
  EXPECT_EQ(ErrorCode::ID_NOT_FOUND, error);

  UTUtils::print("-- get table metadata --");
  UTUtils::print(UTUtils::get_tree_string(table_metadata_got));
}

/**
 * @brief happy test for adding, getting and removing
 *  one new table metadata without initialization of all api.
 */
TEST_F(ApiTestTableMetadata,
       add_get_remove_table_metadata_without_initialized) {
  // prepare test data for adding table metadata.
  UTTableMetadata testdata_table_metadata =
      *(global->testdata_table_metadata.get());
  ptree new_table            = testdata_table_metadata.tables;
  std::string new_table_name = new_table.get<std::string>(Table::NAME) +
                               "_ApiTestTableMetadata" +
                               std::to_string(__LINE__);
  new_table.put(Table::NAME, new_table_name);

  // add table metadata without initialized.
  auto tables_add = get_table_metadata(GlobalTestEnvironment::TEST_DB);

  ObjectIdType ret_table_id = -1;
  ErrorCode error           = tables_add->add(new_table, &ret_table_id);
  new_table.put(Table::ID, ret_table_id);

  EXPECT_EQ(ErrorCode::OK, error);
  EXPECT_GT(ret_table_id, 0);

  // get table metadata by table id without initialized.
  auto tables_get_by_id = get_table_metadata(GlobalTestEnvironment::TEST_DB);

  ptree table_metadata_inserted_by_id;
  error = tables_get_by_id->get(ret_table_id, table_metadata_inserted_by_id);
  EXPECT_EQ(ErrorCode::OK, error);

  UTUtils::print("-- get table metadata --");
  UTUtils::print(UTUtils::get_tree_string(table_metadata_inserted_by_id));

  // verifies that the returned table metadata is expected one.
  TableMetadataHelper::check_table_metadata_expected(
      new_table, table_metadata_inserted_by_id);

  // get table metadata by table name without initialized.
  auto tables_get_by_name = get_table_metadata(GlobalTestEnvironment::TEST_DB);

  ptree table_metadata_inserted_by_name;
  error =
      tables_get_by_name->get(new_table_name, table_metadata_inserted_by_name);
  EXPECT_EQ(ErrorCode::OK, error);

  UTUtils::print("-- get table metadata --");
  UTUtils::print(UTUtils::get_tree_string(table_metadata_inserted_by_name));

  // verifies that the returned table metadata is expected one.
  TableMetadataHelper::check_table_metadata_expected(
      new_table, table_metadata_inserted_by_name);

  // remove table metadata by table name without initialized.
  ObjectIdType table_id_to_remove = -1;
  auto tables_remove_by_name =
      get_table_metadata(GlobalTestEnvironment::TEST_DB);
  error = tables_remove_by_name->remove(new_table_name.c_str(),
                                        &table_id_to_remove);
  EXPECT_EQ(ErrorCode::OK, error);
  EXPECT_EQ(ret_table_id, table_id_to_remove);

  // add table metadata again.
  error = tables_add->add(new_table, &ret_table_id);
  new_table.put(Table::ID, ret_table_id);

  // remove table metadata by table id without initialized.
  auto tables_remove_by_id = get_table_metadata(GlobalTestEnvironment::TEST_DB);
  error                    = tables_remove_by_id->remove(ret_table_id);
  EXPECT_EQ(ErrorCode::OK, error);
}

}  // namespace manager::metadata::testing
