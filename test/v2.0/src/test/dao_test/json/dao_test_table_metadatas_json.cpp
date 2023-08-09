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
#include <gtest/gtest.h>

#include <memory>
#include <string>

#include <boost/foreach.hpp>
#include <boost/property_tree/ptree.hpp>

#include "manager/metadata/dao/json/columns_dao_json.h"
#include "manager/metadata/dao/json/constraints_dao_json.h"
#include "manager/metadata/dao/json/db_session_manager_json.h"
#include "manager/metadata/dao/json/tables_dao_json.h"
#include "test/common/global_test_environment.h"
#include "test/common/ut_utils.h"
#include "test/helper/table_metadata_helper.h"
#include "test/metadata/ut_table_metadata.h"

namespace manager::metadata::testing {

using boost::property_tree::ptree;
using manager::metadata::db::Dao;

class DaoTestTableMetadata : public ::testing::Test {
 public:
  void SetUp() override {}

  /**
   * @brief Add table metadata to table metadata table.
   * @param (new_table)  [in]  table metadata to add.
   * @param (object_id)  [out] ID of the added table metadata.
   * @return ErrorCode::OK if success, otherwise an error code.
   */
  static void add_table(boost::property_tree::ptree new_table,
                        ObjectIdType* object_id) {
    assert(object_id != nullptr);

    ErrorCode error = ErrorCode::UNKNOWN;
    db::DbSessionManagerJson db_session_manager;

    // TablesDAO
    std::shared_ptr<Dao> tables_dao;
    {
      error = db_session_manager.get_tables_dao(tables_dao);
      ASSERT_NE(nullptr, tables_dao);
      ASSERT_EQ(ErrorCode::OK, error);
    }

    // ColumnsDAO
    std::shared_ptr<Dao> columns_dao;
    {
      error = db_session_manager.get_columns_dao(columns_dao);
      ASSERT_NE(nullptr, columns_dao);
      ASSERT_EQ(ErrorCode::OK, error);
    }

    // ConstraintsDAO
    std::shared_ptr<Dao> constraints_dao;
    {
      constraints_dao = db_session_manager.get_constraints_dao();
      ASSERT_NE(nullptr, constraints_dao);

      error = constraints_dao->prepare();
      EXPECT_EQ(ErrorCode::OK, error);
    }

    error = db_session_manager.start_transaction();
    ASSERT_EQ(ErrorCode::OK, error);

    // Add table metadata object to table metadata table.
    ObjectIdType table_id_returned;
    error = tables_dao->insert(new_table, table_id_returned);
    ASSERT_EQ(ErrorCode::OK, error);
    EXPECT_GT(table_id_returned, 0);

    // Add column metadata object to column metadata table.
    BOOST_FOREACH (const ptree::value_type& node,
                   new_table.get_child(Table::COLUMNS_NODE)) {
      ptree column = node.second;

      // Set table-id.
      column.put(Column::TABLE_ID, table_id_returned);

      ObjectIdType added_id = 0;
      // Insert the column metadata.
      error = columns_dao->insert(column, added_id);
      EXPECT_EQ(ErrorCode::OK, error);
    }

    // Add constraint metadata object to constraint metadata table.
    BOOST_FOREACH (const ptree::value_type& node,
                   new_table.get_child(Table::CONSTRAINTS_NODE)) {
      ptree constraint = node.second;

      // Set table-id.
      constraint.put(Constraint::TABLE_ID, table_id_returned);

      ObjectId added_id = 0;
      // Insert the constraint metadata.
      error = constraints_dao->insert(constraint, added_id);
      EXPECT_EQ(ErrorCode::OK, error);
    }

    if (error == ErrorCode::OK) {
      error = db_session_manager.commit();
      EXPECT_EQ(ErrorCode::OK, error);
    } else {
      ErrorCode rollback_error = db_session_manager.rollback();
      EXPECT_EQ(ErrorCode::OK, rollback_error);
    }

    *object_id = table_id_returned;

    UTUtils::print(std::string(30, '-'));
    UTUtils::print("New table id: ", *object_id);
    UTUtils::print(UTUtils::get_tree_string(new_table));
  }

  /**
   * @brief Get table metadata object based on table name.
   * @param (object_name)  [in]  table name. (Value of "name" key.)
   * @param (object)       [out] table metadata object with the specified name.
   * @return ErrorCode::OK if success, otherwise an error code.
   */
  static void get_table_metadata(std::string_view object_name,
                                 boost::property_tree::ptree& object) {
    return get_table_metadata(Table::NAME, object_name, object);
  }

  /**
   * @brief Get table metadata object based on table id.
   * @param (object_id)  [in]  table id. (Value of "id" key.)
   * @param (object)     [out] table metadata object with the specified id.
   * @return ErrorCode::OK if success, otherwise an error code.
   */
  static void get_table_metadata(ObjectIdType object_id,
                                 boost::property_tree::ptree& object) {
    return get_table_metadata(Table::ID, std::to_string(object_id), object);
  }

  /**
   * @brief Get table metadata object based on key name.
   * @param key     [in]  key name.
   * @param value   [in]  key value. (Value of key)
   * @param object  [out] table metadata object with the specified name.
   * @return ErrorCode::OK if success, otherwise an error code.
   */
  static void get_table_metadata(std::string_view key, std::string_view value,
                                 boost::property_tree::ptree& object) {
    ErrorCode error = ErrorCode::UNKNOWN;
    db::DbSessionManagerJson db_session_manager;

    // TablesDAO
    std::shared_ptr<Dao> tables_dao;
    {
      error = db_session_manager.get_tables_dao(tables_dao);
      ASSERT_NE(nullptr, tables_dao);
      ASSERT_EQ(ErrorCode::OK, error);
    }

    // ColumnsDAO
    std::shared_ptr<Dao> columns_dao;
    {
      error = db_session_manager.get_columns_dao(columns_dao);
      ASSERT_NE(nullptr, columns_dao);
      ASSERT_EQ(ErrorCode::OK, error);
    }

    // ConstraintsDAO
    std::shared_ptr<Dao> constraints_dao;
    {
      constraints_dao = db_session_manager.get_constraints_dao();
      ASSERT_NE(nullptr, constraints_dao);

      error = constraints_dao->prepare();
      EXPECT_EQ(ErrorCode::OK, error);
    }

    ptree temp_object;
    error = tables_dao->select(key, {value}, temp_object);
    if (error != ErrorCode::OK) {
      if (key == Tables::ID) {
        EXPECT_EQ(ErrorCode::ID_NOT_FOUND, error);
      } else if (key == Tables::NAME) {
        EXPECT_EQ(ErrorCode::NAME_NOT_FOUND, error);
      } else {
        EXPECT_EQ(ErrorCode::NOT_FOUND, error);
      }
      return;
    }

    object = temp_object.front().second;

    auto o_table_id = object.get_optional<std::string>(Table::ID);
    if (o_table_id) {
      // column metadata
      if (object.find(Table::COLUMNS_NODE) == object.not_found()) {
        ptree columns;
        error =
            columns_dao->select(Column::TABLE_ID, {o_table_id.get()}, columns);
        EXPECT_EQ(ErrorCode::OK, error);
        object.add_child(Table::COLUMNS_NODE, columns);
      }

      // constraint metadata
      if (object.find(Table::CONSTRAINTS_NODE) == object.not_found()) {
        ptree constraints;
        error = constraints_dao->select(Constraint::TABLE_ID,
                                        {o_table_id.get()}, constraints);
        error = (error == ErrorCode::NOT_FOUND ? ErrorCode::OK : error);
        EXPECT_EQ(ErrorCode::OK, error);
        object.add_child(Table::CONSTRAINTS_NODE, constraints);
      }
    }
  }

  /**
   * @brief Update table metadata.
   * @param (object_id) [in]  table id.
   * @param (object)    [in]  table metadata.
   * @return ErrorCode::OK if success, otherwise an error code.
   */
  static void update_table_metadata(ObjectIdType object_id,
                                    boost::property_tree::ptree& object) {
    ErrorCode error = ErrorCode::UNKNOWN;
    db::DbSessionManagerJson db_session_manager;

    // TablesDAO
    std::shared_ptr<Dao> tables_dao;
    {
      error = db_session_manager.get_tables_dao(tables_dao);
      ASSERT_NE(nullptr, tables_dao);
      ASSERT_EQ(ErrorCode::OK, error);
    }

    error = db_session_manager.start_transaction();
    ASSERT_EQ(ErrorCode::OK, error);

    error = tables_dao->update(Tables::ID, {std::to_string(object_id)}, object);
    if (error == ErrorCode::OK) {
      EXPECT_EQ(ErrorCode::OK, error);
    } else {
      EXPECT_EQ(ErrorCode::ID_NOT_FOUND, error);
      return;
    }

    if (error == ErrorCode::OK) {
      error = db_session_manager.commit();
      EXPECT_EQ(ErrorCode::OK, error);
    } else {
      ErrorCode rollback_error = db_session_manager.rollback();
      EXPECT_EQ(ErrorCode::OK, rollback_error);
    }

    UTUtils::print(std::string(30, '-'));
    UTUtils::print("Update table id: ", object_id);
    UTUtils::print(UTUtils::get_tree_string(object));
  }

  /**
   * @brief Remove all metadata-object based on the given table id
   *  (table metadata, column metadata and column statistics)
   *  from metadata-table (the table metadata table,
   *  the column metadata table and the column statistics table).
   * @param (object_id) [in] table id.
   */
  static void remove_table_metadata(const ObjectIdType object_id) {
    ObjectIdType retval_object_id;
    remove_table_metadata(Table::ID, std::to_string(object_id),
                          &retval_object_id);

    EXPECT_EQ(object_id, retval_object_id);
  }

  /**
   * @brief Remove all metadata-object based on the given table name
   *  (table metadata, column metadata and column statistics)
   *  from metadata-table (the table metadata table,
   *  the column metadata table and the column statistics table).
   * @param (object_name) [in]  table name.
   * @param (object_id)   [out] object id of table removed.
   */
  static void remove_table_metadata(const char* object_name,
                                    ObjectIdType* object_id) {
    remove_table_metadata(Table::NAME, object_name, object_id);
  }

  /**
   * @brief Remove all metadata-object based on the given key name.
   * @param key        [in]  key name.
   * @param value      [in]  key value. (Value of key)
   * @param object_id  [out] table id.
   */
  static void remove_table_metadata(std::string_view key,
                                    std::string_view value,
                                    ObjectIdType* object_id) {
    ErrorCode error = ErrorCode::UNKNOWN;
    db::DbSessionManagerJson db_session_manager;

    // TablesDAO
    std::shared_ptr<Dao> tables_dao;
    {
      error = db_session_manager.get_tables_dao(tables_dao);
      ASSERT_NE(nullptr, tables_dao);
      ASSERT_EQ(ErrorCode::OK, error);
    }

    error = db_session_manager.start_transaction();
    EXPECT_EQ(ErrorCode::OK, error);

    ObjectIdType retval_object_id = -1;
    error = tables_dao->remove(key, {value}, retval_object_id);
    EXPECT_EQ(ErrorCode::OK, error);
    EXPECT_NE(-1, retval_object_id);

    if (error == ErrorCode::OK) {
      error = db_session_manager.commit();
      EXPECT_EQ(ErrorCode::OK, error);

      if (object_id != nullptr) {
        *object_id = retval_object_id;
      }
    } else {
      ErrorCode rollback_error = db_session_manager.rollback();
      EXPECT_EQ(ErrorCode::OK, rollback_error);
    }
  }
};

/**
 * @brief happy test for adding one new table metadata
 *  and getting it by table name.
 */
TEST_F(DaoTestTableMetadata, add_get_table_metadata_by_table_name) {
  std::string new_table_name = "DaoTestTableMetadata_" +
                               UTUtils::generate_narrow_uid() + "_" +
                               std::to_string(__LINE__);

  // Generate test metadata.
  UtTableMetadata testdata_table_metadata(new_table_name);
  auto new_table = testdata_table_metadata.get_metadata_ptree();

  // add table metadata.
  ObjectIdType ret_table_id = -1;
  {
    CALL_TRACE;
    DaoTestTableMetadata::add_table(new_table, &ret_table_id);
    new_table.put(Table::ID, ret_table_id);
  }

  // get table metadata by table name.
  ptree table_metadata_inserted;
  {
    CALL_TRACE;
    DaoTestTableMetadata::get_table_metadata(new_table_name,
                                             table_metadata_inserted);
  }

  // verifies that the returned table metadata is expected one.
  {
    CALL_TRACE;
    testdata_table_metadata.CHECK_METADATA_EXPECTED(new_table,
                                                    table_metadata_inserted);
  }

  // cleanup
  {
    CALL_TRACE;
    remove_table_metadata(new_table_name.c_str(), nullptr);
  }
}

/**
 * @brief happy test for adding one new table metadata
 *  and getting it by table id.
 */
TEST_F(DaoTestTableMetadata, add_get_table_metadata_by_table_id) {
  std::string new_table_name = "DaoTestTableMetadata_" +
                               UTUtils::generate_narrow_uid() + "_" +
                               std::to_string(__LINE__);

  // Generate test metadata.
  UtTableMetadata testdata_table_metadata(new_table_name);
  auto new_table = testdata_table_metadata.get_metadata_ptree();

  // add table metadata.
  ObjectIdType ret_table_id = -1;
  {
    CALL_TRACE;
    DaoTestTableMetadata::add_table(new_table, &ret_table_id);
    new_table.put(Table::ID, ret_table_id);
  }

  // get table metadata by table id.
  ptree table_metadata_inserted;
  {
    CALL_TRACE;
    DaoTestTableMetadata::get_table_metadata(ret_table_id,
                                             table_metadata_inserted);
    UTUtils::print("-- get table metadata --");
    UTUtils::print(UTUtils::get_tree_string(table_metadata_inserted));
  }

  // verifies that the returned table metadata is expected one.
  {
    CALL_TRACE;
    testdata_table_metadata.CHECK_METADATA_EXPECTED(new_table,
                                                    table_metadata_inserted);
  }

  // cleanup
  {
    CALL_TRACE;
    remove_table_metadata(new_table_name.c_str(), nullptr);
  }
}

/**
 * @brief happy test adding three table metadata and updating the second case of
 * metadata.
 */
TEST_F(DaoTestTableMetadata, add_update_table_metadata) {
  std::string new_table_name = "DaoTestTableMetadata_" +
                               UTUtils::generate_narrow_uid() + "_" +
                               std::to_string(__LINE__);

  // Generate test metadata.
  UtTableMetadata testdata_table_metadata(new_table_name);

  // #1 add table metadata.
  ptree table_metadata_1;
  ObjectIdType ret_table_id_1 = -1;
  {
    CALL_TRACE;
    auto new_table = testdata_table_metadata.get_metadata_ptree();

    // add table metadata.
    DaoTestTableMetadata::add_table(new_table, &ret_table_id_1);
    // get table metadata.
    DaoTestTableMetadata::get_table_metadata(ret_table_id_1, table_metadata_1);
  }

  // #2 add table metadata.
  ptree table_metadata_2;
  ObjectIdType ret_table_id_2 = -1;
  {
    CALL_TRACE;
    std::string new_table_name = "DaoTestTableMetadata_" +
                                 UTUtils::generate_narrow_uid() + "_" +
                                 std::to_string(__LINE__);

    // Generate test metadata.
    UtTableMetadata testdata_table_metadata(new_table_name);
    auto new_table = testdata_table_metadata.get_metadata_ptree();

    // add table metadata.
    DaoTestTableMetadata::add_table(new_table, &ret_table_id_2);
    // get table metadata.
    DaoTestTableMetadata::get_table_metadata(ret_table_id_2, table_metadata_2);
  }

  // #3 add table metadata.
  ptree table_metadata_3;
  ObjectIdType ret_table_id_3 = -1;
  {
    CALL_TRACE;
    std::string new_table_name = "DaoTestTableMetadata_" +
                                 UTUtils::generate_narrow_uid() + "_" +
                                 std::to_string(__LINE__);

    // Generate test metadata.
    UtTableMetadata testdata_table_metadata(new_table_name);
    auto new_table = testdata_table_metadata.get_metadata_ptree();

    // add table metadata.
    DaoTestTableMetadata::add_table(new_table, &ret_table_id_3);
    // get table metadata.
    DaoTestTableMetadata::get_table_metadata(ret_table_id_3, table_metadata_3);
  }

  // update table metadata.
  ptree expected_table_metadata = table_metadata_2;
  {
    CALL_TRACE;
    expected_table_metadata.put(
        Table::NAME,
        table_metadata_2.get<std::string>(Table::NAME) + "-update");
    expected_table_metadata.put(
        Table::NAMESPACE,
        table_metadata_2.get<std::string>(Table::NAMESPACE) + "-update");

    // column metadata
    BOOST_FOREACH (ptree::value_type& node,
                   expected_table_metadata.get_child(Table::COLUMNS_NODE)) {
      ptree& column = node.second;
      // update column.
      column.put(Column::NAME,
                 column.get<std::string>(Column::NAME) + "-update");
      column.put(Column::COLUMN_NUMBER,
                 column.get<int32_t>(Column::COLUMN_NUMBER) + 1);
    }

    // update table metadata.
    DaoTestTableMetadata::update_table_metadata(ret_table_id_2,
                                                expected_table_metadata);

    // When Update is performed, the constraint metadata check should be
    // exempted.
    expected_table_metadata.erase(Table::CONSTRAINTS_NODE);
    ptree empty_constraints;
    expected_table_metadata.add_child(Table::CONSTRAINTS_NODE,
                                      empty_constraints);
  }

  // get table metadata.
  ptree table_metadata_updated_1;
  {
    CALL_TRACE;
    DaoTestTableMetadata::get_table_metadata(ret_table_id_1,
                                             table_metadata_updated_1);
  }
  ptree table_metadata_updated_2;
  {
    CALL_TRACE;
    DaoTestTableMetadata::get_table_metadata(ret_table_id_2,
                                             table_metadata_updated_2);
  }
  ptree table_metadata_updated_3;
  {
    CALL_TRACE;
    DaoTestTableMetadata::get_table_metadata(ret_table_id_3,
                                             table_metadata_updated_3);
  }

  UTUtils::print(std::string(30, '-'));
  UTUtils::print("-- output table metadata before update --");
  UTUtils::print(UTUtils::get_tree_string(table_metadata_1));
  UTUtils::print(std::string(10, '-'));
  UTUtils::print(UTUtils::get_tree_string(table_metadata_2));
  UTUtils::print(std::string(10, '-'));
  UTUtils::print(UTUtils::get_tree_string(table_metadata_3));
  UTUtils::print(std::string(30, '-'));

  UTUtils::print("-- output table metadata after update --");
  UTUtils::print(UTUtils::get_tree_string(table_metadata_updated_1));
  UTUtils::print(std::string(10, '-'));
  UTUtils::print(UTUtils::get_tree_string(table_metadata_updated_2));
  UTUtils::print(std::string(10, '-'));
  UTUtils::print(UTUtils::get_tree_string(table_metadata_updated_3));

  // Verify that there is no change in the data after the update.
  UTUtils::print(
      "-- Verify that there is no change in the data after the update --");
  testdata_table_metadata.CHECK_METADATA_EXPECTED(table_metadata_1,
                                                  table_metadata_updated_1);
  testdata_table_metadata.CHECK_METADATA_EXPECTED(table_metadata_3,
                                                  table_metadata_updated_3);

  // Verify the data after the update.
  UTUtils::print("-- Verify the data after the update. --");
  testdata_table_metadata.CHECK_METADATA_EXPECTED(expected_table_metadata,
                                                  table_metadata_updated_2);

  // cleanup
  remove_table_metadata(ret_table_id_1);
  remove_table_metadata(ret_table_id_2);
  remove_table_metadata(ret_table_id_3);
}

/**
 * @brief happy test for removing one new table metadata by table name.
 */
TEST_F(DaoTestTableMetadata, remove_table_metadata_by_table_name) {
  std::string new_table_name = "DaoTestTableMetadata_" +
                               UTUtils::generate_narrow_uid() + "_" +
                               std::to_string(__LINE__);

  // Generate test metadata.
  UtTableMetadata testdata_table_metadata(new_table_name);
  auto new_table = testdata_table_metadata.get_metadata_ptree();

  // add table metadata.
  ObjectIdType ret_table_id = -1;
  {
    CALL_TRACE;
    DaoTestTableMetadata::add_table(new_table, &ret_table_id);
  }

  // remove table metadata by table name.
  ObjectIdType table_id_to_remove = -1;
  {
    CALL_TRACE;
    DaoTestTableMetadata::remove_table_metadata(new_table_name.c_str(),
                                                &table_id_to_remove);
    EXPECT_EQ(ret_table_id, table_id_to_remove);
  }

  // verifies that table metadata does not exist.
  ptree table_metadata_got;
  {
    CALL_TRACE;
    DaoTestTableMetadata::get_table_metadata(table_id_to_remove,
                                             table_metadata_got);
    UTUtils::print("-- get table metadata --");
    UTUtils::print(UTUtils::get_tree_string(table_metadata_got));
  }
}

/**
 * @brief happy test for removing one new table metadata by table id.
 */
TEST_F(DaoTestTableMetadata, remove_table_metadata_by_table_id) {
  std::string new_table_name = "DaoTestTableMetadata_" +
                               UTUtils::generate_narrow_uid() + "_" +
                               std::to_string(__LINE__);

  // Generate test metadata.
  UtTableMetadata testdata_table_metadata(new_table_name);
  auto new_table = testdata_table_metadata.get_metadata_ptree();

  // add table metadata.
  ObjectIdType ret_table_id = -1;
  {
    CALL_TRACE;
    DaoTestTableMetadata::add_table(new_table, &ret_table_id);
  }

  // remove table metadata by table id.
  {
    CALL_TRACE;
    DaoTestTableMetadata::remove_table_metadata(ret_table_id);
  }

  // verifies that table metadata does not exist.
  ptree table_metadata_got;
  {
    CALL_TRACE;
    DaoTestTableMetadata::get_table_metadata(ret_table_id, table_metadata_got);

    UTUtils::print("-- get table metadata --");
    UTUtils::print(UTUtils::get_tree_string(table_metadata_got));
  }
}

}  // namespace manager::metadata::testing
