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
#include <gtest/gtest.h>

#include <boost/foreach.hpp>
#include <boost/property_tree/ptree.hpp>

#include "manager/metadata/dao/postgresql/columns_dao_pg.h"
#include "manager/metadata/dao/postgresql/constraints_dao_pg.h"
#include "manager/metadata/dao/postgresql/db_session_manager_pg.h"
#include "manager/metadata/dao/postgresql/tables_dao_pg.h"
#include "test/common/global_test_environment.h"
#include "test/common/ut_utils.h"
#include "test/helper/table_metadata_helper.h"

namespace manager::metadata::testing {

using boost::property_tree::ptree;

class DaoTestTableMetadata : public ::testing::Test {
 public:
  void SetUp() override { UTUtils::skip_if_connection_not_opened(); }

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
    db::DbSessionManagerPg db_session_manager;

    error = db_session_manager.connect();
    ASSERT_EQ(ErrorCode::OK, error);

    // Get TablesDAO.
    std::shared_ptr<db::Dao> tables_dao;
    error = db_session_manager.get_tables_dao(tables_dao);
    ASSERT_NE(nullptr, tables_dao);
    ASSERT_EQ(ErrorCode::OK, error);

    // Get ColumnsDAO.
    std::shared_ptr<db::Dao> columns_dao;
    error = db_session_manager.get_columns_dao(columns_dao);
    ASSERT_NE(nullptr, columns_dao);
    ASSERT_EQ(ErrorCode::OK, error);

    // Get ConstraintsDAO.
    std::shared_ptr<db::Dao> constraints_dao;
    error = db_session_manager.get_constraints_dao(constraints_dao);
    ASSERT_NE(nullptr, constraints_dao);
    ASSERT_EQ(ErrorCode::OK, error);

    // Run the API under test.
    error = db_session_manager.start_transaction();
    EXPECT_EQ(ErrorCode::OK, error);

    // Add table metadata object to table metadata table.
    ObjectIdType added_table_id = 0;
    // Run the API under test.
    error = tables_dao->insert(new_table, added_table_id);
    EXPECT_EQ(ErrorCode::OK, error);
    EXPECT_GT(added_table_id, 0);

    // Add column metadata object to column metadata table.
    BOOST_FOREACH (const ptree::value_type& node,
                   new_table.get_child(Table::COLUMNS_NODE)) {
      ptree column = node.second;
      column.erase(Column::ID);
      column.put(Column::TABLE_ID, added_table_id);

      ObjectIdType added_column_id = 0;
      // Run the API under test.
      error = columns_dao->insert(column, added_column_id);
      EXPECT_EQ(ErrorCode::OK, error);
      EXPECT_GT(added_column_id, 0);
    }

    // Add constraint metadata object to constraint metadata table.
    BOOST_FOREACH (const ptree::value_type& node,
                   new_table.get_child(Table::CONSTRAINTS_NODE)) {
      ptree constraint = node.second;
      constraint.put(Constraint::TABLE_ID, added_table_id);

      ObjectIdType added_constraint_id = 0;
      // Run the API under test.
      error = constraints_dao->insert(constraint, added_constraint_id);
      EXPECT_EQ(ErrorCode::OK, error);
    }

    // Run the API under test.
    error = db_session_manager.commit();
    EXPECT_EQ(ErrorCode::OK, error);

    *object_id = added_table_id;

    UTUtils::print("new table id:", *object_id);
    UTUtils::print(UTUtils::get_tree_string(new_table));
  }

  /**
   * @brief Get table metadata object based on table name.
   * @param (object_name)   [in]  table name. (Value of "name" key.)
   * @param (object)        [out] table metadata object with the specified name.
   * @return ErrorCode::OK if success, otherwise an error code.
   */
  static void get_table_metadata(std::string_view object_name,
                                 boost::property_tree::ptree& object) {
    ErrorCode error = ErrorCode::UNKNOWN;
    db::DbSessionManagerPg db_session_manager;

    error = db_session_manager.connect();
    ASSERT_EQ(ErrorCode::OK, error);

    // Get TablesDAO.
    std::shared_ptr<db::Dao> tables_dao;
    error = db_session_manager.get_tables_dao(tables_dao);
    ASSERT_NE(nullptr, tables_dao);
    ASSERT_EQ(ErrorCode::OK, error);

    // Get ColumnsDAO.
    std::shared_ptr<db::Dao> columns_dao;
    error = db_session_manager.get_columns_dao(columns_dao);
    ASSERT_NE(nullptr, columns_dao);
    ASSERT_EQ(ErrorCode::OK, error);

    // Get ConstraintsDAO.
    std::shared_ptr<db::Dao> constraints_dao;
    error = db_session_manager.get_constraints_dao(constraints_dao);
    ASSERT_NE(nullptr, constraints_dao);
    ASSERT_EQ(ErrorCode::OK, error);

    // Set condition keys for testing.
    std::map<std::string_view, std::string_view> keys = {
        {Table::NAME, object_name}
    };

    // Run the API under test.
    error = tables_dao->select(keys, object);
    EXPECT_EQ(ErrorCode::OK, error);

    BOOST_FOREACH (ptree::value_type& node, object) {
      ptree& table = node.second;

      boost::optional<std::string> o_table_id =
          table.get_optional<std::string>(Table::ID);
      if (o_table_id) {
        ptree columns;
        {
          // Set condition keys for testing.
          std::map<std::string_view, std::string_view> keys = {
              {Column::TABLE_ID, o_table_id.get()}
          };

          // Run the API under test.
          error = columns_dao->select(keys, columns);
          EXPECT_EQ(ErrorCode::OK, error);
        }
        table.add_child(Table::COLUMNS_NODE, columns);

        ptree constraints;
        {
          // Set condition keys for testing.
          std::map<std::string_view, std::string_view> keys = {
              {Constraint::TABLE_ID, o_table_id.get()}
          };

          // Run the API under test.
          error = constraints_dao->select(keys, constraints);
          EXPECT_EQ(ErrorCode::OK, error);
        }
        table.add_child(Table::CONSTRAINTS_NODE, constraints);
      }
    }
  }

  /**
   * @brief Get table metadata.
   * @param (object_id) [in]  table id.
   * @param (object)    [out] table metadata with the specified ID.
   * @return ErrorCode::OK if success, otherwise an error code.
   */
  static void get_table_metadata(ObjectIdType object_id,
                                 boost::property_tree::ptree& object) {
    ErrorCode error = ErrorCode::UNKNOWN;
    db::DbSessionManagerPg db_session_manager;

    error = db_session_manager.connect();
    ASSERT_EQ(ErrorCode::OK, error);

    // Get TablesDAO.
    std::shared_ptr<db::Dao> tables_dao;
    error = db_session_manager.get_tables_dao(tables_dao);
    ASSERT_NE(nullptr, tables_dao);
    ASSERT_EQ(ErrorCode::OK, error);

    // Get ColumnsDAO.
    std::shared_ptr<db::Dao> columns_dao;
    error = db_session_manager.get_columns_dao(columns_dao);
    ASSERT_NE(nullptr, columns_dao);
    ASSERT_EQ(ErrorCode::OK, error);

    // Get ConstraintsDAO.
    std::shared_ptr<db::Dao> constraints_dao;
    error = db_session_manager.get_constraints_dao(constraints_dao);
    ASSERT_NE(nullptr, constraints_dao);
    ASSERT_EQ(ErrorCode::OK, error);

    // Set condition keys for testing.
    auto s_object_id = std::to_string(object_id);
    std::map<std::string_view, std::string_view> keys = {
        {Table::ID, s_object_id}
    };

    // Run the API under test.
    error = tables_dao->select(keys, object);
    if (error != ErrorCode::OK) {
      EXPECT_EQ(ErrorCode::ID_NOT_FOUND, error);
      return;
    }

    BOOST_FOREACH (ptree::value_type& node, object) {
      ptree& table = node.second;

      boost::optional<std::string> o_table_id =
          table.get_optional<std::string>(Table::ID);
      if (o_table_id) {
        ptree columns;
        {
          // Set condition keys for testing.
          std::map<std::string_view, std::string_view> keys = {
              {Column::TABLE_ID, o_table_id.get()}
          };

          // Run the API under test.
          error = columns_dao->select(keys, columns);
          EXPECT_EQ(ErrorCode::OK, error);
        }
        table.add_child(Table::COLUMNS_NODE, columns);

        ptree constraints;
        {
          // Set condition keys for testing.
          std::map<std::string_view, std::string_view> keys = {
              {Constraint::TABLE_ID, o_table_id.get()}
          };

          // Run the API under test.
          error = constraints_dao->select(keys, constraints);
          EXPECT_EQ(ErrorCode::OK, error);
        }
        table.add_child(Table::CONSTRAINTS_NODE, constraints);
      }
    }
  }

  /**
   * @brief Update table metadata to table metadata table.
   * @param (object_id) [in]     ID of the added table metadata.
   * @param (object)    [in/out] table metadata object.
   * @return ErrorCode::OK if success, otherwise an error code.
   */
  static void update_table(const ObjectIdType& object_id,
                           boost::property_tree::ptree& object) {
    ErrorCode error = ErrorCode::UNKNOWN;
    db::DbSessionManagerPg db_session_manager;

    error = db_session_manager.connect();
    ASSERT_EQ(ErrorCode::OK, error);

    auto table_name      = object.get<std::string>(Table::NAME);
    auto table_namespace = object.get<std::string>(Table::NAMESPACE);
    auto table_tuples    = object.get<int64_t>(Table::NUMBER_OF_TUPLES);

    object.put(Table::NAME, table_name + "-update");
    object.put(Table::NAMESPACE, table_namespace + "-update");
    object.put(Table::NUMBER_OF_TUPLES, table_tuples * 2);

    // Get TablesDAO.
    std::shared_ptr<db::Dao> tables_dao;
    error = db_session_manager.get_tables_dao(tables_dao);
    ASSERT_NE(nullptr, tables_dao);
    ASSERT_EQ(ErrorCode::OK, error);

    // Run the API under test.
    error = db_session_manager.start_transaction();
    EXPECT_EQ(ErrorCode::OK, error);

    // Set condition keys for testing.
    auto s_object_id = std::to_string(object_id);
    std::map<std::string_view, std::string_view> keys = {
        {Tables::ID, s_object_id}
    };

    uint64_t updated_rows = 0;
    // Update table metadata object to table metadata table.
    error = tables_dao->update(keys, object, updated_rows);
    EXPECT_EQ(ErrorCode::OK, error);
    EXPECT_EQ(1, updated_rows);

    // Run the API under test.
    error = db_session_manager.commit();
    EXPECT_EQ(ErrorCode::OK, error);
  }

  /**
   * @brief Remove all metadata-object based on the given table id
   *  (table metadata, column metadata and column statistics)
   *  from metadata-table (the table metadata table,
   *  the column metadata table and the column statistics table).
   * @param (object_id) [in] table id.
   * @return ErrorCode::OK if success, otherwise an error code.
   */
  static void remove_table_metadata(const ObjectIdType object_id) {
    ErrorCode error = ErrorCode::UNKNOWN;
    db::DbSessionManagerPg db_session_manager;

    error = db_session_manager.connect();
    ASSERT_EQ(ErrorCode::OK, error);

    // Get TablesDAO.
    std::shared_ptr<db::Dao> tables_dao;
    error = db_session_manager.get_tables_dao(tables_dao);
    ASSERT_NE(nullptr, tables_dao);
    ASSERT_EQ(ErrorCode::OK, error);

    error = db_session_manager.start_transaction();
    EXPECT_EQ(ErrorCode::OK, error);

    // Set condition keys for testing.
    auto s_object_id = std::to_string(object_id);
    std::map<std::string_view, std::string_view> keys = {
        {Tables::ID, s_object_id}
    };

    std::vector<ObjectId> removed_ids;
    // Run the API under test.
    error = tables_dao->remove(keys, removed_ids);
    EXPECT_EQ(ErrorCode::OK, error);
    EXPECT_EQ(1, removed_ids.size());
    if (removed_ids.size() == 1) {
      EXPECT_EQ(object_id, removed_ids[0]);
    }

    if (error == ErrorCode::OK) {
      // Run the API under test.
      error = db_session_manager.commit();
      EXPECT_EQ(ErrorCode::OK, error);
    } else {
      // Run the API under test.
      ErrorCode rollback_error = db_session_manager.rollback();
      EXPECT_EQ(ErrorCode::OK, rollback_error);
    }
  }

  /**
   * @brief Remove all metadata-object based on the given table name
   *  (table metadata, column metadata and column statistics)
   *  from metadata-table (the table metadata table,
   *  the column metadata table and the column statistics table).
   * @param (object_name) [in]  table name.
   * @param (object_id)   [out] object id of table removed.
   * @return ErrorCode::OK if success, otherwise an error code.
   */
  static void remove_table_metadata(const char* object_name,
                                    ObjectIdType* object_id) {
    ErrorCode error = ErrorCode::UNKNOWN;
    db::DbSessionManagerPg db_session_manager;

    error = db_session_manager.connect();
    ASSERT_EQ(ErrorCode::OK, error);

    // Get TablesDAO.
    std::shared_ptr<db::Dao> tables_dao;
    error = db_session_manager.get_tables_dao(tables_dao);
    ASSERT_NE(nullptr, tables_dao);
    ASSERT_EQ(ErrorCode::OK, error);

    // Run the API under test.
    error = db_session_manager.start_transaction();
    EXPECT_EQ(ErrorCode::OK, error);

    // Set condition keys for testing.
    std::map<std::string_view, std::string_view> keys = {
        {Table::NAME, object_name}
    };

    ObjectId removed_id = INVALID_OBJECT_ID;
    std::vector<ObjectId> removed_ids;
    // Run the API under test.
    error = tables_dao->remove(keys, removed_ids);
    EXPECT_EQ(ErrorCode::OK, error);
    EXPECT_EQ(1, removed_ids.size());
    if (removed_ids.size() == 1) {
      EXPECT_NE(-1, removed_ids[0]);
      removed_id = removed_ids[0];
    }

    if (error == ErrorCode::OK) {
      // Run the API under test.
      error = db_session_manager.commit();
      ASSERT_EQ(ErrorCode::OK, error);

      if (error == ErrorCode::OK && object_id != nullptr) {
        *object_id = removed_id;
      }
    } else {
      // Run the API under test.
      ErrorCode rollback_error = db_session_manager.rollback();
      ASSERT_EQ(ErrorCode::OK, rollback_error);
    }
  }
};  // class DaoTestTableMetadata

/**
 * @brief happy test for adding one new table metadata
 *  and getting it by table name.
 */
TEST_F(DaoTestTableMetadata, add_get_table_metadata_by_table_name) {
  std::string new_table_name = TableMetadataHelper::make_table_name(
      "DaoTestTableMetadata", "", __LINE__);

  // Generate test metadata.
  UtTableMetadata testdata_table_metadata(new_table_name);
  auto new_table = testdata_table_metadata.get_metadata_ptree();

  // add table metadata.
  ObjectIdType ret_table_id = -1;
  DaoTestTableMetadata::add_table(new_table, &ret_table_id);
  new_table.put(Table::ID, ret_table_id);

  // get table metadata by table name.
  ptree table_metadata_inserted;
  DaoTestTableMetadata::get_table_metadata(new_table_name,
                                           table_metadata_inserted);

  // verifies that the returned table metadata is expected one.
  ASSERT_EQ(1, table_metadata_inserted.size());
  testdata_table_metadata.CHECK_METADATA_EXPECTED(
      new_table, table_metadata_inserted.front().second);

  // remove table metadata.
  DaoTestTableMetadata::remove_table_metadata(ret_table_id);
}

/**
 * @brief happy test for adding one new table metadata
 *  and getting it by table id.
 */
TEST_F(DaoTestTableMetadata, add_get_table_metadata_by_table_id) {
  std::string new_table_name = TableMetadataHelper::make_table_name(
      "DaoTestTableMetadata", "", __LINE__);

  // Generate test metadata.
  UtTableMetadata testdata_table_metadata(new_table_name);
  auto new_table = testdata_table_metadata.get_metadata_ptree();

  // add table metadata.
  ObjectIdType ret_table_id = -1;
  DaoTestTableMetadata::add_table(new_table, &ret_table_id);
  new_table.put(Table::ID, ret_table_id);

  // get table metadata by table id.
  ptree table_metadata_inserted;
  DaoTestTableMetadata::get_table_metadata(ret_table_id,
                                           table_metadata_inserted);

  UTUtils::print("-- get table metadata --");
  UTUtils::print(UTUtils::get_tree_string(table_metadata_inserted));

  // verifies that the returned table metadata is expected one.
  ASSERT_EQ(1, table_metadata_inserted.size());
  testdata_table_metadata.CHECK_METADATA_EXPECTED(
      new_table, table_metadata_inserted.front().second);

  // remove table metadata.
  DaoTestTableMetadata::remove_table_metadata(ret_table_id);
}

/**
 * @brief update one table metadata.
 */
TEST_F(DaoTestTableMetadata, update_table_metadata) {
  std::string new_table_name = TableMetadataHelper::make_table_name(
      "DaoTestTableMetadata", "", __LINE__);

  // Generate test metadata.
  UtTableMetadata testdata_table_metadata(new_table_name);
  auto new_table = testdata_table_metadata.get_metadata_ptree();

  // add table metadata.
  ObjectIdType ret_table_id = -1;
  DaoTestTableMetadata::add_table(new_table, &ret_table_id);
  new_table.put(Table::ID, ret_table_id);

  // get table metadata before update.
  ptree table_metadata_inserted;
  DaoTestTableMetadata::get_table_metadata(ret_table_id,
                                           table_metadata_inserted);
  ASSERT_EQ(1, table_metadata_inserted.size());

  // update table metadata.
  ptree update_table = table_metadata_inserted.front().second;
  DaoTestTableMetadata::update_table(ret_table_id, update_table);
  new_table.put(Table::ID, ret_table_id);

  // get table metadata after update.
  ptree table_metadata_updated;
  DaoTestTableMetadata::get_table_metadata(ret_table_id,
                                           table_metadata_updated);

  UTUtils::print("-- get table metadata before update --");
  UTUtils::print(UTUtils::get_tree_string(table_metadata_inserted));
  UTUtils::print("-- get table metadata after update --");
  UTUtils::print(UTUtils::get_tree_string(table_metadata_updated));

  // verifies that the returned table metadata is expected one.
  ASSERT_EQ(1, table_metadata_updated.size());
  testdata_table_metadata.CHECK_METADATA_EXPECTED(
      update_table, table_metadata_updated.front().second);

  // remove table metadata.
  DaoTestTableMetadata::remove_table_metadata(ret_table_id);
}

/**
 * @brief happy test for removing one new table metadata by table name.
 */
TEST_F(DaoTestTableMetadata, remove_table_metadata_by_table_name) {
  std::string new_table_name = TableMetadataHelper::make_table_name(
      "DaoTestTableMetadata", "", __LINE__);

  // Generate test metadata.
  UtTableMetadata testdata_table_metadata(new_table_name);
  auto new_table = testdata_table_metadata.get_metadata_ptree();

  // add table metadata.
  ObjectIdType ret_table_id = -1;
  DaoTestTableMetadata::add_table(new_table, &ret_table_id);

  // remove table metadata by table name.
  ObjectIdType table_id_to_remove = -1;
  DaoTestTableMetadata::remove_table_metadata(new_table_name.c_str(),
                                              &table_id_to_remove);
  EXPECT_EQ(ret_table_id, table_id_to_remove);

  // verifies that table metadata does not exist.
  ptree table_metadata_got;
  DaoTestTableMetadata::get_table_metadata(table_id_to_remove,
                                           table_metadata_got);

  UTUtils::print("-- get table metadata --");
  UTUtils::print(UTUtils::get_tree_string(table_metadata_got));
}

/**
 * @brief happy test for removing one new table metadata by table id.
 */
TEST_F(DaoTestTableMetadata, remove_table_metadata_by_table_id) {
  std::string new_table_name = TableMetadataHelper::make_table_name(
      "DaoTestTableMetadata", "", __LINE__);

  // Generate test metadata.
  UtTableMetadata testdata_table_metadata(new_table_name);
  auto new_table = testdata_table_metadata.get_metadata_ptree();

  // add table metadata.
  ObjectIdType ret_table_id = -1;
  DaoTestTableMetadata::add_table(new_table, &ret_table_id);

  // remove table metadata by table id.
  DaoTestTableMetadata::remove_table_metadata(ret_table_id);

  // verifies that table metadata does not exist.
  ptree table_metadata_got;
  DaoTestTableMetadata::get_table_metadata(ret_table_id, table_metadata_got);

  UTUtils::print("-- get table metadata --");
  UTUtils::print(UTUtils::get_tree_string(table_metadata_got));
}

}  // namespace manager::metadata::testing
