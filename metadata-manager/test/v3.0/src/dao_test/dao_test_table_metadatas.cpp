/*
 * Copyright 2020 tsurugi project.
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

#include "test/dao_test/dao_test_table_metadatas.h"

#include <gtest/gtest.h>
#include <boost/foreach.hpp>
#include <boost/property_tree/ptree.hpp>
#include <memory>
#include <string>

#include "manager/metadata/dao/columns_dao.h"
#include "manager/metadata/dao/db_session_manager.h"
#include "manager/metadata/dao/generic_dao.h"
#include "manager/metadata/dao/tables_dao.h"
#include "manager/metadata/error_code.h"
#include "manager/metadata/tables.h"

#include "test/api_test_table_metadatas.h"
#include "test/global_test_environment.h"
#include "test/utility/ut_table_metadata.h"
#include "test/utility/ut_utils.h"

namespace manager::metadata::testing {

using namespace manager::metadata::db;
using namespace boost::property_tree;

/**
 *  @brief  Add table metadata to table metadata table.
 *  @param  (table_name)  [in]  table name of table metadata to add.
 *  @param  (object_id)   [out] ID of the added table metadata.
 *  @return ErrorCode::OK if success, otherwise an error code.
 */
void DaoTestTableMetadata::add_table(std::string table_name,
                                     ObjectIdType *object_id) {
    assert(object_id != nullptr);

    UTTableMetadata *testdata_table_metadata =
        global->testdata_table_metadata.get();
    ptree new_table = testdata_table_metadata->tables;

    new_table.put(Tables::NAME, table_name);

    std::shared_ptr<GenericDAO> t_gdao = nullptr;

    DBSessionManager db_session_manager;

    ErrorCode error =
        db_session_manager.get_dao(GenericDAO::TableName::TABLES, t_gdao);
    EXPECT_EQ(ErrorCode::OK, error);

    std::shared_ptr<TablesDAO> tdao;
    tdao = std::static_pointer_cast<TablesDAO>(t_gdao);

    std::shared_ptr<GenericDAO> c_gdao = nullptr;
    error = db_session_manager.get_dao(GenericDAO::TableName::COLUMNS, c_gdao);
    EXPECT_EQ(ErrorCode::OK, error);

    std::shared_ptr<ColumnsDAO> cdao;
    cdao = std::static_pointer_cast<ColumnsDAO>(c_gdao);

    error = db_session_manager.start_transaction();
    EXPECT_EQ(ErrorCode::OK, error);

    // Add table metadata object to table metadata table.
    ObjectIdType table_id_returned;
    error = tdao->insert_table_metadata(new_table, table_id_returned);

    EXPECT_EQ(ErrorCode::OK, error);
    EXPECT_GT(table_id_returned, 0);

    // Add column metadata object to column metadata table.
    BOOST_FOREACH (const ptree::value_type &node,
                   new_table.get_child(Tables::COLUMNS_NODE)) {
        ptree column = node.second;
        error = cdao->insert_one_column_metadata(table_id_returned, column);
        EXPECT_EQ(ErrorCode::OK, error);
    }

    error = db_session_manager.commit();
    EXPECT_EQ(ErrorCode::OK, error);

    *object_id = table_id_returned;

    UTUtils::print("new table id:", *object_id);
    UTUtils::print(UTUtils::get_tree_string(new_table));
}

/**
 *  @brief  Get table metadata object based on table name.
 *  @param  (object_name)   [in]  table name. (Value of "name"
 * key.)
 *  @param  (object)        [out] table metadata object with the specified name.
 *  @return ErrorCode::OK if success, otherwise an error code.
 */
void DaoTestTableMetadata::get_table_metadata(
    std::string_view object_name, boost::property_tree::ptree &object) {
    std::shared_ptr<GenericDAO> t_gdao = nullptr;

    DBSessionManager db_session_manager;

    ErrorCode error =
        db_session_manager.get_dao(GenericDAO::TableName::TABLES, t_gdao);
    EXPECT_EQ(ErrorCode::OK, error);

    std::shared_ptr<TablesDAO> tdao;
    tdao = std::static_pointer_cast<TablesDAO>(t_gdao);

    std::shared_ptr<GenericDAO> c_gdao = nullptr;
    error = db_session_manager.get_dao(GenericDAO::TableName::COLUMNS, c_gdao);
    EXPECT_EQ(ErrorCode::OK, error);

    std::shared_ptr<ColumnsDAO> cdao;
    cdao = std::static_pointer_cast<ColumnsDAO>(c_gdao);

    error =
        tdao->select_table_metadata(Tables::NAME, object_name.data(), object);
    EXPECT_EQ(ErrorCode::OK, error);

    BOOST_FOREACH (ptree::value_type &node, object) {
        ptree &table = node.second;

        if (table.empty()) {
            boost::optional<std::string> o_table_id =
                object.get_optional<std::string>(Tables::ID);
            if (!o_table_id) {
                break;
            }
            ptree columns;
            error = cdao->select_column_metadata(Tables::Column::TABLE_ID,
                                                 o_table_id.get(), columns);
            EXPECT_EQ(ErrorCode::OK, error);
            object.add_child(Tables::COLUMNS_NODE, columns);
            break;
        } else {
            boost::optional<std::string> o_table_id =
                table.get_optional<std::string>(Tables::ID);
            if (!o_table_id) {
                break;
            }
            ptree columns;
            error = cdao->select_column_metadata(Tables::Column::TABLE_ID,
                                                 o_table_id.get(), columns);
            EXPECT_EQ(ErrorCode::OK, error);
            object.add_child(Tables::COLUMNS_NODE, columns);
        }
    }
}

/**
 *  @brief  Get table metadata.
 *  @param  (object_id) [in]  table id.
 *  @param  (object)    [out] table metadata with the specified ID.
 *  @return ErrorCode::OK if success, otherwise an error code.
 */
void DaoTestTableMetadata::get_table_metadata(
    ObjectIdType object_id, boost::property_tree::ptree &object) {
    std::shared_ptr<GenericDAO> t_gdao = nullptr;

    DBSessionManager db_session_manager;

    ErrorCode error =
        db_session_manager.get_dao(GenericDAO::TableName::TABLES, t_gdao);
    EXPECT_EQ(ErrorCode::OK, error);

    std::shared_ptr<TablesDAO> tdao;
    tdao = std::static_pointer_cast<TablesDAO>(t_gdao);

    std::shared_ptr<GenericDAO> c_gdao = nullptr;
    error = db_session_manager.get_dao(GenericDAO::TableName::COLUMNS, c_gdao);
    EXPECT_EQ(ErrorCode::OK, error);

    std::shared_ptr<ColumnsDAO> cdao;
    cdao = std::static_pointer_cast<ColumnsDAO>(c_gdao);

    error = tdao->select_table_metadata(Tables::ID, std::to_string(object_id),
                                        object);
    if (error == ErrorCode::OK) {
        EXPECT_EQ(ErrorCode::OK, error);
    } else {
        EXPECT_EQ(ErrorCode::INVALID_PARAMETER, error);
        return;
    }

    BOOST_FOREACH (ptree::value_type &node, object) {
        ptree &table = node.second;

        if (table.empty()) {
            boost::optional<std::string> o_table_id =
                object.get_optional<std::string>(Tables::ID);
            if (!o_table_id) {
                break;
            }
            ptree columns;
            error = cdao->select_column_metadata(Tables::Column::TABLE_ID,
                                                 o_table_id.get(), columns);
            EXPECT_EQ(ErrorCode::OK, error);
            object.add_child(Tables::COLUMNS_NODE, columns);
            break;
        } else {
            boost::optional<std::string> o_table_id =
                table.get_optional<std::string>(Tables::ID);
            if (!o_table_id) {
                break;
            }
            ptree columns;
            error = cdao->select_column_metadata(Tables::Column::TABLE_ID,
                                                 o_table_id.get(), columns);
            EXPECT_EQ(ErrorCode::OK, error);
            object.add_child(Tables::COLUMNS_NODE, columns);
        }
    }
}

/**
 *  @brief  Remove all metadata-object based on the given table id
 *  (table metadata, column metadata and column statistics)
 *  from metadata-table (the table metadata table,
 *  the column metadata table and the column statistics table).
 *  @param (object_id) [in] table id.
 *  @return ErrorCode::OK if success, otherwise an error code.
 */
void DaoTestTableMetadata::remove_table_metadata(const ObjectIdType object_id) {
    std::shared_ptr<GenericDAO> t_gdao = nullptr;

    DBSessionManager db_session_manager;

    ErrorCode error =
        db_session_manager.get_dao(GenericDAO::TableName::TABLES, t_gdao);
    EXPECT_EQ(ErrorCode::OK, error);

    error = db_session_manager.start_transaction();
    EXPECT_EQ(ErrorCode::OK, error);

    std::shared_ptr<TablesDAO> tdao;
    tdao = std::static_pointer_cast<TablesDAO>(t_gdao);

    error = tdao->delete_table_metadata_by_table_id(object_id);
    EXPECT_EQ(ErrorCode::OK, error);

    if (error == ErrorCode::OK) {
        error = db_session_manager.commit();
        EXPECT_EQ(ErrorCode::OK, error);
    } else {
        ErrorCode rollback_error = db_session_manager.rollback();
        EXPECT_EQ(ErrorCode::OK, rollback_error);
    }
};

/**
 *  @brief  Remove all metadata-object based on the given table name
 *  (table metadata, column metadata and column statistics)
 *  from metadata-table (the table metadata table,
 *  the column metadata table and the column statistics table).
 *  @param (object_name) [in]  table name.
 *  @param (object_id)   [out] object id of table removed.
 *  @return ErrorCode::OK if success, otherwise an error code.
 */
void DaoTestTableMetadata::remove_table_metadata(const char *object_name,
                                                 ObjectIdType *object_id) {
    std::shared_ptr<GenericDAO> t_gdao = nullptr;

    DBSessionManager db_session_manager;

    ErrorCode error =
        db_session_manager.get_dao(GenericDAO::TableName::TABLES, t_gdao);
    EXPECT_EQ(ErrorCode::OK, error);

    error = db_session_manager.start_transaction();
    EXPECT_EQ(ErrorCode::OK, error);

    std::shared_ptr<TablesDAO> tdao;
    tdao = std::static_pointer_cast<TablesDAO>(t_gdao);

    ObjectIdType retval_object_id;
    error = tdao->delete_table_metadata_by_table_name(std::string(object_name),
                                                      retval_object_id);
    EXPECT_EQ(ErrorCode::OK, error);

    if (error == ErrorCode::OK) {
        error = db_session_manager.commit();
        EXPECT_EQ(ErrorCode::OK, error);

        if (error == ErrorCode::OK && object_id != nullptr) {
            *object_id = retval_object_id;
        }
    } else {
        ErrorCode rollback_error = db_session_manager.rollback();
        EXPECT_EQ(ErrorCode::OK, rollback_error);
    }
};

/**
 *  @brief  happy test for adding one new table metadata
 *  and getting it by table name.
 */
TEST_F(DaoTestTableMetadata, add_get_table_metadata_by_table_name) {
    // prepare test data for adding table metadata.
    UTTableMetadata testdata_table_metadata =
        *(global->testdata_table_metadata.get());
    ptree new_table = testdata_table_metadata.tables;
    std::string new_table_name =
        new_table.get<std::string>(Tables::NAME) + "_DaoTestTableMetadata1";
    new_table.put(Tables::NAME, new_table_name);

    // add table metadata.
    ObjectIdType ret_table_id = -1;
    DaoTestTableMetadata::add_table(new_table_name, &ret_table_id);
    new_table.put(Tables::ID, ret_table_id);

    // get table metadata by table name.
    ptree table_metadata_inserted;
    DaoTestTableMetadata::get_table_metadata(new_table_name,
                                             table_metadata_inserted);

    // verifies that the returned table metadata is expected one.
    ApiTestTableMetadata::check_table_metadata_expected(
        new_table, table_metadata_inserted);
}

/**
 *  @brief  happy test for adding one new table metadata
 *  and getting it by table id.
 */
TEST_F(DaoTestTableMetadata, add_get_table_metadata_by_table_id) {
    // prepare test data for adding table metadata.
    UTTableMetadata testdata_table_metadata =
        *(global->testdata_table_metadata.get());
    ptree new_table = testdata_table_metadata.tables;
    std::string new_table_name =
        new_table.get<std::string>(Tables::NAME) + "_DaoTestTableMetadata2";
    new_table.put(Tables::NAME, new_table_name);

    // add table metadata.
    ObjectIdType ret_table_id = -1;
    DaoTestTableMetadata::add_table(new_table_name, &ret_table_id);
    new_table.put(Tables::ID, ret_table_id);

    // get table metadata by table id.
    ptree table_metadata_inserted;
    DaoTestTableMetadata::get_table_metadata(ret_table_id,
                                             table_metadata_inserted);

    UTUtils::print("-- get table metadata --");
    UTUtils::print(UTUtils::get_tree_string(table_metadata_inserted));

    // verifies that the returned table metadata is expected one.
    ApiTestTableMetadata::check_table_metadata_expected(
        new_table, table_metadata_inserted);
}

/**
 *  @brief  happy test for removing one new table metadata by table name.
 */
TEST_F(DaoTestTableMetadata, remove_table_metadata_by_table_name) {
    // prepare test data for adding table metadata.
    UTTableMetadata testdata_table_metadata =
        *(global->testdata_table_metadata.get());
    ptree new_table = testdata_table_metadata.tables;
    std::string new_table_name =
        new_table.get<std::string>(Tables::NAME) + "_DaoTestTableMetadata3";
    new_table.put(Tables::NAME, new_table_name);

    // add table metadata.
    ObjectIdType ret_table_id = -1;
    DaoTestTableMetadata::add_table(new_table_name, &ret_table_id);

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
 *  @brief  happy test for removing one new table metadata by table id.
 */
TEST_F(DaoTestTableMetadata, remove_table_metadata_by_table_id) {
    // prepare test data for adding table metadata.
    UTTableMetadata testdata_table_metadata =
        *(global->testdata_table_metadata.get());
    ptree new_table = testdata_table_metadata.tables;
    std::string new_table_name =
        new_table.get<std::string>(Tables::NAME) + "_DaoTestTableMetadata4";
    new_table.put(Tables::NAME, new_table_name);

    // add table metadata.
    ObjectIdType ret_table_id = -1;
    DaoTestTableMetadata::add_table(new_table_name, &ret_table_id);

    // remove table metadata by table id.
    DaoTestTableMetadata::remove_table_metadata(ret_table_id);

    // verifies that table metadata does not exist.
    ptree table_metadata_got;
    DaoTestTableMetadata::get_table_metadata(ret_table_id, table_metadata_got);

    UTUtils::print("-- get table metadata --");
    UTUtils::print(UTUtils::get_tree_string(table_metadata_got));
}

}  // namespace manager::metadata::testing
