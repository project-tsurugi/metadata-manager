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

#include "test/api_test_environment.h"
#include "test/utility/ut_table_metadata.h"
#include "test/utility/ut_utils.h"

namespace manager::metadata::testing {

using namespace manager::metadata::db;
using namespace boost::property_tree;

void DaoTestTableMetadata::add_table(std::string table_name,
                                     ObjectIdType *ret_table_id) {
    assert(ret_table_id != nullptr);

    UTTableMetadata *testdata_table_metadata =
        api_test_env->testdata_table_metadata.get();
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

    ObjectIdType table_id_returned;
    error = tdao->insert_table_metadata(new_table, table_id_returned);

    EXPECT_EQ(ErrorCode::OK, error);
    EXPECT_GT(table_id_returned, 0);

    BOOST_FOREACH (const ptree::value_type &node,
                   new_table.get_child(Tables::COLUMNS_NODE)) {
        ptree column = node.second;
        error = cdao->insert_one_column_metadata(table_id_returned, column);
        EXPECT_EQ(ErrorCode::OK, error);
    }

    error = db_session_manager.commit();
    EXPECT_EQ(ErrorCode::OK, error);

    *ret_table_id = table_id_returned;

    UTUtils::print("new table id:", *ret_table_id);
    UTUtils::print(UTUtils::get_tree_string(new_table));
}

}  // namespace manager::metadata::testing
