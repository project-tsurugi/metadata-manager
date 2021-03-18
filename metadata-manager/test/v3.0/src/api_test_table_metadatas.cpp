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
#include "test/api_test_table_metadatas.h"

#include <gtest/gtest.h>
#include <boost/foreach.hpp>
#include <memory>
#include <string>

#include "manager/metadata/error_code.h"
#include "manager/metadata/tables.h"

#include "test/utility/ut_table_metadata.h"
#include "test/utility/ut_utils.h"

using namespace manager::metadata;
using namespace boost::property_tree;

namespace manager::metadata::testing {

void ApiTestTableMetadata::check_table_metadata_expected(
    UTTableMetadata testdata_table_metadata, ptree &table_metadata_inserted) {
    EXPECT_EQ(testdata_table_metadata.name,
              table_metadata_inserted.get<std::string>(Tables::NAME));
    EXPECT_EQ(testdata_table_metadata.id,
              table_metadata_inserted.get<ObjectIdType>(Tables::ID));

    int number_of_pkey = 0;
    boost::optional<ptree &> o_primary_keys =
        table_metadata_inserted.get_child_optional(Tables::PRIMARY_KEY_NODE);
    if (o_primary_keys) {
        ptree &p_primary_keys = o_primary_keys.value();
        BOOST_FOREACH (const ptree::value_type &node, p_primary_keys) {
            ptree primary_key = node.second;
            auto pkey_val = primary_key.get_value<int64_t>();

            EXPECT_EQ(pkey_val,
                      testdata_table_metadata.primary_keys[number_of_pkey]);
            number_of_pkey++;
        }
    }

    EXPECT_EQ(number_of_pkey, testdata_table_metadata.primary_keys.size());

    int number_of_columns = 0;
    BOOST_FOREACH (const ptree::value_type &node,
                   table_metadata_inserted.get_child(Tables::COLUMNS_NODE)) {
        ptree column = node.second;
        UTColumnMetadata testdata_column_metadata =
            testdata_table_metadata.columns[number_of_columns];

        boost::optional<ObjectIdType> id =
            column.get_optional<ObjectIdType>(Tables::Column::ID);

        EXPECT_GT(id.value(), 0);

        boost::optional<ObjectIdType> table_id =
            column.get_optional<ObjectIdType>(Tables::Column::TABLE_ID);

        EXPECT_GT(table_id.value(), 0);

        boost::optional<std::string> name =
            column.get_optional<std::string>(Tables::Column::NAME);

        EXPECT_EQ(name.value(), testdata_column_metadata.name);

        boost::optional<int64_t> ordinal_position =
            column.get_optional<int64_t>(Tables::Column::ORDINAL_POSITION);

        EXPECT_EQ(ordinal_position.value(),
                  testdata_column_metadata.ordinal_position);

        boost::optional<ObjectIdType> data_type_id =
            column.get_optional<ObjectIdType>(Tables::Column::DATA_TYPE_ID);

        EXPECT_EQ(data_type_id.value(), testdata_column_metadata.data_type_id);

        boost::optional<uint64_t> data_length =
            column.get_optional<uint64_t>(Tables::Column::DATA_LENGTH);
        if (data_length) {
            EXPECT_EQ(data_type_id.value(),
                      testdata_column_metadata.data_type_id);
        }

        boost::optional<bool> varying =
            column.get_optional<bool>(Tables::Column::VARYING);
        if (varying) {
            EXPECT_EQ(varying.value(), (bool)testdata_column_metadata.varying);
        }

        boost::optional<bool> nullable =
            column.get_optional<bool>(Tables::Column::NULLABLE);

        EXPECT_EQ(nullable.value(), testdata_column_metadata.nullable);

        boost::optional<std::string> default_expr =
            column.get_optional<std::string>(Tables::Column::DEFAULT);
        if (default_expr) {
            EXPECT_EQ(default_expr.value(),
                      testdata_column_metadata.default_expr);
        }

        boost::optional<int64_t> direction =
            column.get_optional<int64_t>(Tables::Column::DIRECTION);
        if (direction) {
            EXPECT_EQ(direction.value(), testdata_column_metadata.direction);
        }

        number_of_columns++;
    }

    EXPECT_EQ(number_of_columns, testdata_table_metadata.columns.size());
}

void ApiTestTableMetadata::add_table(const std::string &table_name,
                                     ObjectIdType *ret_table_id) {
    assert(ret_table_id != nullptr);

    UTTableMetadata *testdata_table_metadata =
        api_test_env->testdata_table_metadata.get();

    ptree new_table = testdata_table_metadata->tables;
    new_table.put(Tables::NAME, table_name);

    add_table(new_table, ret_table_id);
}

void ApiTestTableMetadata::add_table(ptree new_table,
                                     ObjectIdType *ret_table_id) {
    assert(ret_table_id != nullptr);

    auto tables = std::make_unique<Tables>(ApiTestEnvironment::TEST_DB);

    ErrorCode error = tables->init();
    EXPECT_EQ(ErrorCode::OK, error);

    error = tables->add(new_table, ret_table_id);
    EXPECT_EQ(ErrorCode::OK, error);
    EXPECT_GT(*ret_table_id, 0);

    UTUtils::print("new table id:", *ret_table_id);
    UTUtils::print(UTUtils::get_tree_string(new_table));
}

TEST_F(ApiTestTableMetadata, add_get_table_metadata_by_table_name) {
    UTTableMetadata testdata_table_metadata =
        *(api_test_env->testdata_table_metadata.get());

    testdata_table_metadata.name =
        testdata_table_metadata.name + "_ApiTestTableMetadata1";
    ObjectIdType ret_table_id = -1;
    ApiTestTableMetadata::add_table(testdata_table_metadata.name,
                                    &ret_table_id);
    testdata_table_metadata.id = ret_table_id;

    auto tables = std::make_unique<Tables>(ApiTestEnvironment::TEST_DB);
    ErrorCode error = tables->init();
    EXPECT_EQ(ErrorCode::OK, error);

    ptree table_metadata_inserted;
    error = tables->get(testdata_table_metadata.name, table_metadata_inserted);
    EXPECT_EQ(ErrorCode::OK, error);

    UTUtils::print(UTUtils::get_tree_string(table_metadata_inserted));

    ApiTestTableMetadata::check_table_metadata_expected(
        testdata_table_metadata, table_metadata_inserted);
}

TEST_F(ApiTestTableMetadata,
       add_get_table_metadata_without_primary_keys_by_table_name) {
    UTTableMetadata testdata_table_metadata =
        *(api_test_env->testdata_table_metadata_without_primary_keys);
    testdata_table_metadata.name =
        testdata_table_metadata.name + "_ApiTestTableMetadata2";

    ptree new_table = testdata_table_metadata.tables;

    UTUtils::print(UTUtils::get_tree_string(new_table));

    new_table.put(Tables::NAME, testdata_table_metadata.name);

    ObjectIdType ret_table_id = -1;
    add_table(new_table, &ret_table_id);

    testdata_table_metadata.id = ret_table_id;

    auto tables = std::make_unique<Tables>(ApiTestEnvironment::TEST_DB);
    ErrorCode error = tables->init();
    EXPECT_EQ(ErrorCode::OK, error);

    ptree table_metadata_inserted;
    error = tables->get(testdata_table_metadata.name, table_metadata_inserted);
    EXPECT_EQ(ErrorCode::OK, error);

    UTUtils::print(UTUtils::get_tree_string(table_metadata_inserted));

    ApiTestTableMetadata::check_table_metadata_expected(
        testdata_table_metadata, table_metadata_inserted);
}

TEST_F(ApiTestTableMetadata, get_table_metadata_by_table_id) {
    UTTableMetadata testdata_table_metadata =
        *(api_test_env->testdata_table_metadata.get());

    testdata_table_metadata.name =
        testdata_table_metadata.name + "_ApiTestTableMetadata3";

    ObjectIdType ret_table_id = -1;
    ApiTestTableMetadata::add_table(testdata_table_metadata.name,
                                    &ret_table_id);
    testdata_table_metadata.id = ret_table_id;

    auto tables = std::make_unique<Tables>(ApiTestEnvironment::TEST_DB);
    ErrorCode error = tables->init();
    EXPECT_EQ(ErrorCode::OK, error);

    ptree table_metadata_inserted;
    error = tables->get(ret_table_id, table_metadata_inserted);
    EXPECT_EQ(ErrorCode::OK, error);

    UTUtils::print(UTUtils::get_tree_string(table_metadata_inserted));

    ApiTestTableMetadata::check_table_metadata_expected(
        testdata_table_metadata, table_metadata_inserted);
}

TEST_F(ApiTestTableMetadata, add_and_get_table_metadata_without_initialized) {
    UTTableMetadata testdata_table_metadata =
        *(api_test_env->testdata_table_metadata.get());

    testdata_table_metadata.name =
        testdata_table_metadata.name + "_ApiTestTableMetadata4";
    testdata_table_metadata.tables.put(Tables::NAME,
                                       testdata_table_metadata.name);

    auto tables_add = std::make_unique<Tables>(ApiTestEnvironment::TEST_DB);

    ObjectIdType ret_table_id = -1;
    ErrorCode error =
        tables_add->add(testdata_table_metadata.tables, &ret_table_id);
    testdata_table_metadata.id = ret_table_id;
    EXPECT_EQ(ErrorCode::OK, error);
    EXPECT_GT(ret_table_id, 0);

    auto tables_get_by_id =
        std::make_unique<Tables>(ApiTestEnvironment::TEST_DB);

    ptree table_metadata_inserted_by_id;
    error = tables_get_by_id->get(ret_table_id, table_metadata_inserted_by_id);
    EXPECT_EQ(ErrorCode::OK, error);

    UTUtils::print(UTUtils::get_tree_string(table_metadata_inserted_by_id));

    ApiTestTableMetadata::check_table_metadata_expected(
        testdata_table_metadata, table_metadata_inserted_by_id);

    auto tables_get_by_name =
        std::make_unique<Tables>(ApiTestEnvironment::TEST_DB);

    ptree table_metadata_inserted_by_name;
    error = tables_get_by_name->get(testdata_table_metadata.name,
                                    table_metadata_inserted_by_name);
    EXPECT_EQ(ErrorCode::OK, error);

    UTUtils::print(UTUtils::get_tree_string(table_metadata_inserted_by_name));

    ApiTestTableMetadata::check_table_metadata_expected(
        testdata_table_metadata, table_metadata_inserted_by_name);
}

TEST_F(ApiTestTableMetadata, remove_table_metadata_by_table_name) {
    UTTableMetadata testdata_table_metadata =
        *(api_test_env->testdata_table_metadata.get());

    testdata_table_metadata.name =
        testdata_table_metadata.name + "_ApiTestTableMetadata5";
    ObjectIdType ret_table_id = -1;
    ApiTestTableMetadata::add_table(testdata_table_metadata.name,
                                    &ret_table_id);
    testdata_table_metadata.id = ret_table_id;

    auto tables = std::make_unique<Tables>(ApiTestEnvironment::TEST_DB);
    ErrorCode error = tables->init();
    EXPECT_EQ(ErrorCode::OK, error);

    ObjectIdType table_id_to_remove = -1;
    error = tables->remove(testdata_table_metadata.name.c_str(),
                           &table_id_to_remove);
    EXPECT_EQ(ErrorCode::OK, error);
    EXPECT_EQ(ret_table_id, table_id_to_remove);

    ptree table_metadata_got;
    error = tables->get(table_id_to_remove, table_metadata_got);
    EXPECT_EQ(ErrorCode::INVALID_PARAMETER, error);

    UTUtils::print(UTUtils::get_tree_string(table_metadata_got));
}

TEST_F(ApiTestTableMetadata, remove_table_metadata_by_table_id) {
    UTTableMetadata testdata_table_metadata =
        *(api_test_env->testdata_table_metadata.get());

    testdata_table_metadata.name =
        testdata_table_metadata.name + "_ApiTestTableMetadata6";
    ObjectIdType ret_table_id = -1;
    ApiTestTableMetadata::add_table(testdata_table_metadata.name,
                                    &ret_table_id);
    testdata_table_metadata.id = ret_table_id;

    auto tables = std::make_unique<Tables>(ApiTestEnvironment::TEST_DB);
    ErrorCode error = tables->init();
    EXPECT_EQ(ErrorCode::OK, error);

    error = tables->remove(ret_table_id);
    EXPECT_EQ(ErrorCode::OK, error);

    ptree table_metadata_got;
    error = tables->get(ret_table_id, table_metadata_got);
    EXPECT_EQ(ErrorCode::INVALID_PARAMETER, error);

    UTUtils::print(UTUtils::get_tree_string(table_metadata_got));
}

}  // namespace manager::metadata::testing
