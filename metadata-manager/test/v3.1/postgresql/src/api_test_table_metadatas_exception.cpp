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

#include <gtest/gtest.h>
#include <boost/foreach.hpp>
#include <boost/property_tree/json_parser.hpp>
#include <iostream>
#include <memory>
#include <string>

#include "manager/metadata/error_code.h"
#include "manager/metadata/metadata.h"
#include "manager/metadata/tables.h"

#include "test/api_test_table_metadatas.h"
#include "test/utility/ut_utils.h"

using namespace manager::metadata;
using namespace boost::property_tree;

namespace manager::metadata::testing {

class ApiTestAddTableMetadataException
    : public ::testing::TestWithParam<boost::property_tree::ptree> {
   public:
    void SetUp() override {
        if (!global->is_open()) {
            GTEST_SKIP_("metadata repository is not started.");
        } else {
            invalid_table_metadatas = make_invalid_table_metadatas();
        }
    }
    static std::vector<boost::property_tree::ptree>
    make_invalid_table_metadatas();

   protected:
    /**
     * @brief invalid table metadatas used as test datas.
     */
    std::vector<boost::property_tree::ptree> invalid_table_metadatas;
};

class ApiTestTableMetadataByTableIdException
    : public ::testing::TestWithParam<ObjectIdType> {
    void SetUp() override { UTUtils::skip_if_connection_not_opened(); }
};

class ApiTestTableMetadataByTableNameException
    : public ::testing::TestWithParam<std::string> {
    void SetUp() override { UTUtils::skip_if_connection_not_opened(); }
};

INSTANTIATE_TEST_CASE_P(
    ParamtererizedTest, ApiTestTableMetadataByTableIdException,
    ::testing::Values(-1, 0, INT64_MAX - 1, INT64_MAX,
                      std::numeric_limits<ObjectIdType>::infinity(),
                      -std::numeric_limits<ObjectIdType>::infinity(),
                      std::numeric_limits<ObjectIdType>::quiet_NaN()));

INSTANTIATE_TEST_CASE_P(ParamtererizedTest,
                        ApiTestTableMetadataByTableNameException,
                        ::testing::Values("table_name_not_exists", ""));

/**
 *  @brief  Make invalid table metadatas used as test datas.
 */
std::vector<ptree>
ApiTestAddTableMetadataException::make_invalid_table_metadatas() {
    std::vector<ptree> invalid_table_metadatas;

    // empty ptree
    ptree empty_table;
    invalid_table_metadatas.emplace_back(empty_table);

    // remove table name
    ptree new_table = global->testdata_table_metadata->tables;
    new_table.erase(Tables::NAME);
    invalid_table_metadatas.emplace_back(new_table);

    // remove all column name
    new_table = global->testdata_table_metadata->tables;
    BOOST_FOREACH (ptree::value_type &node,
                   new_table.get_child(Tables::COLUMNS_NODE)) {
        ptree &column = node.second;
        column.erase(Tables::Column::NAME);
    }
    invalid_table_metadatas.emplace_back(new_table);

    // remove all ordinal position
    new_table = global->testdata_table_metadata->tables;
    BOOST_FOREACH (ptree::value_type &node,
                   new_table.get_child(Tables::COLUMNS_NODE)) {
        ptree &column = node.second;
        column.erase(Tables::Column::ORDINAL_POSITION);
    }
    invalid_table_metadatas.emplace_back(new_table);

    // remove all data type id
    new_table = global->testdata_table_metadata->tables;
    BOOST_FOREACH (ptree::value_type &node,
                   new_table.get_child(Tables::COLUMNS_NODE)) {
        ptree &column = node.second;
        column.erase(Tables::Column::DATA_TYPE_ID);
    }
    invalid_table_metadatas.emplace_back(new_table);

    // add invalid data type id
    BOOST_FOREACH (ptree::value_type &node,
                   new_table.get_child(Tables::COLUMNS_NODE)) {
        ptree &column = node.second;
        int invalid_data_type_id = -1;
        column.put(Tables::Column::DATA_TYPE_ID, invalid_data_type_id);
    }
    invalid_table_metadatas.emplace_back(new_table);

    // remove all not null constraint
    new_table = global->testdata_table_metadata->tables;
    BOOST_FOREACH (ptree::value_type &node,
                   new_table.get_child(Tables::COLUMNS_NODE)) {
        ptree &column = node.second;
        column.erase(Tables::Column::NULLABLE);
    }
    invalid_table_metadatas.emplace_back(new_table);

    return invalid_table_metadatas;
}

/**
 *  @brief  Add invalid table metadatas.
 */
TEST_F(ApiTestAddTableMetadataException, add_table_metadata) {
    auto tables = std::make_unique<Tables>(GlobalTestEnvironment::TEST_DB);

    ErrorCode error = tables->init();
    EXPECT_EQ(ErrorCode::OK, error);

    for (auto invalid_table : invalid_table_metadatas) {
        UTUtils::print("-- add invalid table metadata --");
        UTUtils::print(UTUtils::get_tree_string(invalid_table));

        ObjectIdType ret_table_id = -1;
        error = tables->add(invalid_table, &ret_table_id);
        EXPECT_EQ(ErrorCode::INVALID_PARAMETER, error);
        EXPECT_EQ(ret_table_id, -1);
    }
}

/**
 * @brief Exception path test for getting table metadata
 * based on non-existing table id.
 */
TEST_P(ApiTestTableMetadataByTableIdException,
       get_table_metadata_by_non_existing_table_id) {
    auto tables = std::make_unique<Tables>(GlobalTestEnvironment::TEST_DB);

    ErrorCode error = tables->init();
    EXPECT_EQ(ErrorCode::OK, error);

    ptree table;
    error = tables->get(GetParam(), table);
    EXPECT_EQ(ErrorCode::ID_NOT_FOUND, error);
}

/**
 * @brief Exception path test for getting table metadata
 * based on non-existing table name.
 */
TEST_P(ApiTestTableMetadataByTableNameException,
       get_table_metadata_by_non_existing_table_name) {
    auto tables = std::make_unique<Tables>(GlobalTestEnvironment::TEST_DB);

    ErrorCode error = tables->init();
    EXPECT_EQ(ErrorCode::OK, error);

    ptree table;
    error = tables->get(GetParam(), table);
    EXPECT_EQ(ErrorCode::NAME_NOT_FOUND, error);
}

/**
 * @brief Exception path test for removing table metadata
 * based on non-existing table id.
 */
TEST_P(ApiTestTableMetadataByTableIdException,
       remove_table_metadata_by_non_existing_table_id) {
    auto tables = std::make_unique<Tables>(GlobalTestEnvironment::TEST_DB);

    ErrorCode error = tables->init();
    EXPECT_EQ(ErrorCode::OK, error);

    error = tables->remove(GetParam());
    EXPECT_EQ(ErrorCode::ID_NOT_FOUND, error);
}

/**
 * @brief Exception path test for removing table metadata
 * based on non-existing table name.
 */
TEST_P(ApiTestTableMetadataByTableNameException,
       remove_table_metadata_by_non_existing_table_name) {
    auto tables = std::make_unique<Tables>(GlobalTestEnvironment::TEST_DB);

    ErrorCode error = tables->init();
    EXPECT_EQ(ErrorCode::OK, error);

    ObjectIdType ret_table_id = -1;
    error = tables->remove(GetParam().c_str(), &ret_table_id);
    EXPECT_EQ(ErrorCode::NAME_NOT_FOUND, error);
    EXPECT_EQ(-1, ret_table_id);
}

}  // namespace manager::metadata::testing
