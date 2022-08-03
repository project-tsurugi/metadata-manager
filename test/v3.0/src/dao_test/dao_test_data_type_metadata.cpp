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

#include <boost/property_tree/ptree.hpp>
#include <memory>
#include <string>
#include <tuple>
#include <vector>

#include "manager/metadata/dao/datatypes_dao.h"
#include "manager/metadata/dao/db_session_manager.h"
#include "manager/metadata/dao/generic_dao.h"
#include "manager/metadata/error_code.h"

#include "test/api_test_data_types.h"
#include "test/global_test_environment.h"
#include "test/utility/ut_utils.h"

using namespace manager::metadata::db;
using namespace boost::property_tree;

namespace manager::metadata::testing {

class DaoTestDataTypesByKeyValue
    : public ::testing::TestWithParam<TupleApiTestDataTypes> {
    void SetUp() override { UTUtils::skip_if_connection_not_opened(); }
};
/**
 * @brief Happy test for getting all data type metadatas based on data type
 * key/value pair.
 */
TEST_P(DaoTestDataTypesByKeyValue, get_datatypes_by_key_value) {
    auto param = GetParam();
    std::string key = std::get<0>(param);
    std::string value = std::get<1>(param);

    std::shared_ptr<GenericDAO> d_gdao = nullptr;
    DBSessionManager db_session_manager;

    ErrorCode error =
        db_session_manager.get_dao(GenericDAO::TableName::DATATYPES, d_gdao);
    EXPECT_EQ(ErrorCode::OK, error);

    std::shared_ptr<DataTypesDAO> ddao =
        std::static_pointer_cast<DataTypesDAO>(d_gdao);

    ptree datatype;
    error = ddao->select_one_data_type_metadata(key, value, datatype);
    EXPECT_EQ(ErrorCode::OK, error);

    UTUtils::print("-- get data type metadata --");
    UTUtils::print(UTUtils::get_tree_string(datatype));

    // Verifies that returned data type metadata equals expected one.
    ApiTestDataTypes::check_datatype_metadata_expected(datatype);
}

/**
 * @brief Exception path test for getting non-existing data type metadatas
 * based on invalid data type key/value pair.
 */
TEST_F(DaoTestDataTypesByKeyValue, get_non_existing_datatypes_by_key_value) {
    std::shared_ptr<GenericDAO> d_gdao = nullptr;

    DBSessionManager db_session_manager;
    ErrorCode error =
        db_session_manager.get_dao(GenericDAO::TableName::DATATYPES, d_gdao);
    EXPECT_EQ(ErrorCode::OK, error);

    std::shared_ptr<DataTypesDAO> ddao =
        std::static_pointer_cast<DataTypesDAO>(d_gdao);

    std::string key = "invalid_key";
    std::string value = "INT32";

    ptree datatype;
    error = ddao->select_one_data_type_metadata(key.c_str(), value, datatype);

    EXPECT_EQ(ErrorCode::INVALID_PARAMETER, error);

    // Verifies that returned data type metadata equals expected one.
    ptree empty_ptree;
    EXPECT_EQ(UTUtils::get_tree_string(empty_ptree),
              UTUtils::get_tree_string(datatype));
}

INSTANTIATE_TEST_CASE_P(
    ParamtererizedTest, DaoTestDataTypesByKeyValue,
    ::testing::ValuesIn(ApiTestDataTypes::make_datatypes_tuple()));

}  // namespace manager::metadata::testing
