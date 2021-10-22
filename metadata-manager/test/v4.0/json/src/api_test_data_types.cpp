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
#include <gtest/gtest.h>

#include <boost/property_tree/ptree.hpp>
#include <memory>
#include <string>
#include <tuple>

#include "manager/metadata/datatypes.h"
#include "test/global_test_environment.h"
#include "test/helper/data_types_helper.h"
#include "test/utility/ut_utils.h"

namespace manager::metadata::testing {

using boost::property_tree::ptree;

class ApiTestDataTypesByKeyValue
    : public ::testing::TestWithParam<TestDatatypesType> {};

class ApiTestDataTypesByName : public ::testing::TestWithParam<std::string> {};

class ApiTestDataTypesException
    : public ::testing::TestWithParam<std::tuple<std::string, std::string>> {};

INSTANTIATE_TEST_CASE_P(
    ParamtererizedTest, ApiTestDataTypesByName,
    ::testing::ValuesIn(DataTypesHelper::make_datatype_names()));

INSTANTIATE_TEST_CASE_P(
    ParamtererizedTest, ApiTestDataTypesByKeyValue,
    ::testing::ValuesIn(DataTypesHelper::make_datatypes_tuple()));

INSTANTIATE_TEST_CASE_P(
    ParamtererizedTest, ApiTestDataTypesException,
    ::testing::Values(std::make_tuple("", ""),
                      std::make_tuple("", "invalid_value"),
                      std::make_tuple("invalid_key", ""),
                      std::make_tuple("invalid_key", "invalid_value"),
                      std::make_tuple(DataTypes::ID, ""),
                      std::make_tuple(DataTypes::ID, "invalid_value"),
                      std::make_tuple(DataTypes::NAME, ""),
                      std::make_tuple(DataTypes::NAME, "invalid_value")));

/**
 * @brief Happy test for getting all data type metadata based on data type
 * name.
 */
TEST_P(ApiTestDataTypesByName, get_datatypes_by_name) {
  auto datatypes = std::make_unique<DataTypes>(GlobalTestEnvironment::TEST_DB);

  auto param = GetParam();
  ptree datatype;

  ErrorCode error = datatypes->get(param, datatype);
  EXPECT_EQ(ErrorCode::OK, error);

  UTUtils::print("-- get data type metadata --");
  UTUtils::print(UTUtils::get_tree_string(datatype));

  // Verifies that returned data type metadata equals expected one.
  DataTypesHelper::check_datatype_metadata_expected(datatype);
}

/**
 * @brief Happy test for getting all data type metadata based on data type
 * key/value pair.
 */
TEST_P(ApiTestDataTypesByKeyValue, get_datatypes_by_key_value) {
  auto datatypes = std::make_unique<DataTypes>(GlobalTestEnvironment::TEST_DB);

  auto param = GetParam();
  std::string key = std::get<0>(param);
  std::string value = std::get<1>(param);

  ptree datatype;
  ErrorCode error = datatypes->get(key.c_str(), value, datatype);
  EXPECT_EQ(ErrorCode::OK, error);

  UTUtils::print("-- get data type metadata --");
  UTUtils::print(UTUtils::get_tree_string(datatype));

  // Verifies that returned data type metadata equals expected one.
  DataTypesHelper::check_datatype_metadata_expected(datatype);
}

/**
 * @brief Exception path test for getting non-existing data type metadata
 * based on invalid data type name.
 */
TEST_P(ApiTestDataTypesException, get_non_existing_datatypes_by_name) {
  auto datatypes = std::make_unique<DataTypes>(GlobalTestEnvironment::TEST_DB);

  std::string value = std::get<0>(GetParam());

  ptree datatype;
  ErrorCode error = datatypes->get(value, datatype);
  EXPECT_EQ(ErrorCode::NAME_NOT_FOUND, error);

  // Verifies that returned data type metadata equals expected one.
  ptree empty_ptree;
  EXPECT_EQ(UTUtils::get_tree_string(empty_ptree),
            UTUtils::get_tree_string(datatype));
}

/**
 * @brief Exception path test for getting non-existing data type metadata
 * based on invalid data type key/value pair.
 */
TEST_P(ApiTestDataTypesException, get_non_existing_datatypes_by_key_value) {
  auto datatypes = std::make_unique<DataTypes>(GlobalTestEnvironment::TEST_DB);

  std::string key = std::get<0>(GetParam());
  std::string value = std::get<1>(GetParam());

  ptree datatype;
  ErrorCode error = datatypes->get(key.c_str(), value, datatype);
  if (key == DataTypes::ID) {
    EXPECT_EQ(ErrorCode::ID_NOT_FOUND, error);
  } else if (key == DataTypes::NAME) {
    EXPECT_EQ(ErrorCode::NAME_NOT_FOUND, error);
  } else if (!key.empty() && value.empty()) {
    EXPECT_EQ(ErrorCode::NOT_FOUND, error);
  } else {
    EXPECT_EQ(ErrorCode::INVALID_PARAMETER, error);
  }

  // Verifies that returned data type metadata equals expected one.
  ptree empty_ptree;
  EXPECT_EQ(UTUtils::get_tree_string(empty_ptree),
            UTUtils::get_tree_string(datatype));
}

}  // namespace manager::metadata::testing
