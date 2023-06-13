/*
 * Copyright 2020-2021 tsurugi project.
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
#include <tuple>

#include "manager/metadata/metadata_factory.h"
#include "test/common/global_test_environment.h"
#include "test/common/ut_utils.h"
#include "test/helper/metadata_helper.h"
#include "test/metadata/ut_datatypes_metadata.h"
#include "test/helper/api_test_helper.h"

namespace manager::metadata::testing {

using boost::property_tree::ptree;
using BasicTestParameter = std::tuple<std::string, std::string>;

/**
 * @brief internal function of make_datatypes_tuple.
 * @param (key)      [in]  a data types key.
 * @param (values)   [in]  data types values.
 * @param (v)        [out] a list of key/value pair about data types metadata.
 * @return none.
 */
static void make_datatypes_tuple(std::string_view key,
                                 const std::vector<std::string> values,
                                 std::vector<BasicTestParameter>& v) {
  for (auto value : values) {
    v.emplace_back(key, value);
  }
}

/**
 * @brief Make a list of key/value pair about data types metadata.
 * @return a list of key/value pair about data types metadata.
 *   For example, if key = DataTypes::NAME, values are "INT32", "INT64", and
 *   "FLOAT32" etc.
 */
static std::vector<BasicTestParameter> make_datatypes_tuple() {
  std::vector<BasicTestParameter> v;

  make_datatypes_tuple(DataTypes::ID, UtDataTypesMetadata().get_datatype_ids(),
                       v);
  make_datatypes_tuple(DataTypes::NAME,
                       UtDataTypesMetadata().get_datatype_names(), v);
  make_datatypes_tuple(DataTypes::PG_DATA_TYPE,
                       UtDataTypesMetadata().get_pg_datatype_ids(), v);
  make_datatypes_tuple(DataTypes::PG_DATA_TYPE_NAME,
                       UtDataTypesMetadata().get_pg_datatype_names(), v);
  make_datatypes_tuple(DataTypes::PG_DATA_TYPE_QUALIFIED_NAME,
                       UtDataTypesMetadata().get_pg_datatype_qualified_names(),
                       v);
  return v;
}

/**
 * @brief Make a list of list about data type names.
 * @return data type names.
 */
static std::vector<std::string> make_datatype_names() {
  return UtDataTypesMetadata().get_datatype_names();
}

class ApiTestDataTypes : public ::testing::Test {
 public:
  void SetUp() override { UTUtils::skip_if_connection_not_opened(); }
};
class ApiTestDataTypesByKeyValue
    : public ::testing::TestWithParam<BasicTestParameter> {
  void SetUp() override { UTUtils::skip_if_connection_not_opened(); }
};
class ApiTestDataTypesByName : public ::testing::TestWithParam<std::string> {
  void SetUp() override { UTUtils::skip_if_connection_not_opened(); }
};
class ApiTestDataTypesException
    : public ::testing::TestWithParam<std::tuple<std::string, std::string>> {
  void SetUp() override { UTUtils::skip_if_connection_not_opened(); }
};

INSTANTIATE_TEST_CASE_P(ParameterizedTest, ApiTestDataTypesByName,
                        ::testing::ValuesIn(make_datatype_names()));
INSTANTIATE_TEST_CASE_P(ParameterizedTest, ApiTestDataTypesByKeyValue,
                        ::testing::ValuesIn(make_datatypes_tuple()));
INSTANTIATE_TEST_CASE_P(
    ParameterizedTest, ApiTestDataTypesException,
    ::testing::Values(std::make_tuple("", ""),
                      std::make_tuple("", "invalid_value"),
                      std::make_tuple("invalid_key", ""),
                      std::make_tuple("invalid_key", "invalid_value"),
                      std::make_tuple(DataTypes::ID, ""),
                      std::make_tuple(DataTypes::ID, "invalid_value"),
                      std::make_tuple(DataTypes::NAME, ""),
                      std::make_tuple(DataTypes::NAME, "invalid_value")));

/**
 * @brief Test to init datatype metadata.
 */
TEST_F(ApiTestDataTypes, test_init) {
  CALL_TRACE;

  auto manager = get_datatypes_ptr(GlobalTestEnvironment::TEST_DB);

  // Execute the test.
  ApiTestHelper::test_init(manager.get(), ErrorCode::OK);
}

/**
 * @brief Test to add datatype metadata.
 */
TEST_F(ApiTestDataTypes, test_add) {
  CALL_TRACE;

  auto manager = get_datatypes_ptr(GlobalTestEnvironment::TEST_DB);

  ptree inserted_metadata;
  // Execute the test.
  ApiTestHelper::test_add(manager.get(), inserted_metadata, ErrorCode::UNKNOWN);
}

/**
 * @brief Test to get all it in ptree type.
 */
TEST_F(ApiTestDataTypes, test_getall) {
  CALL_TRACE;

  auto manager = get_datatypes_ptr(GlobalTestEnvironment::TEST_DB);

  std::vector<ptree> container = {};
  // Execute the test.
  ApiTestHelper::test_getall(manager.get(), ErrorCode::UNKNOWN, container);
  EXPECT_TRUE(container.empty());
}

/**
 * @brief Test to update it with object name as key.
 */
TEST_F(ApiTestDataTypes, test_update) {
  CALL_TRACE;

  auto manager = get_datatypes_ptr(GlobalTestEnvironment::TEST_DB);

  ptree updated_metadata;
  // Execute the test.
  ApiTestHelper::test_update(manager.get(), INT64_MAX, updated_metadata, ErrorCode::UNKNOWN);
}

/**
 * @brief Test to remove it with object ID as key.
 */
TEST_F(ApiTestDataTypes, test_remove_by_id) {
  CALL_TRACE;

  auto manager = get_datatypes_ptr(GlobalTestEnvironment::TEST_DB);

  // Execute the test.
  ApiTestHelper::test_remove(manager.get(), INT64_MAX, ErrorCode::UNKNOWN);
}

/**
 * @brief Test to remove it with object name as key.
 */
TEST_F(ApiTestDataTypes, test_remove_by_name) {
  CALL_TRACE;

  auto manager = get_datatypes_ptr(GlobalTestEnvironment::TEST_DB);

  // Execute the test.
  ApiTestHelper::test_remove(manager.get(), "INT32", ErrorCode::UNKNOWN);
}

/**
 * @brief Happy test for getting all data type metadata based on data type
 * name.
 */
TEST_P(ApiTestDataTypesByName, get_datatypes_by_name) {
  auto datatypes = get_datatypes_ptr(GlobalTestEnvironment::TEST_DB);

  auto param = GetParam();
  ptree datatype;

  ErrorCode error = datatypes->get(param, datatype);
  EXPECT_EQ(ErrorCode::OK, error);

  UTUtils::print("-- get data type metadata --");
  UTUtils::print(UTUtils::get_tree_string(datatype));

  // Verifies that returned data type metadata equals expected one.
  UtDataTypesMetadata().check_metadata_expected(datatype, __FILE__, __LINE__);
}

/**
 * @brief Happy test for getting all data type metadata based on data type
 * key/value pair.
 */
TEST_P(ApiTestDataTypesByKeyValue, get_datatypes_by_key_value) {
  // TODO(future): Change when changing Metadata class.
  auto datatypes_temp = get_datatypes_ptr(GlobalTestEnvironment::TEST_DB);
  auto datatypes = static_cast<DataTypes*>(datatypes_temp.get());

  auto param        = GetParam();
  std::string key   = std::get<0>(param);
  std::string value = std::get<1>(param);

  ptree datatype;
  ErrorCode error = datatypes->get(key, value, datatype);
  EXPECT_EQ(ErrorCode::OK, error);

  UTUtils::print("-- get data type metadata --");
  UTUtils::print(UTUtils::get_tree_string(datatype));

  // Verifies that returned data type metadata equals expected one.
  UtDataTypesMetadata().check_metadata_expected(datatype, __FILE__, __LINE__);
}

/**
 * @brief Exception path test for getting non-existing data type metadata
 * based on invalid data type name.
 */
TEST_P(ApiTestDataTypesException, get_non_existing_datatypes_by_name) {
  auto datatypes = get_datatypes_ptr(GlobalTestEnvironment::TEST_DB);

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
  // TODO(future): Change when changing Metadata class.
  auto datatypes_temp = get_datatypes_ptr(GlobalTestEnvironment::TEST_DB);
  auto datatypes = static_cast<DataTypes*>(datatypes_temp.get());

  std::string key   = std::get<0>(GetParam());
  std::string value = std::get<1>(GetParam());

  ptree datatype;
  ErrorCode error = datatypes->get(key, value, datatype);
  if (key == DataTypes::ID) {
#ifdef STORAGE_POSTGRESQL
    if (value == "invalid_value") {
      EXPECT_EQ(ErrorCode::INVALID_PARAMETER, error);
    } else {
      EXPECT_EQ(ErrorCode::ID_NOT_FOUND, error);
    }
#else
    EXPECT_EQ(ErrorCode::ID_NOT_FOUND, error);
#endif
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
