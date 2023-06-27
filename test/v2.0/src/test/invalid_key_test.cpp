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
#include "test/test/invalid_key_test.h"

#include <gtest/gtest.h>
#include <boost/format.hpp>
#include <boost/property_tree/ptree.hpp>

#include "manager/metadata/datatypes.h"
#include "manager/metadata/statistics.h"
#include "manager/metadata/tables.h"
#include "test/common/global_test_environment.h"
#include "test/common/ut_utils.h"
#include "test/helper/api_test_helper.h"
#include "test/helper/column_statistics_helper.h"
#include "test/metadata/ut_column_statistics.h"

namespace manager::metadata::testing {

namespace {

std::vector<ObjectId> not_exists_id = {INT64_MAX - 1, INT64_MAX};

std::vector<std::string> not_exists_name = {"metadata_name_not_exists"};

std::vector<ObjectId> invalid_id = {
    -1, 0, -std::numeric_limits<ObjectIdType>::infinity(),
    std::numeric_limits<ObjectIdType>::quiet_NaN()};

std::vector<std::string> invalid_name = {""};

}  // namespace

using boost::property_tree::ptree;

class TestById : public ::testing::TestWithParam<
                     std::tuple<invalid_test::InvalidTestParamId, ObjectId>> {
  void SetUp() override { UTUtils::skip_if_connection_not_opened(); }
};
class TestByName
    : public ::testing::TestWithParam<
          std::tuple<invalid_test::InvalidTestParamName, std::string>> {
  void SetUp() override { UTUtils::skip_if_connection_not_opened(); }
};

INSTANTIATE_TEST_CASE_P(
    NotExistsTest, TestById,
    ::testing::Combine(::testing::ValuesIn(invalid_test::test_by_id),
                       ::testing::ValuesIn(not_exists_id)));
INSTANTIATE_TEST_CASE_P(
    NotExistsTest, TestByName,
    ::testing::Combine(::testing::ValuesIn(invalid_test::test_by_name),
                       ::testing::ValuesIn(not_exists_name)));
INSTANTIATE_TEST_CASE_P(
    InvalidValueTest, TestById,
    ::testing::Combine(::testing::ValuesIn(invalid_test::test_by_invalid_id),
                       ::testing::ValuesIn(invalid_id)));
INSTANTIATE_TEST_CASE_P(
    InvalidValueTest, TestByName,
    ::testing::Combine(::testing::ValuesIn(invalid_test::test_by_invalid_name),
                       ::testing::ValuesIn(invalid_name)));

/**
 * @brief Test when an invalid ID is specified in the APIs.
 */
TEST_P(TestById, test_apis_by_id) {
  auto [metadata_test, get_expected, update_expected, remove_expected] =
      std::get<0>(GetParam());
  auto object_id = std::get<1>(GetParam());

  if (metadata_test->is_test_skip()) {
    GTEST_SKIP();
  }

  auto manager_sptr = metadata_test->get_metadata_manager();
  auto manager      = manager_sptr.get();
  ASSERT_TRUE(manager);

  ptree retrieve_metadata;
  auto class_name = typeid(*metadata_test->get_metadata_manager()).name();
  auto log_format = boost::format(">> Invalid key test: %1%::%2%(%3%)");

  log_format.clear_binds();
  UTUtils::print(log_format % class_name % "get" % object_id);
  {
    CALL_TRACE;
    ErrorCode expected =
        ((get_expected == ErrorCode::UNKNOWN) || (object_id <= 0)
             ? get_expected
             : ErrorCode::ID_NOT_FOUND);
    ApiTestHelper::test_get(manager, object_id, expected, retrieve_metadata);
  }

  log_format.clear_binds();
  UTUtils::print(log_format % class_name % "exists" % object_id);
  {
    CALL_TRACE;
    ApiTestHelper::test_exists(manager, object_id, false);
  }

  log_format.clear_binds();
  UTUtils::print(log_format % class_name % "update" % object_id);
  {
    CALL_TRACE;
    auto updated_metadata =
        metadata_test->get_test_metadata(0)->get_metadata_ptree();
    ErrorCode expected =
        ((update_expected == ErrorCode::UNKNOWN) || (object_id <= 0)
             ? update_expected
             : ErrorCode::ID_NOT_FOUND);
    ApiTestHelper::test_update(manager, object_id, updated_metadata, expected);
  }

  log_format.clear_binds();
  UTUtils::print(log_format % class_name % "remove" % object_id);
  {
    CALL_TRACE;
    ErrorCode expected =
        ((remove_expected == ErrorCode::UNKNOWN) || (object_id <= 0)
             ? remove_expected
             : ErrorCode::ID_NOT_FOUND);
    ApiTestHelper::test_remove(manager, object_id, expected);
  }
}

/**
 * @brief Test when an invalid name is specified in the APIs.
 */
TEST_P(TestByName, test_apis_by_name) {
  auto [metadata_test, get_expected, remove_expected] = std::get<0>(GetParam());
  auto object_name                                    = std::get<1>(GetParam());

  if (metadata_test->is_test_skip()) {
    GTEST_SKIP();
  }

  auto manager_sptr = metadata_test->get_metadata_manager();
  auto manager      = manager_sptr.get();
  ASSERT_TRUE(manager);

  ptree retrieve_metadata;
  auto class_name = typeid(*metadata_test->get_metadata_manager()).name();
  auto log_format = boost::format(">> Invalid key test: %1%::%2%(%3%)");

  log_format.clear_binds();
  UTUtils::print(log_format % class_name % "get" % object_name);
  {
    CALL_TRACE;
    ApiTestHelper::test_get(manager, object_name, get_expected,
                            retrieve_metadata);
  }

  log_format.clear_binds();
  UTUtils::print(log_format % class_name % "exists" % object_name);
  {
    CALL_TRACE;
    ApiTestHelper::test_exists(manager, object_name, false);
  }

  log_format.clear_binds();
  UTUtils::print(log_format % class_name % "remove" % object_name);
  {
    CALL_TRACE;
    ApiTestHelper::test_remove(manager, object_name, remove_expected);
  }
}

}  // namespace manager::metadata::testing
