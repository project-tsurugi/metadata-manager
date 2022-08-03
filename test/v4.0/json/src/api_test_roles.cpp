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

#include <boost/format.hpp>
#include <memory>
#include <string>

#include "manager/metadata/roles.h"
#include "test/global_test_environment.h"
#include "test/utility/ut_utils.h"

namespace manager::metadata::testing {

using boost::property_tree::ptree;

class ApiTestRolesMetadata : public ::testing::Test {};

/**
 * @brief Unsupported test in JSON version.
 */
TEST_F(ApiTestRolesMetadata, get_role) {
  ErrorCode error = ErrorCode::UNKNOWN;

  auto roles = std::make_unique<Roles>(GlobalTestEnvironment::TEST_DB);
  error = roles->init();
  EXPECT_EQ(ErrorCode::NOT_SUPPORTED, error);

  ptree role_metadata;

  UTUtils::print("-- get role metadata by role id --");
  // test getting by role id.
  error = roles->get(9999, role_metadata);
  EXPECT_EQ(ErrorCode::NOT_SUPPORTED, error);

  UTUtils::print("-- get role metadata by role name --");
  // test getting by role name.
  error = roles->get("role_name", role_metadata);
  EXPECT_EQ(ErrorCode::NOT_SUPPORTED, error);
}

/**
 * @brief api test for add role metadata.
 */
TEST_F(ApiTestRolesMetadata, add_role_metadata) {
  ErrorCode error = ErrorCode::UNKNOWN;

  auto roles = std::make_unique<Roles>(GlobalTestEnvironment::TEST_DB);
  error = roles->init();
  EXPECT_EQ(ErrorCode::NOT_SUPPORTED, error);

  ptree role_metadata;

  error = roles->add(role_metadata);
  EXPECT_EQ(ErrorCode::UNKNOWN, error);

  ObjectIdType retval_role_id = -1;
  error = roles->add(role_metadata, &retval_role_id);
  EXPECT_EQ(ErrorCode::UNKNOWN, error);
  EXPECT_EQ(-1, retval_role_id);
}

/**
 * @brief api test for get_all role metadata.
 */
TEST_F(ApiTestRolesMetadata, get_all_role_metadata) {
  ErrorCode error = ErrorCode::UNKNOWN;

  auto roles = std::make_unique<Roles>(GlobalTestEnvironment::TEST_DB);
  error = roles->init();
  EXPECT_EQ(ErrorCode::NOT_SUPPORTED, error);

  std::vector<boost::property_tree::ptree> container = {};
  error = roles->get_all(container);
  EXPECT_EQ(ErrorCode::UNKNOWN, error);
  EXPECT_TRUE(container.empty());
}

/**
 * @brief api test for remove role metadata.
 */
TEST_F(ApiTestRolesMetadata, remove_role_metadata) {
  ErrorCode error = ErrorCode::UNKNOWN;

  auto roles = std::make_unique<Roles>(GlobalTestEnvironment::TEST_DB);
  error = roles->init();
  EXPECT_EQ(ErrorCode::NOT_SUPPORTED, error);

  error = roles->remove(99999);
  EXPECT_EQ(ErrorCode::UNKNOWN, error);

  ObjectIdType retval_role_id = -1;
  error = roles->remove("role_name", &retval_role_id);
  EXPECT_EQ(ErrorCode::UNKNOWN, error);
  EXPECT_EQ(-1, retval_role_id);
}

}  // namespace manager::metadata::testing
