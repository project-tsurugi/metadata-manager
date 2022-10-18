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

#include <memory>
#include <string>

#include <boost/property_tree/ptree.hpp>

#include "manager/metadata/roles.h"
#include "test/postgresql/global_test_environment.h"
#include "test/postgresql/utility/ut_utils.h"

namespace manager::metadata::testing {

using boost::property_tree::ptree;

class ApiTestRolesMetadataException
    : public ::testing::TestWithParam<boost::property_tree::ptree> {
 public:
  void SetUp() override { UTUtils::skip_if_connection_not_opened(); }
};

/**
 * @brief Exception test for getting role metadata.
 */
TEST_F(ApiTestRolesMetadataException, get_role_metadata) {
  ErrorCode error = ErrorCode::UNKNOWN;

  auto roles = std::make_unique<Roles>(GlobalTestEnvironment::TEST_DB);
  error = roles->init();
  EXPECT_EQ(ErrorCode::OK, error);

  ptree role_metadata;
  error = roles->get("invalid_role_name", role_metadata);
  EXPECT_EQ(ErrorCode::NAME_NOT_FOUND, error);

  error = roles->get("", role_metadata);
  EXPECT_EQ(ErrorCode::NAME_NOT_FOUND, error);

  error = roles->get(0, role_metadata);
  EXPECT_EQ(ErrorCode::ID_NOT_FOUND, error);

  error = roles->get(99999, role_metadata);
  EXPECT_EQ(ErrorCode::ID_NOT_FOUND, error);
}

}  // namespace manager::metadata::testing
