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

#include <boost/format.hpp>
#include <memory>
#include <string>

#include "manager/metadata/tables.h"
#include "test/global_test_environment.h"
#include "test/utility/ut_utils.h"

namespace manager::metadata::testing {

class ApiTestTablePrivileges : public ::testing::Test {};

/**
 * @brief Unsupported test in JSON version.
 */
TEST_F(ApiTestTablePrivileges, confirm_permission_in_acls) {
  ErrorCode error = ErrorCode::UNKNOWN;
  bool res_permission = false;

  auto tables = std::make_unique<Tables>(GlobalTestEnvironment::TEST_DB);

  error = tables->init();
  EXPECT_EQ(ErrorCode::OK, error);

  UTUtils::print("-- confirm permission by role id --");
  // test by role id.
  error = tables->confirm_permission_in_acls(9999, "r", res_permission);
  EXPECT_EQ(ErrorCode::NOT_SUPPORTED, error);

  UTUtils::print("-- confirm permission by role name --");
  // test by role name.
  error = tables->confirm_permission_in_acls("role_name", "r", res_permission);
  EXPECT_EQ(ErrorCode::NOT_SUPPORTED, error);
}

}  // namespace manager::metadata::testing
