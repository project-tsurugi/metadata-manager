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
#ifndef TEST_INCLUDE_TEST_COMMON_POSTGRESQL_TEST_ENVIRONMENT_PG_H_
#define TEST_INCLUDE_TEST_COMMON_POSTGRESQL_TEST_ENVIRONMENT_PG_H_

#include "manager/metadata/common/config.h"
#include "manager/metadata/dao/postgresql/dbc_utils_pg.h"
#include "test/common/test_environment.h"

namespace manager::metadata::testing {

class TestEnvironmentPg : public TestEnvironment {
 public:
  ~TestEnvironmentPg() override {}

  void SetUp() override {
    TestEnvironment::SetUp();

    // check if a connection to the metadata repository is opened or not.
    db::postgresql::ConnectionSPtr connection =
        db::postgresql::DbcUtils::make_connection_sptr(
            PQconnectdb(Config::get_connection_string().c_str()));

    this->is_open_ = db::postgresql::DbcUtils::is_open(connection);
  }

  void TearDown() override {
    TestEnvironment::TearDown();
  }
};

}  // namespace manager::metadata::testing

#endif  // TEST_INCLUDE_TEST_COMMON_POSTGRESQL_TEST_ENVIRONMENT_PG_H_
