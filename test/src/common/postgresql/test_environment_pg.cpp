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
#include "test/common/postgresql/test_environment_pg.h"

#include <gtest/gtest.h>

#include <limits>

#include "manager/metadata/common/config.h"
#include "manager/metadata/dao/postgresql/dbc_utils_pg.h"
#include "test/helper/column_statistics_helper.h"
#include "test/helper/table_metadata_helper.h"

namespace manager::metadata::testing {

using db::postgresql::ConnectionSPtr;
using db::postgresql::DbcUtils;

void TestEnvironmentPg::SetUp() {
  // check if a connection to the metadata repository is opened or not.
  ConnectionSPtr connection = DbcUtils::make_connection_sptr(
      PQconnectdb(Config::get_connection_string().c_str()));

  this->is_open_ = DbcUtils::is_open(connection);
}

void TestEnvironmentPg::TearDown() {}

}  // namespace manager::metadata::testing
