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

#include <cmath>
#include <iostream>
#include <limits>
#include <tuple>
#include <vector>

#include "manager/metadata/common/config.h"
#include "manager/metadata/dao/postgresql/dbc_utils_pg.h"
#include "manager/metadata/dao/postgresql/pg_common.h"
#include "manager/metadata/error_code.h"
#include "test/common/ut_utils.h"

// extern "C" {
// #include <libpq-fe.h>
// }

namespace manager::metadata::testing {

class DaoTestCommonIfConnectionOpened : public ::testing::Test {
  void SetUp() override { UTUtils::skip_if_connection_not_opened(); }
};  // class DaoTestCommonIfConnectionOpened

class DaoTestCommonIfConnectionNotOpened : public ::testing::Test {
  void SetUp() override { UTUtils::skip_if_connection_opened(); }
};  // class DaoTestCommonIfConnectionNotOpened

class DaoTestCommon : public ::testing::Test {};

/**
 * @brief Gets Connection Strings from OS environment variable.
 */
TEST_F(DaoTestCommon, get_connection_string) {
  const char* tmp_cs = std::getenv("TSURUGI_CONNECTION_STRING");

  if (tmp_cs == nullptr) {
    EXPECT_EQ("dbname=tsurugi", Config::get_connection_string());
    UTUtils::print("Connection Strings:", Config::get_connection_string());
  } else {
    EXPECT_EQ(tmp_cs, Config::get_connection_string());
    UTUtils::print("Connection Strings:", Config::get_connection_string());
  }
}

/**
 * @brief Verifies that a connection is opened or not
 * if a connection to metadata repository is opened.
 */
TEST_F(DaoTestCommonIfConnectionOpened, is_open) {
  db::PgConnectionPtr no_connection;
  // If input nullptr, returned false
  EXPECT_EQ(false, db::DbcUtils::is_open(no_connection));

  // Verifies that a connection is opened if it is opened.
  db::PgConnectionPtr connection = db::DbcUtils::make_connection_sptr(
      PQconnectdb(Config::get_connection_string().c_str()));
  EXPECT_EQ(true, db::DbcUtils::is_open(connection));
}

/**
 * @brief Verifies that a connection is closed
 * if a connection to metadata repository is closed.
 */
TEST_F(DaoTestCommonIfConnectionNotOpened, is_open) {
  db::PgConnectionPtr no_connection;
  // If input nullptr, returns false
  EXPECT_EQ(false, db::DbcUtils::is_open(no_connection));

  // Verifies that a connection is closed if it is closed.
  db::PgConnectionPtr connection = db::DbcUtils::make_connection_sptr(
      PQconnectdb(Config::get_connection_string().c_str()));
  EXPECT_EQ(false, db::DbcUtils::is_open(connection));
}

/**
 * @brief Converts boolean expression ("t" or "f") in metadata repository
 * to "true" or "false" in application.
 */
TEST_F(DaoTestCommon, convert_boolean_expression) {
  EXPECT_EQ("true", db::DbcUtils::convert_boolean_expression("t"));
  EXPECT_EQ("true", db::DbcUtils::convert_boolean_expression("T"));
  EXPECT_EQ("true", db::DbcUtils::convert_boolean_expression("true"));
  EXPECT_EQ("true", db::DbcUtils::convert_boolean_expression("True"));
  EXPECT_EQ("true", db::DbcUtils::convert_boolean_expression("TRUE"));
  EXPECT_EQ("true", db::DbcUtils::convert_boolean_expression("yes"));
  EXPECT_EQ("true", db::DbcUtils::convert_boolean_expression("Yes"));
  EXPECT_EQ("true", db::DbcUtils::convert_boolean_expression("YES"));
  EXPECT_EQ("true", db::DbcUtils::convert_boolean_expression("1"));
  EXPECT_EQ("false", db::DbcUtils::convert_boolean_expression("f"));
  EXPECT_EQ("false", db::DbcUtils::convert_boolean_expression("F"));
  EXPECT_EQ("false", db::DbcUtils::convert_boolean_expression("false"));
  EXPECT_EQ("false", db::DbcUtils::convert_boolean_expression("False"));
  EXPECT_EQ("false", db::DbcUtils::convert_boolean_expression("FALSE"));
  EXPECT_EQ("false", db::DbcUtils::convert_boolean_expression("no"));
  EXPECT_EQ("false", db::DbcUtils::convert_boolean_expression("No"));
  EXPECT_EQ("false", db::DbcUtils::convert_boolean_expression("NO"));
  EXPECT_EQ("false", db::DbcUtils::convert_boolean_expression("0"));
  EXPECT_EQ("", db::DbcUtils::convert_boolean_expression(nullptr));
  EXPECT_EQ("", db::DbcUtils::convert_boolean_expression(""));
  EXPECT_EQ("", db::DbcUtils::convert_boolean_expression("Unknown"));
}

/**
 * @brief By inputting nullptr, make ConnectionSPtr.
 */
TEST_F(DaoTestCommon, make_connection_sptr) {
  db::PgConnectionPtr conn_sptr = db::DbcUtils::make_connection_sptr(nullptr);
  EXPECT_EQ(nullptr, conn_sptr.get());
  EXPECT_EQ(nullptr, conn_sptr);
}

/**
 * @brief By inputting nullptr, make ResultUPtr.
 */
TEST_F(DaoTestCommon, make_result_uptr) {
  db::ResultPtr res_uptr = db::DbcUtils::make_result_uptr(nullptr);
  EXPECT_EQ(nullptr, res_uptr.get());
  EXPECT_EQ(nullptr, res_uptr);
}

}  // namespace manager::metadata::testing
