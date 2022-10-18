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
#include "manager/metadata/dao/postgresql/common_pg.h"
#include "manager/metadata/dao/postgresql/dbc_utils_pg.h"
#include "manager/metadata/error_code.h"
#include "manager/metadata/metadata.h"
#include "test/postgresql/utility/ut_utils.h"

// extern "C" {
// #include <libpq-fe.h>
// }

namespace manager::metadata::testing {

using db::postgresql::ConnectionSPtr;
using db::postgresql::DbcUtils;
using db::postgresql::ResultUPtr;

class DaoTestCommonStrToFloat
    : public ::testing::TestWithParam<std::tuple<const char*, float>> {};

class DaoTestCommonStrToUint64_t
    : public ::testing::TestWithParam<std::tuple<const char*, uint64_t>> {};

class DaoTestCommonStrToInt64_t
    : public ::testing::TestWithParam<std::tuple<const char*, ObjectIdType>> {};

class DaoTestCommonStrToFloatException
    : public ::testing::TestWithParam<const char*> {};

class DaoTestCommonStrToUint64_tException
    : public ::testing::TestWithParam<const char*> {};

class DaoTestCommonStrToInt64_tException
    : public ::testing::TestWithParam<const char*> {};

class DaoTestCommonIfConnectionOpened : public ::testing::Test {
  void SetUp() override { UTUtils::skip_if_connection_not_opened(); }
};  // class DaoTestCommonIfConnectionOpened

class DaoTestCommonIfConnectionNotOpened : public ::testing::Test {
  void SetUp() override { UTUtils::skip_if_connection_opened(); }
};  // class DaoTestCommonIfConnectionNotOpened

class DaoTestCommon : public ::testing::Test {};

INSTANTIATE_TEST_CASE_P(
    ParameterizedTest, DaoTestCommonStrToFloat,
    ::testing::Values(
        std::make_tuple("0", 0), std::make_tuple("1", 1),
        std::make_tuple("00", 0), std::make_tuple("01", 1),
        std::make_tuple("0.", 0), std::make_tuple("1.", 1),
        std::make_tuple("0.0", 0), std::make_tuple("1.0", 1),
        std::make_tuple("0.5", .5), std::make_tuple(".5", .5),
        std::make_tuple(".25", .25), std::make_tuple(".125", .125),
        std::make_tuple(".0625", .0625), std::make_tuple(".4375", .4375),
        std::make_tuple("-0", 0), std::make_tuple("-1", -1),
        std::make_tuple("-00", 0), std::make_tuple("-01", -1),
        std::make_tuple("-0.", 0), std::make_tuple("-1.", -1),
        std::make_tuple("-0.0", 0), std::make_tuple("-1.0", -1),
        std::make_tuple("-0.5", -.5), std::make_tuple("-.5", -.5),
        std::make_tuple("-.25", -.25), std::make_tuple("-.125", -.125),
        std::make_tuple("-.0625", -.0625), std::make_tuple("-.4375", -.4375),
        std::make_tuple("3.1415927410125732421875", 3.1415927410125732421875),
        std::make_tuple("0000000000000000000000000000000000000."
                        "0000000000000000000000000000000000000",
                        0),
        std::make_tuple("0000000000000000000000000000000000001."
                        "0000000000000000000000000000000000000",
                        1),
        std::make_tuple("3.4028235e+38", FLT_MAX),
        std::make_tuple("inf", std::numeric_limits<float>::infinity()),
        std::make_tuple("INF", std::numeric_limits<float>::infinity()),
        std::make_tuple("infinity", std::numeric_limits<float>::infinity()),
        std::make_tuple("INFINITY", std::numeric_limits<float>::infinity()),
        std::make_tuple("-inf", -std::numeric_limits<float>::infinity()),
        std::make_tuple("-INF", -std::numeric_limits<float>::infinity()),
        std::make_tuple("-infinity", -std::numeric_limits<float>::infinity()),
        std::make_tuple("-INFINITY", -std::numeric_limits<float>::infinity()),
        std::make_tuple("nan", std::numeric_limits<float>::quiet_NaN()),
        std::make_tuple("NaN", std::numeric_limits<float>::quiet_NaN()),
        std::make_tuple("NAN", std::numeric_limits<float>::quiet_NaN())));

INSTANTIATE_TEST_CASE_P(
    ParameterizedTest, DaoTestCommonStrToUint64_t,
    ::testing::Values(
        std::make_tuple("0", 0), std::make_tuple("+0", 0),
        std::make_tuple("-0", 0), std::make_tuple("00", 0),
        std::make_tuple("+00", 0), std::make_tuple("-00", 0),
        std::make_tuple("1", 1), std::make_tuple("+1", 1),
        std::make_tuple("-1", -1), std::make_tuple("01", 1),
        std::make_tuple("+01", 1), std::make_tuple("-01", -1),
        std::make_tuple("-01", -1), std::make_tuple("0000000000000000000", 0),
        std::make_tuple("+0000000000000000000", 0),
        std::make_tuple("-0000000000000000000", 0),
        std::make_tuple("0000000000000000001", 1),
        std::make_tuple("+0000000000000000001", 1),
        std::make_tuple("-0000000000000000001", -1),
        std::make_tuple("18446744073709551615",
                        std::numeric_limits<uint64_t>::max()),
        std::make_tuple("+18446744073709551615",
                        std::numeric_limits<uint64_t>::max()),
        std::make_tuple("00000000000000000000000000000000000000", 0),
        std::make_tuple("+00000000000000000000000000000000000000", 0),
        std::make_tuple("-00000000000000000000000000000000000000", 0),
        std::make_tuple("00000000000000000000000000000000000001", 1),
        std::make_tuple("+00000000000000000000000000000000000001", 1),
        std::make_tuple("-00000000000000000000000000000000000001", -1),
        std::make_tuple("000000000000000000018446744073709551615",
                        std::numeric_limits<uint64_t>::max()),
        std::make_tuple("+000000000000000000018446744073709551615",
                        std::numeric_limits<uint64_t>::max())));

INSTANTIATE_TEST_CASE_P(
    ParameterizedTest, DaoTestCommonStrToInt64_t,
    ::testing::Values(
        std::make_tuple("0", 0), std::make_tuple("+0", 0),
        std::make_tuple("-0", 0), std::make_tuple("00", 0),
        std::make_tuple("+00", 0), std::make_tuple("-00", 0),
        std::make_tuple("1", 1), std::make_tuple("+1", 1),
        std::make_tuple("-1", -1), std::make_tuple("01", 1),
        std::make_tuple("+01", 1), std::make_tuple("-01", -1),
        std::make_tuple("-01", -1), std::make_tuple("0000000000000000000", 0),
        std::make_tuple("+0000000000000000000", 0),
        std::make_tuple("-0000000000000000000", 0),
        std::make_tuple("0000000000000000001", 1),
        std::make_tuple("+0000000000000000001", 1),
        std::make_tuple("-0000000000000000001", -1),
        std::make_tuple("9223372036854775807",
                        std::numeric_limits<ObjectIdType>::max()),
        std::make_tuple("+9223372036854775807",
                        std::numeric_limits<ObjectIdType>::max()),
        std::make_tuple("-9223372036854775808",
                        std::numeric_limits<ObjectIdType>::min()),
        std::make_tuple("00000000000000000000000000000000000000", 0),
        std::make_tuple("+00000000000000000000000000000000000000", 0),
        std::make_tuple("-00000000000000000000000000000000000000", 0),
        std::make_tuple("00000000000000000000000000000000000001", 1),
        std::make_tuple("+00000000000000000000000000000000000001", 1),
        std::make_tuple("-00000000000000000000000000000000000001", -1),
        std::make_tuple("00000000000000000009223372036854775807",
                        std::numeric_limits<ObjectIdType>::max()),
        std::make_tuple("+00000000000000000009223372036854775807",
                        std::numeric_limits<ObjectIdType>::max()),
        std::make_tuple("-00000000000000000009223372036854775808",
                        std::numeric_limits<ObjectIdType>::min())));

INSTANTIATE_TEST_CASE_P(
    ParameterizedTest, DaoTestCommonStrToFloatException,
    ::testing::Values("", " ", " 0", " 1", " +0", " +1", " -0", " -1", "+",
                      "++", "+-", "-", "--", "-+", "++0", "+-0", "--0", "-+0",
                      "+0+", "+0-", "-0-", "-0+", "0+", "0-", "0 ", "0x", "1 ",
                      "1e10000", "-1e10000", "1e-10000", "-1e-10000"));

INSTANTIATE_TEST_SUITE_P(
    ParameterizedTest, DaoTestCommonStrToUint64_tException,
    ::testing::Values("", " ", " 0", " 1", " +0", " +1", " -0", " -1", "+",
                      "++", "+-", "-", "--", "-+", "++0", "+-0", "--0", "-+0",
                      "+0+", "+0-", "-0-", "-0+", "0+", "0-", "0 ", "0x", "1 ",
                      "18446744073709551616", "99999999999999999999",
                      "99999999999999999999999999999999999999"));

INSTANTIATE_TEST_SUITE_P(
    ParameterizedTest, DaoTestCommonStrToInt64_tException,
    ::testing::Values("", " ", " 0", " 1", " +0", " +1", " -0", " -1", "+",
                      "++", "+-", "-", "--", "-+", "++0", "+-0", "--0", "-+0",
                      "+0+", "+0-", "-0-", "-0+", "0+", "0-", "0 ", "0x", "1 ",
                      "9223372036854775808", "+9223372036854775808",
                      "-9223372036854775809", "9999999999999999999",
                      "+9999999999999999999", "-9999999999999999999",
                      "99999999999999999999999999999999999999",
                      "+99999999999999999999999999999999999999",
                      "-99999999999999999999999999999999999999"));

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
  ConnectionSPtr no_connection;
  // If input nullptr, returned false
  EXPECT_EQ(false, DbcUtils::is_open(no_connection));

  // Verifies that a connection is opened if it is opened.
  ConnectionSPtr connection = DbcUtils::make_connection_sptr(
      PQconnectdb(Config::get_connection_string().c_str()));
  EXPECT_EQ(true, DbcUtils::is_open(connection));
}

/**
 * @brief Verifies that a connection is closed
 * if a connection to metadata repository is closed.
 */
TEST_F(DaoTestCommonIfConnectionNotOpened, is_open) {
  ConnectionSPtr no_connection;
  // If input nullptr, returns false
  EXPECT_EQ(false, DbcUtils::is_open(no_connection));

  // Verifies that a connection is closed if it is closed.
  ConnectionSPtr connection = DbcUtils::make_connection_sptr(
      PQconnectdb(Config::get_connection_string().c_str()));
  EXPECT_EQ(false, DbcUtils::is_open(connection));
}

/**
 * @brief Converts boolean expression ("t" or "f") in metadata repository
 * to "true" or "false" in application.
 */
TEST_F(DaoTestCommon, convert_boolean_expression) {
  EXPECT_EQ("true", DbcUtils::convert_boolean_expression("t"));
  EXPECT_EQ("true", DbcUtils::convert_boolean_expression("T"));
  EXPECT_EQ("true", DbcUtils::convert_boolean_expression("true"));
  EXPECT_EQ("true", DbcUtils::convert_boolean_expression("True"));
  EXPECT_EQ("true", DbcUtils::convert_boolean_expression("TRUE"));
  EXPECT_EQ("true", DbcUtils::convert_boolean_expression("yes"));
  EXPECT_EQ("true", DbcUtils::convert_boolean_expression("Yes"));
  EXPECT_EQ("true", DbcUtils::convert_boolean_expression("YES"));
  EXPECT_EQ("true", DbcUtils::convert_boolean_expression("1"));
  EXPECT_EQ("false", DbcUtils::convert_boolean_expression("f"));
  EXPECT_EQ("false", DbcUtils::convert_boolean_expression("F"));
  EXPECT_EQ("false", DbcUtils::convert_boolean_expression("false"));
  EXPECT_EQ("false", DbcUtils::convert_boolean_expression("False"));
  EXPECT_EQ("false", DbcUtils::convert_boolean_expression("FALSE"));
  EXPECT_EQ("false", DbcUtils::convert_boolean_expression("no"));
  EXPECT_EQ("false", DbcUtils::convert_boolean_expression("No"));
  EXPECT_EQ("false", DbcUtils::convert_boolean_expression("NO"));
  EXPECT_EQ("false", DbcUtils::convert_boolean_expression("0"));
  EXPECT_EQ("", DbcUtils::convert_boolean_expression(nullptr));
  EXPECT_EQ("", DbcUtils::convert_boolean_expression(""));
  EXPECT_EQ("", DbcUtils::convert_boolean_expression("Unknown"));
}

/**
 * @brief Converts boolean expression ("t" or "f") in metadata repository
 * to "true" or "false" in application.
 */
TEST_F(DaoTestCommon, str_to_boolean) {
  EXPECT_TRUE(DbcUtils::str_to_boolean("t"));
  EXPECT_TRUE(DbcUtils::str_to_boolean("T"));
  EXPECT_TRUE(DbcUtils::str_to_boolean("true"));
  EXPECT_TRUE(DbcUtils::str_to_boolean("True"));
  EXPECT_TRUE(DbcUtils::str_to_boolean("TRUE"));
  EXPECT_TRUE(DbcUtils::str_to_boolean("yes"));
  EXPECT_TRUE(DbcUtils::str_to_boolean("Yes"));
  EXPECT_TRUE(DbcUtils::str_to_boolean("YES"));
  EXPECT_TRUE(DbcUtils::str_to_boolean("1"));
  EXPECT_FALSE(DbcUtils::str_to_boolean("f"));
  EXPECT_FALSE(DbcUtils::str_to_boolean("F"));
  EXPECT_FALSE(DbcUtils::str_to_boolean("false"));
  EXPECT_FALSE(DbcUtils::str_to_boolean("False"));
  EXPECT_FALSE(DbcUtils::str_to_boolean("FALSE"));
  EXPECT_FALSE(DbcUtils::str_to_boolean("no"));
  EXPECT_FALSE(DbcUtils::str_to_boolean("No"));
  EXPECT_FALSE(DbcUtils::str_to_boolean("NO"));
  EXPECT_FALSE(DbcUtils::str_to_boolean("0"));
  EXPECT_FALSE(DbcUtils::str_to_boolean(nullptr));
  EXPECT_FALSE(DbcUtils::str_to_boolean(""));
  EXPECT_FALSE(DbcUtils::str_to_boolean("Unknown"));
}

/**
 * @brief Converts boolean expression ("t" or "f") in metadata repository
 * to "true" or "false" in application.
 */
TEST_F(DaoTestCommon, boolean_to_str) {
  EXPECT_EQ("true", DbcUtils::boolean_to_str(true));
  EXPECT_EQ("true", DbcUtils::boolean_to_str(1));
  EXPECT_EQ("false", DbcUtils::boolean_to_str(false));
  EXPECT_EQ("false", DbcUtils::boolean_to_str(0));
}

/**
 * @brief Happy test for converting string to floating point.
 */
TEST_P(DaoTestCommonStrToFloat, str_to_float) {
  auto params = GetParam();

  const char* input = std::get<0>(params);
  float actual = -10;

  ErrorCode error = DbcUtils::str_to_floating_point(input, actual);
  EXPECT_EQ(ErrorCode::OK, error);

  if (std::isnan(actual)) {
    EXPECT_TRUE(std::isnan(actual));
  } else {
    float expected = std::get<1>(params);
    EXPECT_FLOAT_EQ(expected, actual);
  }
}

/**
 * @brief Exception path test for converting string to floating point.
 */
TEST_P(DaoTestCommonStrToFloatException, str_to_float) {
  const char* input = GetParam();

  float actual = -10;
  ErrorCode error = DbcUtils::str_to_floating_point(input, actual);

  EXPECT_EQ(ErrorCode::INTERNAL_ERROR, error);
  EXPECT_EQ(-10, actual);
}

/**
 * @brief Converts nullptr to floating point.
 */
TEST_F(DaoTestCommonStrToFloatException, null_to_float) {
  float actual = -10;
  ErrorCode error = DbcUtils::str_to_floating_point(nullptr, actual);

  EXPECT_EQ(ErrorCode::INTERNAL_ERROR, error);
  EXPECT_EQ(-10, actual);
}

/**
 * @brief Happy path test for converting string to uint64_t.
 */
TEST_P(DaoTestCommonStrToUint64_t, str_to_integral) {
  auto params = GetParam();
  const char* input = std::get<0>(params);

  uint64_t actual = -10;
  ErrorCode error = DbcUtils::str_to_integral(input, actual);

  EXPECT_EQ(ErrorCode::OK, error);

  uint64_t expected = std::get<1>(params);
  EXPECT_EQ(expected, actual);
}

/**
 * @brief Exception path test for converting string to uint64_t.
 */
TEST_P(DaoTestCommonStrToUint64_tException, str_to_integral) {
  const char* input = GetParam();

  uint64_t actual = -10;
  ErrorCode error = DbcUtils::str_to_integral(input, actual);

  EXPECT_EQ(ErrorCode::INTERNAL_ERROR, error);
  EXPECT_EQ(-10, actual);
}

/**
 * @brief Exception path test for converting nullptr to uint64_t.
 */
TEST_F(DaoTestCommonStrToUint64_tException, null_to_integral) {
  uint64_t actual = -10;
  ErrorCode error = DbcUtils::str_to_integral(nullptr, actual);

  EXPECT_EQ(ErrorCode::INTERNAL_ERROR, error);
  EXPECT_EQ(-10, actual);
}

/**
 * @brief Happy path test for converting string to int64_t.
 */
TEST_P(DaoTestCommonStrToInt64_t, str_to_integral) {
  auto params = GetParam();
  const char* input = std::get<0>(params);

  ObjectIdType actual = -10;
  ErrorCode error = DbcUtils::str_to_integral(input, actual);

  EXPECT_EQ(ErrorCode::OK, error);

  ObjectIdType expected = std::get<1>(params);
  EXPECT_EQ(expected, actual);
}

/**
 * @brief Exception path test for converting string to int64_t.
 */
TEST_P(DaoTestCommonStrToInt64_tException, str_to_integral) {
  const char* input = GetParam();

  ObjectIdType actual = -10;
  ErrorCode error = DbcUtils::str_to_integral(input, actual);

  EXPECT_EQ(ErrorCode::INTERNAL_ERROR, error);
  EXPECT_EQ(-10, actual);
}

/**
 * @brief Converts nullptr to int64_t.
 */
TEST_F(DaoTestCommonStrToInt64_tException, null_to_integral) {
  ObjectIdType actual = -10;
  ErrorCode error = DbcUtils::str_to_integral(nullptr, actual);

  EXPECT_EQ(ErrorCode::INTERNAL_ERROR, error);
  EXPECT_EQ(-10, actual);
}

/**
 * @brief By inputting nullptr, make ConnectionSPtr.
 */
TEST_F(DaoTestCommon, make_connection_sptr) {
  ConnectionSPtr conn_sptr = DbcUtils::make_connection_sptr(nullptr);
  EXPECT_EQ(nullptr, conn_sptr.get());
  EXPECT_EQ(nullptr, conn_sptr);
}

/**
 * @brief By inputting nullptr, make ResultUPtr.
 */
TEST_F(DaoTestCommon, make_result_uptr) {
  ResultUPtr res_uptr = DbcUtils::make_result_uptr(nullptr);
  EXPECT_EQ(nullptr, res_uptr.get());
  EXPECT_EQ(nullptr, res_uptr);
}

}  // namespace manager::metadata::testing
