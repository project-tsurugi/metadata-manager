/*
 * Copyright 2020-2021 Project Tsurugi.
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
#include <limits>
#include <tuple>

#include "manager/metadata/common/utility.h"

namespace manager::metadata::testing {

class UtilityTestStrToFloat
    : public ::testing::TestWithParam<std::tuple<const char*, float>> {};

class UtilityTestStrToUnsignedInt64
    : public ::testing::TestWithParam<std::tuple<const char*, uint64_t>> {};

class UtilityTestStrToInt64
    : public ::testing::TestWithParam<std::tuple<const char*, int64_t>> {};

class UtilityTestStrToFloatException
    : public ::testing::TestWithParam<const char*> {};

class UtilityTestStrToUnsignedInt64Exception
    : public ::testing::TestWithParam<const char*> {};

class UtilityTestStrToInt64Exception
    : public ::testing::TestWithParam<const char*> {};

class UtilityTest : public ::testing::Test {};

INSTANTIATE_TEST_CASE_P(
    ParameterizedTest, UtilityTestStrToFloat,
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

INSTANTIATE_TEST_CASE_P(ParameterizedTest, UtilityTestStrToFloatException,
                        ::testing::Values("", " ", "+", "++", "+-", "-", "--",
                                          "-+", "++0", "+-0", "--0", "-+0",
                                          "1e10000", "-1e10000", "1e-10000",
                                          "-1e-10000"));

INSTANTIATE_TEST_CASE_P(
    ParameterizedTest, UtilityTestStrToUnsignedInt64,
    ::testing::Values(
        std::make_tuple("0", 0), std::make_tuple("00", 0),
        std::make_tuple("1", 1), std::make_tuple("01", 1),
        std::make_tuple("0000000000000000000", 0),
        std::make_tuple("0000000000000000001", 1),
        std::make_tuple("18446744073709551615",
                        std::numeric_limits<uint64_t>::max()),
        std::make_tuple("00000000000000000000000000000000000000", 0),
        std::make_tuple("00000000000000000000000000000000000001", 1),
        std::make_tuple("000000000000000000018446744073709551615",
                        std::numeric_limits<uint64_t>::max())));

INSTANTIATE_TEST_SUITE_P(
    ParameterizedTest, UtilityTestStrToUnsignedInt64Exception,
    ::testing::Values("-0", "-1", "", " ", "+", "++", "+-", "-", "--", "-+",
                      "++0", "+-0", "--0", "-+0", "18446744073709551616",
                      "99999999999999999999",
                      "99999999999999999999999999999999999999"));

INSTANTIATE_TEST_CASE_P(
    ParameterizedTest, UtilityTestStrToInt64,
    ::testing::Values(
        std::make_tuple("0", 0), std::make_tuple("-0", 0),
        std::make_tuple("00", 0), std::make_tuple("-00", 0),
        std::make_tuple("1", 1), std::make_tuple("-1", -1),
        std::make_tuple("-01", -1), std::make_tuple("-01", -1),
        std::make_tuple("0000000000000000000", 0),
        std::make_tuple("-0000000000000000000", 0),
        std::make_tuple("0000000000000000001", 1),
        std::make_tuple("-0000000000000000001", -1),
        std::make_tuple("9223372036854775807",
                        std::numeric_limits<int64_t>::max()),
        std::make_tuple("-9223372036854775808",
                        std::numeric_limits<int64_t>::min()),
        std::make_tuple("00000000000000000000000000000000000000", 0),
        std::make_tuple("-00000000000000000000000000000000000000", 0),
        std::make_tuple("00000000000000000000000000000000000001", 1),
        std::make_tuple("-00000000000000000000000000000000000001", -1),
        std::make_tuple("00000000000000000009223372036854775807",
                        std::numeric_limits<int64_t>::max()),
        std::make_tuple("-00000000000000000009223372036854775808",
                        std::numeric_limits<int64_t>::min())));

INSTANTIATE_TEST_SUITE_P(
    ParameterizedTest, UtilityTestStrToInt64Exception,
    ::testing::Values("+0", "+1", "", " ", "+", "++", "+-", "-", "--", "-+",
                      "++0", "+-0", "--0", "-+0", "9223372036854775808",
                      "+9223372036854775808", "-9223372036854775809",
                      "9999999999999999999", "+9999999999999999999",
                      "-9999999999999999999",
                      "99999999999999999999999999999999999999",
                      "+99999999999999999999999999999999999999",
                      "-99999999999999999999999999999999999999"));

/**
 * @brief Happy test for converting string to floating point.
 */
TEST_P(UtilityTestStrToFloat, str_to_numeric) {
  auto params = GetParam();

  auto input    = std::get<0>(params);
  auto expected = std::get<1>(params);
  auto actual   = -10.0F;

  ErrorCode error = Utility::str_to_numeric(input, actual);
  EXPECT_EQ(ErrorCode::OK, error);
  if (std::isnan(actual)) {
    EXPECT_TRUE(std::isnan(actual));
  } else {
    EXPECT_FLOAT_EQ(expected, actual);
  }
}

/**
 * @brief Exception path test for converting string to floating point.
 */
TEST_P(UtilityTestStrToFloatException, str_to_numeric) {
  auto input    = GetParam();
  auto expected = -10.0F;
  auto actual   = -10.0F;

  ErrorCode error = Utility::str_to_numeric(input, actual);

  EXPECT_EQ(ErrorCode::INTERNAL_ERROR, error);
  EXPECT_EQ(expected, actual);
}

/**
 * @brief Happy path test for converting string to uint64_t.
 */
TEST_P(UtilityTestStrToUnsignedInt64, str_to_numeric) {
  auto params = GetParam();

  auto input    = std::get<0>(params);
  auto actual   = -10UL;
  auto expected = std::get<1>(params);

  ErrorCode error = Utility::str_to_numeric(input, actual);

  EXPECT_EQ(ErrorCode::OK, error);
  EXPECT_EQ(expected, actual);
}

/**
 * @brief Exception path test for converting string to uint64_t.
 */
TEST_P(UtilityTestStrToUnsignedInt64Exception, str_to_numeric) {
  auto input    = GetParam();
  auto expected = -10UL;
  auto actual   = -10UL;

  ErrorCode error = Utility::str_to_numeric(input, actual);

  EXPECT_EQ(ErrorCode::INTERNAL_ERROR, error);
  EXPECT_EQ(expected, actual);
}

/**
 * @brief Happy path test for converting string to int64_t.
 */
TEST_P(UtilityTestStrToInt64, str_to_numeric) {
  auto params = GetParam();

  auto input    = std::get<0>(params);
  auto expected = std::get<1>(params);
  auto actual   = -10L;

  ErrorCode error = Utility::str_to_numeric(input, actual);

  EXPECT_EQ(ErrorCode::OK, error);
  EXPECT_EQ(expected, actual);
}

/**
 * @brief Exception path test for converting string to int64_t.
 */
TEST_P(UtilityTestStrToInt64Exception, str_to_numeric) {
  auto input    = GetParam();
  auto expected = -10L;
  auto actual   = -10L;

  ErrorCode error = Utility::str_to_numeric(input, actual);

  EXPECT_EQ(ErrorCode::INTERNAL_ERROR, error);
  EXPECT_EQ(expected, actual);
}

/**
 * @brief Converts boolean expression ("t" or "f") in metadata repository
 * to "true" or "false" in application.
 */
TEST_F(UtilityTest, str_to_boolean) {
  // Testing for normal patterns.
  EXPECT_TRUE(Utility::str_to_boolean("true"));
  EXPECT_TRUE(Utility::str_to_boolean("True"));
  EXPECT_TRUE(Utility::str_to_boolean("TRUE"));
  EXPECT_FALSE(Utility::str_to_boolean("false"));
  EXPECT_FALSE(Utility::str_to_boolean("False"));
  EXPECT_FALSE(Utility::str_to_boolean("FALSE"));
  // Testing for abnormal patterns.
  EXPECT_FALSE(Utility::str_to_boolean("t"));
  EXPECT_FALSE(Utility::str_to_boolean("T"));
  EXPECT_FALSE(Utility::str_to_boolean("yes"));
  EXPECT_FALSE(Utility::str_to_boolean("Yes"));
  EXPECT_FALSE(Utility::str_to_boolean("YES"));
  EXPECT_FALSE(Utility::str_to_boolean("1"));
  EXPECT_FALSE(Utility::str_to_boolean("f"));
  EXPECT_FALSE(Utility::str_to_boolean("F"));
  EXPECT_FALSE(Utility::str_to_boolean("no"));
  EXPECT_FALSE(Utility::str_to_boolean("No"));
  EXPECT_FALSE(Utility::str_to_boolean("NO"));
  EXPECT_FALSE(Utility::str_to_boolean("0"));
  EXPECT_FALSE(Utility::str_to_boolean(""));
  EXPECT_FALSE(Utility::str_to_boolean("Unknown"));
}

/**
 * @brief Converts boolean expression ("t" or "f") in metadata repository
 * to "true" or "false" in application.
 */
TEST_F(UtilityTest, boolean_to_str) {
  EXPECT_EQ("true", Utility::boolean_to_str(true));
  EXPECT_EQ("true", Utility::boolean_to_str(1));
  EXPECT_EQ("false", Utility::boolean_to_str(false));
  EXPECT_EQ("false", Utility::boolean_to_str(0));
}

}  // namespace manager::metadata::testing
