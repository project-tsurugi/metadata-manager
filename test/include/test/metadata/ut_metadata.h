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
#ifndef TEST_INCLUDE_TEST_METADATA_UT_METADATA_H_
#define TEST_INCLUDE_TEST_METADATA_UT_METADATA_H_

#include <gtest/gtest.h>

#include <memory>
#include <string>
#include <utility>
#include <vector>

#include <boost/property_tree/ptree.hpp>

#include "manager/metadata/metadata.h"
#include "test/common/ut_utils.h"

namespace manager::metadata::testing {

#define CHECK_METADATA_EXPECTED(exp, act) \
  check_metadata_expected(exp, act, __FILE__, __LINE__);

#define EXPECT_EQ_EX(expected, actual, file, line) \
  EXPECT_EQ(expected, actual)                      \
      << "Caller: " + std::string(file) + ":" + std::to_string(line);
#define ASSERT_EQ_EX(expected, actual, file, line) \
  ASSERT_EQ(expected, actual)                      \
      << "Caller: " + std::string(file) + ":" + std::to_string(line);

class UtMetadata {
 public:
  UtMetadata() {}
  virtual ~UtMetadata() {}

  virtual void generate_test_metadata() = 0;

  virtual const manager::metadata::Object* get_metadata_struct() const = 0;
  virtual boost::property_tree::ptree get_metadata_ptree() const       = 0;

  /**
   * @brief Verifies that the actual metadata equals expected one.
   * @param expected  [in]  expected metadata.
   * @param actual    [in]  actual metadata.
   * @param file      [in]  file name of the caller.
   * @param line      [in]  line number of the caller.
   * @return none.
   */
  virtual void check_metadata_expected(
      const boost::property_tree::ptree& expected,
      const boost::property_tree::ptree& actual, const char* file,
      const int64_t line) const = 0;

  /**
   * @brief Verifies that the actual metadata equals expected one.
   * @param expected  [in]  expected metadata.
   * @param actual    [in]  actual metadata.
   * @param file      [in]  file name of the caller.
   * @param line      [in]  line number of the caller.
   * @return none.
   */
  void check_metadata_expected(const boost::property_tree::ptree& expected,
                               const ::manager::metadata::Object& actual,
                               const char* file, const int64_t line) const {
    // Transform metadata from structure object to ptree object.
    check_metadata_expected(expected, actual.convert_to_ptree(), file, line);
  }

 protected:
  static constexpr int64_t NOT_INITIALIZED = -1;

  /**
   * @brief Verifies that the actual metadata equals expected one.
   * @param expected  [in]  expected metadata.
   * @param actual    [in]  actual metadata.
   * @param meta_name [in]  name of metadata table.
   * @param file      [in]  file name of the caller.
   * @param line      [in]  line number of the caller.
   * @return none.
   */
  void check_child_expected(const boost::property_tree::ptree& expected,
                            const boost::property_tree::ptree& actual,
                            const char* meta_name, const char* file,
                            const int64_t line) const {
    const std::string message = "Value of \"" + std::string(meta_name) +
                                "\" does not match: " + std::string(file) +
                                ":" + std::to_string(line);

    auto o_expected = expected.get_child_optional(meta_name);
    auto o_actual   = actual.get_child_optional(meta_name);

    if (o_expected && o_actual) {
      auto expected_value = UTUtils::get_tree_string(o_expected.value());
      auto actual_value   = UTUtils::get_tree_string(o_actual.value());
      EXPECT_EQ(expected_value, actual_value) << message;
    } else if (o_expected) {
      EXPECT_EQ(o_expected.value().empty(), !o_actual.is_initialized())
          << message;
    } else if (o_actual) {
      EXPECT_EQ(!o_expected.is_initialized(), o_actual.value().empty())
          << message;
    } else {
      EXPECT_EQ(o_expected.is_initialized(), o_actual.is_initialized())
          << message;
    }
  }

  /**
   * @brief Verifies that the actual metadata equals expected one.
   * @param expected  [in]  expected metadata.
   * @param actual    [in]  actual metadata.
   * @param meta_name [in]  name of metadata table.
   * @param file      [in]  file name of the caller.
   * @param line      [in]  line number of the caller.
   * @return none.
   */
  template <typename T>
  void check_child_expected(const std::vector<T>& expected,
                            const std::vector<T>& actual, const char* meta_name,
                            const char* file, const int64_t line) const {
    ASSERT_EQ(expected.size(), actual.size())
        << "Vectors in \"" << std::string(meta_name)
        << "\" are of unequal length: " + std::string(file) + ":" +
               std::to_string(line);

    for (size_t idx = 0; idx < expected.size(); ++idx) {
      EXPECT_EQ(expected[idx], actual[idx])
          << "Vectors in \"" << std::string(meta_name)
          << "\" differ at index: " + std::string(file) + ":" +
                 std::to_string(line);
    }
  }

  /**
   * @brief Verifies that the actual metadata equals expected one.
   * @tparam T Derived class of metadata object.
   * @param expected  [in]  expected column metadata.
   * @param actual    [in]  actual column metadata.
   * @param meta_name [in]  name of column metadata table.
   * @param file      [in]  file name of the caller.
   * @param line      [in]  line number of the caller.
   * @return none.
   */
  template <typename T>
  void check_expected(const boost::property_tree::ptree& expected,
                      const boost::property_tree::ptree& actual,
                      const char* meta_name, const char* file,
                      const int64_t line) const {
    const std::string message = "Value of \"" + std::string(meta_name) +
                                "\" does not match: " + std::string(file) +
                                ":" + std::to_string(line);

    auto value_expected = expected.get_optional<T>(meta_name);
    auto value_actual   = actual.get_optional<T>(meta_name);

    if (value_expected && value_actual) {
      EXPECT_EQ(value_expected.value(), value_actual.value()) << message;
    } else {
      if (value_expected) {
        const auto& value_expected = expected.get<std::string>(meta_name);
        EXPECT_EQ(value_expected.empty(), !value_actual.is_initialized())
            << message;
      } else if (value_actual) {
        const auto& value_actual = actual.get<std::string>(meta_name);
        EXPECT_EQ(!value_expected.is_initialized(), value_actual.empty())
            << message;
      } else {
        EXPECT_EQ(value_expected.is_initialized(),
                  value_actual.is_initialized())
            << message;
      }
    }
  }

  /**
   * @brief Verifies that the actual metadata equals expected one.
   * @tparam T Derived class of metadata object.
   * @param expected  [in]  expected column metadata.
   * @param actual    [in]  actual column metadata.
   * @param file      [in]  file name of the caller.
   * @param line      [in]  line number of the caller.
   * @return none.
   */
  template <typename T>
  void check_expected(const T& expected, const T& actual, const char* meta_name,
                      const char* file, const int64_t line) const {
    const std::string message = "Value of \"" + std::string(meta_name) +
                                "\" does not match: " + std::string(file) +
                                ":" + std::to_string(line);

    EXPECT_EQ(expected, actual) << message;
  }
};

}  // namespace manager::metadata::testing

#endif  // TEST_INCLUDE_TEST_METADATA_UT_METADATA_H_
