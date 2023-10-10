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
#ifndef TEST_INCLUDE_TEST_COMMON_TEST_ENVIRONMENT_H_
#define TEST_INCLUDE_TEST_COMMON_TEST_ENVIRONMENT_H_

#include <gtest/gtest.h>

#include <limits>
#include <string>
#include <vector>

namespace manager::metadata::testing {

class TestEnvironment : public ::testing::Environment {
 public:
  void SetUp() override {}
  void TearDown() override {}

  /**
   * @brief Is a connection to the metadata repository opened?
   */
  bool is_open() { return is_open_; }

  /**
   * @brief Set file name of json schema file.
   */
  void set_json_schema_file_name(std::string file_name) {
    json_schema_file_name_ = file_name;
  }

  /**
   * @brief Get file name of json schema file.
   */
  std::string get_json_schema_file_name() { return json_schema_file_name_; }

  const std::vector<int64_t> invalid_ids = {
      -1,
      0,
      INT64_MAX - 1,
      INT64_MAX,
      std::numeric_limits<int64_t>::infinity(),
      -std::numeric_limits<int64_t>::infinity(),
      std::numeric_limits<int64_t>::quiet_NaN()};

 protected:
  /**
   * @brief Is a connection to the metadata repository opened?
   */
  bool is_open_ = true;

 private:
  /**
   * @brief file name of json schema file.
   */
  std::string json_schema_file_name_;
};

}  // namespace manager::metadata::testing

#endif  // TEST_INCLUDE_TEST_COMMON_TEST_ENVIRONMENT_H_
