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
#ifndef TEST_INCLUDE_TEST_ENVIRONMENT_GLOBAL_TEST_ENVIRONMENT_H_
#define TEST_INCLUDE_TEST_ENVIRONMENT_GLOBAL_TEST_ENVIRONMENT_H_

#include <gtest/gtest.h>

#include <memory>
#include <string>
#include <vector>

#include <boost/property_tree/ptree.hpp>

#include "manager/metadata/metadata.h"
#include "test/metadata/ut_table_metadata.h"

namespace manager::metadata::testing {

class GlobalTestEnvironment : public ::testing::Environment {
 public:
  ~GlobalTestEnvironment() override {}
  void SetUp() override;
  void TearDown() override;

  /**
   * @brief table metadata used as test data.
   */
  std::unique_ptr<UTTableMetadata> testdata_table_metadata;

  /**
   * @brief column statistics used as test data.
   */
  std::vector<boost::property_tree::ptree> column_statistics;

  /**
   * @brief database name assigned to each API constructor argument.
   */
  static constexpr char TEST_DB[] = "test";

  /**
   * @brief a list of non-existing table id.
   */
  std::vector<ObjectIdType> table_id_not_exists;

  /**
   * @brief a list of non-existing column number.
   */
  std::vector<ObjectIdType> column_number_not_exists;

  /**
   * @brief Is a connection to the metadata repository opened?
   */
  bool is_open() { return is_open_; }

  /**
   * @brief Set file name of json schema file.
   */
  void set_json_schema_file_name(std::string file_name) {
    json_schema_file_name = file_name;
  }

  /**
   * @brief Get file name of json schema file.
   */
  std::string get_json_schema_file_name() { return json_schema_file_name; }

 private:
  /**
   * @brief Is a connection to the metadata repository opened?
   */
  bool is_open_ = true;

  /**
   * @brief file name of json schema file.
   */
  std::string json_schema_file_name;
};

/**
 * @brief GlobalTestEnvironment instance that is a global variable.
 */
extern GlobalTestEnvironment* const global;

}  // namespace manager::metadata::testing

#endif  // TEST_INCLUDE_TEST_ENVIRONMENT_GLOBAL_TEST_ENVIRONMENT_H_
