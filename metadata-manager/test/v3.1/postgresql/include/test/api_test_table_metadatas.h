/*
 * Copyright 2020 tsurugi project.
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
#ifndef API_TEST_TABLE_METADATAS_H_
#define API_TEST_TABLE_METADATAS_H_

#include <gtest/gtest.h>
#include <boost/property_tree/ptree.hpp>
#include <string>
#include <vector>

#include "manager/metadata/metadata.h"
#include "test/global_test_environment.h"
#include "test/utility/ut_table_metadata.h"

namespace manager::metadata::testing {

class ApiTestTableMetadata : public ::testing::Test {
 public:
  void SetUp() override;

  static void add_table(const std::string& table_name,
                        ObjectIdType* ret_table_id);
  static void add_table(boost::property_tree::ptree new_table,
                        ObjectIdType* ret_table_id);
  static void check_table_metadata_expected(
      boost::property_tree::ptree& expected,
      boost::property_tree::ptree& actual);
  static std::vector<boost::property_tree::ptree>
  make_testdata_table_metadata();

 private:
  static void check_metadata_expected(boost::property_tree::ptree& expected,
                                      boost::property_tree::ptree& actual,
                                      const char* meta_name);
  template <typename T>
  static void check_column_metadata_expecetd(
      boost::property_tree::ptree& expected,
      boost::property_tree::ptree& actual, const char* meta_name);
};

}  // namespace manager::metadata::testing

#endif  // API_TEST_TABLE_METADATAS_H_
