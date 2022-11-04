/*
 * Copyright 2020-2022 tsurugi project.
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
#ifndef TEST_INCLUDE_TEST_METADATA_UT_TABLE_METADATA_H_
#define TEST_INCLUDE_TEST_METADATA_UT_TABLE_METADATA_H_

#include <string>
#include <vector>

#include <boost/property_tree/ptree.hpp>

#include "test/metadata/ut_metadata.h"

#include "manager/metadata/tables.h"
#include "test/metadata/ut_column_metadata.h"
#include "test/metadata/ut_constraint_metadata.h"

namespace manager::metadata::testing {

class UTTableMetadata : public UtMetadata<manager::metadata::Table> {
 public:
  using UtMetadata::UtMetadata;

  explicit UTTableMetadata() { this->generate_test_metadata(); }
  explicit UTTableMetadata(std::string_view table_name)
      : table_name_(table_name) {
    this->generate_test_metadata();
  }

  void generate_test_metadata() override;

  void check_metadata_expected(const boost::property_tree::ptree& expected,
                               const boost::property_tree::ptree& actual,
                               const char* file,
                               const int64_t line) const override;
  void check_metadata_expected(const manager::metadata::Table& expected,
                               const boost::property_tree::ptree& actual,
                               const char* file, const int64_t line) const;
  void check_metadata_expected(const boost::property_tree::ptree& expected,
                               const manager::metadata::Table& actual,
                               const char* file, const int64_t line) const;
  void check_metadata_expected(const manager::metadata::Table& expected,
                               const manager::metadata::Table& actual,
                               const char* file, const int64_t line) const;

 private:
  std::string table_name_;
};

}  // namespace manager::metadata::testing

#endif  // TEST_INCLUDE_TEST_METADATA_UT_TABLE_METADATA_H_
