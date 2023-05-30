/*
 * Copyright 2021-2022 tsurugi project.
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
#ifndef TEST_POSTGRESQL_INCLUDE_TEST_HELPER_TABLE_METADATA_HELPER_H_
#define TEST_POSTGRESQL_INCLUDE_TEST_HELPER_TABLE_METADATA_HELPER_H_

#include <map>
#include <memory>
#include <string_view>

#include <boost/property_tree/ptree.hpp>

#include "manager/metadata/metadata.h"
#include "test/utility/ut_table_metadata.h"

namespace manager::metadata::testing {

class TableMetadataHelper {
 public:
  static std::int64_t get_record_count();

  static void generate_table_metadata(
      std::unique_ptr<UTTableMetadata>& testdata_table_metadata);

  static void add_table(std::string_view table_name,
                        ObjectIdType* ret_table_id = nullptr);
  static void add_table(const boost::property_tree::ptree& new_table,
                        ObjectIdType* ret_table_id = nullptr);

  static void remove_table(const ObjectIdType table_id);
  static void remove_table(std::string_view table_name);

  static void print_column_metadata(const UTColumnMetadata& column_metadata);
  static void print_table_statistics(
      const boost::property_tree::ptree& table_statistics);

  static void check_table_metadata_expected(
      const boost::property_tree::ptree& expected,
      const boost::property_tree::ptree& actual);

  static void check_table_acls_expected(
      const std::map<std::string_view, std::string_view>& expected,
      const boost::property_tree::ptree& actual);

 private:
  static void check_child_expected(const boost::property_tree::ptree& expected,
                                   const boost::property_tree::ptree& actual,
                                   const char* meta_name);
  template <typename T>
  static void check_expected(const boost::property_tree::ptree& expected,
                             const boost::property_tree::ptree& actual,
                             const char* meta_name);
};

}  // namespace manager::metadata::testing

#endif  // TEST_POSTGRESQL_INCLUDE_TEST_HELPER_TABLE_METADATA_HELPER_H_
