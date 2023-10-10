/*
 * Copyright 2021-2023 Project Tsurugi.
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
#ifndef TEST_INCLUDE_TEST_HELPER_TABLE_METADATA_HELPER_H_
#define TEST_INCLUDE_TEST_HELPER_TABLE_METADATA_HELPER_H_

#include <map>
#include <memory>
#include <string>
#include <string_view>
#include <vector>

#include <boost/property_tree/ptree.hpp>

#include "manager/metadata/tables.h"
#include "test/helper/metadata_helper.h"
#include "test/metadata/ut_table_metadata.h"

#if defined(STORAGE_POSTGRESQL)
#include "test/helper/postgresql/metadata_helper_pg.h"
#elif defined(STORAGE_JSON)
#include "test/helper/json/metadata_helper_json.h"
#endif

namespace manager::metadata::testing {

class TableMetadataHelper : public MetadataHelper {
 public:
#if defined(STORAGE_POSTGRESQL)
  TableMetadataHelper()
      : helper_(std::make_unique<MetadataHelperPg>(kTableName)) {}
#elif defined(STORAGE_JSON)
  TableMetadataHelper()
      : helper_(std::make_unique<MetadataHelperJson>(kMetadataName, kRootNode,
                                                     kSubNode)) {}
#endif

  int64_t get_record_count() const override {
    return helper_->get_record_count();
  }

  static std::string make_table_name(std::string_view prefix,
                                     std::string_view identifier,
                                     int32_t line_num);

  static std::vector<UtTableMetadata> make_valid_table_metadata();

  static void add_table(std::string_view table_name,
                        ObjectId* ret_table_id = nullptr);
  static void add_table(const boost::property_tree::ptree& new_table,
                        ObjectId* ret_table_id = nullptr);

  static boost::property_tree::ptree get_table(ObjectId table_id);

  static void remove_table(const ObjectId table_id);
  static void remove_table(std::string_view table_name);

  static void print_column_metadata(const UTColumnMetadata& column_metadata);
  static void print_table_statistics(
      const boost::property_tree::ptree& table_statistics);

  // add
  static void add_table(const manager::metadata::Table& new_table,
                        ObjectId* table_id);

  static void check_table_acls_expected(
      const std::map<std::string_view, std::string_view>& expected,
      const boost::property_tree::ptree& actual);

 private:
#if defined(STORAGE_POSTGRESQL)
  static constexpr const char* const kTableName = "tables";
#elif defined(STORAGE_JSON)
  static constexpr const char* const kMetadataName = "tables";
  static constexpr const char* const kRootNode     = "tables";
  static constexpr const char* const kSubNode      = "constraints";
#endif

#if defined(STORAGE_POSTGRESQL)
  std::unique_ptr<MetadataHelperPg> helper_;
#elif defined(STORAGE_JSON)
  std::unique_ptr<MetadataHelperJson> helper_;
#endif
};

}  // namespace manager::metadata::testing

#endif  // TEST_INCLUDE_TEST_HELPER_TABLE_METADATA_HELPER_H_
