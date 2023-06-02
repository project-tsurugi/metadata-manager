/*
 * Copyright 2022 tsurugi project.
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
#ifndef TEST_JSON_INCLUDE_TEST_HELPER_INDEX_METADATA_HELPER_H_
#define TEST_JSON_INCLUDE_TEST_HELPER_INDEX_METADATA_HELPER_H_

#include <memory>
#include <string>

#include <boost/property_tree/ptree.hpp>

#include "manager/metadata/indexes.h"
#include "test/utility/ut_index_metadata.h"

namespace manager::metadata::testing {

class IndexMetadataHelper {
 public:
  static std::int64_t get_record_count();

  static void generate_test_metadata(const ObjectId& table_id,
                                     std::unique_ptr<UTIndexMetadata>& index_metadata);

  static void add(const Indexes* indexes, const boost::property_tree::ptree& index_metadata,
                  ObjectIdType* index_id = nullptr);
  static void add(const Metadata* indexes, const boost::property_tree::ptree& index_metadata,
                  ObjectIdType* index_id = nullptr);
  static void add(const Metadata* indexes, const Index& index_metadata,
                  ObjectIdType* index_id = nullptr);

  static void remove(const Indexes* indexes, const ObjectIdType index_id);
  static void remove(const Metadata* indexes, const ObjectIdType index_id);
  static void remove(const Metadata* indexes, std::string_view index_name,
                     ObjectIdType* removed_id);

  static void check_metadata_expected(const boost::property_tree::ptree& expected,
                                      const boost::property_tree::ptree& actual);

 private:
  static void check_child_expected(const boost::property_tree::ptree& expected,
                                   const boost::property_tree::ptree& actual,
                                   const char* meta_name);
  template <typename T>
  static void check_expected(const boost::property_tree::ptree& expected,
                             const boost::property_tree::ptree& actual, const char* meta_name);
};

}  // namespace manager::metadata::testing

#endif  // TEST_JSON_INCLUDE_TEST_HELPER_INDEX_METADATA_HELPER_H_
