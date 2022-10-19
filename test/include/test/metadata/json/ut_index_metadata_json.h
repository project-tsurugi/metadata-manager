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
#ifndef TEST_INCLUDE_TEST_METADATA_JSON_UT_INDEX_METADATA_JSON_H_
#define TEST_INCLUDE_TEST_METADATA_JSON_UT_INDEX_METADATA_JSON_H_

#include <string>
#include <vector>

#include <boost/property_tree/ptree.hpp>

#include "manager/metadata/indexes.h"

namespace manager::metadata::testing {

class UTIndexMetadata {
 public:
  int64_t id;
  std::string name;
  std::string namespace_name;
  int64_t owner_id;
  std::string acl;
  ObjectId table_id;
  int64_t access_method;  //!< @brief refer to enumeration of Method.
  int64_t
      number_of_key_columns;  //!< @brief exclude non-key (included) columns.
  bool is_unique;
  bool is_primary;
  std::vector<int64_t> columns;
  std::vector<ObjectId> columns_id;
  std::vector<int64_t> options;  //!< @brief refer to enumeration of Option.

  boost::property_tree::ptree indexes_metadata;

  UTIndexMetadata()
      : id(NOT_INITIALIZED),
        name(""),
        namespace_name(""),
        owner_id(NOT_INITIALIZED),
        acl(""),
        table_id(NOT_INITIALIZED),
        access_method(NOT_INITIALIZED),
        number_of_key_columns(NOT_INITIALIZED),
        is_unique(false),
        is_primary(false),
        columns({}),
        columns_id({}),
        options({}) {}

  void generate_ptree();

 private:
  static constexpr int64_t NOT_INITIALIZED = -1;
};

}  // namespace manager::metadata::testing

#endif  // TEST_INCLUDE_TEST_METADATA_JSON_UT_INDEX_METADATA_JSON_H_
