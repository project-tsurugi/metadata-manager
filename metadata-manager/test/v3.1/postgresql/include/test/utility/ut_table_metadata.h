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
#ifndef UT_TABLE_METADATA_H_
#define UT_TABLE_METADATA_H_

#include <boost/property_tree/ptree.hpp>
#include <string>
#include <vector>

#include "test/utility/ut_column_metadata.h"

namespace manager::metadata::testing {

class UTTableMetadata {
 public:
  int64_t id = NOT_INITIALIZED;
  std::string name;
  std::string namespace_name;
  std::vector<int64_t> primary_keys;
  float reltuples = 0;
  boost::property_tree::ptree tables;
  std::vector<UTColumnMetadata> columns;
  UTTableMetadata() = delete;
  explicit UTTableMetadata(std::string name) : name(name){};
  void generate_ptree();

 private:
  static constexpr int64_t NOT_INITIALIZED = -1;
};

}  // namespace manager::metadata::testing

#endif  // UT_TABLE_METADATA_H_
