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
#ifndef TEST_JSON_INCLUDE_TEST_UTILITY_UT_COLUMN_METADATA_H_
#define TEST_JSON_INCLUDE_TEST_UTILITY_UT_COLUMN_METADATA_H_

#include <string>
#include <string_view>
#include <vector>

#include <boost/property_tree/ptree.hpp>

namespace manager::metadata::testing {

class UTColumnMetadata {
 public:
  int64_t id       = NOT_INITIALIZED;
  int64_t table_id = NOT_INITIALIZED;
  std::string name;
  int64_t column_number = NOT_INITIALIZED;
  int64_t data_type_id  = NOT_INITIALIZED;
  boost::property_tree::ptree p_data_length;  //!< @brief array of data length
  std::vector<int64_t> data_length;
  int varying = NOT_INITIALIZED;
  bool is_not_null;
  std::string default_expr;
  UTColumnMetadata() = delete;
  UTColumnMetadata(std::string name, int64_t column_number,
                   int64_t data_type_id, bool is_not_null)
      : name(name),
        column_number(column_number),
        data_type_id(data_type_id),
        is_not_null(is_not_null) {}
  void show();

 private:
  static constexpr int64_t NOT_INITIALIZED = -1;
};

}  // namespace manager::metadata::testing

#endif  // TEST_JSON_INCLUDE_TEST_UTILITY_UT_COLUMN_METADATA_H_
