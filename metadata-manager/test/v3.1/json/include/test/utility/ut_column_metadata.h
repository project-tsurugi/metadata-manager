/*
 * Copyright 2021 tsurugi project.
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
#ifndef UT_COLUMN_METADATA_H_
#define UT_COLUMN_METADATA_H_

#include <boost/property_tree/ptree.hpp>
#include <string>
#include <string_view>

namespace manager::metadata::testing {

class UTColumnMetadata {
 public:
  int64_t id = NOT_INITIALIZED;
  int64_t table_id = NOT_INITIALIZED;
  std::string name;
  int64_t ordinal_position = NOT_INITIALIZED;
  int64_t data_type_id = NOT_INITIALIZED;
  int64_t data_length =
      NOT_INITIALIZED;  //!< @brief single value of data length
  boost::property_tree::ptree p_data_lengths;  //!< @brief array of data length
  int varying = NOT_INITIALIZED;
  bool nullable;
  std::string default_expr;
  int64_t direction = NOT_INITIALIZED;
  UTColumnMetadata() = delete;
  UTColumnMetadata(std::string name, int64_t ordinal_position,
                   int64_t data_type_id, bool nullable)
      : name(name),
        ordinal_position(ordinal_position),
        data_type_id(data_type_id),
        nullable(nullable){};
  void show();

 private:
  static constexpr int64_t NOT_INITIALIZED = -1;
};

}  // namespace manager::metadata::testing

#endif  // UT_COLUMN_METADATA_H_
