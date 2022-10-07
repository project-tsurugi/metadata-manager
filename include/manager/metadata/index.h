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
#pragma once

#include <string>
#include <vector>
#include <boost/property_tree/ptree.hpp>
#include "manager/metadata/metadata.h"

namespace manager::metadata {
/**
 * @brief Index metadata object.
 */
struct Index : public ClassObject {
  enum class AccessMethod : int64_t {
    DEFAULT = 0,
    MASS_TREE_METHOD,
  };

  enum class Direction : int64_t {
    //  LSB 0th bit : 0 = ASC,        1 = DESC
    //  LSB 1st bit : 0 = NULLS_LAST, 1 = NULLS_FIRST
    ASC_NULLS_LAST    = 0b0000,   //  0
    ASC_NULLS_FIRST   = 0b0001,   //  1
    DESC_NULLS_LAST   = 0b0010,   //  2
    DESC_NULLS_FIRST  = 0b0011,   //  3
    DEFAULT           = 0b1111,   // 15
  };

  static constexpr const char* const TABLE_ID           = "tableId";
  static constexpr const char* const ACCESS_METHOD      = "accessMethod";
  static constexpr const char* const NUMBER_OF_COLUMNS  = "numberOfColumns";
  static constexpr const char* const NUMBER_OF_KEY_COLUMNS  
      = "numberOfKeyColumns";
  static constexpr const char* const IS_UNIQUE          = "IsUnique";
  static constexpr const char* const IS_PRIMARY         = "IsPrimary";
  static constexpr const char* const KEYS               = "columns";
  static constexpr const char* const KEYS_ID            = "columnsId";
  static constexpr const char* const OPTIONS            = "options";

  ObjectId  table_id;
  int64_t   access_method;           // refer to enumlation of AccessMethod.
  int64_t   number_of_columns;       // include non-key (included) columns.
  int64_t   number_of_key_columns;   // exclude non-key (included) columns.
  bool      is_unique;
  bool      is_primary;
  std::vector<int64_t>  keys;      // include non-key (included) columns.
  std::vector<int64_t>  keys_id;
  std::vector<int64_t>  options;   // refer to enumlation of Direction.

  Index()
      :ClassObject(),
        table_id(INVALID_OBJECT_ID),
        access_method(INVALID_VALUE),
        number_of_columns(INVALID_VALUE),
        number_of_key_columns(INVALID_VALUE),
        is_unique(false),
        is_primary(false) {}
  boost::property_tree::ptree convert_to_ptree() const override;
  void convert_from_ptree(const boost::property_tree::ptree& pt) override;
};

} // namespace manager::metadata
