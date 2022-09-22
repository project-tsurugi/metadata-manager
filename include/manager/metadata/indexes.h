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
#include "metadata.h"

namespace manager::metadata {
/**
 * @brief Index metadata object.
 */
struct Index : public ClassObject {
  Index()
      : ClassObject(),
        owner_id(INVALID_OBJECT_ID),
        access_method(INVALID_VALUE),
        number_of_columns(INVALID_VALUE),
        number_of_key_columns(INVALID_VALUE),
        is_unique(false),
        is_primary(false) 
      {}

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

  int64_t               owner_id;
  int64_t               access_method;          // refer to enumlation of AccessMethod.
  int64_t               number_of_columns;      // include non-key (included) columns.
  int64_t               number_of_key_columns;  // exclude non-key (included) columns.
  bool                  is_unique;
  bool                  is_primary;
  std::vector<int64_t>  keys;                   // include non-key (included) columns.
  std::vector<int64_t>  keys_id;
  std::vector<int64_t>  options;                // refer to enumlation of Direction.

  static constexpr const char* OWNER_ID               = "owner_id";
  static constexpr const char* ACCESS_METHOD          = "access_method";
  static constexpr const char* NUMBER_OF_COLUMNS      = "number_of_columns";
  static constexpr const char* NUMBER_OF_KEY_COLUMNS  = "number_of_key_columns";
  static constexpr const char* IS_UNIQUE              = "is_unique";
  static constexpr const char* IS_PRIMARY             = "is_primary";
  static constexpr const char* KEYS                   = "keys";
  static constexpr const char* KEYS_ID                = "keys_id";
  static constexpr const char* OPTIONS                = "options";
};

/**
 * @brief Container of index metadata objects.
 */
class Indexes : public Metadata {
  explicit Indexes(std::string_view database)
      : Indexes(database, kDefaultComponent) {}
  Indexes(std::string_view database, std::string_view component);

  Indexes(const Indexes&) = delete;
  Indexes& operator=(const Indexes&) = delete;

  ErrorCode init() const override;

  ErrorCode add(const boost::property_tree::ptree& object) const override;
  ErrorCode add(const boost::property_tree::ptree& object,
                ObjectId* object_id) const override;

  ErrorCode get(const ObjectId object_id,
                boost::property_tree::ptree& object) const override;
  ErrorCode get(std::string_view object_name,
                boost::property_tree::ptree& object) const override;
  ErrorCode get_all(
      std::vector<boost::property_tree::ptree>& container) const override;

  ErrorCode remove(const ObjectId object_id) const override;
  ErrorCode remove(std::string_view object_name,
                   ObjectId* object_id) const override;

  ErrorCode add(const manager::metadata::Index& index) const;
  ErrorCode add(const manager::metadata::Index& index,
                ObjectId* object_id) const;
  
  ErrorCode get(const ObjectId object_id,
                manager::metadata::Index& index) const;
  ErrorCode get_all(
      std::vector<manager::metadata::Index>& container) const;
};
} // namespace manager::metadata
