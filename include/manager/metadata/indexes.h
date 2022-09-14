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

#include <vector>
#include "metadata.h"

namespace manager::metadata {
/**
 * @brief Index metadata object.
 */
struct Index : public Object {
  Index() {}
  enum class AccessMethod : int64_t {
    TSURGI_DEFAULT_METHOD = 0,  // temporary name.
  };
  enum class Direction : int64_t {
    ASC_NULLS_LAST    = 0b00,   // 0, default value.
    ASC_NULLS_FIRST   = 0b01,   // 1
    DESC_NULLS_LAST   = 0b10,   // 2
    DESC_NULLS_FIRST  = 0b11,   // 3
  };
  std::string   namespace_name;
  int64_t       owner_id;
  int64_t       access_method;
  std::string   acl;
  int64_t       number_of_indexs;
  int64_t       number_of_key_indexs;  // exclude non-key (included) indexs.
  bool          is_unique;
  bool          is_primary;
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
