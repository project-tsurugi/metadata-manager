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
 * @brief Column metadata object.
 */
struct Column : public Object {
	Column() {}
  static constexpr int64_t ORDINAL_POSITION_BASE_INDEX = 1;
  int64_t               table_id;
  int64_t               ordinal_position;
  int64_t               data_type_id;
  int64_t               data_length;
  std::vector<int64_t>  data_lengths;
  bool                  varying;
  bool                  nullable;
  std::string           default_expr;
};

/**
 * @brief Container of Column metadata objects.
 */
class Columns : public Metadata {
 public:
  explicit Columns(std::string_view database)
      : Columns(database, kDefaultComponent) {}
  Columns(std::string_view database, std::string_view component);

  Columns(const Columns&) = delete;
  Columns& operator=(const Columns&) = delete;

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

  ErrorCode add(const manager::metadata::Column& column) const;
  ErrorCode add(const manager::metadata::Column& column,
                ObjectId* object_id) const;
  
  ErrorCode get(const ObjectId object_id,
                manager::metadata::Column& column) const;
  ErrorCode get_all(
      std::vector<manager::metadata::Column>& container) const;
};
} // namespace manager::metadata
