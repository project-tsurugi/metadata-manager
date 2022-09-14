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
#include <string>
#include "metadata.h"

namespace manager::metadata {
/**
 * @brief Constraint metadata object.
 */
struct Constraint : public Object {
  Constraint() {}
  std::string           namespace_name;
  int64_t               type;
  int64_t               table_id;
  int64_t               constraint_id;
  std::vector<int64_t>  keys;
  std::vector<ObjectId> keys_id;
  std::string           expression;
};

/**
 * @brief Container of constraint metadata objects.
 */
class Constraints : public Metadata {
 public:
  explicit Constraints(std::string_view database)
      : Constraints(database, kDefaultComponent) {}
  Constraints(std::string_view database, std::string_view component);

  Constraints(const Constraints&) = delete;
  Constraints& operator=(const Constraints&) = delete;

  ErrorCode init() const override;

  ErrorCode add(const boost::property_tree::ptree& object) const override;
  ErrorCode add(const boost::property_tree::ptree& object,
                ObjectIdType* object_id) const override;

  ErrorCode get(const ObjectIdType object_id,
                boost::property_tree::ptree& object) const override;
  ErrorCode get(std::string_view object_name,
                boost::property_tree::ptree& object) const override;
  ErrorCode get_all(
      std::vector<boost::property_tree::ptree>& container) const override;

  ErrorCode remove(const ObjectIdType object_id) const override;
  ErrorCode remove(std::string_view object_name,
                   ObjectIdType* object_id) const override;

  ErrorCode add(const manager::metadata::Constraint& constraint) const;
  ErrorCode add(const manager::metadata::Constraint& constraint,
                ObjectIdType* object_id) const;
  
  ErrorCode get(const ObjectIdType object_id,
                manager::metadata::Constraint& constraint) const; 
  ErrorCode get_all(
      std::vector<manager::metadata::Constraint>& container) const;
};
} // namespace manager::metadata
