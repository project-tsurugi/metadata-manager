/*
 * Copyright 2022-2023 tsurugi project.
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
#ifndef MANAGER_METADATA_CONSTRAINTS_H_
#define MANAGER_METADATA_CONSTRAINTS_H_

#include <string_view>
#include <vector>

#include <boost/property_tree/ptree.hpp>

#include "manager/metadata/constraint.h"
#include "manager/metadata/error_code.h"
#include "manager/metadata/metadata.h"

namespace manager::metadata {

/**
 * @brief Container of constraint metadata objects.
 */
class Constraints : public Metadata {
 public:
  explicit Constraints(std::string_view database) : Constraints(database, kDefaultComponent) {}
  Constraints(std::string_view database, std::string_view component);

  Constraints(const Constraints&)            = delete;
  Constraints& operator=(const Constraints&) = delete;
  virtual ~Constraints() {}

  ErrorCode init() const override;

  ErrorCode add(const boost::property_tree::ptree& object) const override;
  ErrorCode add(const boost::property_tree::ptree& object, ObjectId* object_id) const override;

  ErrorCode get(const ObjectId object_id, boost::property_tree::ptree& object) const override;
  ErrorCode get([[maybe_unused]] std::string_view object_name,
                [[maybe_unused]] boost::property_tree::ptree& object) const override {
    return ErrorCode::UNKNOWN;
  }
  ErrorCode get_all(std::vector<boost::property_tree::ptree>& container) const override;

  ErrorCode update([[maybe_unused]] const ObjectIdType object_id,
                   [[maybe_unused]] const boost::property_tree::ptree& object) const override {
    return ErrorCode::UNKNOWN;
  }

  ErrorCode remove(const ObjectId object_id) const override;
  ErrorCode remove([[maybe_unused]] std::string_view object_name,
                   [[maybe_unused]] ObjectId* object_id) const override {
    return ErrorCode::UNKNOWN;
  }

 private:
  manager::metadata::ErrorCode param_check_metadata_add(
      const boost::property_tree::ptree& object) const;
};  // class Constraints

}  // namespace manager::metadata

#endif  // MANAGER_METADATA_CONSTRAINTS_H_
