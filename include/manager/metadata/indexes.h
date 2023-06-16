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
#include "manager/metadata/index.h"
#include "manager/metadata/provider/metadata_provider.h"

namespace manager::metadata {
/**
 * @brief Container of index metadata objects.
 */
class Indexes : public Metadata {
 public:
  explicit Indexes(std::string_view database)
      : Indexes(database, kDefaultComponent) {}
  Indexes(std::string_view database, std::string_view component);
  Indexes(const Indexes&) = delete;
  Indexes& operator=(const Indexes&) = delete;
  virtual ~Indexes() {}

  ErrorCode init() const override;

  ErrorCode add(const boost::property_tree::ptree& object) const override;
  ErrorCode add(const boost::property_tree::ptree& object,
                ObjectId* object_id) const override;

  ErrorCode get(const ObjectId object_id,
                boost::property_tree::ptree& object) const override;
  ErrorCode get(std::string_view object_name,
                boost::property_tree::ptree& object) const override;
  ErrorCode get_all(
      std::vector<boost::property_tree::ptree>& objects) const override;

  ErrorCode update(const ObjectIdType object_id,
                  const boost::property_tree::ptree& object) const override;

  ErrorCode remove(const ObjectId object_id) const override;
  ErrorCode remove(std::string_view object_name,
                   ObjectId* object_id) const override;

 private:
  std::unique_ptr<manager::metadata::db::MetadataProvider> provider_ = nullptr;

  ErrorCode param_check_metadata_add(
      const boost::property_tree::ptree& object) const;
};

} // namespace manager::metadata
