/*
 * Copyright 2022 tsurugi project.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
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

#include <memory>
#include <string_view>
#include <vector>

#include <boost/property_tree/ptree.hpp>

#include "manager/metadata/provider/provider.h"
#include "manager/metadata/dao/dao.h"
#include "manager/metadata/error_code.h"
#include "manager/metadata/metadata.h"

namespace manager::metadata::db {

class MetadataProvider : public Provider {
 public:
  manager::metadata::ErrorCode init();

  manager::metadata::ErrorCode add_index_metadata(
      const boost::property_tree::ptree& object, 
      ObjectId& object_id);

  manager::metadata::ErrorCode get_index_metadata(
      std::string_view key, std::string_view value,
      boost::property_tree::ptree& object);

  manager::metadata::ErrorCode get_index_metadata(
      std::vector<boost::property_tree::ptree>& objects);

  manager::metadata::ErrorCode update_index_metadata(
      const ObjectIdType object_id,
      const boost::property_tree::ptree& object);

  manager::metadata::ErrorCode remove_index_metadata(
      std::string_view key, std::string_view value,
      ObjectId& object_id);

 private:
  std::shared_ptr<Dao> table_dao_ = nullptr;
  std::shared_ptr<Dao> constraint_dao_ = nullptr;
  std::shared_ptr<Dao> index_dao_ = nullptr;
};

}  // namespace manager::metadata::db
