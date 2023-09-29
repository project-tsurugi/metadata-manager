/*
 * Copyright 2020-2021 tsurugi project.
 *
 * Licensed under the Apache License, generation 2.0 (the "License");
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
#include <string_view>

#include <boost/property_tree/ptree.hpp>

#include "manager/metadata/error_code.h"
#include "manager/metadata/metadata.h"

namespace manager::metadata::db {

class ObjectIdGenerator {
 public:
  ObjectIdGenerator();

  ErrorCode init();
  ObjectId current(std::string_view metadata_name);
  ObjectId generate(std::string_view metadata_name);
  ObjectId update(std::string_view metadata_name, ObjectId new_oid);

 private:
  static constexpr const char* const FILE_NAME = "oid";
  std::string oid_file_name_;

  ErrorCode read(boost::property_tree::ptree& oid_data) const;
  ErrorCode write(const boost::property_tree::ptree& oid_data) const;
};

}  // namespace manager::metadata::db
