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
#ifndef MANAGER_METADATA_DAO_JSON_OBJECT_ID_JSON_H_
#define MANAGER_METADATA_DAO_JSON_OBJECT_ID_JSON_H_

#include <string>
#include <string_view>

#include <boost/property_tree/ptree.hpp>

#include "manager/metadata/error_code.h"
#include "manager/metadata/metadata.h"

namespace manager::metadata::db::json {

class ObjectId {
 public:
  ObjectId();

  manager::metadata::ErrorCode init();
  ObjectIdType current(std::string_view table_name);
  ObjectIdType generate(std::string_view table_name);
  ObjectIdType update(std::string_view table_name, ObjectIdType new_oid);

 private:
  static constexpr const char* const OID_NAME = "oid";
  std::string oid_file_name_;

  ErrorCode read(boost::property_tree::ptree& oid_data) const;
  ErrorCode write(const boost::property_tree::ptree& oid_data) const;
};

}  // namespace manager::metadata::db::json

#endif  // MANAGER_METADATA_DAO_JSON_OBJECT_ID_JSON_H_
