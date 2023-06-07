/*
 * Copyright 2020-2021 tsurugi project.
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
#include "manager/metadata/dao/json/dao_json.h"

#include <boost/format.hpp>

#include "manager/metadata/common/config.h"

// =============================================================================
namespace manager::metadata::db {

ErrorCode DaoJson::prepare() {
  ErrorCode error = ErrorCode::UNKNOWN;

  // Filename of the metadata.
  boost::format file_path = boost::format("%s/%s.json") %
                            Config::get_storage_dir_path() % source_name_;
  // Set metadata file name.
  database_ = file_path.str();

  // Generate object ID generator.
  oid_generator_ = std::make_unique<ObjectIdGenerator>();

  error = ErrorCode::OK;
  return error;
}

}  // namespace manager::metadata::db
