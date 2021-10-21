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
#ifndef MANAGER_METADATA_MANAGER_INCLUDE_MANAGER_METADATA_DAO_GENERIC_DAO_H_
#define MANAGER_METADATA_MANAGER_INCLUDE_MANAGER_METADATA_DAO_GENERIC_DAO_H_

#include "manager/metadata/error_code.h"

namespace manager::metadata::db {

class GenericDAO {
 public:
  enum class TableName {
    STATISTICS = 0,
    TABLES,
    DATATYPES,
    COLUMNS,
    ROLES,
    PRIVILEGES
  };

  virtual ~GenericDAO() {}

  virtual manager::metadata::ErrorCode prepare() const = 0;
};  // class GenericDAO

}  // namespace manager::metadata::db

#endif  // MANAGER_METADATA_MANAGER_INCLUDE_MANAGER_METADATA_DAO_GENERIC_DAO_H_
