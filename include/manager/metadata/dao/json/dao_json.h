/*
 * Copyright 2020-2022 tsurugi project.
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
#ifndef MANAGER_METADATA_DAO_JSON_DAO_JSON_H_
#define MANAGER_METADATA_DAO_JSON_DAO_JSON_H_

#include <string>
#include <vector>

#include "manager/metadata/dao/dao.h"
#include "manager/metadata/dao/json/db_session_manager_json.h"

namespace manager::metadata::db {

/**
 * @brief DAO base class for JSON.
 */
class DaoJson : public Dao {
 public:
  explicit DaoJson(DbSessionManagerJson* session) : session_(session) {}
  virtual ~DaoJson() {}

 protected:
  DbSessionManagerJson* session_;
};  // class DaoPg

}  // namespace manager::metadata::db

#endif  // MANAGER_METADATA_DAO_JSON_DAO_JSON_H_
