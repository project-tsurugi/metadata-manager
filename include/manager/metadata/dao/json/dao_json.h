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

#include <memory>
#include <string>

#include "manager/metadata/dao/dao.h"
#include "manager/metadata/dao/json/db_session_manager_json.h"
#include "manager/metadata/dao/json/object_id_json.h"

namespace manager::metadata::db {

/**
 * @brief DAO base class for JSON.
 */
class DaoJson : public Dao {
 public:
  /**
    * @brief Construct a new DAO class for JSON data.
    * @param session pointer to DB session manager for JSON.
    * @param source_name name of the source file.
    */
  DaoJson(DbSessionManagerJson* session, std::string_view source_name)
      : session_(session), source_name_(source_name) {}

  virtual ~DaoJson() {}

  /**
   * @brief Prepare to access the metadata JSON file.
   * @return ErrorCode::OK if success, otherwise an error code.
   */
  virtual manager::metadata::ErrorCode prepare();

 protected:
  DbSessionManagerJson* session() const { return session_; }
  std::string database() const { return database_; }
  ObjectIdGenerator* oid_generator() const { return oid_generator_.get(); }

 private:
  DbSessionManagerJson* session_;
  std::string source_name_;
  std::string database_;
  std::unique_ptr<ObjectIdGenerator> oid_generator_;
};  // class DaoJson

}  // namespace manager::metadata::db

#endif  // MANAGER_METADATA_DAO_JSON_DAO_JSON_H_
