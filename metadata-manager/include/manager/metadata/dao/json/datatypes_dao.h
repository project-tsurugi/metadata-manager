/*
 * Copyright 2020 tsurugi project.
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
#ifndef MANAGER_METADATA_DAO_JSON_DATA_TYPES_DAO_H_
#define MANAGER_METADATA_DAO_JSON_DATA_TYPES_DAO_H_

#include <boost/property_tree/ptree.hpp>
#include <string>

#include "manager/metadata/dao/datatypes_dao.h"
#include "manager/metadata/dao/json/db_session_manager.h"

namespace manager::metadata::db::json {

class DataTypesDAO : public manager::metadata::db::DataTypesDAO {
 public:
  explicit DataTypesDAO(DBSessionManager* session_manager)
      : session_manager_(session_manager){};

  manager::metadata::ErrorCode prepare() const override;

  manager::metadata::ErrorCode select_one_data_type_metadata(
      std::string_view object_key, std::string_view object_value,
      boost::property_tree::ptree& object) const;

 private:
  DBSessionManager* session_manager_;
};  // class DataTypesDAO

}  // namespace manager::metadata::db::json

#endif  // MANAGER_METADATA_DAO_JSON_DATA_TYPES_DAO_H_
