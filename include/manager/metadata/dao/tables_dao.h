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
#ifndef MANAGER_METADATA_DAO_TABLES_DAO_H_
#define MANAGER_METADATA_DAO_TABLES_DAO_H_

#include <string>
#include <string_view>
#include <vector>

#include <boost/property_tree/ptree.hpp>

#include "manager/metadata/dao/generic_dao.h"
#include "manager/metadata/error_code.h"
#include "manager/metadata/tables.h"

namespace manager::metadata::db {

class TablesDAO : public GenericDAO {
 public:
  virtual ~TablesDAO() {}

  virtual manager::metadata::ErrorCode insert_table_metadata(
      const boost::property_tree::ptree& table_metadata,
      ObjectIdType& table_id) const = 0;

  virtual manager::metadata::ErrorCode select_table_metadata(
      std::string_view object_key, std::string_view object_value,
      boost::property_tree::ptree& table_metadata) const = 0;
  virtual manager::metadata::ErrorCode select_table_metadata(
      std::vector<boost::property_tree::ptree>& table_container) const = 0;

  virtual manager::metadata::ErrorCode update_table_metadata(
      const ObjectIdType table_id,
      const boost::property_tree::ptree& table_metadata) const = 0;

  virtual manager::metadata::ErrorCode update_reltuples(
      const float reltuples, std::string_view object_key,
      std::string_view object_value, ObjectIdType& table_id) const = 0;

  virtual manager::metadata::ErrorCode delete_table_metadata(
      std::string_view object_key, std::string_view object_value,
      ObjectIdType& table_id) const = 0;
};  // class TablesDAO

}  // namespace manager::metadata::db

#endif  // MANAGER_METADATA_DAO_TABLES_DAO_H_
