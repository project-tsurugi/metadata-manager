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
#ifndef MANAGER_METADATA_DAO_TABLES_DAO_H_
#define MANAGER_METADATA_DAO_TABLES_DAO_H_

#include <boost/property_tree/ptree.hpp>
#include <string>
#include <string_view>

#include "manager/metadata/dao/generic_dao.h"
#include "manager/metadata/entity/table_statistic.h"
#include "manager/metadata/metadata.h"

namespace manager::metadata::db {

class TablesDAO : public GenericDAO {
 public:
  virtual manager::metadata::ErrorCode update_reltuples_by_table_id(
      float reltuples, ObjectIdType table_id) const = 0;
  virtual manager::metadata::ErrorCode update_reltuples_by_table_name(
      float reltuples, std::string_view table_name,
      ObjectIdType& table_id) const = 0;
  virtual manager::metadata::ErrorCode select_table_statistic_by_table_id(
      ObjectIdType table_id,
      manager::metadata::TableStatistic& table_statistic) const = 0;
  virtual manager::metadata::ErrorCode select_table_statistic_by_table_name(
      std::string_view table_name, TableStatistic& table_statistic) const = 0;
  virtual manager::metadata::ErrorCode insert_table_metadata(
      boost::property_tree::ptree& table, ObjectIdType& table_id) const = 0;
  virtual manager::metadata::ErrorCode select_table_metadata(
      std::string_view object_key, std::string_view object_value,
      boost::property_tree::ptree& object) const = 0;
  virtual manager::metadata::ErrorCode delete_table_metadata(
      std::string_view object_key, std::string_view object_value,
      ObjectIdType& table_id) const = 0;
};  // class TablesDAO

}  // namespace manager::metadata::db

#endif  // MANAGER_METADATA_DAO_TABLES_DAO_H_
