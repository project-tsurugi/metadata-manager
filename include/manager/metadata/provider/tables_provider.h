/*
 * Copyright 2021-2022 tsurugi project.
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
#ifndef MANAGER_METADATA_PROVIDER_TABLES_PROVIDER_H_
#define MANAGER_METADATA_PROVIDER_TABLES_PROVIDER_H_

#include <memory>
#include <string_view>
#include <vector>

#include <boost/property_tree/ptree.hpp>

#include "manager/metadata/dao/columns_dao.h"
#include "manager/metadata/dao/constraints_dao.h"
#include "manager/metadata/dao/privileges_dao.h"
#include "manager/metadata/dao/tables_dao.h"
#include "manager/metadata/error_code.h"
#include "manager/metadata/metadata.h"
#include "manager/metadata/provider/provider_base.h"

namespace manager::metadata::db {

class TablesProvider : public ProviderBase {
 public:
  manager::metadata::ErrorCode init();

  manager::metadata::ErrorCode add_table_metadata(const boost::property_tree::ptree& object,
                                                  ObjectIdType& table_id);

  manager::metadata::ErrorCode get_table_metadata(std::string_view key, std::string_view value,
                                                  boost::property_tree::ptree& object);
  manager::metadata::ErrorCode get_table_metadata(
      std::vector<boost::property_tree::ptree>& container);
  manager::metadata::ErrorCode get_table_statistic(std::string_view key, std::string_view value,
                                                   boost::property_tree::ptree& object);

  manager::metadata::ErrorCode update_table_metadata(
      const ObjectIdType table_id,
      const boost::property_tree::ptree& object);

  manager::metadata::ErrorCode set_table_statistic(
      const boost::property_tree::ptree& object, ObjectIdType& table_id);

  manager::metadata::ErrorCode remove_table_metadata(std::string_view key,
                                                     std::string_view value,
                                                     ObjectIdType& table_id);

  manager::metadata::ErrorCode remove_table_metadata(std::string_view key, std::string_view value,
                                                     ObjectIdType& table_id);

  manager::metadata::ErrorCode confirm_permission(std::string_view key, std::string_view value,
                                                  std::string_view permission, bool& check_result);

 private:
  std::shared_ptr<TablesDAO> tables_dao_           = nullptr;
  std::shared_ptr<ColumnsDAO> columns_dao_         = nullptr;
  std::shared_ptr<ConstraintsDAO> constraints_dao_ = nullptr;
  std::shared_ptr<PrivilegesDAO> privileges_dao_   = nullptr;

  manager::metadata::ErrorCode get_column_metadata(std::string_view table_id,
                                                   boost::property_tree::ptree& tables) const;
  manager::metadata::ErrorCode get_constraint_metadata(std::string_view table_id,
                                                       boost::property_tree::ptree& tables) const;
};  // class TablesProvider

}  // namespace manager::metadata::db

#endif  // MANAGER_METADATA_PROVIDER_TABLES_PROVIDER_H_
