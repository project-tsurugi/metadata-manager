/*
 * Copyright 2021-2023 tsurugi project.
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

#include <map>
#include <memory>
#include <string>
#include <vector>

#include <boost/property_tree/ptree.hpp>

#include "manager/metadata/dao/dao.h"
#include "manager/metadata/error_code.h"
#include "manager/metadata/provider/provider_base.h"
#include "manager/metadata/table.h"

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

  manager::metadata::ErrorCode confirm_permission(std::string_view key, std::string_view value,
                                                  std::string_view permission, bool& check_result);

 private:
  /**
   * @brief Mapping information between privilege codes and column names.
   */
  const std::map<char, std::string> privileges_map_ = {
      {'r',     Dao::PrivilegeColumn::kSelect},
      {'a',     Dao::PrivilegeColumn::kInsert},
      {'w',     Dao::PrivilegeColumn::kUpdate},
      {'d',     Dao::PrivilegeColumn::kDelete},
      {'D',   Dao::PrivilegeColumn::kTruncate},
      {'x', Dao::PrivilegeColumn::kReferences},
      {'t',    Dao::PrivilegeColumn::kTrigger}
  };

  std::shared_ptr<Dao> tables_dao_      = nullptr;
  std::shared_ptr<Dao> columns_dao_     = nullptr;
  std::shared_ptr<Dao> constraints_dao_ = nullptr;
  std::shared_ptr<Dao> privileges_dao_  = nullptr;

  manager::metadata::ErrorCode get_column_metadata(std::string_view table_id,
                                                   boost::property_tree::ptree& tables) const;
  manager::metadata::ErrorCode get_constraint_metadata(std::string_view table_id,
                                                       boost::property_tree::ptree& tables) const;

  bool check_of_privilege(const boost::property_tree::ptree& object,
                          std::string_view privileges) const;
};  // class TablesProvider

}  // namespace manager::metadata::db

#endif  // MANAGER_METADATA_PROVIDER_TABLES_PROVIDER_H_
