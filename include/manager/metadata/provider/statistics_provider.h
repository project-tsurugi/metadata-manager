/*
 * Copyright 2021 tsurugi project.
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
#ifndef MANAGER_METADATA_PROVIDER_STATISTICS_PROVIDER_H_
#define MANAGER_METADATA_PROVIDER_STATISTICS_PROVIDER_H_

#include <memory>
#include <string_view>
#include <vector>

#include <boost/property_tree/ptree.hpp>

#include "manager/metadata/dao/dao.h"
#include "manager/metadata/error_code.h"
#include "manager/metadata/provider/provider_base.h"
#include "manager/metadata/statistics.h"

namespace manager::metadata::db {

class StatisticsProvider : public ProviderBase {
 public:
  manager::metadata::ErrorCode init();

  manager::metadata::ErrorCode add_column_statistic(
      const boost::property_tree::ptree& object,
      ObjectIdType& statistic_id);

  manager::metadata::ErrorCode get_column_statistic(
      std::string_view key, std::string_view value,
      boost::property_tree::ptree& object);
  manager::metadata::ErrorCode get_column_statistic(
      const ObjectIdType table_id, std::string_view key, std::string_view value,
      boost::property_tree::ptree& object);
  manager::metadata::ErrorCode get_column_statistics(
      std::vector<boost::property_tree::ptree>& container);
  manager::metadata::ErrorCode get_column_statistics(
      const ObjectIdType table_id,
      std::vector<boost::property_tree::ptree>& container);

  manager::metadata::ErrorCode remove_column_statistic(
      std::string_view key, std::string_view value,
      ObjectIdType& statistic_id);
  manager::metadata::ErrorCode remove_column_statistics(
      const ObjectIdType table_id);
  manager::metadata::ErrorCode remove_column_statistic(
      const ObjectIdType table_id, std::string_view key, std::string_view value,
      ObjectIdType& statistic_id);

 private:
  std::shared_ptr<Dao> statistics_dao_ = nullptr;
};  // class StatisticsProvider

}  // namespace manager::metadata::db

#endif  // MANAGER_METADATA_PROVIDER_STATISTICS_PROVIDER_H_
