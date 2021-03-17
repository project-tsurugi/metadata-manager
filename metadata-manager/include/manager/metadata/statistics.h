/*
 * Copyright 2020 tsurugi project.
 *
 * Licensed under the Apache License, version 2.0 (the "License");
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
#pragma once

#include <boost/property_tree/ptree.hpp>
#include <memory>
#include <string_view>
#include <unordered_map>

#include "manager/metadata/dao/statistics_dao.h"
#include "manager/metadata/dao/tables_dao.h"
#include "manager/metadata/entity/column_statistic.h"
#include "manager/metadata/entity/table_statistic.h"
#include "manager/metadata/error_code.h"
#include "manager/metadata/metadata.h"

namespace manager::metadata {
class Statistics : public Metadata {
   public:
    static constexpr const char *const COLUMN_STATISTIC = "columnStatistic";

    /**
     *  @brief  Constructor
     *  @param  (database) [in]  database name.
     *  @return none.
     */
    Statistics(std::string_view database,
               std::string_view component = "visitor")
        : Metadata(database, component) {}

    Statistics(const Statistics &) = delete;
    Statistics &operator=(const Statistics &) = delete;

    ErrorCode init() override;
    ErrorCode add_one_column_statistic(
        ObjectIdType table_id, ObjectIdType ordinal_position,
        boost::property_tree::ptree &column_statistic);
    ErrorCode add_table_statistic(ObjectIdType table_id, float reltuples);
    ErrorCode add_table_statistic(std::string_view table_name, float reltuples,
                                  ObjectIdType *table_id);
    ErrorCode get_one_column_statistic(ObjectIdType table_id,
                                       ObjectIdType ordinal_position,
                                       ColumnStatistic &column_statistic);
    ErrorCode get_all_column_statistics(
        ObjectIdType table_id,
        std::unordered_map<ObjectIdType, ColumnStatistic> &column_statistics);
    ErrorCode get_table_statistic(
        ObjectIdType table_id,
        manager::metadata::TableStatistic &table_statistic);
    ErrorCode get_table_statistic(
        std::string_view table_name,
        manager::metadata::TableStatistic &table_statistic);
    ErrorCode remove_one_column_statistic(ObjectIdType table_id,
                                          ObjectIdType ordinal_position);
    ErrorCode remove_all_column_statistics(ObjectIdType table_id);

   private:
    std::shared_ptr<manager::metadata::db::TablesDAO> tdao;
    std::shared_ptr<manager::metadata::db::StatisticsDAO> sdao;
};  // class Statistics

}  // namespace manager::metadata
