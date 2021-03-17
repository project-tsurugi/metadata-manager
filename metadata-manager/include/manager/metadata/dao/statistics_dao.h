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

#ifndef STATISTICS_DAO_H_
#define STATISTICS_DAO_H_

extern "C" {
#include <libpq-fe.h>
}

#include <string>
#include <unordered_map>

#include "manager/metadata/dao/common/dbc_utils.h"
#include "manager/metadata/dao/generic_dao.h"
#include "manager/metadata/entity/column_statistic.h"
#include "manager/metadata/metadata.h"

namespace manager::metadata::db {

class StatisticsDAO : public GenericDAO {
   public:
    explicit StatisticsDAO(ConnectionSPtr connection)
        : GenericDAO(connection, TableName::STATISTICS){};

    manager::metadata::ErrorCode prepare() const override;

    manager::metadata::ErrorCode
    upsert_one_column_statistic_by_table_id_column_ordinal_position(
        ObjectIdType table_id, ObjectIdType ordinal_position,
        const std::string& column_statistic) const;
    manager::metadata::ErrorCode
    select_one_column_statistic_by_table_id_column_ordinal_position(
        ObjectIdType table_id, ObjectIdType ordinal_position,
        ColumnStatistic& column_statistic) const;
    manager::metadata::ErrorCode select_all_column_statistic_by_table_id(
        ObjectIdType table_id,
        std::unordered_map<ObjectIdType, ColumnStatistic>& column_statistics)
        const;
    manager::metadata::ErrorCode delete_all_column_statistic_by_table_id(
        ObjectIdType table_id) const;
    manager::metadata::ErrorCode
    delete_one_column_statistic_by_table_id_column_ordinal_position(
        ObjectIdType table_id, ObjectIdType ordinal_position) const;

   private:
    manager::metadata::ErrorCode get_column_statistic_from_p_gresult(
        PGresult*& res, int ordinal_position,
        ColumnStatistic& column_statistic) const;
};

}  // namespace manager::metadata::db

#endif  // STATISTICS_DAO_H_
