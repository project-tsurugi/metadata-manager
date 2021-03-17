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

#ifndef POSTGRESQL_DIALECT_H_
#define POSTGRESQL_DIALECT_H_

#include <string>

#include "manager/metadata/dao/dialect/dialect.h"

namespace manager::metadata::db {

class PostgreSQLDialect : public Dialect {
   public:
    // StatisticsDAO
    std::string
    statistics_dao_upsert_one_column_statistic_by_table_id_column_ordinal_position()
        override;
    std::string
    statistics_dao_select_one_column_statistic_by_table_id_column_ordinal_position()
        override;
    std::string statistics_dao_select_all_column_statistic_by_table_id()
        override;
    std::string statistics_dao_delete_all_column_statistic_by_table_id()
        override;
    std::string
    statistics_dao_delete_one_column_statistic_by_table_id_column_ordinal_position()
        override;

    // TablesDAO
    std::string tables_dao_update_reltuples_by_table_id() override;
    std::string tables_dao_update_reltuples_by_table_name() override;
    std::string tables_dao_select_table_statistic_by_table_id() override;
    std::string tables_dao_select_table_statistic_by_table_name() override;
    std::string tables_dao_insert_table_metadata() override;
    std::string tables_dao_delete_table_metadata_by_table_id() override;
    std::string tables_dao_delete_table_metadata_by_table_name() override;

    // ColumnsDAO
    std::string columns_dao_insert_one_column_metadata() override;

    // DAO
    std::string dao_select_equal_to(const std::string &table_name,
                                    const std::string &column_name) override;
};

}  // namespace manager::metadata::db

#endif  // POSTGRESQL_DIALECT_H_
