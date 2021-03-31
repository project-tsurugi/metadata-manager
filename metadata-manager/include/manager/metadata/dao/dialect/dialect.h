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

#ifndef DIALECT_H_
#define DIALECT_H_

#include <string>

namespace manager::metadata::db {
class Dialect {
   public:
    virtual ~Dialect(){};

    /**
     * @struct TableName
     * @brief table name of table
     * where metadata is stored in metadata repository.
     */
    struct TableName {
        static constexpr const char *const TABLE_METADATA_TABLE =
            "tsurugi_class";  //!< @brief table metadata table.
        static constexpr const char *const COLUMN_METADATA_TABLE =
            "tsurugi_attribute";  //!< @brief column metadata table.
        static constexpr const char *const COLUMN_STATISTICS_TABLE =
            "tsurugi_statistic";  //!< @brief column statistics table.
        static constexpr const char *const DATA_TYPES_TABLE =
            "tsurugi_type";  //!< @brief  data type metadata table.
    };                       // TableName

    // StatisticsDAO
    virtual std::string
    statistics_dao_upsert_one_column_statistic_by_table_id_column_ordinal_position() = 0;
    virtual std::string
    statistics_dao_select_one_column_statistic_by_table_id_column_ordinal_position() = 0;
    virtual std::string
    statistics_dao_select_all_column_statistic_by_table_id() = 0;
    virtual std::string
    statistics_dao_delete_all_column_statistic_by_table_id() = 0;
    virtual std::string
    statistics_dao_delete_one_column_statistic_by_table_id_column_ordinal_position() = 0;

    // TablesDAO
    virtual std::string tables_dao_update_reltuples_by_table_id() = 0;
    virtual std::string tables_dao_update_reltuples_by_table_name() = 0;
    virtual std::string tables_dao_select_table_statistic_by_table_id() = 0;
    virtual std::string tables_dao_select_table_statistic_by_table_name() = 0;
    virtual std::string tables_dao_insert_table_metadata() = 0;
    virtual std::string tables_dao_delete_table_metadata_by_table_id() = 0;
    virtual std::string tables_dao_delete_table_metadata_by_table_name() = 0;

    // ColumnsDAO
    virtual std::string columns_dao_insert_one_column_metadata() = 0;
    virtual std::string
    columns_dao_select_all_column_metadata_by_table_id() = 0;

    // DAO
    virtual std::string dao_select_equal_to(const std::string &table_name,
                                            const std::string &column_name) = 0;

   protected:
    /**
     * @brief schema name
     * where metadata is stored in metadata repository.
     */
    static constexpr const char *const SCHEMA_NAME = "tsurugi_catalog";
};
}  // namespace manager::metadata::db

#endif  // DIALECT_H_
