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

#include "manager/metadata/dao/dialect/postgresql_dialect.h"

#include <string>

#include "manager/metadata/metadata.h"
#include "manager/metadata/statistics.h"
#include "manager/metadata/tables.h"

using namespace manager::metadata;

namespace manager::metadata::db {

/**
 *  @brief  Returnes an UPSERT stetement for one column statistic
 *  based on table id and column ordinal position.
 *  @param  none.
 *  @return an UPSERT stetement to upsert one column statistic
 *  based on table id and column ordinal position.
 */
std::string PostgreSQLDialect::
    statistics_dao_upsert_one_column_statistic_by_table_id_column_ordinal_position() {
    std::string query =
        "insert into " + std::string(SCHEMA_NAME) + "." +
        TableName::COLUMN_STATISTICS_TABLE + " (" + Tables::Column::TABLE_ID +
        ", " + Tables::Column::ORDINAL_POSITION + ", " +
        Statistics::COLUMN_STATISTIC + ") values($1, $2, $3) on conflict(" +
        Tables::Column::TABLE_ID + ", " + Tables::Column::ORDINAL_POSITION +
        ") do update set " + Tables::Column::TABLE_ID + " = $1," +
        Tables::Column::ORDINAL_POSITION + " = $2," +
        Statistics::COLUMN_STATISTIC + " = $3";
    return query;
}

/**
 *  @brief  Returnes a SELECT stetement for one column statistic
 *  based on table id and column ordinal position.
 *  @param  none.
 *  @return a SELECT stetement to get one column statistic
 *  based on table id and column ordinal position.
 */
std::string PostgreSQLDialect::
    statistics_dao_select_one_column_statistic_by_table_id_column_ordinal_position() {
    std::string query = "select * from " + std::string(SCHEMA_NAME) + "." +
                        TableName::COLUMN_STATISTICS_TABLE + " where " +
                        Tables::Column::TABLE_ID + " = $1 and " +
                        Tables::Column::ORDINAL_POSITION + " = $2";
    return query;
}

/**
 *  @brief  Returnes a SELECT stetement for all column statistics
 *  based on table id.
 *  @param  none.
 *  @return a SELECT stetement to get all column statistics
 *  based on table id.
 */
std::string
PostgreSQLDialect::statistics_dao_select_all_column_statistic_by_table_id() {
    std::string query = "select * from " + std::string(SCHEMA_NAME) + "." +
                        TableName::COLUMN_STATISTICS_TABLE + " where " +
                        Tables::Column::TABLE_ID + " = $1";
    return query;
}

/**
 *  @brief  Returnes a DELETE stetement for all column statistics
 *  based on table id.
 *  @param  none.
 *  @return a DELETE stetement to delete all column statistics
 *  based on table id.
 */
std::string
PostgreSQLDialect::statistics_dao_delete_all_column_statistic_by_table_id() {
    std::string query = "delete from " + std::string(SCHEMA_NAME) + "." +
                        TableName::COLUMN_STATISTICS_TABLE + " where " +
                        Tables::Column::TABLE_ID + " = $1";
    return query;
}

/**
 *  @brief  Returnes a DELETE stetement for one column statistic
 *  based on table id and column ordinal position.
 *  @param  none.
 *  @return a DELETE stetement to delete all column statistics
 *  based on table id and column ordinal position.
 */
std::string PostgreSQLDialect::
    statistics_dao_delete_one_column_statistic_by_table_id_column_ordinal_position() {
    std::string query = "delete from " + std::string(SCHEMA_NAME) + "." +
                        TableName::COLUMN_STATISTICS_TABLE + " where " +
                        Tables::Column::TABLE_ID + " = $1 and " +
                        Tables::Column::ORDINAL_POSITION + " = $2";
    return query;
}

/**
 *  @brief  Returnes an UPDATE stetement for the number of rows
 *  based on table id.
 *  @param  none.
 *  @return an UPDATE stetement to update the number of rows
 *  based on table id.
 */
std::string PostgreSQLDialect::tables_dao_update_reltuples_by_table_id() {
    std::string query = "update " + std::string(SCHEMA_NAME) + "." +
                        TableName::TABLE_METADATA_TABLE + " set " +
                        Tables::RELTUPLES + " = $1 where " + Tables::ID +
                        " = $2";
    return query;
}

/**
 *  @brief  Returnes an UPDATE stetement for the number of rows
 *  based on table name.
 *  @param  none.
 *  @return an UPDATE stetement to update the number of rows
 *  based on table name.
 */
std::string PostgreSQLDialect::tables_dao_update_reltuples_by_table_name() {
    std::string query = "update " + std::string(SCHEMA_NAME) + "." +
                        TableName::TABLE_METADATA_TABLE + " set " +
                        Tables::RELTUPLES + " = $1 where " + Tables::NAME +
                        " = $2 RETURNING " + Tables::ID;
    return query;
}

/**
 *  @brief  Returnes a SELECT stetement for table statistics
 *  based on table id.
 *  @param  none.
 *  @return a SELECT stetement to get table statistics
 *  based on table id.
 */
std::string PostgreSQLDialect::tables_dao_select_table_statistic_by_table_id() {
    std::string query = "select * from " + std::string(SCHEMA_NAME) + "." +
                        TableName::TABLE_METADATA_TABLE + " where " +
                        Metadata::ID + " = $1";
    return query;
}

/**
 *  @brief  Returnes a SELECT stetement for table statistics
 *  based on table name.
 *  @param  none.
 *  @return a SELECT stetement to get table statistics
 *  based on table name.
 */
std::string
PostgreSQLDialect::tables_dao_select_table_statistic_by_table_name() {
    std::string query = "select * from " + std::string(SCHEMA_NAME) + "." +
                        TableName::TABLE_METADATA_TABLE + " where " +
                        Metadata::NAME + " = $1";
    return query;
}

/**
 *  @brief  Returnes an INSERT stetement for table metadata.
 *  @param  none.
 *  @return an INSERT stetement to insert table metadata.
 */
std::string PostgreSQLDialect::tables_dao_insert_table_metadata() {
    std::string query = "insert into " + std::string(SCHEMA_NAME) + "." +
                        Dialect::TableName::TABLE_METADATA_TABLE + " (" +
                        Metadata::NAME + ", " + Tables::NAMESPACE + ", " +
                        Tables::PRIMARY_KEY_NODE + ", " + Tables::RELTUPLES +
                        ") values ($1, $2, $3, $4) RETURNING " + Tables::ID;
    return query;
}

/**
 *  @brief  Returnes a DELETE stetement for table metadata
 *  based on table id.
 *  @param  none.
 *  @return a DELETE stetement to delete table metadata
 *  based on table id.
 */
std::string PostgreSQLDialect::tables_dao_delete_table_metadata_by_table_id() {
    std::string query = "delete from " + std::string(SCHEMA_NAME) + "." +
                        TableName::TABLE_METADATA_TABLE + " where " +
                        Tables::ID + " = $1";
    return query;
}

/**
 *  @brief  Returnes a DELETE stetement for table metadata
 *  based on table name.
 *  @param  none.
 *  @return a DELETE stetement to delete table metadata
 *  based on table name.
 */
std::string
PostgreSQLDialect::tables_dao_delete_table_metadata_by_table_name() {
    std::string query = "delete from " + std::string(SCHEMA_NAME) + "." +
                        TableName::TABLE_METADATA_TABLE + " where " +
                        Tables::NAME + " = $1 RETURNING " + Tables::ID;
    return query;
}

/**
 *  @brief  Returnes an INSERT stetement for one column metadata.
 *  @param  none.
 *  @return an INSERT stetement to insert one column metadata.
 */
std::string PostgreSQLDialect::columns_dao_insert_one_column_metadata() {
    std::string query =
        "insert into " + std::string(SCHEMA_NAME) + "." +
        Dialect::TableName::COLUMN_METADATA_TABLE + " (" +
        Tables::Column::TABLE_ID + ", " + Tables::Column::NAME + ", " +
        Tables::Column::ORDINAL_POSITION + ", " + Tables::Column::DATA_TYPE_ID +
        ", " + Tables::Column::DATA_LENGTH + ", " + Tables::Column::VARYING +
        ", " + Tables::Column::NULLABLE + ", " + Tables::Column::DEFAULT +
        ", " + Tables::Column::DIRECTION +
        ") values ($1, $2, $3, $4, $5, $6, $7, $8, $9)";
    return query;
}

/**
 *  @brief  Returnes a SELECT stetement to get all column metadata
 *  from column metadata table,
 *  based on table id.
 *  @return a SELECT stetement to get all column metadata,
 *  based on table id.
 */
std::string
PostgreSQLDialect::columns_dao_select_all_column_metadata_by_table_id() {
    std::string query = "select * from " + std::string(SCHEMA_NAME) + "." +
                        Dialect::TableName::COLUMN_METADATA_TABLE + " where " +
                        Tables::Column::TABLE_ID + " = $1 order by " +
                        Tables::Column::ORDINAL_POSITION;
    return query;
}

/**
 *  @brief  Returnes a SELECT stetement to get metadata:
 *  select * from table_name where column_name = $1.
 *  @param  (table_name)   [in]  metadata-table name.
 *  @param  (column_name)  [in]  column name of metadata-table.
 *  @return a SELECT stetement:
 *  select * from table_name where column_name = $1.
 */
std::string PostgreSQLDialect::dao_select_equal_to(
    const std::string &table_name, const std::string &column_name) {
    std::string query = "select * from " + std::string(SCHEMA_NAME) + "." +
                        table_name + " where " + column_name + " = $1";
    return query;
}

}  // namespace manager::metadata::db
