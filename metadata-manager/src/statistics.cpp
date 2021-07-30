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
#include "manager/metadata/statistics.h"

#include <memory>

#include "manager/metadata/dao/common/message.h"
#include "manager/metadata/provider/statistics_provider.h"

// =============================================================================
namespace {

std::unique_ptr<manager::metadata::db::StatisticsProvider> provider = nullptr;

}  // namespace

// =============================================================================
namespace manager::metadata {

using manager::metadata::ErrorCode;
using manager::metadata::db::Message;

/**
 *  @brief  Constructor
 *  @param  (database) [in]  database name.
 *  @param  (component) [in]  component name.
 */
Statistics::Statistics(std::string_view database, std::string_view component)
    : Metadata(database, component) {
  // Create the provider.
  provider = std::make_unique<db::StatisticsProvider>();
}

/**
 *  @brief  Initialization.
 *  @param  none.
 *  @return  ErrorCode::OK if success, otherwise an error code.
 */
ErrorCode Statistics::init() {
  // Initialize the provider.
  ErrorCode error = provider->init();

  return error;
}

/**
 *  @brief  Adds or updates one column statistic
 *  to the column statistics table
 *  based on the given table id and the given column ordinal position.
 *  Adds one column statistic if it not exists in the metadata repository.
 *  Updates one column statistic if it already exists.
 *  @param  (table_id)          [in]  table id.
 *  @param  (ordinal_position)  [in]  column ordinal position.
 *  @param  (column_statistic)  [in]  one column statistic to add or update.
 *  @return ErrorCode::OK if success, otherwise an error code.
 */
ErrorCode Statistics::add_one_column_statistic(
    ObjectIdType table_id, ObjectIdType ordinal_position,
    boost::property_tree::ptree &column_statistic) {
  // Adds or updates the column statistic through the provider.
  ErrorCode error = provider->add_column_statistic(table_id, ordinal_position,
                                                   column_statistic);

  return error;
}

/**
 *  @brief  Adds or updates table statistic
 *  to the table metadata table based on the given table id.
 *  Adds table statistic if it not exists in the metadata repository.
 *  Updates table statistic if it already exists.
 *  @param  (table_id)   [in]  table id.
 *  @param  (reltuples)  [in]  the number of rows to add or update.
 *  @return ErrorCode::OK if success, otherwise an error code.
 */
ErrorCode Statistics::add_table_statistic(ObjectIdType table_id,
                                          float reltuples) {
  // Adds or updates the table statistic through the provider.
  ErrorCode error = provider->add_table_statistic(table_id, reltuples);

  return error;
}

/**
 *  @brief  Adds or updates table statistic
 *  to the table metadata table based on the given table name.
 *  Adds table statistic if it not exists in the metadata repository.
 *  Updates table statistic if it already exists.
 *  @param  (table_name) [in]  table name.
 *  @param  (reltuples)  [in]  the number of rows to add or update.
 *  @param  (table_id)   [out] table id of the row updated.
 *  @return ErrorCode::OK if success, otherwise an error code.
 */
ErrorCode Statistics::add_table_statistic(std::string_view table_name,
                                          float reltuples,
                                          ObjectIdType *table_id) {
  // Adds or updates the table statistic through the provider.
  ErrorCode error =
      provider->add_table_statistic(table_name, reltuples, table_id);

  return error;
}

/**
 *  @brief  Gets one column statistic from the column statistics table
 *  based on the given table id and the given column ordinal position.
 *  @param  (table_id)          [in]  table id.
 *  @param  (ordinal_position)  [in]  column ordinal position.
 *  @param  (column_statistic)  [out] one column statistic
 *  with the specified table id and column ordinal position.
 *  @return ErrorCode::OK if success, otherwise an error code.
 */
ErrorCode Statistics::get_one_column_statistic(
    ObjectIdType table_id, ObjectIdType ordinal_position,
    ColumnStatistic &column_statistic) {
  // Get the column statistic through the provider.
  ErrorCode error = provider->get_column_statistic(table_id, ordinal_position,
                                                   column_statistic);

  return error;
}

/**
 *  @brief  Gets all column statistics from the column statistics table
 *  based on the given table id.
 *  @param  (table_id)           [in]  table id.
 *  @param  (column_statistics)  [out] all column statistics
 *  with the specified table id.
 *  key : column ordinal position
 *  value : one column statistic
 *  @return ErrorCode::OK if success, otherwise an error code.
 */
ErrorCode Statistics::get_all_column_statistics(
    ObjectIdType table_id,
    std::unordered_map<ObjectIdType, ColumnStatistic> &column_statistics) {
  // Get the all column statistics through the provider.
  ErrorCode error =
      provider->get_all_column_statistics(table_id, column_statistics);

  return error;
}

/**
 *  @brief  Gets one table statistic from the table metadata table
 *  based on the given table id.
 *  @param  (table_id)         [in]  table id.
 *  @param  (table_statistic)  [out] one table statistic
 *  with the specified table id.
 *  @return ErrorCode::OK if success, otherwise an error code.
 */
ErrorCode Statistics::get_table_statistic(
    ObjectIdType table_id, manager::metadata::TableStatistic &table_statistic) {
  // Get the table statistic through the provider.
  ErrorCode error = provider->get_table_statistic(table_id, table_statistic);

  return error;
}

/**
 *  @brief  Gets one table statistic from the table metadata table
 *  based on the given table name.
 *  @param  (table_name)       [in]  table name.
 *  @param  (table_statistic)  [out] one table statistic
 *  with the specified table name.
 *  @return ErrorCode::OK if success, otherwise an error code.
 */
ErrorCode Statistics::get_table_statistic(
    std::string_view table_name,
    manager::metadata::TableStatistic &table_statistic) {
  // Get the table statistic through the provider.
  ErrorCode error = provider->get_table_statistic(table_name, table_statistic);

  return error;
}

/**
 *  @brief  Removes one column statistic from the column statistics table
 *  based on the given table id and the given column ordinal position.
 *  @param  (table_id)          [in]  table id.
 *  @param  (ordinal_position)  [in]  column ordinal position.
 *  @return ErrorCode::OK if success, otherwise an error code.
 */
ErrorCode Statistics::remove_one_column_statistic(
    ObjectIdType table_id, ObjectIdType ordinal_position) {
  // Remove the column statistic through the provider.
  ErrorCode error =
      provider->remove_column_statistic(table_id, ordinal_position);

  return error;
}

/**
 *  @brief  Removes all column statistics
 *  from the column statistics table
 *  based on the given table id.
 *  @param  (table_id)          [in]  table id.
 *  @return ErrorCode::OK if success, otherwise an error code.
 */
ErrorCode Statistics::remove_all_column_statistics(ObjectIdType table_id) {
  // Remove the all column statistics through the provider.
  ErrorCode error = provider->remove_all_column_statistics(table_id);

  return error;
}

}  // namespace manager::metadata
