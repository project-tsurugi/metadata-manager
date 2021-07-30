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
#include "manager/metadata/provider/statistics_provider.h"

#include <boost/property_tree/json_parser.hpp>
#include <iostream>

#include "manager/metadata/dao/common/message.h"

// =============================================================================
namespace manager::metadata::db {

namespace json_parser = boost::property_tree::json_parser;
using boost::property_tree::ptree;
using manager::metadata::ErrorCode;

/**
 *  @brief  Initialize and prepare to access the metadata repository.
 *  @return  ErrorCode::OK if success, otherwise an error code.
 */
ErrorCode StatisticsProvider::init() {
  ErrorCode result = ErrorCode::OK;
  std::shared_ptr<GenericDAO> gdao = nullptr;

  if (tables_dao_ == nullptr) {
    // Get an instance of the TablesDAO class.
    result = session_manager_->get_dao(GenericDAO::TableName::TABLES, gdao);
    tables_dao_ = (result == ErrorCode::OK)
                      ? std::static_pointer_cast<TablesDAO>(gdao)
                      : nullptr;
  }
  if ((statistics_dao_ == nullptr) && (result == ErrorCode::OK)) {
    // Get an instance of the StatisticsDAO class.
    result = session_manager_->get_dao(GenericDAO::TableName::STATISTICS, gdao);
    statistics_dao_ = (result == ErrorCode::OK)
                          ? std::static_pointer_cast<StatisticsDAO>(gdao)
                          : nullptr;
  }

  return result;
}

/**
 *  @brief  Adds or updates table statistic
 *  to the table metadata table based on the given table id.
 *  Adds table statistic if it not exists in the metadata repository.
 *  Updates table statistic if it already exists.
 *  @param  (table_id)   [in]  table id.
 *  @param  (reltuples)  [in]  the number of rows to add or update.
 *  @return  ErrorCode::OK if success, otherwise an error code.
 */
ErrorCode StatisticsProvider::add_table_statistic(ObjectIdType table_id,
                                                  float reltuples) {
  // Initialization
  ErrorCode result = init();
  if (result != ErrorCode::OK) {
    return result;
  }

  if (table_id <= 0) {
    return ErrorCode::INVALID_PARAMETER;
  }

  result = session_manager_->start_transaction();
  if (result != ErrorCode::OK) {
    return result;
  }

  result = tables_dao_->update_reltuples_by_table_id(reltuples, table_id);
  if (result == ErrorCode::OK) {
    result = session_manager_->commit();
  } else {
    ErrorCode rollback_result = session_manager_->rollback();
    if (rollback_result != ErrorCode::OK) {
      return rollback_result;
    }
  }
  return result;
}

/**
 *  @brief  Adds or updates table statistic
 *  to the table metadata table based on the given table name.
 *  Adds table statistic if it not exists in the metadata repository.
 *  Updates table statistic if it already exists.
 *  @param  (table_name) [in]  table name.
 *  @param  (reltuples)  [in]  the number of rows to add or update.
 *  @param  (table_id)   [out] table id of the row updated.
 *  @return  ErrorCode::OK if success, otherwise an error code.
 */
ErrorCode StatisticsProvider::add_table_statistic(std::string_view table_name,
                                                  float reltuples,
                                                  ObjectIdType *table_id) {
  // Initialization
  ErrorCode result = init();
  if (result != ErrorCode::OK) {
    return result;
  }

  if (table_name.empty()) {
    return ErrorCode::INVALID_PARAMETER;
  }

  result = session_manager_->start_transaction();
  if (result != ErrorCode::OK) {
    return result;
  }

  ObjectIdType retval_object_id = 0;
  result = tables_dao_->update_reltuples_by_table_name(
      reltuples, table_name.data(), retval_object_id);
  if (result == ErrorCode::OK) {
    result = session_manager_->commit();
    if ((result == ErrorCode::OK) && (table_id != nullptr)) {
      *table_id = retval_object_id;
    }
  } else {
    ErrorCode rollback_result = session_manager_->rollback();
    if (rollback_result != ErrorCode::OK) {
      return rollback_result;
    }
  }
  return result;
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
 *  @return  ErrorCode::OK if success, otherwise an error code.
 */
ErrorCode StatisticsProvider::add_column_statistic(
    ObjectIdType table_id, ObjectIdType ordinal_position,
    ptree &column_statistic) {
  // Initialization
  ErrorCode result = init();
  if (result != ErrorCode::OK) {
    return result;
  }

  if ((table_id <= 0) || (ordinal_position <= 0)) {
    return ErrorCode::INVALID_PARAMETER;
  }

  std::string s_column_statistic;
  if (!column_statistic.empty()) {
    std::stringstream ss;
    try {
      json_parser::write_json(ss, column_statistic, false);
    } catch (json_parser::json_parser_error &e) {
      std::cerr << Message::WRITE_JSON_FAILURE << e.what() << std::endl;
      return ErrorCode::INTERNAL_ERROR;
    } catch (...) {
      std::cerr << Message::WRITE_JSON_FAILURE << std::endl;
      return ErrorCode::INTERNAL_ERROR;
    }

    s_column_statistic = ss.str();
  }

  result = session_manager_->start_transaction();
  if (result != ErrorCode::OK) {
    return result;
  }

  result =
      statistics_dao_
          ->upsert_one_column_statistic_by_table_id_column_ordinal_position(
              table_id, ordinal_position, s_column_statistic);
  if (result == ErrorCode::OK) {
    result = session_manager_->commit();
  } else {
    ErrorCode rollback_result = session_manager_->rollback();
    if (rollback_result != ErrorCode::OK) {
      return rollback_result;
    }
  }
  return result;
}

/**
 *  @brief  Gets one column statistic from the column statistics table
 *  based on the given table id and the given column ordinal position.
 *  @param  (table_id)          [in]  table id.
 *  @param  (ordinal_position)  [in]  column ordinal position.
 *  @param  (column_statistic)  [out] one column statistic
 *  with the specified table id and column ordinal position.
 *  @return  ErrorCode::OK if success, otherwise an error code.
 */
ErrorCode StatisticsProvider::get_column_statistic(
    ObjectIdType table_id, ObjectIdType ordinal_position,
    ColumnStatistic &column_statistic) {
  // Initialization
  ErrorCode result = init();
  if (result != ErrorCode::OK) {
    return result;
  }

  if ((table_id <= 0) || (ordinal_position <= 0)) {
    return ErrorCode::INVALID_PARAMETER;
  }

  result =
      statistics_dao_
          ->select_one_column_statistic_by_table_id_column_ordinal_position(
              table_id, ordinal_position, column_statistic);

  return result;
}

/**
 *  @brief  Gets all column statistics from the column statistics table
 *  based on the given table id.
 *  @param  (table_id)           [in]  table id.
 *  @param  (column_statistics)  [out] all column statistics
 *  with the specified table id.
 *  key : column ordinal position
 *  value : one column statistic
 *  @return  ErrorCode::OK if success, otherwise an error code.
 */
ErrorCode StatisticsProvider::get_all_column_statistics(
    ObjectIdType table_id,
    std::unordered_map<ObjectIdType, ColumnStatistic> &column_statistics) {
  // Initialization
  ErrorCode result = init();
  if (result != ErrorCode::OK) {
    return result;
  }

  if (table_id <= 0) {
    return ErrorCode::INVALID_PARAMETER;
  }

  result = statistics_dao_->select_all_column_statistic_by_table_id(
      table_id, column_statistics);

  return result;
}

/**
 *  @brief  Gets one table statistic from the table metadata table
 *  based on the given table id.
 *  @param  (table_id)         [in]  table id.
 *  @param  (table_statistic)  [out] one table statistic
 *  with the specified table id.
 *  @return  ErrorCode::OK if success, otherwise an error code.
 */
ErrorCode StatisticsProvider::get_table_statistic(
    ObjectIdType table_id, manager::metadata::TableStatistic &table_statistic) {
  // Initialization
  ErrorCode result = init();
  if (result != ErrorCode::OK) {
    return result;
  }

  if (table_id <= 0) {
    return ErrorCode::INVALID_PARAMETER;
  }

  result = tables_dao_->select_table_statistic_by_table_id(table_id,
                                                           table_statistic);

  return result;
}

/**
 *  @brief  Gets one table statistic from the table metadata table
 *  based on the given table name.
 *  @param  (table_name)       [in]  table name.
 *  @param  (table_statistic)  [out] one table statistic
 *  with the specified table name.
 *  @return  ErrorCode::OK if success, otherwise an error code.
 */
ErrorCode StatisticsProvider::get_table_statistic(
    std::string_view table_name,
    manager::metadata::TableStatistic &table_statistic) {
  // Initialization
  ErrorCode result = init();
  if (result != ErrorCode::OK) {
    return result;
  }

  if (table_name.empty()) {
    return ErrorCode::INVALID_PARAMETER;
  }

  result = tables_dao_->select_table_statistic_by_table_name(table_name.data(),
                                                             table_statistic);

  return result;
}

/**
 *  @brief  Removes one column statistic from the column statistics table
 *  based on the given table id and the given column ordinal position.
 *  @param  (table_id)          [in]  table id.
 *  @param  (ordinal_position)  [in]  column ordinal position.
 *  @return  ErrorCode::OK if success, otherwise an error code.
 */
ErrorCode StatisticsProvider::remove_column_statistic(
    ObjectIdType table_id, ObjectIdType ordinal_position) {
  // Initialization
  ErrorCode result = init();
  if (result != ErrorCode::OK) {
    return result;
  }

  if ((table_id <= 0) || (ordinal_position <= 0)) {
    return ErrorCode::INVALID_PARAMETER;
  }

  result = session_manager_->start_transaction();
  if (result != ErrorCode::OK) {
    return result;
  }

  result =
      statistics_dao_
          ->delete_one_column_statistic_by_table_id_column_ordinal_position(
              table_id, ordinal_position);
  if (result == ErrorCode::OK) {
    result = session_manager_->commit();
  } else {
    ErrorCode rollback_result = session_manager_->rollback();
    if (rollback_result != ErrorCode::OK) {
      return rollback_result;
    }
  }
  return result;
}

/**
 *  @brief  Removes all column statistics
 *  from the column statistics table
 *  based on the given table id.
 *  @param  (table_id)          [in]  table id.
 *  @return  ErrorCode::OK if success, otherwise an error code.
 */
ErrorCode StatisticsProvider::remove_all_column_statistics(
    ObjectIdType table_id) {
  // Initialization
  ErrorCode result = init();
  if (result != ErrorCode::OK) {
    return result;
  }

  if (table_id <= 0) {
    return ErrorCode::INVALID_PARAMETER;
  }

  result = session_manager_->start_transaction();
  if (result != ErrorCode::OK) {
    return result;
  }

  result = statistics_dao_->delete_all_column_statistic_by_table_id(table_id);
  if (result == ErrorCode::OK) {
    result = session_manager_->commit();
  } else {
    ErrorCode rollback_result = session_manager_->rollback();
    if (rollback_result != ErrorCode::OK) {
      return rollback_result;
    }
  }
  return result;
}

}  // namespace manager::metadata::db
