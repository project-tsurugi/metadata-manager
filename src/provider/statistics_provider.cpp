/*
 * Copyright 2021 tsurugi project.
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

#if defined(STORAGE_POSTGRESQL)
#include "manager/metadata/dao/postgresql/statistics_dao_pg.h"
#elif defined(STORAGE_JSON)
#include "manager/metadata/dao/json/statistics_dao_json.h"
#endif

// =============================================================================
namespace manager::metadata::db {

namespace json_parser = boost::property_tree::json_parser;
using boost::property_tree::ptree;

/**
 * @brief Initialize and prepare to access the metadata repository.
 * @return ErrorCode::OK if success, otherwise an error code.
 */
ErrorCode StatisticsProvider::init() {
  ErrorCode error = ErrorCode::UNKNOWN;

  // StatisticsDAO
  if (!statistics_dao_) {
    // Get an instance of the StatisticsDAO.
    statistics_dao_ = session_manager_->get_statistics_dao();
    if (!statistics_dao_) {
      error = ErrorCode::DATABASE_ACCESS_FAILURE;
      return error;
    }
    // Prepare to access table metadata.
    error = statistics_dao_->prepare();
    if (error != ErrorCode::OK) {
      statistics_dao_.reset();
      return error;
    }
  }

  error = ErrorCode::OK;
  return error;
}

/**
 * @brief Adds or updates one column statistic
 *   to the column statistics table
 *   based on the given table id and the given column ordinal position.
 *   Adds one column statistic if it not exists in the metadata repository.
 *   Updates one column statistic if it already exists.
 * @param (object)        [in]  one column statistic to add or update.
 * @param (statistic_id)  [out] ID of the added column statistic.
 * @return ErrorCode::OK if success, otherwise an error code.
 */
ErrorCode StatisticsProvider::add_column_statistic(
    const boost::property_tree::ptree& object, ObjectIdType& statistic_id) {
  ErrorCode error = ErrorCode::UNKNOWN;

  // Initialization
  error = init();
  if (error != ErrorCode::OK) {
    return error;
  }

  // Start the transaction.
  error = session_manager_->start_transaction();
  if (error != ErrorCode::OK) {
    return error;
  }

  // Register column statistics via DAO.
  error = statistics_dao_->insert(object, statistic_id);

  if (error == ErrorCode::OK) {
    // Commit the transaction.
    error = session_manager_->commit();
  } else {
    // Roll back the transaction.
    ErrorCode rollback_result = session_manager_->rollback();
    if (rollback_result != ErrorCode::OK) {
      return rollback_result;
    }
  }

  return error;
}

/**
 * @brief Gets column statistic from the column statistics table,
 *   where key = value.
 * @param (key)     [in]  key of column statistics object.
 * @param (value)   [in]  value of column statistics object.
 * @param (object)  [out] one column statistics object to get,
 *   where key = value.
 * @retval ErrorCode::OK if success.
 * @retval ErrorCode::ID_NOT_FOUND if the statistic id or column id
 *   does not exist.
 * @retval ErrorCode::NAME_NOT_FOUND if the statistic name does not exist.
 * @retval otherwise an error code.
 */
ErrorCode StatisticsProvider::get_column_statistic(
    std::string_view key, std::string_view value,
    boost::property_tree::ptree& object) {
  ErrorCode error = ErrorCode::UNKNOWN;

  // Initialization
  error = init();
  if (error != ErrorCode::OK) {
    return error;
  }

  error = statistics_dao_->select(key, value, object);

  return error;
}

/**
 * @brief Gets one column statistic from the column statistics table
 *   based on the given table id and the given column ordinal position.
 * @param (table_id)  [in]  table id.
 * @param (key)       [in]  key. column name of a column statistic table.
 * @param (value)     [in]  value to be filtered.
 * @param (object)    [out] one column statistics object to get,
 *   where key = value.
 * @retval ErrorCode::OK if success.
 * @retval ErrorCode::ID_NOT_FOUND if the ordinal position does not exist.
 * @retval ErrorCode::NAME_NOT_FOUND if the statistic name does not exist.
 * @retval otherwise an error code.
 */
ErrorCode StatisticsProvider::get_column_statistic(
    const ObjectIdType table_id, std::string_view key, std::string_view value,
    boost::property_tree::ptree& object) {
  ErrorCode error = ErrorCode::UNKNOWN;

  // Initialization
  error = init();
  if (error != ErrorCode::OK) {
    return error;
  }

#if defined(STORAGE_POSTGRESQL)
  auto statistics_dao =
      std::static_pointer_cast<StatisticsDaoPg>(statistics_dao_);
#elif defined(STORAGE_JSON)
  auto statistics_dao =
      std::static_pointer_cast<StatisticsDaoJson>(statistics_dao_);
#endif

  error = statistics_dao->select(table_id, key, value, object);

  return error;
}

/**
 * @brief Get column statistics from the column statistics table.
 *   If the column statistic does not exist, return the container as empty.
 * @param (container)  [out] all column statistics.
 * @return ErrorCode::OK if success, otherwise an error code.
 */
ErrorCode StatisticsProvider::get_column_statistics(
    std::vector<boost::property_tree::ptree>& container) {
  ErrorCode error = ErrorCode::UNKNOWN;

  // Initialization
  error = init();
  if (error != ErrorCode::OK) {
    return error;
  }

  error = statistics_dao_->select_all(container);

  return error;
}

/**
 * @brief Retrieves statistics of a column from the column statistics table
 *   based on the specified table ID.
 *   If the column statistic does not exist, return the container as empty.
 * @param (table_id)   [in]  table id.
 * @param (container)  [out] all column statistics.
 * @retval ErrorCode::OK if success.
 * @retval ErrorCode::ID_NOT_FOUND if the table id does not exist.
 * @retval otherwise an error code.
 */
ErrorCode StatisticsProvider::get_column_statistics(
    const ObjectIdType table_id,
    std::vector<boost::property_tree::ptree>& container) {
  ErrorCode error = ErrorCode::UNKNOWN;

  // Initialization
  error = init();
  if (error != ErrorCode::OK) {
    return error;
  }

#if defined(STORAGE_POSTGRESQL)
  auto statistics_dao =
      std::static_pointer_cast<StatisticsDaoPg>(statistics_dao_);
#elif defined(STORAGE_JSON)
  auto statistics_dao =
      std::static_pointer_cast<StatisticsDaoJson>(statistics_dao_);
#endif

  error = statistics_dao->select_all(table_id, container);

  return error;
}

/**
 * @brief Removes one column statistic from the column statistics table,
 *   where key = value.
 * @param (key)           [in]  key of column statistics object.
 * @param (value)         [in]  value of column statistics object.
 * @param (statistic_id)  [out] statistic id of the row deleted.
 * @retval ErrorCode::OK if success.
 * @retval ErrorCode::ID_NOT_FOUND if the statistic id does not exist.
 * @retval ErrorCode::NAME_NOT_FOUND if the statistic name does not exist.
 * @retval otherwise an error code.
 */
ErrorCode StatisticsProvider::remove_column_statistic(
    std::string_view key, std::string_view value, ObjectIdType& statistic_id) {
  ErrorCode error = ErrorCode::UNKNOWN;

  // Initialization
  error = init();
  if (error != ErrorCode::OK) {
    return error;
  }

  // Start the transaction.
  error = session_manager_->start_transaction();
  if (error != ErrorCode::OK) {
    return error;
  }

  // Remove a statistics from the column statistics table.
  error = statistics_dao_->remove(key, value, statistic_id);
  if (error == ErrorCode::OK) {
    // Commit the transaction.
    error = session_manager_->commit();
  } else {
    // Roll back the transaction.
    ErrorCode rollback_result = session_manager_->rollback();
    if (rollback_result != ErrorCode::OK) {
      error = rollback_result;
    }
  }

  return error;
}

/**
 * @brief Removes all column statistic from the column statistics table
 *   based on the given table id.
 * @param (table_id)  [in]  table id.
 * @retval ErrorCode::OK if success.
 * @retval ErrorCode::ID_NOT_FOUND if the table id does not exist.
 * @retval otherwise an error code.
 */
ErrorCode StatisticsProvider::remove_column_statistics(
    const ObjectIdType table_id) {
  ErrorCode error = ErrorCode::UNKNOWN;

  ObjectId removed_id = 0;
  // Remove a statistics from the column statistics table.
  error = this->remove_column_statistic(Statistics::TABLE_ID,
                                        std::to_string(table_id), removed_id);

  return error;
}

/**
 * @brief Removes one column statistic from the column statistics table
 *   based on the given table id and the given column name or ordinal position.
 * @param (table_id)      [in]  table id.
 * @param (key)           [in]  key. column name of a column statistic table.
 * @param (value)         [in]  value to be filtered.
 * @param (statistic_id)  [out] statistic id of the row deleted.
 * @retval ErrorCode::OK if success.
 * @retval ErrorCode::ID_NOT_FOUND if the ordinal position does not exist.
 * @retval ErrorCode::NAME_NOT_FOUND if the statistic name does not exist.
 * @retval otherwise an error code.
 */
ErrorCode StatisticsProvider::remove_column_statistic(
    const ObjectIdType table_id, std::string_view key, std::string_view value,
    ObjectIdType& statistic_id) {
  ErrorCode error = ErrorCode::UNKNOWN;

  // Initialization
  error = init();
  if (error != ErrorCode::OK) {
    return error;
  }

  // Start the transaction.
  error = session_manager_->start_transaction();
  if (error != ErrorCode::OK) {
    return error;
  }

#if defined(STORAGE_POSTGRESQL)
  auto statistics_dao =
      std::static_pointer_cast<StatisticsDaoPg>(statistics_dao_);
#elif defined(STORAGE_JSON)
  auto statistics_dao =
      std::static_pointer_cast<StatisticsDaoJson>(statistics_dao_);
#endif

  // Remove a statistics from the column statistics table.
  error = statistics_dao->remove(table_id, key, value, statistic_id);
  if (error == ErrorCode::OK) {
    // Commit the transaction.
    error = session_manager_->commit();
  } else {
    // Roll back the transaction.
    ErrorCode rollback_result = session_manager_->rollback();
    if (rollback_result != ErrorCode::OK) {
      error = rollback_result;
    }
  }

  return error;
}

}  // namespace manager::metadata::db
