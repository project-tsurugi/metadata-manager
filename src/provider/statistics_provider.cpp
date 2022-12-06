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

#include "manager/metadata/statistics.h"

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
  std::shared_ptr<GenericDAO> gdao = nullptr;

  if (statistics_dao_ != nullptr) {
    // Instance of the StatisticsDAO class has already been obtained.
    error = ErrorCode::OK;
  } else {
    // Get an instance of the StatisticsDAO class.
    error = session_manager_->get_dao(GenericDAO::TableName::STATISTICS, gdao);
    if (error != ErrorCode::OK) {
      return error;
    }
    // Set StatisticsDAO instance.
    statistics_dao_ = std::static_pointer_cast<StatisticsDAO>(gdao);
  }

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

  // column_id
  boost::optional<ObjectIdType> optional_column_id =
      object.get_optional<ObjectIdType>(Statistics::COLUMN_ID);
  // table_id
  boost::optional<ObjectIdType> optional_table_id =
      object.get_optional<ObjectIdType>(Statistics::TABLE_ID);
  // column_number
  boost::optional<std::int64_t> optional_column_number =
      object.get_optional<std::int64_t>(Statistics::COLUMN_NUMBER);
  // column_name
  boost::optional<std::string> optional_column_name =
      object.get_optional<std::string>(Statistics::COLUMN_NAME);

  // statistic_name
  boost::optional<std::string> optional_statistic_name =
      object.get_optional<std::string>(Statistics::NAME);
  std::string* statistic_name =
      (optional_statistic_name ? optional_statistic_name.get_ptr() : nullptr);

  // column_statistic
  boost::optional<const ptree&> optional_column_statistic =
      object.get_child_optional(Statistics::COLUMN_STATISTIC);
  ptree column_statistic;
  if (optional_column_statistic) {
    column_statistic = optional_column_statistic.value();
  }

  // Start the transaction.
  error = session_manager_->start_transaction();
  if (error != ErrorCode::OK) {
    return error;
  }

  if (optional_column_id) {
    // Register column statistics via DAO using column_id.
    error = statistics_dao_->upsert_column_statistic(
        optional_column_id.get(), statistic_name, column_statistic,
        statistic_id);
  } else {
    // Set the key items and values to be register.
    std::string key;
    std::string value;
    if (optional_column_number) {
      key = Statistics::COLUMN_NUMBER;
      value = std::to_string(optional_column_number.get());
    } else {
      key = Statistics::COLUMN_NAME;
      value = optional_column_name.get();
    }

    // Register column statistics via DAO.
    error = statistics_dao_->upsert_column_statistic(
        optional_table_id.get(), key, value, statistic_name, column_statistic,
        statistic_id);
  }

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

  error = statistics_dao_->select_column_statistic(key, value, object);

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

  error =
      statistics_dao_->select_column_statistic(table_id, key, value, object);

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

  error = statistics_dao_->select_column_statistic(container);

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

  error = statistics_dao_->select_column_statistic(table_id, container);

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

  error = statistics_dao_->delete_column_statistic(key, value, statistic_id);
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

  error = statistics_dao_->delete_column_statistic(table_id);
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

  error = statistics_dao_->delete_column_statistic(table_id, key, value,
                                                   statistic_id);
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
