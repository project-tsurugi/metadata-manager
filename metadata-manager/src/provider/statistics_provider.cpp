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
#include <iostream>

#include "manager/metadata/dao/common/message.h"
#include "manager/metadata/statistics.h"

// =============================================================================
namespace manager::metadata::db {

namespace json_parser = boost::property_tree::json_parser;
using boost::property_tree::ptree;
using manager::metadata::ErrorCode;

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
ErrorCode StatisticsProvider::add_column_statistic(ptree& object,
                                                   ObjectIdType& statistic_id) {
  ErrorCode error = ErrorCode::UNKNOWN;

  // Initialization
  error = init();
  if (error != ErrorCode::OK) {
    return error;
  }

  // Parameter value check
  error = fill_parameters(object);
  if (error != ErrorCode::OK) {
    return error;
  }

  // column_id
  boost::optional<ObjectIdType> optional_column_id =
      object.get_optional<ObjectIdType>(Statistics::COLUMN_ID);
  // table_id
  boost::optional<ObjectIdType> optional_table_id =
      object.get_optional<ObjectIdType>(Statistics::TABLE_ID);
  // ordinal_position
  boost::optional<std::int64_t> optional_ordinal_position =
      object.get_optional<std::int64_t>(Statistics::ORDINAL_POSITION);
  // column_name
  boost::optional<std::string> optional_column_name =
      object.get_optional<std::string>(Statistics::COLUMN_NAME);

  // Parameter value check
  //   If column_id is not specified,
  //   and table_id and column_name or ordinal_position are not specified,
  //   it will return a parameter error.
  if (!optional_column_id) {
    if (!optional_table_id &&
        (!optional_ordinal_position || !optional_column_name)) {
      error = ErrorCode::INVALID_PARAMETER;
      return error;
    }
  }

  // statistic_name
  boost::optional<std::string> optional_statistic_name =
      object.get_optional<std::string>(Statistics::NAME);
  std::string* statistic_name =
      (optional_statistic_name ? optional_statistic_name.get_ptr()
                                 : nullptr);

  // column_statistic
  boost::optional<ptree&> optional_column_statistic =
      object.get_child_optional(Statistics::COLUMN_STATISTIC);
  ptree* column_statistic =
      (optional_column_statistic ? optional_column_statistic.get_ptr()
                                 : nullptr);

  error = session_manager_->start_transaction();
  if (error != ErrorCode::OK) {
    return error;
  }

  if (optional_column_id) {
    // Register column statistics via DAO using column_id.
    error = statistics_dao_->upsert_column_statistic(
        optional_column_id.get(), statistic_name, column_statistic);
  } else {
    // Set the key items and values to be register.
    std::string key;
    std::string value;
    if (optional_ordinal_position) {
      key = Statistics::ORDINAL_POSITION;
      value = std::to_string(optional_ordinal_position.get());
    } else {
      key = Statistics::NAME;
      value = optional_column_name.get();
    }

    // Register column statistics via DAO.
    error = statistics_dao_->upsert_column_statistic(
        optional_table_id.get(), key, value, statistic_name, column_statistic);
  }

  if (error == ErrorCode::OK) {
    error = session_manager_->commit();
  } else {
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
 * @retval ErrorCode::NOT_FOUND if the statistic id or statistic name
 *   does not exist.
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
 * @retval ErrorCode::NOT_FOUND if the table id or column id
 *   or culumn name or ordinal position does not exist.
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
 * @param (table_id)   [in]  table id.
 * @param (container)  [out] all column statistics.
 * @return ErrorCode::OK if success, otherwise an error code.
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
 * @return ErrorCode::OK if success, otherwise an error code.
 */
ErrorCode StatisticsProvider::remove_column_statistic(
    std::string_view key, std::string_view value, ObjectIdType* statistic_id) {
  ErrorCode error = ErrorCode::UNKNOWN;

  // Initialization
  error = init();
  if (error != ErrorCode::OK) {
    return error;
  }

  error = session_manager_->start_transaction();
  if (error != ErrorCode::OK) {
    return error;
  }

  ObjectIdType retval_statistic_id;
  error =
      statistics_dao_->delete_column_statistic(key, value, retval_statistic_id);
  if (error == ErrorCode::OK) {
    error = session_manager_->commit();
  } else {
    ErrorCode rollback_result = session_manager_->rollback();
    if (rollback_result != ErrorCode::OK) {
      error = rollback_result;
    }
  }

  // Set a value if statistic_id is not null.
  if ((error == ErrorCode::OK) && (statistic_id != nullptr)) {
    *statistic_id = retval_statistic_id;
  }

  return error;
}

/**
 * @brief Removes all column statistic from the column statistics table
 *   based on the given table id.
 * @param (table_id)  [in]  table id.
 * @return ErrorCode::OK if success, otherwise an error code.
 */
ErrorCode StatisticsProvider::remove_column_statistics(
    const ObjectIdType table_id) {
  ErrorCode error = ErrorCode::UNKNOWN;

  // Initialization
  error = init();
  if (error != ErrorCode::OK) {
    return error;
  }

  error = session_manager_->start_transaction();
  if (error != ErrorCode::OK) {
    return error;
  }

  error = statistics_dao_->delete_column_statistic(table_id);
  if (error == ErrorCode::OK) {
    error = session_manager_->commit();
  } else {
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
 * @return ErrorCode::OK if success, otherwise an error code.
 */
ErrorCode StatisticsProvider::remove_column_statistic(
    const ObjectIdType table_id, std::string_view key, std::string_view value,
    ObjectIdType* statistic_id) {
  ErrorCode error = ErrorCode::UNKNOWN;

  // Initialization
  error = init();
  if (error != ErrorCode::OK) {
    return error;
  }

  error = session_manager_->start_transaction();
  if (error != ErrorCode::OK) {
    return error;
  }

  ObjectIdType retval_statistic_id;
  error = statistics_dao_->delete_column_statistic(table_id, key, value,
                                                   retval_statistic_id);
  if (error == ErrorCode::OK) {
    error = session_manager_->commit();
  } else {
    ErrorCode rollback_result = session_manager_->rollback();
    if (rollback_result != ErrorCode::OK) {
      error = rollback_result;
    }
  }

  // Set a value if statistic_id is not null.
  if ((error == ErrorCode::OK) && (statistic_id != nullptr)) {
    *statistic_id = retval_statistic_id;
  }

  return error;
}

//_/_/_/_/_/_/_/_/_/_/_/_/_/_/_/_/_/_/_/_/_/_/_/_/_/_/_/_/_/_/_/_/_/_/_/_/_/_/_/
// Private method area

/**
 * @brief Checks if the parameters are correct.
 * @param (table)  [in]  metadata-object
 * @return ErrorCode::OK if success, otherwise an error code.
 */
ErrorCode StatisticsProvider::fill_parameters(ptree& object) const {
  ErrorCode error = ErrorCode::UNKNOWN;

  // Check the specified parameters.
  // column_id
  boost::optional<ObjectIdType> column_id =
      object.get_optional<ObjectIdType>(Statistics::COLUMN_ID);
  bool specified_column_id = (column_id && (column_id.get() > 0));

  // table_id
  boost::optional<ObjectIdType> table_id =
      object.get_optional<ObjectIdType>(Statistics::TABLE_ID);
  bool specified_table_id = (table_id && (table_id.get() > 0));

  // ordinal_position
  boost::optional<std::int64_t> ordinal_position =
      object.get_optional<std::int64_t>(Statistics::ORDINAL_POSITION);
  bool specified_ordinal_position =
      (ordinal_position && (ordinal_position.get() > 0));

  // column_name
  boost::optional<std::string> column_name =
      object.get_optional<std::string>(Statistics::COLUMN_NAME);
  bool specified_column_name = (column_name && !(column_name.get().empty()));

  // Check for required parameters.
  if (specified_column_id) {
    // column_id is specified.
    error = ErrorCode::OK;
  } else if (specified_table_id) {
    // table_id is specified.
    if (specified_ordinal_position || specified_column_name) {
      // ordinal_position or column_name is specified.
      error = ErrorCode::OK;
    } else {
      // ordinal_position and column_name is not specified.
      error = ErrorCode::INVALID_PARAMETER;
    }
  } else {
    // column_id and table_id is not specified.
    error = ErrorCode::INVALID_PARAMETER;
  }

  return error;
}

}  // namespace manager::metadata::db
