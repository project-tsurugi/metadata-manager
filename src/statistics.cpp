/*
 * Copyright 2020-2021 tsurugi project.
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

#include <boost/format.hpp>

#include "manager/metadata/common/message.h"
#include "manager/metadata/helper/logging_helper.h"
#include "manager/metadata/provider/statistics_provider.h"

// =============================================================================
namespace {

std::unique_ptr<manager::metadata::db::StatisticsProvider> provider = nullptr;

}  // namespace

// =============================================================================
namespace manager::metadata {

/**
 * @brief Constructor
 * @param (database)   [in]  database name.
 * @param (component)  [in]  component name.
 */
Statistics::Statistics(std::string_view database, std::string_view component)
    : Metadata(database, component) {
  // Create the provider.
  provider = std::make_unique<db::StatisticsProvider>();
}

/**
 * @brief Initialization.
 * @param none.
 * @return ErrorCode::OK if success, otherwise an error code.
 */
ErrorCode Statistics::init() const {
  ErrorCode error = ErrorCode::UNKNOWN;

  // Log of API function start.
  log::function_start("Statistics::init()");

  // Initialize the provider.
  error = provider->init();

  // Log of API function finish.
  log::function_finish("Statistics::init()", error);

  return error;
}

/**
 * @brief Add column statistics to statistics table.
 * @param (object) [in]  statistics to add.
 * @return ErrorCode::OK if success, otherwise an error code.
 */
ErrorCode Statistics::add(const boost::property_tree::ptree& object) const {
  ErrorCode error = ErrorCode::UNKNOWN;

  // Adds the column statistics through the class method.
  error = this->add(object, nullptr);

  return error;
}

/**
 * @brief Add column statistics to statistics table.
 * @param (object)      [in]  statistics to add.
 * @param (object_id)   [out] ID of the added statistic.
 * @return ErrorCode::OK if success, otherwise an error code.
 */
ErrorCode Statistics::add(const boost::property_tree::ptree& object,
                          ObjectIdType* object_id) const {
  ErrorCode error = ErrorCode::UNKNOWN;

  // Log of API function start.
  log::function_start("Statistics::add()");

  // Parameter value check.
  error = param_check_statistics_add(object);

  // Adds the column statistics through the provider.
  ObjectIdType retval_object_id = 0;
  if (error == ErrorCode::OK) {
    error = provider->add_column_statistic(object, retval_object_id);
  }

  // Set a value if object_id is not null.
  if ((error == ErrorCode::OK) && (object_id != nullptr)) {
    *object_id = retval_object_id;
  }

  // Log of API function finish.
  log::function_finish("Statistics::add()", error);

  return error;
}

/**
 * @brief Get column statistics.
 * @param (object_id)  [in]  statistic id.
 * @param (object)     [out] column statistics with the specified ID.
 * @retval ErrorCode::OK if success.
 * @retval ErrorCode::ID_NOT_FOUND if the statistic id does not exist.
 * @retval otherwise an error code.
 */
ErrorCode Statistics::get(const ObjectIdType object_id,
                          boost::property_tree::ptree& object) const {
  ErrorCode error = ErrorCode::UNKNOWN;

  // Log of API function start.
  log::function_start("Statistics::get(StatisticId)");

  // Parameter value check.
  if (object_id > 0) {
    error = ErrorCode::OK;
  } else {
    LOG_WARNING
        << "An out-of-range value (0 or less) was specified for StatisticId.: "
        << object_id;
    error = ErrorCode::ID_NOT_FOUND;
  }

  // Get the column statistics through the provider.
  if (error == ErrorCode::OK) {
    error = provider->get_column_statistic(Statistics::ID,
                                           std::to_string(object_id), object);
  }

  // Log of API function finish.
  log::function_finish("Statistics::get(StatisticId)", error);

  return error;
}

/**
 * @brief Get column statistics object based on statistic name.
 * @param (object_name)  [in]  statistic name. (Value of "name" key.)
 * @param (object)       [out] column statistics object
 *   with the specified name.
 * @retval ErrorCode::OK if success.
 * @retval ErrorCode::NAME_NOT_FOUND if the statistic name does not exist.
 * @retval otherwise an error code.
 */
ErrorCode Statistics::get(std::string_view object_name,
                          boost::property_tree::ptree& object) const {
  ErrorCode error = ErrorCode::UNKNOWN;

  // Log of API function start.
  log::function_start("Statistics::get(StatisticName)");

  // Parameter value check.
  if (!object_name.empty()) {
    error = ErrorCode::OK;
  } else {
    LOG_WARNING << "An empty value was specified for StatisticName.";
    error = ErrorCode::NAME_NOT_FOUND;
  }

  // Get the column statistics through the provider.
  if (error == ErrorCode::OK) {
    error =
        provider->get_column_statistic(Statistics::NAME, object_name, object);
  }

  // Log of API function finish.
  log::function_finish("Statistics::get(StatisticName)", error);

  return error;
}

/**
 * @brief Gets one column statistic from the column statistics table
 *   based on the given column id.
 * @param (column_id)  [in]  column id.
 * @param (object)     [out] one column statistic
 *   with the specified column id.
 * @retval ErrorCode::OK if success.
 * @retval ErrorCode::ID_NOT_FOUND if the column id does not exist.
 * @retval otherwise an error code.
 */
ErrorCode Statistics::get_by_column_id(
    const ObjectIdType column_id, boost::property_tree::ptree& object) const {
  ErrorCode error = ErrorCode::UNKNOWN;

  // Log of API function start.
  log::function_start("Statistics::get_by_column_id()");

  // Parameter value check.
  if (column_id > 0) {
    error = ErrorCode::OK;
  } else {
    LOG_WARNING
        << "An out-of-range value (0 or less) was specified for ColumnId.: "
        << column_id;
    error = ErrorCode::ID_NOT_FOUND;
  }

  // Get the column statistics through the provider.
  if (error == ErrorCode::OK) {
    error = provider->get_column_statistic(Statistics::COLUMN_ID,
                                           std::to_string(column_id), object);
  }

  // Log of API function finish.
  log::function_finish("Statistics::get_by_column_id()", error);

  return error;
}

/**
 * @brief Gets one column statistic from the column statistics table
 *   based on the given table id and the given column ordinal position.
 * @param (table_id)          [in]  table id.
 * @param (ordinal_position)  [in]  column ordinal position.
 * @param (object)            [out] one column statistic
 *   with the specified table id and column ordinal position.
 * @retval ErrorCode::OK if success.
 * @retval ErrorCode::ID_NOT_FOUND if the table id or ordinal position
 *   does not exist.
 * @retval otherwise an error code.
 */
ErrorCode Statistics::get_by_column_number(
    const ObjectIdType table_id, const int64_t ordinal_position,
    boost::property_tree::ptree& object) const {
  ErrorCode error = ErrorCode::UNKNOWN;

  // Log of API function start.
  log::function_start("Statistics::get_by_column_number()");

  // Parameter value check.
  if ((table_id > 0) && (ordinal_position > 0)) {
    error = ErrorCode::OK;
  } else {
    LOG_WARNING << "An out-of-range value (0 or less) was specified for "
                   "TableId or OrdinalPosition.: "
                << "TableId: " << table_id
                << ", OrdinalPosition: " << ordinal_position;
    error = ErrorCode::ID_NOT_FOUND;
  }

  // Get the column statistic through the provider.
  if (error == ErrorCode::OK) {
    error = provider->get_column_statistic(table_id, Statistics::COLUMN_NUMBER,
                                           std::to_string(ordinal_position),
                                           object);
  }

  // Log of API function finish.
  log::function_finish("Statistics::get_by_column_number()", error);

  return error;
}

/**
 * @brief Gets one column statistic from the column statistics table
 *   based on the given table id and the given column name.
 * @param (table_id)     [in]  table id.
 * @param (column_name)  [in]  column name.
 * @param (object)       [out] one column statistic
 *   with the specified table id and column name.
 * @retval ErrorCode::OK if success.
 * @retval ErrorCode::ID_NOT_FOUND if the table id does not exist.
 * @retval ErrorCode::NAME_NOT_FOUND if the column name does not exist.
 * @retval otherwise an error code.
 */
ErrorCode Statistics::get_by_column_name(
    const ObjectIdType table_id, std::string_view column_name,
    boost::property_tree::ptree& object) const {
  ErrorCode error = ErrorCode::UNKNOWN;

  // Log of API function start.
  log::function_start("Statistics::get_by_column_name()");

  // Parameter value check.
  if ((table_id > 0) && (!column_name.empty())) {
    error = ErrorCode::OK;
  } else if (table_id <= 0) {
    LOG_WARNING
        << "An out-of-range value (0 or less) was specified for TableId.: "
        << table_id;
    error = ErrorCode::ID_NOT_FOUND;
  } else {
    LOG_WARNING << "An empty value was specified for ColumnName.";
    error = ErrorCode::NAME_NOT_FOUND;
  }

  // Get the column statistic through the provider.
  if (error == ErrorCode::OK) {
    error = provider->get_column_statistic(table_id, Statistics::COLUMN_NAME,
                                           column_name, object);
  }

  // Log of API function finish.
  log::function_finish("Statistics::get_by_column_name()", error);

  return error;
}

/**
 * @brief Gets all column statistics from the column statistics table.
 *   If the column statistic does not exist, return the container as empty.
 * @param (container)  [out] Container for statistics-objects.
 * @return ErrorCode::OK if success, otherwise an error code.
 */
ErrorCode Statistics::get_all(
    std::vector<boost::property_tree::ptree>& container) const {
  ErrorCode error = ErrorCode::UNKNOWN;

  // Log of API function start.
  log::function_start("Statistics::get_all()");

  // Get the column statistic through the provider.
  error = provider->get_column_statistics(container);

  // Log of API function finish.
  log::function_finish("Statistics::get_all()", error);

  return error;
}

/**
 * @brief Gets all column statistics from the column statistics table
 *   based on the given table id.
 *   If the column statistic does not exist, return the container as empty.
 * @param (table_id)   [in]  table id.
 * @param (container)  [out] Container for statistics-objects.
 *   with the specified table id.
 *   key : column ordinal position
 *   value : one column statistic
 * @return ErrorCode::OK if success, otherwise an error code.
 */
ErrorCode Statistics::get_all(
    const ObjectIdType table_id,
    std::vector<boost::property_tree::ptree>& container) const {
  ErrorCode error = ErrorCode::UNKNOWN;

  // Log of API function start.
  log::function_start("Statistics::get_all(TableId)");

  // Parameter value check.
  if (table_id > 0) {
    error = ErrorCode::OK;
  } else {
    LOG_WARNING
        << "An out-of-range value (0 or less) was specified for TableId.: "
        << table_id;
    error = ErrorCode::ID_NOT_FOUND;
  }

  // Get the column statistic through the provider.
  if (error == ErrorCode::OK) {
    error = provider->get_column_statistics(table_id, container);
  }

  // Log of API function finish.
  log::function_finish("Statistics::get_all(ObjectId)", error);

  return error;
}

/**
 * @brief Remove column statistics based on the given statistics id.
 * @param (object_id)  [in]  statistic id.
 * @retval ErrorCode::OK if success.
 * @retval ErrorCode::ID_NOT_FOUND if the statistic id does not exist.
 * @retval otherwise an error code.
 */
ErrorCode Statistics::remove(const ObjectIdType object_id) const {
  ErrorCode error = ErrorCode::UNKNOWN;

  // Log of API function start.
  log::function_start("Statistics::remove(StatisticId)");

  // Parameter value check.
  if (object_id > 0) {
    error = ErrorCode::OK;
  } else {
    LOG_WARNING
        << "An out-of-range value (0 or less) was specified for StatisticId.: "
        << object_id;
    error = ErrorCode::ID_NOT_FOUND;
  }

  if (error == ErrorCode::OK) {
    ObjectIdType retval_object_id = 0;
    // Remove the column statistic through the provider.
    error = provider->remove_column_statistic(
        Statistics::ID, std::to_string(object_id), retval_object_id);
  }

  // Log of API function finish.
  log::function_finish("Statistics::remove(StatisticId)", error);

  return error;
}

/**
 * @brief Remove column statistics based on the given statistics name.
 * @param (object_name)  [in]  statistic name.
 * @param (object_id)    [out] object id of statistic removed.
 * @retval ErrorCode::OK if success.
 * @retval ErrorCode::NAME_NOT_FOUND if the statistic name does not exist.
 * @retval otherwise an error code.
 */
ErrorCode Statistics::remove(std::string_view object_name,
                             ObjectIdType* object_id) const {
  ErrorCode error = ErrorCode::UNKNOWN;

  // Log of API function start.
  log::function_start("Statistics::remove(StatisticName)");

  // Parameter value check.
  if (!object_name.empty()) {
    error = ErrorCode::OK;
  } else {
    LOG_WARNING << "An empty value was specified for StatisticName.";
    error = ErrorCode::NAME_NOT_FOUND;
  }

  ObjectIdType retval_object_id = 0;
  if (error == ErrorCode::OK) {
    // Remove the table metadata through the provider.
    error = provider->remove_column_statistic(Statistics::NAME, object_name,
                                              retval_object_id);
  }

  // Set a value if object_id is not null.
  if ((error == ErrorCode::OK) && (object_id != nullptr)) {
    *object_id = retval_object_id;
  }

  // Log of API function finish.
  log::function_finish("Statistics::remove(StatisticName)", error);

  return error;
}

/**
 * @brief Removes all column statistics
 *   from the column statistics table based on the given table id.
 * @param (table_id)  [in]  table id.
 * @retval ErrorCode::OK if success.
 * @retval ErrorCode::ID_NOT_FOUND if the table id does not exist.
 * @retval otherwise an error code.
 */
ErrorCode Statistics::remove_by_table_id(const ObjectIdType table_id) const {
  ErrorCode error = ErrorCode::UNKNOWN;

  // Log of API function start.
  log::function_start("Statistics::remove_by_table_id()");

  // Parameter value check.
  if (table_id > 0) {
    error = ErrorCode::OK;
  } else {
    LOG_WARNING
        << "An out-of-range value (0 or less) was specified for TableId.: "
        << table_id;
    error = ErrorCode::ID_NOT_FOUND;
  }

  if (error == ErrorCode::OK) {
    // Remove the all column statistics through the provider.
    error = provider->remove_column_statistics(table_id);
  }

  // Log of API function finish.
  log::function_finish("Statistics::remove_by_table_id()", error);

  return error;
}

/**
 * @brief Removes column statistic from the column statistics table
 *   based on the given column id.
 * @param (column_id)  [in]  column id.
 * @retval ErrorCode::OK if success.
 * @retval ErrorCode::ID_NOT_FOUND if the column id does not exist.
 * @retval otherwise an error code.
 */
ErrorCode Statistics::remove_by_column_id(const ObjectIdType column_id) const {
  ErrorCode error = ErrorCode::UNKNOWN;

  // Log of API function start.
  log::function_start("Statistics::remove_by_column_id()");

  // Parameter value check.
  if (column_id > 0) {
    error = ErrorCode::OK;
  } else {
    LOG_WARNING
        << "An out-of-range value (0 or less) was specified for ColumnId.: "
        << column_id;
    error = ErrorCode::ID_NOT_FOUND;
  }

  if (error == ErrorCode::OK) {
    ObjectIdType retval_object_id = 0;
    // Remove the column statistic through the provider.
    error = provider->remove_column_statistic(
        Statistics::COLUMN_ID, std::to_string(column_id), retval_object_id);
  }

  // Log of API function finish.
  log::function_finish("Statistics::remove_by_column_id()", error);

  return error;
}

/**
 * @brief Removes column statistic from the column statistics table
 *   based on the given table id and the given column ordinal position.
 * @param (table_id)          [in]  table id.
 * @param (ordinal_position)  [in]  column ordinal position.
 * @retval ErrorCode::OK if success.
 * @retval ErrorCode::ID_NOT_FOUND if the table id or ordinal position does not
 * exist.
 * @retval otherwise an error code.
 */
ErrorCode Statistics::remove_by_column_number(
    const ObjectIdType table_id, const int64_t ordinal_position) const {
  ErrorCode error = ErrorCode::UNKNOWN;

  // Log of API function start.
  log::function_start("Statistics::remove_by_column_number()");

  // Parameter value check.
  if ((table_id > 0) && (ordinal_position > 0)) {
    error = ErrorCode::OK;
  } else {
    LOG_WARNING << "An out-of-range value (0 or less) was specified for "
                   "TableId or OrdinalPosition.: "
                << "TableId: " << table_id
                << ", OrdinalPosition: " << ordinal_position;
    error = ErrorCode::ID_NOT_FOUND;
  }

  if (error == ErrorCode::OK) {
    ObjectIdType retval_object_id = 0;
    // Remove the column statistic through the provider.
    error = provider->remove_column_statistic(
        table_id, Statistics::COLUMN_NUMBER, std::to_string(ordinal_position),
        retval_object_id);
  }

  // Log of API function finish.
  log::function_finish("Statistics::remove_by_column_number()", error);

  return error;
}

/**
 * @brief Removes one column statistic from the column statistics table
 *   based on the given table id and the given column ordinal position.
 * @param (table_id)     [in]  table id.
 * @param (column_name)  [in]  column name.
 * @retval ErrorCode::OK if success.
 * @retval ErrorCode::ID_NOT_FOUND if the table id does not exist.
 * @retval ErrorCode::NAME_NOT_FOUND if the column name does not exist.
 * @retval otherwise an error code.
 */
ErrorCode Statistics::remove_by_column_name(
    const ObjectIdType table_id, std::string_view column_name) const {
  ErrorCode error = ErrorCode::UNKNOWN;

  // Log of API function start.
  log::function_start("Statistics::remove_by_column_name()");

  // Parameter value check.
  if ((table_id > 0) && (!column_name.empty())) {
    error = ErrorCode::OK;
  } else if (table_id <= 0) {
    LOG_WARNING
        << "An out-of-range value (0 or less) was specified for TableId.: "
        << table_id;
    error = ErrorCode::ID_NOT_FOUND;
  } else {
    LOG_WARNING << "An empty value was specified for ColumnName.";
    error = ErrorCode::NAME_NOT_FOUND;
  }

  if (error == ErrorCode::OK) {
    ObjectIdType retval_object_id = 0;
    // Remove the column statistic through the provider.
    error = provider->remove_column_statistic(table_id, Statistics::COLUMN_NAME,
                                              column_name, retval_object_id);
  }

  // Log of API function finish.
  log::function_finish("Statistics::remove_by_column_name()", error);

  return error;
}

/**
 *  @brief
 */
std::shared_ptr<Object> Statistics::create_object() const {
  // ToDo: implements.
  return nullptr;
}

/* =============================================================================
 * Private method area
 */

/**
 * @brief Checks if the parameters are correct.
 * @param (object)  [in]  metadata-object
 * @return ErrorCode::OK if success, otherwise an error code.
 */
ErrorCode Statistics::param_check_statistics_add(
    const boost::property_tree::ptree& object) const {
  ErrorCode error = ErrorCode::UNKNOWN;
  constexpr const char* const kLogFormat =
      R"("%s" and "%s" => undefined or empty)";

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
      object.get_optional<std::int64_t>(Statistics::COLUMN_NUMBER);
  bool specified_ordinal_position =
      (ordinal_position && (ordinal_position.get() > 0));

  // column_name
  boost::optional<std::string> column_name =
      object.get_optional<std::string>(Statistics::COLUMN_NAME);
  bool specified_column_name = (column_name && !(column_name.get().empty()));

  // Check for required parameters.
  //   If column_id is not specified,
  //   and table_id and column_name or ordinal_position are not specified,
  //   it will return a parameter error.
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
      LOG_ERROR << Message::PARAMETER_FAILED
                << (boost::format(kLogFormat) % Statistics::COLUMN_NUMBER %
                    Statistics::COLUMN_NAME)
                       .str();
      error = ErrorCode::INVALID_PARAMETER;
    }
  } else {
    // column_id and table_id is not specified.
    LOG_ERROR << Message::PARAMETER_FAILED
              << (boost::format(kLogFormat) % Statistics::COLUMN_ID %
                  Statistics::TABLE_ID)
                     .str();
    error = ErrorCode::INVALID_PARAMETER;
  }

  return error;
}

}  // namespace manager::metadata
