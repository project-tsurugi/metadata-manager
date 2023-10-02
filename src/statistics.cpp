/*
 * Copyright 2020-2023 tsurugi project.
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

#include <boost/format.hpp>

#include "manager/metadata/common/message.h"
#include "manager/metadata/helper/logging_helper.h"
#include "manager/metadata/helper/ptree_helper.h"
#include "manager/metadata/provider/metadata_provider.h"

// =============================================================================
namespace {

auto& provider = manager::metadata::db::MetadataProvider::get_instance();

}  // namespace

// =============================================================================
namespace manager::metadata {

using boost::property_tree::ptree;

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
  error = provider.init();

  // Log of API function finish.
  log::function_finish("Statistics::init()", error);

  return error;
}

/**
 * @brief Add column statistics to statistics table.
 * @param object  [in]  statistics to add.
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
 * @param object     [in]  statistics to add.
 * @param object_id  [out] ID of the added statistic.
 * @return ErrorCode::OK if success, otherwise an error code.
 */
ErrorCode Statistics::add(const boost::property_tree::ptree& object,
                          ObjectIdType* object_id) const {
  ErrorCode error = ErrorCode::UNKNOWN;

  // Log of API function start.
  log::function_start("Statistics::add()");

  // Parameter value check.
  error = param_check_statistics_add(object);

  ObjectId added_oid = INVALID_OBJECT_ID;
  if (error == ErrorCode::OK) {
    // Add column statistic within a transaction.
    error = provider.transaction([&object, &added_oid]() -> ErrorCode {
      // Adds the column statistic through the provider.
      return provider.add_column_statistic(object, &added_oid);
    });
  }

  // Set a value if object_id is not null.
  if ((error == ErrorCode::OK) && (object_id != nullptr)) {
    *object_id = added_oid;
  }

  // Log of API function finish.
  log::function_finish("Statistics::add()", error);

  return error;
}

/**
 * @brief Get column statistics.
 * @param object_id  [in]  statistic id.
 * @param object     [out] column statistics with the specified ID.
 * @retval ErrorCode::OK if success.
 * @retval ErrorCode::ID_NOT_FOUND if the statistic id does not exist.
 * @retval otherwise an error code.
 */
ErrorCode Statistics::get(const ObjectIdType object_id,
                          boost::property_tree::ptree& object) const {
  ErrorCode error = ErrorCode::UNKNOWN;

  // Log of API function start.
  log::function_start("Statistics::get(object_id)");

  // Specify the key for the column statistic you want to retrieve.
  std::string statistic_id(std::to_string(object_id));
  std::map<std::string_view, std::string_view> keys = {
      {Statistics::ID, statistic_id}
  };

  // Retrieve column statistic.
  ptree tmp_object;
  if (object_id > 0) {
    // Get the column statistic through the provider.
    error = provider.get_column_statistic(keys, tmp_object);
  } else {
    LOG_WARNING
        << "An out-of-range value (0 or less) was specified for object ID.: "
        << object_id;
    error = ErrorCode::ID_NOT_FOUND;
  }

  if (error == ErrorCode::OK) {
    if (tmp_object.size() == 1) {
      object = tmp_object.front().second;
    } else {
      error = ErrorCode::RESULT_MULTIPLE_ROWS;
      LOG_WARNING << "Multiple rows retrieved.: " << keys
                  << " exists " << tmp_object.size() << " rows";
    }
  }

  // Log of API function finish.
  log::function_finish("Statistics::get(object_id)", error);

  return error;
}

/**
 * @brief Get column statistics object based on statistic name.
 * @param object_name  [in]  statistic name. (Value of "name" key.)
 * @param object       [out] column statistics object
 *   with the specified name.
 * @retval ErrorCode::OK if success.
 * @retval ErrorCode::NAME_NOT_FOUND if the statistic name does not exist.
 * @retval otherwise an error code.
 */
ErrorCode Statistics::get(std::string_view object_name,
                          boost::property_tree::ptree& object) const {
  ErrorCode error = ErrorCode::UNKNOWN;

  // Log of API function start.
  log::function_start("Statistics::get(object_name)");

  // Specify the key for the column statistic you want to retrieve.
  std::map<std::string_view, std::string_view> keys = {
      {Statistics::NAME, object_name}
  };

  // Retrieve column statistic.
  ptree tmp_object;
  if (!object_name.empty()) {
    // Get the column statistic through the provider.
    error = provider.get_column_statistic(keys, tmp_object);
  } else {
    LOG_WARNING << "An empty value was specified for object name.";
    error = ErrorCode::NAME_NOT_FOUND;
  }

  if (error == ErrorCode::OK) {
    if (tmp_object.size() == 1) {
      object = tmp_object.front().second;
    } else {
      error = ErrorCode::RESULT_MULTIPLE_ROWS;
      LOG_WARNING << "Multiple rows retrieved.: " << keys
                  << " exists " << tmp_object.size() << " rows";
    }
  }

  // Log of API function finish.
  log::function_finish("Statistics::get(object_name)", error);

  return error;
}

/**
 * @brief Gets one column statistic from the column statistics table
 *   based on the given column id.
 * @param column_id  [in]  column id.
 * @param object     [out] one column statistic
 *   with the specified column id.
 * @retval ErrorCode::OK if success.
 * @retval ErrorCode::NOT_FOUND if the column id does not exist.
 * @retval otherwise an error code.
 */
ErrorCode Statistics::get_by_column_id(
    const ObjectIdType column_id, boost::property_tree::ptree& object) const {
  ErrorCode error = ErrorCode::UNKNOWN;

  // Log of API function start.
  log::function_start("Statistics::get_by_column_id()");

  // Specify the key for the column statistic you want to retrieve.
  std::string s_column_id(std::to_string(column_id));
  std::map<std::string_view, std::string_view> keys = {
      {Statistics::COLUMN_ID, s_column_id}
  };

  // Retrieve column statistic.
  ptree tmp_object;
  if (column_id > 0) {
    // Get the column statistic through the provider.
    error = provider.get_column_statistic(keys, tmp_object);
  } else {
    LOG_WARNING << "An empty value was specified for column id.";
    error = ErrorCode::NOT_FOUND;
  }

  if (error == ErrorCode::OK) {
    if (tmp_object.size() == 1) {
      object = tmp_object.front().second;
    } else {
      error = ErrorCode::RESULT_MULTIPLE_ROWS;
      LOG_WARNING << "Multiple rows retrieved.: " << keys
                  << " exists " << tmp_object.size() << " rows";
    }
  }

  // Log of API function finish.
  log::function_finish("Statistics::get_by_column_id()", error);

  return error;
}

/**
 * @brief Gets one column statistic from the column statistics table
 *   based on the given table id and the given column ordinal position.
 * @param table_id          [in]  table id.
 * @param ordinal_position  [in]  column ordinal position.
 * @param object            [out] one column statistic
 *   with the specified table id and column ordinal position.
 * @retval ErrorCode::OK if success.
 * @retval ErrorCode::NOT_FOUND if the table id or ordinal position
 *   does not exist.
 * @retval otherwise an error code.
 */
ErrorCode Statistics::get_by_column_number(
    const ObjectIdType table_id, const int64_t ordinal_position,
    boost::property_tree::ptree& object) const {
  ErrorCode error = ErrorCode::UNKNOWN;

  // Log of API function start.
  log::function_start("Statistics::get_by_column_number()");

  // Specify the key for the column statistic you want to retrieve.
  std::string s_table_id(std::to_string(table_id));
  std::string s_ordinal_position(std::to_string(ordinal_position));
  std::map<std::string_view, std::string_view> keys = {
      {Statistics::TABLE_ID, s_table_id},
      {Statistics::COLUMN_NUMBER, s_ordinal_position}
  };

  // Retrieve column statistic.
  ptree tmp_object;
  if ((table_id > 0) && (ordinal_position > 0)) {
    // Get the column statistic through the provider.
    error = provider.get_column_statistic(keys, tmp_object);
  } else {
    LOG_WARNING << "An out-of-range value (0 or less) was specified for "
                   "table-ID or ordinal-position.: "
                << "table-ID: " << table_id
                << ", ordinal-position: " << ordinal_position;
    error = ErrorCode::NOT_FOUND;
  }

  if (error == ErrorCode::OK) {
    if (tmp_object.size() == 1) {
      object = tmp_object.front().second;
    } else {
      error = ErrorCode::RESULT_MULTIPLE_ROWS;
      LOG_WARNING << "Multiple rows retrieved.: " << keys
                  << " exists " << tmp_object.size() << " rows";
    }
  }

  // Log of API function finish.
  log::function_finish("Statistics::get_by_column_number()", error);

  return error;
}

/**
 * @brief Gets one column statistic from the column statistics table
 *   based on the given table id and the given column name.
 * @param table_id     [in]  table id.
 * @param column_name  [in]  column name.
 * @param object       [out] one column statistic
 *   with the specified table id and column name.
 * @retval ErrorCode::OK if success.
 * @retval ErrorCode::NOT_FOUND if the table id or column name does not exist.
 * @retval otherwise an error code.
 */
ErrorCode Statistics::get_by_column_name(
    const ObjectIdType table_id, std::string_view column_name,
    boost::property_tree::ptree& object) const {
  ErrorCode error = ErrorCode::UNKNOWN;

  // Log of API function start.
  log::function_start("Statistics::get_by_column_name()");

  // Specify the key for the column statistic you want to retrieve.
  std::string s_table_id(std::to_string(table_id));
  std::map<std::string_view, std::string_view> keys = {
      {Statistics::TABLE_ID, s_table_id},
      {Statistics::COLUMN_NAME, column_name}
  };

  // Retrieve column statistic.
  ptree tmp_object;
  if ((table_id > 0) && (!column_name.empty())) {
    // Get the column statistic through the provider.
    error = provider.get_column_statistic(keys, tmp_object);
  } else {
    LOG_WARNING
        << "An out-of-range value (0 or less) was specified for "
           "table-ID, or column-name was specified as an empty string.: "
        << "table-ID: " << table_id << ", column-name: " << column_name;
    error = ErrorCode::NOT_FOUND;
  }

  if (error == ErrorCode::OK) {
    if (tmp_object.size() == 1) {
      object = tmp_object.front().second;
    } else {
      error = ErrorCode::RESULT_MULTIPLE_ROWS;
      LOG_WARNING << "Multiple rows retrieved.: " << keys
                  << " exists " << tmp_object.size() << " rows";
    }
  }

  // Log of API function finish.
  log::function_finish("Statistics::get_by_column_name()", error);

  return error;
}

/**
 * @brief Gets all column statistics from the column statistics table.
 *   If the column statistic does not exist, return the container as empty.
 * @param objects  [out] Container for statistics-objects.
 * @return ErrorCode::OK if success, otherwise an error code.
 */
ErrorCode Statistics::get_all(
    std::vector<boost::property_tree::ptree>& objects) const {
  ErrorCode error = ErrorCode::UNKNOWN;

  // Log of API function start.
  log::function_start("Statistics::get_all()");

  ptree tmp_object;
  std::map<std::string_view, std::string_view> keys = {};

  // Get the column statistic through the provider.
  error = provider.get_column_statistic(keys, tmp_object);

  // Converts object types.
  if (error == ErrorCode::OK) {
    objects = ptree_helper::array_to_vector(tmp_object);
  } else if (error == ErrorCode::NOT_FOUND) {
    // Converts error code.
    error = ErrorCode::OK;
  }

  // Log of API function finish.
  log::function_finish("Statistics::get_all()", error);

  return error;
}

/**
 * @brief Gets all column statistics from the column statistics table
 *   based on the given table id.
 *   If the column statistic does not exist, return the container as empty.
 * @param table_id  [in]  table id.
 * @param objects   [out] Container for statistics-objects.
 *   with the specified table id.
 * @retval ErrorCode::OK if success.
 * @retval ErrorCode::NOT_FOUND if the table id does not exist.
 * @retval otherwise an error code.
 */
ErrorCode Statistics::get_all(
    const ObjectIdType table_id,
    std::vector<boost::property_tree::ptree>& objects) const {
  ErrorCode error = ErrorCode::UNKNOWN;

  // Log of API function start.
  log::function_start("Statistics::get_all(table_id)");

  // Specify the key for the column statistic you want to retrieve.
  std::string s_table_id(std::to_string(table_id));
  std::map<std::string_view, std::string_view> keys = {
      {Statistics::TABLE_ID, s_table_id}
  };

  // Retrieve column statistic.
  ptree tmp_object;
  if (table_id > 0) {
    // Get the column statistic through the provider.
    error = provider.get_column_statistic(keys, tmp_object);
  } else {
    LOG_WARNING
        << "An out-of-range value (0 or less) was specified for table ID.: "
        << table_id;
    error = ErrorCode::NOT_FOUND;
  }

  // Converts object types.
  if (error == ErrorCode::OK) {
    objects = ptree_helper::array_to_vector(tmp_object);
  }

  // Log of API function finish.
  log::function_finish("Statistics::get_all(table_id)", error);

  return error;
}

/**
 * @brief Remove column statistics based on the given statistics id.
 * @param object_id  [in]  statistic id.
 * @retval ErrorCode::OK if success.
 * @retval ErrorCode::ID_NOT_FOUND if the statistic id does not exist.
 * @retval otherwise an error code.
 */
ErrorCode Statistics::remove(const ObjectIdType object_id) const {
  ErrorCode error = ErrorCode::UNKNOWN;

  // Log of API function start.
  log::function_start("Statistics::remove(object_id)");

  // Remove the column statistic.
  if (object_id > 0) {
    // Specify the key for the column statistic you want to remove.
    std::string statistic_id(std::to_string(object_id));
    std::map<std::string_view, std::string_view> keys = {
        {Statistics::ID, statistic_id}
    };

    // Remove column statistic within a transaction.
    error = provider.transaction([&keys]() -> ErrorCode {
      // Remove the column statistic through the provider.
      return provider.remove_column_statistics(keys);
    });
  } else {
    LOG_WARNING
        << "An out-of-range value (0 or less) was specified for object ID.: "
        << object_id;
    error = ErrorCode::ID_NOT_FOUND;
  }

  // Log of API function finish.
  log::function_finish("Statistics::remove(object_id)", error);

  return error;
}

/**
 * @brief Remove column statistics based on the given statistics name.
 * @param object_name  [in]  statistic name.
 * @param object_id    [out] object id of statistic removed.
 * @retval ErrorCode::OK if success.
 * @retval ErrorCode::NAME_NOT_FOUND if the statistic name does not exist.
 * @retval otherwise an error code.
 */
ErrorCode Statistics::remove(std::string_view object_name,
                             ObjectIdType* object_id) const {
  ErrorCode error = ErrorCode::UNKNOWN;

  // Log of API function start.
  log::function_start("Statistics::remove(object_name)");

  // Specify the key for the column statistic you want to retrieve.
  std::map<std::string_view, std::string_view> keys = {
      {Statistics::NAME, object_name}
  };

  // Remove the column statistic.
  std::vector<ObjectId> removed_ids = {};
  if (!object_name.empty()) {
    // Remove column statistic within a transaction.
    error = provider.transaction([&keys, &removed_ids]() -> ErrorCode {
      // Remove the column statistic through the provider.
      return provider.remove_column_statistics(keys, &removed_ids);
    });
  } else {
    LOG_WARNING << "An empty value was specified for object name.";
    error = ErrorCode::NAME_NOT_FOUND;
  }

  // Set a value if object_id is not null.
  if ((error == ErrorCode::OK) && (object_id != nullptr)) {
    *object_id = removed_ids.front();
  }

  // Log of API function finish.
  log::function_finish("Statistics::remove(object_name)", error);

  return error;
}

/**
 * @brief Removes all column statistics
 *   from the column statistics table based on the given table id.
 * @param table_id  [in]  table id.
 * @retval ErrorCode::OK if success.
 * @retval ErrorCode::NOT_FOUND if the table id does not exist.
 * @retval otherwise an error code.
 */
ErrorCode Statistics::remove_by_table_id(const ObjectIdType table_id) const {
  ErrorCode error = ErrorCode::UNKNOWN;

  // Log of API function start.
  log::function_start("Statistics::remove_by_table_id()");

  // Remove the column statistic.
  ptree tmp_object;
  if (table_id > 0) {
    // Specify the key for the column statistic you want to remove.
    std::string s_table_id(std::to_string(table_id));
    std::map<std::string_view, std::string_view> keys = {
        {Statistics::TABLE_ID, s_table_id}
    };

    // Remove column statistic within a transaction.
    error = provider.transaction([&keys]() -> ErrorCode {
      // Remove the all column statistics through the provider.
      return provider.remove_column_statistics(keys);
    });
  } else {
    LOG_WARNING
        << "An out-of-range value (0 or less) was specified for table ID.: "
        << table_id;
    error = ErrorCode::NOT_FOUND;
  }

  // Log of API function finish.
  log::function_finish("Statistics::remove_by_table_id()", error);

  return error;
}

/**
 * @brief Removes column statistic from the column statistics table
 *   based on the given column id.
 * @param column_id  [in]  column id.
 * @retval ErrorCode::OK if success.
 * @retval ErrorCode::NOT_FOUND if the column id does not exist.
 * @retval otherwise an error code.
 */
ErrorCode Statistics::remove_by_column_id(const ObjectIdType column_id) const {
  ErrorCode error = ErrorCode::UNKNOWN;

  // Log of API function start.
  log::function_start("Statistics::remove_by_column_id()");

  // Remove the column statistic.
  ptree tmp_object;
  if (column_id > 0) {
    // Specify the key for the column statistic you want to remove.
    std::string s_column_id(std::to_string(column_id));
    std::map<std::string_view, std::string_view> keys = {
        {Statistics::COLUMN_ID, s_column_id}
    };

    // Remove column statistic within a transaction.
    error = provider.transaction([&keys]() -> ErrorCode {
      // Remove the all column statistics through the provider.
      return provider.remove_column_statistics(keys);
    });
  } else {
    LOG_WARNING
        << "An out-of-range value (0 or less) was specified for table ID.: "
        << column_id;
    error = ErrorCode::NOT_FOUND;
  }

  // Log of API function finish.
  log::function_finish("Statistics::remove_by_column_id()", error);

  return error;
}

/**
 * @brief Removes column statistic from the column statistics table
 *   based on the given table id and the given column ordinal position.
 * @param table_id          [in]  table id.
 * @param ordinal_position  [in]  column ordinal position.
 * @retval ErrorCode::OK if success.
 * @retval ErrorCode::NOT_FOUND if the table id or ordinal position does not
 * exist.
 * @retval otherwise an error code.
 */
ErrorCode Statistics::remove_by_column_number(
    const ObjectIdType table_id, const int64_t ordinal_position) const {
  ErrorCode error = ErrorCode::UNKNOWN;

  // Log of API function start.
  log::function_start("Statistics::remove_by_column_number()");

  // Remove the column statistic.
  if ((table_id > 0) && (ordinal_position > 0)) {
    // Specify the key for the column statistic you want to remove.
    std::string s_table_id(std::to_string(table_id));
    std::string s_ordinal_position(std::to_string(ordinal_position));
    std::map<std::string_view, std::string_view> keys = {
        {Statistics::TABLE_ID, s_table_id},
        {Statistics::COLUMN_NUMBER, s_ordinal_position}
    };

    // Remove column statistic within a transaction.
    error = provider.transaction([&keys]() -> ErrorCode {
      // Remove the all column statistics through the provider.
      return provider.remove_column_statistics(keys);
    });
  } else {
    LOG_WARNING << "An out-of-range value (0 or less) was specified for "
                   "table-ID or ordinal-position.: "
                << "table-ID: " << table_id
                << ", ordinal-position: " << ordinal_position;
    error = ErrorCode::NOT_FOUND;
  }

  // Log of API function finish.
  log::function_finish("Statistics::remove_by_column_number()", error);

  return error;
}

/**
 * @brief Removes one column statistic from the column statistics table
 *   based on the given table id and the given column ordinal position.
 * @param table_id     [in]  table id.
 * @param column_name  [in]  column name.
 * @retval ErrorCode::OK if success.
 * @retval ErrorCode::NOT_FOUND if the table id or column name does not exist.
 * @retval otherwise an error code.
 */
ErrorCode Statistics::remove_by_column_name(
    const ObjectIdType table_id, std::string_view column_name) const {
  ErrorCode error = ErrorCode::UNKNOWN;

  // Log of API function start.
  log::function_start("Statistics::remove_by_column_name()");

  // Remove the column statistic.
  if ((table_id > 0) && (!column_name.empty())) {
    // Specify the key for the column statistic you want to remove.
    std::string s_table_id(std::to_string(table_id));
    std::map<std::string_view, std::string_view> keys = {
        {Statistics::TABLE_ID, s_table_id},
        {Statistics::COLUMN_NAME, column_name}
    };

    // Remove column statistic within a transaction.
    error = provider.transaction([&keys]() -> ErrorCode {
      // Remove the all column statistics through the provider.
      return provider.remove_column_statistics(keys);
    });
  } else {
    LOG_WARNING
        << "An out-of-range value (0 or less) was specified for "
           "table-ID, or column-name was specified as an empty string.: "
        << "table-ID: " << table_id << ", column-name: " << column_name;
    error = ErrorCode::NOT_FOUND;
  }

  // Log of API function finish.
  log::function_finish("Statistics::remove_by_column_name()", error);

  return error;
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
      LOG_ERROR << Message::PARAMETER_FAILED << Statistics::COLUMN_NUMBER
                << " and " << Statistics::COLUMN_NAME
                << " => undefined or empty";
      error = ErrorCode::INSUFFICIENT_PARAMETERS;
    }
  } else {
    // column_id and table_id is not specified.
    LOG_ERROR << Message::PARAMETER_FAILED
              << (boost::format(kLogFormat) % Statistics::COLUMN_ID %
                  Statistics::TABLE_ID)
                     .str();
    error = ErrorCode::INSUFFICIENT_PARAMETERS;
  }

  return error;
}

}  // namespace manager::metadata
