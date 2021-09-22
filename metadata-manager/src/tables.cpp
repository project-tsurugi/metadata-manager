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
#include "manager/metadata/tables.h"

#include <memory>

#include "manager/metadata/provider/tables_provider.h"

// =============================================================================
namespace {

std::unique_ptr<manager::metadata::db::TablesProvider> provider = nullptr;

}  // namespace

// =============================================================================
namespace manager::metadata {

using manager::metadata::ErrorCode;

/**
 * @brief Constructor
 * @param (database)   [in]  database name.
 * @param (component)  [in]  component name.
 */
Tables::Tables(std::string_view database, std::string_view component)
    : Metadata(database, component) {
  // Create the provider.
  provider = std::make_unique<db::TablesProvider>();
}

/**
 * @brief Initialization.
 * @param none.
 * @return ErrorCode::OK if success, otherwise an error code.
 */
ErrorCode Tables::init() {
  ErrorCode error = ErrorCode::UNKNOWN;

  // Initialize the provider.
  error = provider->init();

  return error;
}

/**
 * @brief Add table metadata to table metadata table.
 * @param (object)  [in]  table metadata to add.
 * @return ErrorCode::OK if success, otherwise an error code.
 */
ErrorCode Tables::add(boost::property_tree::ptree& object) {
  ErrorCode error = ErrorCode::UNKNOWN;

  // Adds the table metadata through the class method.
  error = add(object, nullptr);

  return error;
}

/**
 * @brief Add table metadata to table metadata table.
 * @param (object)      [in]  table metadata to add.
 * @param (object_id)   [out] ID of the added table metadata.
 * @return ErrorCode::OK if success, otherwise an error code.
 */
ErrorCode Tables::add(boost::property_tree::ptree& object,
                      ObjectIdType* object_id) {
  ErrorCode error = ErrorCode::UNKNOWN;
  ObjectIdType retval_object_id;

  // Adds the table metadata through the provider.
  error = provider->add_table_metadata(object, retval_object_id);

  // Set a value if object_id is not null.
  if ((error == ErrorCode::OK) && (object_id != nullptr)) {
    *object_id = retval_object_id;
  }

  return error;
}

/**
 * @brief Get table metadata.
 * @param (object_id)  [in]  table id.
 * @param (object)     [out] table metadata with the specified ID.
 * @return ErrorCode::OK if success, otherwise an error code.
 */
ErrorCode Tables::get(const ObjectIdType object_id,
                      boost::property_tree::ptree& object) {
  ErrorCode error = ErrorCode::UNKNOWN;

  // Parameter value check
  if (object_id <= 0) {
    error = ErrorCode::ID_NOT_FOUND;
    return error;
  }

  // Get the table metadata through the provider.
  std::string s_object_id = std::to_string(object_id);
  error = provider->get_table_metadata(Tables::ID, s_object_id, object);

  // Convert the return value.
  if (error == ErrorCode::NOT_FOUND) {
    error = ErrorCode::ID_NOT_FOUND;
  }

  return error;
}

/**
 * @brief Get table metadata object based on table name.
 * @param (object_name)  [in]  table name. (Value of "name" key.)
 * @param (object)       [out] table metadata object with the specified name.
 * @return ErrorCode::OK if success, otherwise an error code.
 */
ErrorCode Tables::get(std::string_view object_name,
                      boost::property_tree::ptree& object) {
  ErrorCode error = ErrorCode::UNKNOWN;

  // Parameter value check
  if (object_name.empty()) {
    error = ErrorCode::NAME_NOT_FOUND;
    return error;
  }

  // Get the table metadata through the provider.
  error = provider->get_table_metadata(Tables::NAME, object_name, object);

  // Convert the return value
  if (error == ErrorCode::NOT_FOUND) {
    error = ErrorCode::NAME_NOT_FOUND;
  }

  return error;
}

/**
 * @brief Gets all table metadata object from the table metadata table.
 * @param (container)  [out] Container for metadata-objects.
 * @return ErrorCode::OK if success, otherwise an error code.
 */
ErrorCode Tables::get_all(std::vector<boost::property_tree::ptree>& container) {
  ErrorCode error = ErrorCode::UNKNOWN;

  // Get the table metadata through the provider.
  error = provider->get_table_metadata(container);

  return error;
}

/**
 * @brief Gets one table statistic from the table metadata table
 *   based on the given table id.
 * @param (table_id)         [in]  table id.
 * @param (table_statistic)  [out] one table statistic
 *   with the specified table id.
 * @return ErrorCode::OK if success, otherwise an error code.
 */
ErrorCode Tables::get_statistic(const ObjectIdType table_id,
                                boost::property_tree::ptree& object) {
  ErrorCode error = ErrorCode::UNKNOWN;

  // Parameter value check
  if (table_id <= 0) {
    error = ErrorCode::ID_NOT_FOUND;
    return error;
  }

  // Get the table statistic through the provider.
  error = provider->get_table_statistic(Tables::ID, std::to_string(table_id),
                                        object);

  // Convert the return value
  if (error == ErrorCode::NOT_FOUND) {
    error = ErrorCode::ID_NOT_FOUND;
  }

  return error;
}

/**
 * @brief Gets one table statistic from the table metadata table
 *   based on the given table name.
 * @param (table_name)       [in]  table name.
 * @param (table_statistic)  [out] one table statistic
 *   with the specified table name.
 * @return ErrorCode::OK if success, otherwise an error code.
 */
ErrorCode Tables::get_statistic(std::string_view table_name,
                                boost::property_tree::ptree& object) {
  ErrorCode error = ErrorCode::UNKNOWN;

  // Parameter value check
  if (table_name.empty()) {
    error = ErrorCode::NAME_NOT_FOUND;
    return error;
  }

  // Get the table statistic through the provider.
  error = provider->get_table_statistic(Tables::NAME, table_name, object);

  // Convert the return value
  if (error == ErrorCode::NOT_FOUND) {
    error = ErrorCode::NAME_NOT_FOUND;
  }

  return error;
}

/**
 * @brief Set table metadata table with the specified table statistics.
 * @param (object)  [in] Table statistic object.
 * @return ErrorCode::OK if success, otherwise an error code.
 */
ErrorCode Tables::set_statistic(boost::property_tree::ptree& object) {
  ErrorCode error = ErrorCode::UNKNOWN;

  boost::optional<std::string> o_id =
      object.get_optional<std::string>(Tables::ID);
  boost::optional<std::string> o_name =
      object.get_optional<std::string>(Tables::NAME);

  // Parameter value check
  if (!o_id && !o_name) {
    error = ErrorCode::INVALID_PARAMETER;
    return error;
  }

  ObjectIdType retval_object_id;
  // Adds or updates the table statistic through the provider.
  error = provider->set_table_statistic(object, retval_object_id);

  // Convert the return value
  if (error == ErrorCode::NOT_FOUND) {
    error = (o_id ? ErrorCode::ID_NOT_FOUND : ErrorCode::NAME_NOT_FOUND);
  }

  return error;
}

/**
 * @brief Remove all metadata-object based on the given table id
 *   (table metadata, column metadata and column statistics)
 *   from metadata-table (the table metadata table,
 *   the column metadata table and the column statistics table).
 * @param (object_id)  [in]  table id.
 * @return ErrorCode::OK if success, otherwise an error code.
 */
ErrorCode Tables::remove(const ObjectIdType object_id) {
  ErrorCode error = ErrorCode::UNKNOWN;

  // Parameter value check
  if (object_id <= 0) {
    error = ErrorCode::ID_NOT_FOUND;
    return error;
  }

  ObjectIdType retval_object_id;
  // Remove the table metadata through the provider.
  error = provider->remove_table_metadata(Tables::ID, std::to_string(object_id),
                                          retval_object_id);

  // Convert the return value
  if (error == ErrorCode::NOT_FOUND) {
    error = ErrorCode::ID_NOT_FOUND;
  }

  return error;
}

/**
 * @brief Remove all metadata-object based on the given table name
 *   (table metadata, column metadata and column statistics)
 *   from metadata-table (the table metadata table,
 *   the column metadata table and the column statistics table).
 * @param (object_name)  [in]  table name.
 * @param (object_id)    [out] object id of table removed.
 * @return ErrorCode::OK if success, otherwise an error code.
 */
ErrorCode Tables::remove(std::string_view object_name,
                         ObjectIdType* object_id) {
  ErrorCode error = ErrorCode::UNKNOWN;
  std::string_view s_object_name = std::string_view(object_name);

  // Parameter value check
  if (s_object_name.empty()) {
    error = ErrorCode::NAME_NOT_FOUND;
    return error;
  }

  ObjectIdType retval_object_id;
  // Remove the table metadata through the provider.
  error = provider->remove_table_metadata(Tables::NAME, s_object_name,
                                          retval_object_id);

  // Convert the return value
  if (error == ErrorCode::NOT_FOUND) {
    error = ErrorCode::NAME_NOT_FOUND;
  }

  // Set a value if object_id is not null.
  if ((error == ErrorCode::OK) && (object_id != nullptr)) {
    *object_id = retval_object_id;
  }

  return error;
}

}  // namespace manager::metadata
