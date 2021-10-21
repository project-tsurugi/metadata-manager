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
#include "manager/metadata/tables.h"

#include <boost/foreach.hpp>
#include <memory>

#include "manager/metadata/provider/datatypes_provider.h"
#include "manager/metadata/provider/tables_provider.h"

// =============================================================================
namespace {

std::unique_ptr<manager::metadata::db::TablesProvider> provider = nullptr;

}  // namespace

// =============================================================================
namespace manager::metadata {

using boost::property_tree::ptree;
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
ErrorCode Tables::init() const {
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
ErrorCode Tables::add(const boost::property_tree::ptree& object) const {
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
ErrorCode Tables::add(const boost::property_tree::ptree& object,
                      ObjectIdType* object_id) const {
  ErrorCode error = ErrorCode::UNKNOWN;

  // Parameter value check.
  error = param_check_metadata_add(object);
  if (error != ErrorCode::OK) {
    return error;
  }

  ObjectIdType retval_object_id = 0;
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
 * @retval ErrorCode::OK if success,
 * @retval ErrorCode::ID_NOT_FOUND if the table id does not exist.
 * @retval otherwise an error code.
 */
ErrorCode Tables::get(const ObjectIdType object_id,
                      boost::property_tree::ptree& object) const {
  ErrorCode error = ErrorCode::UNKNOWN;

  // Parameter value check.
  if (object_id <= 0) {
    error = ErrorCode::ID_NOT_FOUND;
    return error;
  }

  // Get the table metadata through the provider.
  std::string s_object_id = std::to_string(object_id);
  error = provider->get_table_metadata(Tables::ID, s_object_id, object);

  return error;
}

/**
 * @brief Get table metadata object based on table name.
 * @param (object_name)  [in]  table name. (Value of "name" key.)
 * @param (object)       [out] table metadata object with the specified name.
 * @retval ErrorCode::OK if success,
 * @retval ErrorCode::NAME_NOT_FOUND if the table name does not exist.
 * @retval otherwise an error code.
 */
ErrorCode Tables::get(std::string_view object_name,
                      boost::property_tree::ptree& object) const {
  ErrorCode error = ErrorCode::UNKNOWN;

  // Parameter value check.
  if (object_name.empty()) {
    error = ErrorCode::NAME_NOT_FOUND;
    return error;
  }

  // Get the table metadata through the provider.
  error = provider->get_table_metadata(Tables::NAME, object_name, object);

  return error;
}

/**
 * @brief Gets all table metadata object from the table metadata table.
 *   If the table metadata does not exist, return the container as empty.
 * @param (container)  [out] Container for metadata-objects.
 * @return ErrorCode::OK if success, otherwise an error code.
 */
ErrorCode Tables::get_all(
    std::vector<boost::property_tree::ptree>& container) const {
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
 * @retval ErrorCode::OK if success,
 * @retval ErrorCode::ID_NOT_FOUND if the table id does not exist.
 * @retval otherwise an error code.
 */
ErrorCode Tables::get_statistic(const ObjectIdType table_id,
                                boost::property_tree::ptree& object) const {
  ErrorCode error = ErrorCode::UNKNOWN;

  // Parameter value check.
  if (table_id <= 0) {
    error = ErrorCode::ID_NOT_FOUND;
    return error;
  }

  // Get the table statistic through the provider.
  error = provider->get_table_statistic(Tables::ID, std::to_string(table_id),
                                        object);

  return error;
}

/**
 * @brief Gets one table statistic from the table metadata table
 *   based on the given table name.
 * @param (table_name)       [in]  table name.
 * @param (table_statistic)  [out] one table statistic
 *   with the specified table name.
 * @retval ErrorCode::OK if success,
 * @retval ErrorCode::NAME_NOT_FOUND if the table name does not exist.
 * @retval otherwise an error code.
 */
ErrorCode Tables::get_statistic(std::string_view table_name,
                                boost::property_tree::ptree& object) const {
  ErrorCode error = ErrorCode::UNKNOWN;

  // Parameter value check.
  if (table_name.empty()) {
    error = ErrorCode::NAME_NOT_FOUND;
    return error;
  }

  // Get the table statistic through the provider.
  error = provider->get_table_statistic(Tables::NAME, table_name, object);

  return error;
}

/**
 * @brief Set table metadata table with the specified table statistics.
 * @param (object)  [in] Table statistic object.
 * @retval ErrorCode::OK if success,
 * @retval ErrorCode::ID_NOT_FOUND if the table id does not exist.
 * @retval ErrorCode::NAME_NOT_FOUND if the table name does not exist.
 * @retval otherwise an error code.
 */
ErrorCode Tables::set_statistic(boost::property_tree::ptree& object) const {
  ErrorCode error = ErrorCode::UNKNOWN;

  // Parameter value check.
  error = param_check_statistic_update(object);
  if (error != ErrorCode::OK) {
    return error;
  }

  ObjectIdType retval_object_id = 0;
  // Adds or updates the table statistic through the provider.
  error = provider->set_table_statistic(object, retval_object_id);

  return error;
}

/**
 * @brief Remove all metadata-object based on the given table id
 *   (table metadata, column metadata and column statistics)
 *   from metadata-table (the table metadata table,
 *   the column metadata table and the column statistics table).
 * @param (object_id)  [in]  table id.
 * @retval ErrorCode::OK if success,
 * @retval ErrorCode::ID_NOT_FOUND if the table id does not exist.
 * @retval otherwise an error code.
 */
ErrorCode Tables::remove(const ObjectIdType object_id) const {
  ErrorCode error = ErrorCode::UNKNOWN;

  // Parameter value check.
  if (object_id <= 0) {
    error = ErrorCode::ID_NOT_FOUND;
    return error;
  }

  ObjectIdType retval_object_id = 0;
  // Remove the table metadata through the provider.
  error = provider->remove_table_metadata(Tables::ID, std::to_string(object_id),
                                          retval_object_id);

  return error;
}

/**
 * @brief Remove all metadata-object based on the given table name
 *   (table metadata, column metadata and column statistics)
 *   from metadata-table (the table metadata table,
 *   the column metadata table and the column statistics table).
 * @param (object_name)  [in]  table name.
 * @param (object_id)    [out] object id of table removed.
 * @retval ErrorCode::OK if success,
 * @retval ErrorCode::NAME_NOT_FOUND if the table name does not exist.
 * @retval otherwise an error code.
 */
ErrorCode Tables::remove(std::string_view object_name,
                         ObjectIdType* object_id) const {
  ErrorCode error = ErrorCode::UNKNOWN;

  // Parameter value check.
  if (object_name.empty()) {
    error = ErrorCode::NAME_NOT_FOUND;
    return error;
  }

  ObjectIdType retval_object_id = 0;
  // Remove the table metadata through the provider.
  error = provider->remove_table_metadata(Tables::NAME, object_name,
                                          retval_object_id);

  // Set a value if object_id is not null.
  if ((error == ErrorCode::OK) && (object_id != nullptr)) {
    *object_id = retval_object_id;
  }

  return error;
}

/**
 * @brief Gets whether the specified access permissions are included.
 * @param (object_id)     [in]  role id.
 * @param (permission)    [in]  permission.
 * @param (check_result)  [out] presence or absence of the specified
 *   permissions.
 * @retval ErrorCode::OK if success.
 * @retval ErrorCode::NOT_FOUND if the foreign table does not exist.
 * @retval ErrorCode::ID_NOT_FOUND if the role id does not exist.
 * @retval otherwise an error code.
 */
ErrorCode Tables::confirm_permission_in_acls(const ObjectIdType object_id,
                                             const char* permission,
                                             bool& check_result) const {
  ErrorCode error = ErrorCode::UNKNOWN;

  // Parameter value check.
  if (object_id <= 0) {
    error = ErrorCode::ID_NOT_FOUND;
    return error;
  }

  // Get the table metadata through the provider.
  std::string s_object_id = std::to_string(object_id);
  error = provider->confirm_permission(Metadata::ID, s_object_id, permission,
                                       check_result);

  return error;
}

/**
 * @brief Gets whether or not the specified permissions have been granted.
 * @param (object_name)   [in]  role name.
 * @param (permission)    [in]  permissions.
 * @param (check_result)  [out] presence or absence of the specified
 *   permissions.
 * @retval ErrorCode::OK if success.
 * @retval ErrorCode::NOT_FOUND if the foreign table does not exist.
 * @retval ErrorCode::NAME_NOT_FOUND if the role name does not exist.
 * @retval otherwise an error code.
 */
ErrorCode Tables::confirm_permission_in_acls(std::string_view object_name,
                                             const char* permission,
                                             bool& check_result) const {
  ErrorCode error = ErrorCode::UNKNOWN;

  // Parameter value check.
  if (object_name.empty()) {
    error = ErrorCode::NAME_NOT_FOUND;
    return error;
  }

  // Get the table metadata through the provider.
  error = provider->confirm_permission(Metadata::NAME, object_name, permission,
                                       check_result);

  return error;
}

/* =============================================================================
 * Private method area
 */

/**
 * @brief Checks if the parameters for additional are correct.
 * @param (object)  [in]  metadata-object
 * @return ErrorCode::OK if success, otherwise an error code.
 */
ErrorCode Tables::param_check_metadata_add(
    const boost::property_tree::ptree& object) const {
  ErrorCode error = ErrorCode::UNKNOWN;

  boost::optional<std::string> table_name =
      object.get_optional<std::string>(Tables::NAME);
  if (!table_name || table_name.get().empty()) {
    error = ErrorCode::INVALID_PARAMETER;
    return error;
  }

  //
  // column metadata
  //
  error = ErrorCode::OK;
  BOOST_FOREACH (ptree::value_type node,
                 object.get_child(Tables::COLUMNS_NODE)) {
    auto& column = node.second;

    // name
    boost::optional<std::string> column_name =
        column.get_optional<std::string>(Tables::Column::NAME);
    if (!column_name || (column_name.get().empty())) {
      error = ErrorCode::INVALID_PARAMETER;
      break;
    }

    // ordinal position
    boost::optional<std::int64_t> ordinal_position =
        column.get_optional<std::int64_t>(Tables::Column::ORDINAL_POSITION);
    if (!ordinal_position || (ordinal_position.get() <= 0)) {
      error = ErrorCode::INVALID_PARAMETER;
      break;
    }

    // datatype id
    boost::optional<ObjectIdType> datatype_id =
        column.get_optional<ObjectIdType>(Tables::Column::DATA_TYPE_ID);
    if (!datatype_id || (datatype_id.get() < 0)) {
      error = ErrorCode::INVALID_PARAMETER;
      break;
    }
    // DataTypes check provider.
    db::DataTypesProvider provider_data_types;
    error = provider_data_types.init();
    if (error != ErrorCode::OK) {
      break;
    }

    // Check the data types.
    boost::property_tree::ptree datatype_metadata;
    error = provider_data_types.get_datatype_metadata(
        DataTypes::ID, std::to_string(datatype_id.get()), datatype_metadata);
    if (error != ErrorCode::OK) {
      if (error == ErrorCode::ID_NOT_FOUND) {
        error = ErrorCode::INVALID_PARAMETER;
      }
      break;
    }

    // nullable
    boost::optional<std::string> nullable =
        column.get_optional<std::string>(Tables::Column::NULLABLE);
    if (!nullable || (nullable.get().empty())) {
      error = ErrorCode::INVALID_PARAMETER;
      break;
    }
  }

  return error;
}

/**
 * @brief Checks if the parameters for updating table statistics are correct.
 * @param (object)  [in]  metadata-object
 * @return ErrorCode::OK if success, otherwise an error code.
 */
ErrorCode Tables::param_check_statistic_update(
    const boost::property_tree::ptree& object) const {
  ErrorCode error = ErrorCode::UNKNOWN;

  // id
  boost::optional<std::string> optional_id =
      object.get_optional<std::string>(Tables::ID);
  // name
  boost::optional<std::string> optional_name =
      object.get_optional<std::string>(Tables::NAME);
  // tuples
  boost::optional<float> optional_tuples =
      object.get_optional<float>(Tables::TUPLES);

  // Parameter value check.
  if ((optional_id || optional_name) && (optional_tuples)) {
    error = ErrorCode::OK;
  } else {
    error = ErrorCode::INVALID_PARAMETER;
  }

  return error;
}

}  // namespace manager::metadata
