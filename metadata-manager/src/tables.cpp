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
 *  @brief  Constructor
 *  @param  (database) [in]  database name.
 *  @param  (component) [in]  component name.
 */
Tables::Tables(std::string_view database, std::string_view component)
    : Metadata(database, component) {
  // Create the provider.
  provider = std::make_unique<db::TablesProvider>();
}

/**
 *  @brief  Initialization.
 *  @param  none.
 *  @return  ErrorCode::OK if success, otherwise an error code.
 */
ErrorCode Tables::init() {
  // Initialize the provider.
  ErrorCode error = provider->init();

  return error;
}

/**
 *  @brief  Add table metadata to table metadata table.
 *  @param  (object) [in]  table metadata to add.
 *  @return ErrorCode::OK if success, otherwise an error code.
 */
ErrorCode Tables::add(boost::property_tree::ptree &object) {
  return add(object, nullptr);
}

/**
 *  @brief  Add table metadata to table metadata table.
 *  @param  (object)      [in]  table metadata to add.
 *  @param  (object_id)   [out] ID of the added table metadata.
 *  @return ErrorCode::OK if success, otherwise an error code.
 */
ErrorCode Tables::add(boost::property_tree::ptree &object,
                      ObjectIdType *object_id) {
  // Adds the table metadata through the provider.
  ObjectIdType retval_object_id;
  ErrorCode error = provider->add_table_metadata(object, retval_object_id);

  // Set a value if object_id is not null.
  if ((error == ErrorCode::OK) && (object_id != nullptr)) {
    *object_id = retval_object_id;
  }

  return error;
}

/**
 *  @brief  Get table metadata.
 *  @param  (object_id) [in]  table id.
 *  @param  (object)    [out] table metadata with the specified ID.
 *  @return ErrorCode::OK if success, otherwise an error code.
 */
ErrorCode Tables::get(const ObjectIdType object_id,
                      boost::property_tree::ptree &object) {
  if (object_id <= 0) {
    return ErrorCode::ID_NOT_FOUND;
  }

  // Get the table metadata through the provider.
  std::string s_object_id = std::to_string(object_id);
  ErrorCode error =
      provider->get_table_metadata(Tables::ID, s_object_id, object);

  return error;
}

/**
 *  @brief  Get table metadata object based on table name.
 *  @param  (object_name)   [in]  table name. (Value of "name"
 * key.)
 *  @param  (object)        [out] table metadata object with the specified name.
 *  @return ErrorCode::OK if success, otherwise an error code.
 */
ErrorCode Tables::get(std::string_view object_name,
                      boost::property_tree::ptree &object) {
  if (object_name.empty()) {
    return ErrorCode::NAME_NOT_FOUND;
  }

  // Get the table metadata through the provider.
  ErrorCode error =
      provider->get_table_metadata(Tables::NAME, object_name, object);

  return error;
}

/**
 *  @brief  Remove all metadata-object based on the given table id
 *  (table metadata, column metadata and column statistics)
 *  from metadata-table (the table metadata table,
 *  the column metadata table and the column statistics table).
 *  @param (object_id) [in] table id.
 *  @return ErrorCode::OK if success, otherwise an error code.
 */
ErrorCode Tables::remove(const ObjectIdType object_id) {
  if (object_id <= 0) {
    return ErrorCode::ID_NOT_FOUND;
  }

  // Remove the table metadata through the provider.
  ErrorCode error = provider->remove_table_metadata(object_id);

  return error;
}

/**
 *  @brief  Remove all metadata-object based on the given table name
 *  (table metadata, column metadata and column statistics)
 *  from metadata-table (the table metadata table,
 *  the column metadata table and the column statistics table).
 *  @param (object_name) [in]  table name.
 *  @param (object_id)   [out] object id of table removed.
 *  @return ErrorCode::OK if success, otherwise an error code.
 */
ErrorCode Tables::remove(const char *object_name, ObjectIdType *object_id) {
  std::string_view s_object_name = std::string_view(object_name);

  if (s_object_name.empty()) {
    return ErrorCode::NAME_NOT_FOUND;
  }

  // Remove the table metadata through the provider.
  ObjectIdType retval_object_id;
  ErrorCode error =
      provider->remove_table_metadata(s_object_name, retval_object_id);

  // Set a value if object_id is not null.
  if ((error == ErrorCode::OK) && (object_id != nullptr)) {
    *object_id = retval_object_id;
  }

  return error;
}

}  // namespace manager::metadata
