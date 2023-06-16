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
#include "manager/metadata/indexes.h"

#include <memory>
#include <jwt-cpp/jwt.h>

#include "manager/metadata/common/config.h"
#include "manager/metadata/common/jwt_claims.h"
#include "manager/metadata/common/message.h"
#include "manager/metadata/helper/logging_helper.h"
#include "manager/metadata/helper/ptree_helper.h"
#include "manager/metadata/provider/provider_factory.h"

// =============================================================================
namespace manager::metadata {

using boost::property_tree::ptree;

// ==========================================================================
// Indexes class methods.
/**
 * @brief Constructor
 * @param database   [in]  database name.
 * @param component  [in]  component name.
 */
Indexes::Indexes(std::string_view database, std::string_view component)
    : Metadata(database, component) {
  provider_ = manager::metadata::db::get_metadata_provider();
}

/**
 * @brief Initialization.
 * @return ErrorCode::OK if success, otherwise an error code.
 */
ErrorCode Indexes::init() const {
  ErrorCode error = ErrorCode::UNKNOWN;

  log::function_start("Index::init()");

  error = provider_->init();

  log::function_finish("Index::init()", error);

  return error;
}

/**
 * @brief
 */
ErrorCode Indexes::add(const boost::property_tree::ptree& object) const {

  return this->add(object, nullptr);
}

/**
 * @brief Add index metadata to the metadata table.
 * @param object      [in]  index metadata to add.
 * @param object_id   [out] ID of the added index metadata.
 * @return ErrorCode::OK if success, otherwise an error code.
 */
ErrorCode Indexes::add(const boost::property_tree::ptree& object,
                      ObjectId* object_id) const {
  
  log::function_start("Indexes::add()");

  ErrorCode error = ErrorCode::UNKNOWN;
  ObjectId id = INVALID_OBJECT_ID;

  // Parameter value check.
  error = param_check_metadata_add(object);

  // Adds the index metadata through the provider.
  ObjectId retval_object_id = 0;
  if (error == ErrorCode::OK) {
    error = provider_->add_index_metadata(object, id);
  }

  // Set a value if object_id is not null.
  if ((error == ErrorCode::OK) && (object_id != nullptr)) {
    *object_id = id;
  }

  log::function_finish("Index::add()", error);
  
  return error;
}

/**
 * @brief Get index metadata.
 * @param object_id  [in]  object ID to get.
 * @param object     [out] index metadata with the specified ID.
 * @retval ErrorCode::OK if success,
 * @retval ErrorCode::ID_NOT_FOUND if the index id does not exist.
 * @retval otherwise an error code.
 */
ErrorCode Indexes::get(const ObjectId object_id,
                      boost::property_tree::ptree& object) const {

  log::function_start("Index::get(object_id)");

  ErrorCode error = ErrorCode::UNKNOWN;

  if (object_id > 0) {
    error = provider_->get_index_metadata(Object::ID, 
                                        std::to_string(object_id), 
                                        object);
  } else {
    LOG_WARNING
        << "An out-of-range value (0 or less) was specified for object ID.: "
        << object_id;
    error = ErrorCode::INVALID_PARAMETER;
  }

  log::function_finish("Index::get(object_id)", error);

  return error;
}

/**
 * @brief Get index metadata object based on name.
 * @param object_name [in]  object name.
 * @param object      [out] index metadata object with the specified name.
 * @retval ErrorCode::OK if success,
 * @retval ErrorCode::NAME_NOT_FOUND if the table name does not exist.
 * @retval otherwise an error code.
 */
ErrorCode Indexes::get(std::string_view object_name,
                      boost::property_tree::ptree& object) const {

  log::function_start("Indexes::get(object_name)");

  ErrorCode error = ErrorCode::UNKNOWN;

  if (!object_name.empty()) {
    error = provider_->get_index_metadata(Object::NAME, object_name, object);
  } else {
    LOG_WARNING << "An empty value was specified for TableName.";
    error = ErrorCode::INVALID_PARAMETER;
  }

  log::function_finish("Indexes::get(object_name)", error);

  return error;
}

/**
 * @brief Get all index metadata objects from the metadata table.
 *   If no index metadata existst, return the container as empty.
 * @param objects  [out] Container of metadata objects.
 * @return ErrorCode::OK if success, otherwise an error code.
 */
ErrorCode Indexes::get_all(
    std::vector<boost::property_tree::ptree>& objects) const {

  log::function_start("Tables::get_all()");

  ErrorCode error = provider_->get_index_metadata(objects);

  log::function_finish("Tables::get_all()", error);

  return error;
}

/**
 * @brief Update metadata-index with metadata-object.
 * @param object_id [in]  ID of the metadata-index to update.
 * @param object    [in]  metadata-object to update.
 * @return ErrorCode::OK if success, otherwise an error code.
 */
ErrorCode Indexes::update(const ObjectIdType object_id,
                          const boost::property_tree::ptree& object) const {

  log::function_start("Indexes::get(object_name)");

  ErrorCode error = ErrorCode::UNKNOWN;

  if (object_id > 0) {
    error = provider_->update_index_metadata(object_id, object);
  } else {
    LOG_WARNING
        << "An out-of-range value (0 or less) was specified for object ID.: "
        << object_id;
    error = ErrorCode::INVALID_PARAMETER;
  }

  log::function_finish("Indexes::get(object_name)", error);

  return error;
}

/**
 * @brief Remove a index metadata object which has the specified ID.
 * @param object_id  [in]  object id.
 * @retval ErrorCode::OK if success,
 * @retval ErrorCode::ID_NOT_FOUND if the object id does not exist.
 * @retval otherwise an error code.
 */
ErrorCode Indexes::remove(const ObjectId object_id) const {

  log::function_start("Indexes::remove(object_id)");

  ErrorCode error = ErrorCode::UNKNOWN;

  if (object_id > 0) {
    ObjectIdType ret_object_id = INVALID_OBJECT_ID;
    error = provider_->remove_index_metadata(
        Object::ID, std::to_string(object_id), ret_object_id);
  } else {
    LOG_WARNING
        << "An out-of-range value (0 or less) was specified for TableId.: "
        << object_id;
    error = ErrorCode::INVALID_PARAMETER;
  }

  log::function_finish("Indexes::remove(object_id)", error);

  return error;
}

/**
 * @brief Remove a indexmetadata object which has the specified name.
 * @param object_name  [in]  object name.
 * @param object_id    [out] ID of removed object.
 * @retval ErrorCode::OK if success,
 * @retval ErrorCode::NAME_NOT_FOUND if the table name does not exist.
 * @retval otherwise an error code.
 */
ErrorCode Indexes::remove(std::string_view object_name,
                          ObjectId* object_id) const {

  log::function_start("Indexes::remove(object_name)");

  ErrorCode error = ErrorCode::UNKNOWN;

  if (!object_name.empty()) {
    ObjectIdType ret_object_id = INVALID_OBJECT_ID;
    error = provider_->remove_index_metadata(Object::NAME, object_name,
                                            ret_object_id);

    if ((error == ErrorCode::OK) && (object_id != nullptr)) {
      *object_id = ret_object_id;
    }
  } else {
    LOG_WARNING << "An empty value was specified for TableName.";
    error = ErrorCode::INVALID_PARAMETER;
  }

  log::function_finish("Indexes::remove(object_name)", error);

  return error;
}

/* =============================================================================
 * Private method area
 */

/**
 * @brief Checks if the parameters for additional are correct.
 * @param object  [in]  metadata-object
 * @return ErrorCode::OK if success, otherwise an error code.
 */
ErrorCode Indexes::param_check_metadata_add(
    const boost::property_tree::ptree& object) const {
  ErrorCode error                        = ErrorCode::UNKNOWN;
  constexpr const char* const kLogFormat = R"("%s" => undefined or empty)";

  auto table_id = object.get_optional<ObjectId>(Index::TABLE_ID);
  if (table_id.value_or(0) > 0) {
    error = ErrorCode::OK;
  } else {
    LOG_ERROR << Message::PARAMETER_FAILED << "\"" << Index::TABLE_ID
              << "\" => undefined or empty";

    error = ErrorCode::INSUFFICIENT_PARAMETERS;
  }

  return error;
}

} // namespace manager::metadata
