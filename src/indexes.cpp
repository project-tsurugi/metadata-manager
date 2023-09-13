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
#include "manager/metadata/indexes.h"

#include <memory>

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

// ==========================================================================
// Indexes class methods.
/**
 * @brief Initialization.
 * @return ErrorCode::OK if success, otherwise an error code.
 */
ErrorCode Indexes::init() const {
  ErrorCode error = ErrorCode::UNKNOWN;

  // Log of API function start.
  log::function_start("Indexes::init()");

  error = provider.init();

  // Log of API function finish.
  log::function_finish("Indexes::init()", error);

  return error;
}

/**
 * @brief Add index metadata to index metadata table.
 * @param object  [in]  index metadata to add.
 * @return ErrorCode::OK if success, otherwise an error code.
 */
ErrorCode Indexes::add(const boost::property_tree::ptree& object) const {
  // Adds the index metadata through the class method.
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
  ErrorCode error = ErrorCode::UNKNOWN;

  // Log of API function start.
  log::function_start("Indexes::add()");

  // Parameter value check.
  error = param_check_metadata_add(object);

  ObjectId added_oid = INVALID_OBJECT_ID;
  if (error == ErrorCode::OK) {
    // Add index metadata within a transaction.
    error = provider.transaction([&object, &added_oid]() -> ErrorCode {
      // Adds the index metadata through the provider.
      return provider.add_index_metadata(object, &added_oid);
    });
  }

  // Set a value if object_id is not null.
  if ((error == ErrorCode::OK) && (object_id != nullptr)) {
    *object_id = added_oid;
  }

  // Log of API function finish.
  log::function_finish("Indexes::add()", error);

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
  ErrorCode error = ErrorCode::UNKNOWN;

  // Log of API function start.
  log::function_start("Indexes::get(object_id)");

  // Specify the key for the index metadata you want to retrieve.
  std::string index_id(std::to_string(object_id));
  std::map<std::string_view, std::string_view> keys = {
      {Index::ID, index_id}
  };

  // Retrieve index metadata.
  ptree tmp_object;
  if (object_id > 0) {
    // Get the index metadata through the provider.
    error = provider.get_index_metadata(keys, tmp_object);
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
  log::function_finish("Indexes::get(object_id)", error);

  return error;
  }

/**
 * @brief Get index metadata object based on name.
 * @param object_name [in]  object name.
 * @param object      [out] index metadata object with the specified name.
 * @retval ErrorCode::OK if success,
 * @retval ErrorCode::NAME_NOT_FOUND if the index name does not exist.
 * @retval otherwise an error code.
 */
ErrorCode Indexes::get(std::string_view object_name,
                      boost::property_tree::ptree& object) const {
  ErrorCode error = ErrorCode::UNKNOWN;

  // Log of API function start.
  log::function_start("Indexes::get(object_name)");

  // Specify the key for the index metadata you want to retrieve.
  std::map<std::string_view, std::string_view> keys = {
      {Index::NAME, object_name}
  };

  // Retrieve index metadata.
  ptree tmp_object;
  if (!object_name.empty()) {
    // Get the index metadata through the provider.
    error = provider.get_index_metadata(keys, tmp_object);
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
  log::function_finish("Indexes::get(object_name)", error);

  return error;
}

/**
 * @brief Get all index metadata objects from the metadata table.
 *   If no index metadata exists, return the container as empty.
 * @param objects  [out] Container of metadata objects.
 * @return ErrorCode::OK if success, otherwise an error code.
 */
ErrorCode Indexes::get_all(
    std::vector<boost::property_tree::ptree>& objects) const {
  ErrorCode error = ErrorCode::UNKNOWN;

  // Log of API function start.
  log::function_start("Indexes::get_all()");

  ptree tmp_object;
  std::map<std::string_view, std::string_view> keys = {};

  // Get the index metadata through the provider.
  error = provider.get_index_metadata(keys, tmp_object);

  // Converts object types.
  if (error == ErrorCode::OK) {
    objects = ptree_helper::array_to_vector(tmp_object);
  } else if (error == ErrorCode::NOT_FOUND) {
    // Converts error code.
    error = ErrorCode::OK;
  }

  // Log of API function finish.
  log::function_finish("Indexes::get_all()", error);

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
  ErrorCode error = ErrorCode::UNKNOWN;

  // Log of API function start.
  log::function_start("Indexes::update(object_id)");

  // Updated index metadata.
  if (object_id > 0) {
    // Specify the key for the index metadata you want to retrieve.
    std::string index_id(std::to_string(object_id));
    std::map<std::string_view, std::string_view> keys = {
        {Index::ID, index_id}
    };

    // Update index metadata within a transaction.
    error = provider.transaction([&keys, &object]() -> ErrorCode {
      // Update the index metadata through the provider.
      return provider.update_index_metadata(keys, object);
    });
  } else {
    LOG_WARNING
        << "An out-of-range value (0 or less) was specified for object ID.: "
        << object_id;
    error = ErrorCode::ID_NOT_FOUND;
  }

  // Log of API function finish.
  log::function_finish("Indexes::update(object_id)", error);

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
  ErrorCode error = ErrorCode::UNKNOWN;

  // Log of API function start.
  log::function_start("Indexes::remove(object_id)");

  // Remove the index metadata.
  if (object_id > 0) {
    // Specify the key for the index metadata you want to remove.
    std::string index_id(std::to_string(object_id));
    std::map<std::string_view, std::string_view> keys = {
        {Index::ID, index_id}
    };

    // Remove index metadata within a transaction.
    error = provider.transaction([&keys]() -> ErrorCode {
      // Remove the index metadata through the provider.
      return provider.remove_index_metadata(keys);
    });
  } else {
    LOG_WARNING
        << "An out-of-range value (0 or less) was specified for object ID.: "
        << object_id;
    error = ErrorCode::ID_NOT_FOUND;
  }

  // Log of API function finish.
  log::function_finish("Indexes::remove(object_id)", error);

  return error;
}

/**
 * @brief Remove a index metadata object which has the specified name.
 * @param object_name  [in]  object name.
 * @param object_id    [out] ID of removed object.
 * @retval ErrorCode::OK if success,
 * @retval ErrorCode::NAME_NOT_FOUND if the index name does not exist.
 * @retval otherwise an error code.
 */
ErrorCode Indexes::remove(std::string_view object_name,
                          ObjectId* object_id) const {
  ErrorCode error = ErrorCode::UNKNOWN;

  // Log of API function start.
  log::function_start("Indexes::remove(object_name)");

  // Specify the key for the index metadata you want to retrieve.
  std::map<std::string_view, std::string_view> keys = {
      {Index::NAME, object_name}
  };

  // Remove the index metadata.
  std::vector<ObjectId> removed_ids = {};
  if (!object_name.empty()) {
    // Remove index metadata within a transaction.
    error = provider.transaction([&keys, &removed_ids]() -> ErrorCode {
      // Remove the index metadata through the provider.
      return provider.remove_index_metadata(keys, &removed_ids);
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

}  // namespace manager::metadata
