/*
 * Copyright 2021-2023 tsurugi project.
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
#include "manager/metadata/roles.h"

#include <memory>

#include "manager/metadata/helper/logging_helper.h"
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
ErrorCode Roles::init() const {
  // Log of API function start.
  log::function_start("Roles::init()");

  // Initialize the provider.
  ErrorCode error = provider.init();

  // Log of API function finish.
  log::function_finish("Roles::init()", error);

  return error;
}

/**
 * @brief Get role object based on role id.
 * @param (object_id)  [in]  role id.
 * @param (object)     [out] role with the specified ID.
 * @retval ErrorCode::OK if success.
 * @retval ErrorCode::ID_NOT_FOUND if the role id does not exist.
 * @retval otherwise an error code.
 */
ErrorCode Roles::get(const ObjectIdType object_id,
                     boost::property_tree::ptree& object) const {
  ErrorCode error = ErrorCode::UNKNOWN;

  // Log of API function start.
  log::function_start("Roles::get(object_id)");

  // Specify the key for the role object you want to retrieve.
  std::string role_id(std::to_string(object_id));
  std::map<std::string_view, std::string_view> keys = {
      {Roles::ROLE_OID, role_id}
  };

  // Retrieve role object.
  ptree tmp_object;
  if (object_id > 0) {
    // Get the role object through the provider.
    error = provider.get_role_metadata(keys, tmp_object);
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
  log::function_finish("Roles::get(object_id)", error);

  return error;
}

/**
 * @brief Get role object based on role name.
 * @param (object_name)   [in]  role name.
 * @param (object)        [out] role object with the specified name.
 * @retval ErrorCode::OK if success.
 * @retval ErrorCode::NAME_NOT_FOUND if the role name does not exist.
 * @retval otherwise an error code.
 */
ErrorCode Roles::get(std::string_view object_name,
                     boost::property_tree::ptree& object) const {
  ErrorCode error = ErrorCode::UNKNOWN;

  // Log of API function start.
  log::function_start("Roles::get(object_name)");

  // Specify the key for the role object you want to retrieve.
  std::map<std::string_view, std::string_view> keys = {
      {Roles::ROLE_ROLNAME, object_name}
  };

  // Retrieve role object.
  ptree tmp_object;
  if (!object_name.empty()) {
    // Get the role object through the provider.
    error = provider.get_role_metadata(keys, tmp_object);
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
  log::function_finish("Roles::get(object_name)", error);

  return error;
}

}  // namespace manager::metadata
