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
#include "manager/metadata/roles.h"

#include <memory>

#include "manager/metadata/provider/roles_provider.h"

// =============================================================================
namespace {

std::unique_ptr<manager::metadata::db::RolesProvider> provider = nullptr;

}  // namespace

// =============================================================================
namespace manager::metadata {

using manager::metadata::ErrorCode;

/**
 * @brief Constructor
 * @param (database)   [in]  database name.
 * @param (component)  [in]  component name.
 */
Roles::Roles(std::string_view database, std::string_view component)
    : Metadata(database, component) {
  // Create the provider.
  provider = std::make_unique<db::RolesProvider>();
}

/**
 * @brief Initialization.
 * @param none.
 * @return ErrorCode::OK if success, otherwise an error code.
 */
ErrorCode Roles::init() const {
  // Initialize the provider.
  ErrorCode error = provider->init();

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
  if (object_id <= 0) {
    return ErrorCode::ID_NOT_FOUND;
  }

  // Get the role metadata through the provider.
  std::string s_object_id = std::to_string(object_id);
  ErrorCode error =
      provider->get_role_metadata(Roles::ROLE_OID, s_object_id, object);

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
  if (object_name.empty()) {
    return ErrorCode::NAME_NOT_FOUND;
  }

  // Get the role metadata through the provider.
  ErrorCode error =
      provider->get_role_metadata(Roles::ROLE_ROLNAME, object_name, object);

  return error;
}

}  // namespace manager::metadata
