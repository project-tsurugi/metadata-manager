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
#include "manager/authentication/provider/authentication_provider.h"

#include "manager/authentication/common/config.h"

#if defined(AUTHENTICATE_POSTGRESQL)
#include "manager/authentication/dao/postgresql/db_session_manager.h"
#endif

// =============================================================================
namespace manager::authentication::db {

#if defined(AUTHENTICATE_POSTGRESQL)
namespace storage = postgresql;
#endif

using manager::authentication::ErrorCode;
using storage::DBSessionManager;

/**
 * @brief Authentication is performed based on the connection information of the
 *   specified database.
 * @param (connection_params)  [in]  connection parameters.
 * @retval ErrorCode::OK if success.
 * @retval ErrorCode::AUTHENTICATION_FAILURE If the authentication failed.
 * @retval ErrorCode::CONNECTION_FAILURE If the connection to the
 *   database failed.
 */
ErrorCode AuthenticationProvider::auth_user(
    const boost::property_tree::ptree& connection_params) {
  ErrorCode error = ErrorCode::UNKNOWN;

  // Attempt to connect via the Session Manager.
  error = DBSessionManager::attempt_connection(connection_params);

  return error;
}

/**
 * @brief Authentication is performed based on the connection string of the
 *   specified database.
 * @param (connection_string)  [in]  connection string.
 * @retval ErrorCode::OK if success.
 * @retval ErrorCode::AUTHENTICATION_FAILURE If the authentication failed.
 * @retval ErrorCode::CONNECTION_FAILURE If the connection to the
 *   database failed.
 */
ErrorCode AuthenticationProvider::auth_user(
    std::string_view connection_string) {
  ErrorCode error = ErrorCode::UNKNOWN;

  // Attempt to connect via the Session Manager.
  error = DBSessionManager::attempt_connection(connection_string, std::nullopt,
                                               std::nullopt);

  return error;
}

/**
 * @brief Authentication is performed based on the specified user name and
 *   password.
 * @param (connection_string)  [in]  connection string.
 * @param (user_name)          [in]  user name to authenticate.
 * @param (password)           [in]  passward.
 * @retval ErrorCode::OK if success.
 * @retval ErrorCode::AUTHENTICATION_FAILURE If the authentication failed.
 * @retval ErrorCode::CONNECTION_FAILURE If the connection to the
 *   database failed.
 */
ErrorCode AuthenticationProvider::auth_user(
    const std::optional<std::string> connection_string,
    std::string_view user_name, std::string_view password) {
  ErrorCode error = ErrorCode::UNKNOWN;

  // Get the specified value or environment variable value.
  std::string conninfo =
      connection_string.value_or(Config::get_connection_string());

  // Attempt to connect via the Session Manager.
  error = DBSessionManager::attempt_connection(conninfo,
                                               std::optional(user_name.data()),
                                               std::optional(password.data()));

  return error;
}

}  // namespace manager::authentication::db
