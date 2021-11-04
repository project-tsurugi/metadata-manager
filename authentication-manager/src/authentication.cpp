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
#include "manager/authentication/authentication.h"

#include <boost/property_tree/ptree.hpp>
#include <memory>

#include "manager/authentication/provider/authentication_provider.h"

// =============================================================================
namespace manager::authentication {

/**
 * @brief Authentication is performed based on the connection information of the
 *   specified database.
 * @param (params)  [in]  connection parameters.
 * @retval ErrorCode::OK if success.
 * @retval ErrorCode::AUTHENTICATION_FAILURE If the authentication failed.
 * @retval ErrorCode::CONNECTION_FAILURE If the connection to the
 *   database failed.
 */
ErrorCode Authentication::auth_user(const boost::property_tree::ptree& params) {
  ErrorCode error = ErrorCode::UNKNOWN;

  db::AuthenticationProvider provider;

  error = provider.auth_user(params);

  return error;
}

/**
 * @brief Authentication is performed based on the connection string of the
 *   specified database.
 * @param (conninfo)  [in]  connection string.
 * @retval ErrorCode::OK if success.
 * @retval ErrorCode::AUTHENTICATION_FAILURE If the authentication failed.
 * @retval ErrorCode::CONNECTION_FAILURE If the connection to the
 *   database failed.
 */
ErrorCode Authentication::auth_user(std::string_view conninfo) {
  ErrorCode error = ErrorCode::UNKNOWN;

  db::AuthenticationProvider provider;

  error = provider.auth_user(conninfo);

  return error;
}

}  // namespace manager::authentication
