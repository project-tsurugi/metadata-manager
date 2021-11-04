/*
 * Copyright 2021 tsurugi project.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
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
#include "manager/authentication/dao/postgresql/db_session_manager.h"

#include <libpq-fe.h>

#include <boost/format.hpp>
#include <iostream>
#include <memory>
#include <regex>

// =============================================================================
namespace manager::authentication::db::postgresql {

using manager::authentication::ErrorCode;

/**
 * @brief Check a connection to the database.
 * @param (params)  [in]  connection parameters.
 * @retval ErrorCode::OK if success.
 * @retval ErrorCode::AUTHENTICATION_FAILURE If the authentication failed.
 * @retval ErrorCode::CONNECTION_FAILURE If the connection to the
 *   database failed.
 */
ErrorCode DBSessionManager::check_connect(
    const boost::property_tree::ptree& params) {
  ErrorCode error = ErrorCode::UNKNOWN;

  // Generating connection information.
  std::string conninfo = "";
  auto optional_conninfo = params.get_optional<std::string>(kKeyConnInfo);
  if (optional_conninfo) {
    conninfo = optional_conninfo.value();
  } else {
    for (auto pos = params.begin(); pos != params.end(); pos++) {
      boost::format key_value = boost::format("%s %s=%s") % conninfo %
                                pos->first % pos->second.data();
      conninfo = key_value.str();
    }
  }

  // Inspect the connection to the DB host.
  if (PQping(conninfo.data()) == PQPING_OK) {
    // Attempt to login to the DB.
    PGconn* pgconn = PQconnectdb(conninfo.data());
    error = (PQstatus(pgconn) == CONNECTION_OK)
                ? ErrorCode::OK
                : ErrorCode::AUTHENTICATION_FAILURE;
    PQfinish(pgconn);
  } else {
    error = ErrorCode::CONNECTION_FAILURE;
  }

  return error;
}

}  // namespace manager::authentication::db::postgresql
