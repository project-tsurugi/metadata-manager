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

#include <regex>

#include <boost/format.hpp>

// =============================================================================
namespace {

namespace uri {

static constexpr const char* const kRegexUri =
    R"(^(postgres(ql)://)(.*@|)(.*))";
static constexpr std::uint32_t kRegexUriPrefixPos = 1;
static constexpr std::uint32_t kRegexUriSuffixPos = 4;

}  // namespace uri

namespace key_value {

static constexpr const char* const kUserName = "user";
static constexpr const char* const kPassword = "password";

}  // namespace key_value

}  // namespace

// =============================================================================
namespace manager::authentication::db::postgresql {

using manager::authentication::ErrorCode;

/**
 * @brief Attempt to connect to the database.
 * @param (params)  [in]  connection parameters.
 * @retval ErrorCode::OK if success.
 * @retval ErrorCode::AUTHENTICATION_FAILURE If the authentication failed.
 * @retval ErrorCode::CONNECTION_FAILURE If the connection to the
 *   database failed.
 */
ErrorCode DBSessionManager::attempt_connection(
    const boost::property_tree::ptree& params) {
  ErrorCode error = ErrorCode::UNKNOWN;

  // Generating connection information.
  std::string connection_string = "";
  for (auto pos = params.begin(); pos != params.end(); pos++) {
    boost::format key_value = boost::format("%s %s=%s") % connection_string %
                              pos->first % pos->second.data();
    connection_string = key_value.str();
  }

  // Inspect the connection to the DB host.
  if (PQping(connection_string.c_str()) == PQPING_OK) {
    // Attempt to login to the DB.
    PGconn* pgconn = PQconnectdb(connection_string.c_str());
    error = (PQstatus(pgconn) == CONNECTION_OK)
                ? ErrorCode::OK
                : ErrorCode::AUTHENTICATION_FAILURE;
    PQfinish(pgconn);
  } else {
    error = ErrorCode::CONNECTION_FAILURE;
  }

  return error;
}

/**
 * @brief Attempt to connect to the database.
 * @param (params)     [in]  connection parameters.
 * @param (user_name)  [in]  user name to authenticate.
 * @param (password)   [in]  passward.
 * @retval ErrorCode::OK if success.
 * @retval ErrorCode::AUTHENTICATION_FAILURE If the authentication failed.
 * @retval ErrorCode::CONNECTION_FAILURE If the connection to the
 *   database failed.
 */
ErrorCode DBSessionManager::attempt_connection(
    std::string_view params, const std::optional<std::string> user_name,
    const std::optional<std::string> password) {
  ErrorCode error = ErrorCode::UNKNOWN;

  std::string connection_string = std::string(params);

  // Insert the user name into the connection string.
  if (!user_name.value_or("").empty()) {
    std::string conn_string_prefix = "";
    std::string conn_string_suffix = "";
    std::string conn_string_auth_info = "";

    std::smatch regex_results;
    std::regex_match(connection_string, regex_results,
                     std::regex(uri::kRegexUri));

    if (!regex_results.empty()) {
      // Parameter type is URI.
      //   postgres[ql]://[user[:password]@][netloc][:port][/dbname][?param1=value1&...]
      conn_string_prefix =
          regex_results[uri::kRegexUriPrefixPos].str();  // postgres[ql]://
      conn_string_suffix =
          regex_results[uri::kRegexUriSuffixPos].str();  // [netloc][:port]...

      // <user-name>[:<password>]@
      auto fmter_auth_info = boost::format("%s%s%s@") % *user_name %
                             (!password.value_or("").empty() ? ":" : "") %
                             password.value_or("");
      conn_string_auth_info = fmter_auth_info.str();

    } else {
      // Parameter type is Key/Value.
      //   [host=<netloc>] [port=<port>] [dbname=<dbname>] ...
      conn_string_prefix = connection_string;  // [key=value]...
      conn_string_suffix = "";

      // Deletes the user and password on the connection string.
      auto fmter_replace_regex =
          boost::format(R"(\s*(%1%|%2%)\s*=\s*(["'].*['"]|\S+)\s*)") %
          key_value::kUserName % key_value::kPassword;
      conn_string_prefix = std::regex_replace(
          conn_string_prefix, std::regex(fmter_replace_regex.str()), " ");

      // user='<user-name>' [password='<password>']
      boost::format fmter_auth_info(" %s='%s'");
      // user='<user-name>'
      conn_string_auth_info =
          (fmter_auth_info % key_value::kUserName % *user_name).str() +
          (!password.value_or("").empty()
               ? (fmter_auth_info % key_value::kPassword % *password).str()
               : "");
    }

    // Compose a connection string.
    //   postgres[ql]://<user-name>[:<password>]@[netloc][:port][/dbname][?param1=value1&...]
    //   [key=value...] user='<user name>'[ password='<password>']
    connection_string =
        conn_string_prefix + conn_string_auth_info + conn_string_suffix;
  }

  // Inspect the connection to the DB host.
  if (PQping(connection_string.c_str()) == PQPING_OK) {
    // Attempt to login to the DB.
    PGconn* pgconn = PQconnectdb(connection_string.c_str());
    error = (PQstatus(pgconn) == ConnStatusType::CONNECTION_OK)
                ? ErrorCode::OK
                : ErrorCode::AUTHENTICATION_FAILURE;
    PQfinish(pgconn);
  } else {
    error = ErrorCode::CONNECTION_FAILURE;
  }

  return error;
}

}  // namespace manager::authentication::db::postgresql
