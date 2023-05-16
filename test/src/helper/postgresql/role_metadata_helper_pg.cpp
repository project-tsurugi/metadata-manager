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
#include "test/helper/postgresql/role_metadata_helper_pg.h"

#include <gtest/gtest.h>

#include <memory>
#include <string>

#include <boost/format.hpp>

#include "manager/metadata/common/config.h"
#include "manager/metadata/common/utility.h"
#include "manager/metadata/dao/postgresql/dbc_utils_pg.h"
#include "manager/metadata/roles.h"
#include "test/common/ut_utils.h"

namespace {

manager::metadata::db::PgConnectionPtr connection;

}  // namespace

namespace manager::metadata::testing {

using manager::metadata::db::DbcUtils;

/**
 * @brief create a role for testing.
 * @param (role_name)  [in]   role name.
 * @param (options)    [in]   options to pass to CREATE ROLE.
 * @return role id.
 */
ObjectIdType RoleMetadataHelperPg::create_role(std::string_view role_name,
                                               std::string_view options) {
  std::int64_t role_id = 0;
  boost::format statement;

  // db connection.
  db_connection();

  UTUtils::print("-- create role --");
  UTUtils::print(" ", role_name, " (", options, ")");

  statement     = boost::format("CREATE ROLE %s %s") % role_name % options;
  PGresult* res = PQexec(connection.get(), statement.str().c_str());
  PQclear(res);

  statement =
      boost::format("SELECT oid FROM pg_authid WHERE rolname='%s'") % role_name;
  res = PQexec(connection.get(), statement.str().c_str());
  Utility::str_to_numeric(PQgetvalue(res, 0, 0), role_id);
  PQclear(res);

  UTUtils::print(" >> new role_id: ", role_id);

  return role_id;
}

/**
 * @brief remove a role for testing.
 * @param (role_name)  [in]   role name.
 */
void RoleMetadataHelperPg::drop_role(std::string_view role_name) {
  // db connection.
  db_connection();

  // remove dummy data for ROLE.
  boost::format statement = boost::format("DROP ROLE %s") % role_name;
  PGresult* res           = PQexec(connection.get(), statement.str().c_str());
  PQclear(res);
}

/**
 * @brief Connect to the database.
 */
void RoleMetadataHelperPg::db_connection() {
  if (!DbcUtils::is_open(connection)) {
    // db connection.
    PGconn* pgconn = PQconnectdb(Config::get_connection_string().c_str());
    connection     = DbcUtils::make_connection_sptr(pgconn);

    ASSERT_TRUE(DbcUtils::is_open(connection));
  }
}

}  // namespace manager::metadata::testing
