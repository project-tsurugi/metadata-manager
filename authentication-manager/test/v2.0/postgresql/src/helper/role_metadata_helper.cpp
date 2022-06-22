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
#include "test/helper/role_metadata_helper.h"

#include <gtest/gtest.h>
#include <libpq-fe.h>

#include <boost/format.hpp>
#include <memory>
#include <string>

namespace {

std::shared_ptr<PGconn> connection;

}  // namespace

namespace manager::authentication::testing {

/**
 * @brief create a role for testing.
 * @param (role_name)  [in]   role name.
 * @param (options)    [in]   options to pass to CREATE ROLE.
 * @return role id.
 */
std::int64_t RoleMetadataHelper::create_role(std::string_view role_name,
                                             std::string_view options) {
  std::int64_t role_id = 0;
  boost::format statement;

  // db connection.
  db_connection();

  statement = boost::format("CREATE ROLE %s %s") % role_name % options;
  PGresult* res = PQexec(connection.get(), statement.str().c_str());
  EXPECT_EQ(PGRES_COMMAND_OK, PQresultStatus(res));
  PQclear(res);

  statement =
      boost::format("SELECT oid FROM pg_authid WHERE rolname='%s'") % role_name;
  res = PQexec(connection.get(), statement.str().c_str());
  EXPECT_EQ(PGRES_TUPLES_OK, PQresultStatus(res));

  if (PQresultStatus(res) == PGRES_TUPLES_OK) {
    char* end;
    char* res_value = PQgetvalue(res, 0, 0);
    if (res_value != nullptr) {
      role_id = std::strtoul(res_value, &end, 10);
    }
  }
  PQclear(res);

  return role_id;
}

/**
 * @brief remove a role for testing.
 * @param (role_name)  [in]   role name.
 */
void RoleMetadataHelper::drop_role(std::string_view role_name) {
  // db connection.
  db_connection();

  // remove dummy data for ROLE.
  boost::format statement = boost::format("DROP ROLE %s") % role_name;
  PGresult* res = PQexec(connection.get(), statement.str().c_str());
  PQclear(res);
}

/**
 * @brief Connect to the database.
 */
void RoleMetadataHelper::db_connection() {
  if (PQstatus(connection.get()) != CONNECTION_OK) {
    // db connection.
    PGconn* pgconn = PQconnectdb("dbname=tsurugi");
    std::shared_ptr<PGconn> conn(pgconn, [](PGconn* c) { ::PQfinish(c); });
    connection = conn;

    ASSERT_EQ(CONNECTION_OK, PQstatus(connection.get()));
  }
}

}  // namespace manager::authentication::testing
