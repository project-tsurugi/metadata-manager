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

#include <memory>
#include <string>

#include <boost/format.hpp>

#include "manager/metadata/common/config.h"
#include "manager/metadata/dao/postgresql/dbc_utils_pg.h"
#include "manager/metadata/roles.h"

namespace {

manager::metadata::db::postgresql::ConnectionSPtr connection;

}  // namespace

namespace manager::metadata::testing {

using manager::metadata::db::postgresql::DbcUtils;

/**
 * @brief create a role for testing.
 * @param (role_name)  [in]   role name.
 * @param (options)    [in]   options to pass to CREATE ROLE.
 * @return role id.
 */
ObjectIdType RoleMetadataHelper::create_role(std::string_view role_name,
                                             std::string_view options) {
  std::int64_t role_id = 0;
  boost::format statement;

  // db connection.
  db_connection();

  statement = boost::format("CREATE ROLE %s %s") % role_name % options;
  PGresult* res = PQexec(connection.get(), statement.str().c_str());
  PQclear(res);

  statement =
      boost::format("SELECT oid FROM pg_authid WHERE rolname='%s'") % role_name;
  res = PQexec(connection.get(), statement.str().c_str());
  DbcUtils::str_to_integral(PQgetvalue(res, 0, 0), role_id);
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
 * @brief Verifies that returned role metadata equals expected one.
 * @param (actual)    [in] role metadata returned from api to get
 *   role metadata
 * @param (expected)  [in] Expected role metadata.
 */
void RoleMetadataHelper::check_roles_expected(
    const boost::property_tree::ptree& actual,
    const boost::property_tree::ptree& expected) {
  // Check the value of the format_version.
  auto format_version_actual =
      actual.get<FormatVersionType>(Roles::FORMAT_VERSION);
  auto format_version_expect =
      expected.get_optional<FormatVersionType>(Roles::FORMAT_VERSION);
  if (format_version_expect) {
    EXPECT_EQ(format_version_actual, format_version_expect.value());
  }

  // Check the value of the generation.
  auto generation_actual = actual.get<GenerationType>(Roles::GENERATION);
  auto generation_expect =
      expected.get_optional<GenerationType>(Roles::GENERATION);
  if (generation_expect) {
    EXPECT_EQ(generation_actual, generation_expect.value());
  }

  // Check the value of the oid.
  auto oid_actual = actual.get<ObjectIdType>(Roles::ROLE_OID);
  auto oid_expect = expected.get_optional<ObjectIdType>(Roles::ROLE_OID);
  if (oid_expect) {
    EXPECT_EQ(oid_actual, oid_expect.value());
  } else {
    EXPECT_GT(oid_actual, 0);
  }

  // Check the value of the rolname.
  auto name_actual = actual.get<std::string>(Roles::ROLE_ROLNAME);
  auto name_expect = expected.get_optional<std::string>(Roles::ROLE_ROLNAME);
  if (name_expect) {
    EXPECT_EQ(name_actual, name_expect.value());
  }

  // Check the value of the rolsuper.
  auto super_actual = actual.get<std::string>(Roles::ROLE_ROLSUPER);
  auto super_expect = expected.get_optional<std::string>(Roles::ROLE_ROLSUPER);
  if (super_expect) {
    EXPECT_EQ(super_actual, super_expect.value());
  }

  // Check the value of the rolinherit.
  auto inherit_actual = actual.get<std::string>(Roles::ROLE_ROLINHERIT);
  auto inherit_expect =
      expected.get_optional<std::string>(Roles::ROLE_ROLINHERIT);
  if (inherit_expect) {
    EXPECT_EQ(inherit_actual, inherit_expect.value());
  }

  // Check the value of the rolcreaterole.
  auto createrole_actual = actual.get<std::string>(Roles::ROLE_ROLCREATEROLE);
  auto createrole_expect =
      expected.get_optional<std::string>(Roles::ROLE_ROLCREATEROLE);
  if (createrole_expect) {
    EXPECT_EQ(createrole_actual, createrole_expect.value());
  }

  // Check the value of the rolcreatedb.
  auto createdb_actual = actual.get<std::string>(Roles::ROLE_ROLCREATEDB);
  auto createdb_expect =
      expected.get_optional<std::string>(Roles::ROLE_ROLCREATEDB);
  if (createdb_expect) {
    EXPECT_EQ(createdb_actual, createdb_expect.value());
  }

  // Check the value of the rolcanlogin.
  auto canlogin_actual = actual.get<std::string>(Roles::ROLE_ROLCANLOGIN);
  auto canlogin_expect =
      expected.get_optional<std::string>(Roles::ROLE_ROLCANLOGIN);
  if (canlogin_expect) {
    EXPECT_EQ(canlogin_actual, canlogin_expect.value());
  }

  // Check the value of the rolreplication.
  auto replication_actual = actual.get<std::string>(Roles::ROLE_ROLREPLICATION);
  auto replication_expect =
      expected.get_optional<std::string>(Roles::ROLE_ROLREPLICATION);
  if (replication_expect) {
    EXPECT_EQ(replication_actual, replication_expect.value());
  }

  // Check the value of the rolbypassrls.
  auto bypassrls_actual = actual.get<std::string>(Roles::ROLE_ROLBYPASSRLS);
  auto bypassrls_expect =
      expected.get_optional<std::string>(Roles::ROLE_ROLBYPASSRLS);
  if (bypassrls_expect) {
    EXPECT_EQ(bypassrls_actual, bypassrls_expect.value());
  }

  // Check the value of the rolconnlimit.
  auto connlimit_actual = actual.get<std::int32_t>(Roles::ROLE_ROLCONNLIMIT);
  auto connlimit_expect =
      expected.get_optional<std::int32_t>(Roles::ROLE_ROLCONNLIMIT);
  if (connlimit_expect) {
    EXPECT_EQ(connlimit_actual, connlimit_expect.value());
  }

  // Check the value of the rolpassword.
  auto password_actual = actual.get<std::string>(Roles::ROLE_ROLPASSWORD);
  auto password_expect =
      expected.get_optional<std::string>(Roles::ROLE_ROLPASSWORD);
  if (password_expect) {
    EXPECT_EQ(password_actual, password_expect.value());
  }

  // Check the value of the rolvaliduntil.
  auto validuntil_actual = actual.get<std::string>(Roles::ROLE_ROLVALIDUNTIL);
  auto validuntil_expect =
      expected.get_optional<std::string>(Roles::ROLE_ROLVALIDUNTIL);
  if (validuntil_expect) {
    EXPECT_EQ(validuntil_actual, validuntil_expect.value());
  }
}

/**
 * @brief Connect to the database.
 */
void RoleMetadataHelper::db_connection() {
  if (!DbcUtils::is_open(connection)) {
    // db connection.
    PGconn* pgconn = PQconnectdb(Config::get_connection_string().c_str());
    connection = DbcUtils::make_connection_sptr(pgconn);

    ASSERT_TRUE(DbcUtils::is_open(connection));
  }
}

}  // namespace manager::metadata::testing
