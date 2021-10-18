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
#include "test/api_test_roles.h"

#include <boost/format.hpp>
#include <memory>
#include <string>

#include "manager/metadata/dao/common/config.h"
#include "manager/metadata/dao/postgresql/dbc_utils.h"
#include "manager/metadata/roles.h"
#include "test/global_test_environment.h"
#include "test/utility/ut_utils.h"

namespace {

std::shared_ptr<PGconn> connection;
constexpr std::string_view role_name = "tsurugi_api_ut_role_user_1";
Oid role_id = 0;

}  // namespace

namespace manager::metadata::testing {

namespace storage = manager::metadata::db::postgresql;
using namespace manager::metadata;
using namespace boost::property_tree;

void ApiTestRolesMetadata::SetUp() { UTUtils::skip_if_connection_not_opened(); }

/**
 * @brief create dummy data for ROLE.
 */
void ApiTestRolesMetadata::create_role() {
  connection = storage::DbcUtils::make_connection_sptr(
      PQconnectdb(db::Config::get_connection_string().c_str()));

  boost::format statement = boost::format(
                                "CREATE ROLE %s NOINHERIT CREATEROLE CREATEDB"
                                " REPLICATION CONNECTION LIMIT 10") %
                            role_name;
  PGresult* res = PQexec(connection.get(), statement.str().c_str());
  PQclear(res);

  statement =
      boost::format("SELECT oid FROM pg_authid WHERE rolname='%s'") % role_name;
  res = PQexec(connection.get(), statement.str().c_str());
  role_id = str_to_oid(PQgetvalue(res, 0, 0));
  PQclear(res);
}

/**
 * @brief remove dummy data for ROLE.
 */
void ApiTestRolesMetadata::drop_role() {
  connection = storage::DbcUtils::make_connection_sptr(
      PQconnectdb(db::Config::get_connection_string().c_str()));

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
void ApiTestRolesMetadata::check_roles_expected(const ptree& actual,
                                                const ptree& expected) {
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
 * @brief Convert string to Oid.
 * @param (source)  [in]   C-string containing the representation of
 *   decimal integer literal (base 10).
 * @return the converted integral number.
 */
Oid ApiTestRolesMetadata::str_to_oid(const char* source) {
  std::int64_t res_val = 0;
  storage::DbcUtils::str_to_integral(source, res_val);
  return static_cast<Oid>(res_val);
}

/**
 * @brief Happy test for getting all data type metadata based on data type
 * name.
 */
TEST_F(ApiTestRolesMetadata, get_role) {
  ErrorCode error = ErrorCode::UNKNOWN;

  // create dummy data for ROLE.
  ApiTestRolesMetadata::create_role();

  auto roles = std::make_unique<Roles>(GlobalTestEnvironment::TEST_DB);
  error = roles->init();
  EXPECT_EQ(ErrorCode::OK, error);

  ptree role_metadata;
  ptree expect_metadata;
  expect_metadata.put(Roles::FORMAT_VERSION, Roles::format_version());
  expect_metadata.put(Roles::GENERATION, Roles::generation());
  expect_metadata.put(Roles::ROLE_ROLNAME, role_name);
  expect_metadata.put(Roles::ROLE_ROLSUPER, "f");        // false
  expect_metadata.put(Roles::ROLE_ROLINHERIT, "f");      // false
  expect_metadata.put(Roles::ROLE_ROLCREATEROLE, "t");   // true
  expect_metadata.put(Roles::ROLE_ROLCREATEDB, "t");     // true
  expect_metadata.put(Roles::ROLE_ROLCANLOGIN, "f");     // false
  expect_metadata.put(Roles::ROLE_ROLREPLICATION, "t");  // true
  expect_metadata.put(Roles::ROLE_ROLBYPASSRLS, "f");    // false
  expect_metadata.put(Roles::ROLE_ROLCONNLIMIT, "10");   // 10
  expect_metadata.put(Roles::ROLE_ROLPASSWORD, "");      // empty
  expect_metadata.put(Roles::ROLE_ROLVALIDUNTIL, "");    // empty

  // test getting by role id.
  error = roles->get(role_id, role_metadata);
  EXPECT_EQ(ErrorCode::OK, error);

  UTUtils::print("-- get role metadata by role id --");
  UTUtils::print(UTUtils::get_tree_string(role_metadata));

  // verifies that returned role metadata equals expected one.
  ApiTestRolesMetadata::check_roles_expected(role_metadata, expect_metadata);

  // clear property_tree.
  role_metadata.clear();

  // test getting by role name.
  error = roles->get(role_name, role_metadata);
  EXPECT_EQ(ErrorCode::OK, error);

  UTUtils::print("-- get role metadata by role name --");
  UTUtils::print(UTUtils::get_tree_string(role_metadata));

  // verifies that returned role metadata equals expected one.
  ApiTestRolesMetadata::check_roles_expected(role_metadata, expect_metadata);

  // remove dummy data for ROLE.
  ApiTestRolesMetadata::drop_role();
}

/**
 * @brief Happy test for getting all data type metadata based on data type
 * name.
 */
TEST_F(ApiTestRolesMetadata, role_does_not_exist) {
  ErrorCode error = ErrorCode::UNKNOWN;

  auto roles = std::make_unique<Roles>(GlobalTestEnvironment::TEST_DB);
  error = roles->init();
  EXPECT_EQ(ErrorCode::OK, error);

  ptree role_metadata;
  ObjectIdType role_id;
  std::string role_name;

  // the role id (0) does not exist.
  role_id = 0;
  UTUtils::print("  Test pattern: [", role_id, "]");
  error = roles->get(0, role_metadata);
  EXPECT_EQ(ErrorCode::ID_NOT_FOUND, error);

  // the role id (9999999) does not exist.
  role_id = 9999999L;
  UTUtils::print("  Test pattern: [", role_id, "]");
  error = roles->get(role_id, role_metadata);
  EXPECT_EQ(ErrorCode::ID_NOT_FOUND, error);

  // the role name is empty.
  role_name = "";
  UTUtils::print("  Test pattern: [", role_name, "]");
  error = roles->get(role_name, role_metadata);
  EXPECT_EQ(ErrorCode::NAME_NOT_FOUND, error);

  // the role name does not exist.
  role_name = "undefined-name";
  UTUtils::print("  Test pattern: [", role_name, "]");
  error = roles->get(role_name, role_metadata);
  EXPECT_EQ(ErrorCode::NAME_NOT_FOUND, error);
}

}  // namespace manager::metadata::testing
