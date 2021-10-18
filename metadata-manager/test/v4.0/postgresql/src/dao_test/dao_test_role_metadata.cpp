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
#include "test/dao_test/dao_test_roles.h"

#include <libpq-fe.h>
#include <boost/format.hpp>
#include <memory>
#include <string>

#include "manager/metadata/dao/common/config.h"
#include "manager/metadata/dao/postgresql/db_session_manager.h"
#include "manager/metadata/dao/postgresql/dbc_utils.h"
#include "manager/metadata/dao/roles_dao.h"
#include "test/global_test_environment.h"
#include "test/utility/ut_utils.h"

namespace {

std::shared_ptr<PGconn> connection;
constexpr std::string_view role_name = "tsurugi_dao_ut_role_user_1";
std::string role_id = "";

}  // namespace

namespace manager::metadata::testing {

namespace storage = manager::metadata::db::postgresql;
using namespace manager::metadata::db;
using namespace boost::property_tree;

void DaoTestRolesMetadata::SetUp() {
  if (global->is_open()) {
    connection = storage::DbcUtils::make_connection_sptr(
        PQconnectdb(Config::get_connection_string().c_str()));

    boost::format statement =
        boost::format("CREATE ROLE %s NOINHERIT SUPERUSER LOGIN BYPASSRLS") %
        role_name;
    PGresult* res = PQexec(connection.get(), statement.str().c_str());
    PQclear(res);

    statement = boost::format("SELECT oid FROM pg_authid WHERE rolname='%s'") %
                role_name;
    res = PQexec(connection.get(), statement.str().c_str());
    role_id = std::string(PQgetvalue(res, 0, 0));
    PQclear(res);
  } else {
    GTEST_SKIP_("metadata repository is not started.");
  }
}

void DaoTestRolesMetadata::TearDown() {
  if (global->is_open()) {
    connection = storage::DbcUtils::make_connection_sptr(
        PQconnectdb(Config::get_connection_string().c_str()));

    boost::format statement = boost::format("DROP ROLE %s") % role_name;
    PGresult* res = PQexec(connection.get(), statement.str().c_str());
    PQclear(res);
  }
}

/**
 * @brief Verifies that returned role metadata equals expected one.
 * @param (actual)  [in] role metadata returned from api to get
 *   role metadata
 * @param (expect)  [in] Expected role metadata.
 */
void DaoTestRolesMetadata::check_roles_expected(const ptree& actual,
                                                const ptree& expect) {
  // Check the value of the format_version.
  auto format_version_actual =
      actual.get<FormatVersionType>(Roles::FORMAT_VERSION);
  auto format_version_expect =
      expect.get_optional<FormatVersionType>(Roles::FORMAT_VERSION);
  if (format_version_expect) {
    EXPECT_EQ(format_version_actual, format_version_expect.value());
  }

  // Check the value of the generation.
  auto generation_actual = actual.get<GenerationType>(Roles::GENERATION);
  auto generation_expect =
      expect.get_optional<GenerationType>(Roles::GENERATION);
  if (generation_expect) {
    EXPECT_EQ(generation_actual, generation_expect.value());
  }

  // Check the value of the oid.
  auto oid_actual = actual.get<ObjectIdType>(Roles::ROLE_OID);
  auto oid_expect = expect.get_optional<ObjectIdType>(Roles::ROLE_OID);
  if (oid_expect) {
    EXPECT_EQ(oid_actual, oid_expect.value());
  } else {
    EXPECT_GT(oid_actual, 0);
  }

  // Check the value of the rolname.
  auto name_actual = actual.get<std::string>(Roles::ROLE_ROLNAME);
  auto name_expect = expect.get_optional<std::string>(Roles::ROLE_ROLNAME);
  if (name_expect) {
    EXPECT_EQ(name_actual, name_expect.value());
  }

  // Check the value of the rolsuper.
  auto super_actual = actual.get<std::string>(Roles::ROLE_ROLSUPER);
  auto super_expect = expect.get_optional<std::string>(Roles::ROLE_ROLSUPER);
  if (super_expect) {
    EXPECT_EQ(super_actual, super_expect.value());
  }

  // Check the value of the rolinherit.
  auto inherit_actual = actual.get<std::string>(Roles::ROLE_ROLINHERIT);
  auto inherit_expect =
      expect.get_optional<std::string>(Roles::ROLE_ROLINHERIT);
  if (inherit_expect) {
    EXPECT_EQ(inherit_actual, inherit_expect.value());
  }

  // Check the value of the rolcreaterole.
  auto createrole_actual = actual.get<std::string>(Roles::ROLE_ROLCREATEROLE);
  auto createrole_expect =
      expect.get_optional<std::string>(Roles::ROLE_ROLCREATEROLE);
  if (createrole_expect) {
    EXPECT_EQ(createrole_actual, createrole_expect.value());
  }

  // Check the value of the rolcreatedb.
  auto createdb_actual = actual.get<std::string>(Roles::ROLE_ROLCREATEDB);
  auto createdb_expect =
      expect.get_optional<std::string>(Roles::ROLE_ROLCREATEDB);
  if (createdb_expect) {
    EXPECT_EQ(createdb_actual, createdb_expect.value());
  }

  // Check the value of the rolcanlogin.
  auto canlogin_actual = actual.get<std::string>(Roles::ROLE_ROLCANLOGIN);
  auto canlogin_expect =
      expect.get_optional<std::string>(Roles::ROLE_ROLCANLOGIN);
  if (canlogin_expect) {
    EXPECT_EQ(canlogin_actual, canlogin_expect.value());
  }

  // Check the value of the rolreplication.
  auto replication_actual = actual.get<std::string>(Roles::ROLE_ROLREPLICATION);
  auto replication_expect =
      expect.get_optional<std::string>(Roles::ROLE_ROLREPLICATION);
  if (replication_expect) {
    EXPECT_EQ(replication_actual, replication_expect.value());
  }

  // Check the value of the rolbypassrls.
  auto bypassrls_actual = actual.get<std::string>(Roles::ROLE_ROLBYPASSRLS);
  auto bypassrls_expect =
      expect.get_optional<std::string>(Roles::ROLE_ROLBYPASSRLS);
  if (bypassrls_expect) {
    EXPECT_EQ(bypassrls_actual, bypassrls_expect.value());
  }

  // Check the value of the rolconnlimit.
  auto connlimit_actual = actual.get<std::int32_t>(Roles::ROLE_ROLCONNLIMIT);
  auto connlimit_expect =
      expect.get_optional<std::int32_t>(Roles::ROLE_ROLCONNLIMIT);
  if (connlimit_expect) {
    EXPECT_EQ(connlimit_actual, connlimit_expect.value());
  }

  // Check the value of the rolpassword.
  auto password_actual = actual.get<std::string>(Roles::ROLE_ROLPASSWORD);
  auto password_expect =
      expect.get_optional<std::string>(Roles::ROLE_ROLPASSWORD);
  if (password_expect) {
    EXPECT_EQ(password_actual, password_expect.value());
  }

  // Check the value of the rolvaliduntil.
  auto validuntil_actual = actual.get<std::string>(Roles::ROLE_ROLVALIDUNTIL);
  auto validuntil_expect =
      expect.get_optional<std::string>(Roles::ROLE_ROLVALIDUNTIL);
  if (validuntil_expect) {
    EXPECT_EQ(validuntil_actual, validuntil_expect.value());
  }
}

/**
 * @brief Happy test for getting all data type metadata based on data type
 * name.
 */
TEST_F(DaoTestRolesMetadata, select_role_metadata) {
  ErrorCode error = ErrorCode::UNKNOWN;

  std::shared_ptr<GenericDAO> gdao = nullptr;

  storage::DBSessionManager db_session_manager;

  error = db_session_manager.get_dao(GenericDAO::TableName::ROLES, gdao);
  EXPECT_EQ(ErrorCode::OK, error);

  std::shared_ptr<RolesDAO> rdao = std::static_pointer_cast<RolesDAO>(gdao);

  ptree role_metadata;
  ptree expect_metadata;
  expect_metadata.put(Roles::FORMAT_VERSION, Roles::format_version());
  expect_metadata.put(Roles::GENERATION, Roles::generation());
  expect_metadata.put(Roles::ROLE_ROLNAME, role_name);
  expect_metadata.put(Roles::ROLE_ROLSUPER, "t");        // true
  expect_metadata.put(Roles::ROLE_ROLINHERIT, "f");      // false
  expect_metadata.put(Roles::ROLE_ROLCREATEROLE, "f");   // false
  expect_metadata.put(Roles::ROLE_ROLCREATEDB, "f");     // false
  expect_metadata.put(Roles::ROLE_ROLCANLOGIN, "t");     // true
  expect_metadata.put(Roles::ROLE_ROLREPLICATION, "f");  // false
  expect_metadata.put(Roles::ROLE_ROLBYPASSRLS, "t");    // true
  expect_metadata.put(Roles::ROLE_ROLCONNLIMIT, "-1");   // -1
  expect_metadata.put(Roles::ROLE_ROLPASSWORD, "");      // empty
  expect_metadata.put(Roles::ROLE_ROLVALIDUNTIL, "");    // empty

  // Test getting by role name.
  error =
      rdao->select_role_metadata(Roles::ROLE_ROLNAME, role_name, role_metadata);
  EXPECT_EQ(ErrorCode::OK, error);

  UTUtils::print("-- get role metadata by role name --");
  UTUtils::print(UTUtils::get_tree_string(role_metadata));

  // Verifies that returned role metadata equals expected one.
  DaoTestRolesMetadata::check_roles_expected(role_metadata, expect_metadata);

  role_metadata.clear();

  // Test getting by role id.
  error = rdao->select_role_metadata(Roles::ROLE_OID, role_id, role_metadata);
  EXPECT_EQ(ErrorCode::OK, error);

  UTUtils::print("-- get role metadata by role id --");
  UTUtils::print(UTUtils::get_tree_string(role_metadata));

  // Verifies that returned role metadata equals expected one.
  DaoTestRolesMetadata::check_roles_expected(role_metadata, expect_metadata);

  // Testing for invalid parameters.
  error =
      rdao->select_role_metadata(Roles::ROLE_ROLCANLOGIN, "", role_metadata);
  EXPECT_EQ(ErrorCode::INVALID_PARAMETER, error);

  error = rdao->select_role_metadata(Roles::ROLE_OID, "0", role_metadata);
  EXPECT_EQ(ErrorCode::ID_NOT_FOUND, error);

  error = rdao->select_role_metadata(Roles::ROLE_OID, "", role_metadata);
  EXPECT_EQ(ErrorCode::INVALID_PARAMETER, error);

  error = rdao->select_role_metadata(Roles::ROLE_ROLNAME, "invalid_role_name",
                                     role_metadata);
  EXPECT_EQ(ErrorCode::NAME_NOT_FOUND, error);

  error = rdao->select_role_metadata(Roles::ROLE_ROLNAME, "", role_metadata);
  EXPECT_EQ(ErrorCode::NAME_NOT_FOUND, error);

  error = rdao->select_role_metadata("", "", role_metadata);
  EXPECT_EQ(ErrorCode::INVALID_PARAMETER, error);
}

}  // namespace manager::metadata::testing
