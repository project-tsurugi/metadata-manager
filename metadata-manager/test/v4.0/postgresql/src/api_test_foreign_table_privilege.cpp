/*
 * Copyright 2020-2021 tsurugi project.
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
#include "test/api_test_foreign_table_privilege.h"

#include <boost/format.hpp>
#include <memory>
#include <string>

#include "manager/metadata/dao/common/config.h"
#include "manager/metadata/dao/postgresql/dbc_utils.h"
#include "manager/metadata/tables.h"
#include "test/api_test_table_metadata.h"
#include "test/global_test_environment.h"
#include "test/utility/ut_utils.h"

namespace {

constexpr std::string_view role_name = "tsurugi_api_ut_privileges_user_1";
constexpr std::string_view foreign_table_name_ro =
    "tsurugi_api_ut_foreign_table_ro";
constexpr std::string_view foreign_table_name_rw =
    "tsurugi_api_ut_foreign_table_rw";
constexpr std::string_view foreign_table_name_none =
    "tsurugi_api_ut_foreign_table_none";

std::shared_ptr<PGconn> connection;
std::unique_ptr<manager::metadata::Tables> tables;
Oid role_id = 0;
Oid table_id_ro = 0;
Oid table_id_rw = 0;
Oid foreign_table_id_ro = 0;
Oid foreign_table_id_rw = 0;

}  // namespace

namespace manager::metadata::testing {

namespace storage = manager::metadata::db::postgresql;
using namespace manager::metadata;
using namespace boost::property_tree;

/**
 * @brief ApiTestTablePrivilegesSingle
 */
class ApiTestTablePrivilegesSingle
    : public ::testing::TestWithParam<
          std::vector<std::tuple<const char*, bool>>> {
 public:
  void SetUp() override {
    ApiTestForeignTablePrivileges::test_setup();
    if (global->is_open()) {
      // add read-write table metadata.
      ApiTestTableMetadata::add_table(foreign_table_name_rw);
    }
  }
  void TearDown() override {
    if (global->is_open()) {
      // remove table metadata.
      ApiTestTableMetadata::remove_table(foreign_table_name_rw);
    }
    ApiTestForeignTablePrivileges::test_teardown();
  }
};

/**
 * @brief ApiTestTablePrivilegesMultiple
 */
class ApiTestTablePrivilegesMultiple
    : public ::testing::TestWithParam<
          std::vector<std::tuple<const char*, bool>>> {
 public:
  void SetUp() override {
    ApiTestForeignTablePrivileges::test_setup();

    if (global->is_open()) {
      // add read-write table metadata.
      ApiTestTableMetadata::add_table(foreign_table_name_rw);
      // add read-only table metadata.
      ApiTestTableMetadata::add_table(foreign_table_name_ro);
    }
  }
  void TearDown() override {
    if (global->is_open()) {
      // remove table metadata.
      ApiTestTableMetadata::remove_table(foreign_table_name_rw);
      ApiTestTableMetadata::remove_table(foreign_table_name_ro);
    }
    ApiTestForeignTablePrivileges::test_teardown();
  }
};

/**
 * @brief ApiTestForeignTableNotExists
 */
class ApiTestForeignTableNotExists : public ::testing::Test {
 public:
  void SetUp() override { ApiTestForeignTablePrivileges::test_setup(); }
  void TearDown() override { ApiTestForeignTablePrivileges::test_teardown(); }
};

/**
 * @brief ApiTestTablePrivilegesInvalid
 */
class ApiTestTablePrivilegesInvalid
    : public ::testing::TestWithParam<std::vector<const char*>> {
 public:
  void SetUp() override {
    ApiTestForeignTablePrivileges::test_setup();

    if (global->is_open()) {
      // add read-write table metadata.
      ApiTestTableMetadata::add_table(foreign_table_name_rw);
    }
  }
  void TearDown() override {
    if (global->is_open()) {
      // remove table metadata.
      ApiTestTableMetadata::remove_table(foreign_table_name_rw);
    }
    ApiTestForeignTablePrivileges::test_teardown();
  }
};

std::vector<std::tuple<const char*, bool>> test_pattern_list_single = {
    std::make_tuple("r", true),    std::make_tuple("a", true),
    std::make_tuple("w", true),    std::make_tuple("d", true),
    std::make_tuple("D", false),   std::make_tuple("x", false),
    std::make_tuple("t", false),   std::make_tuple("rwa", true),
    std::make_tuple("rwad", true), std::make_tuple("arwdDxt", false)};
std::vector<std::tuple<const char*, bool>> test_pattern_list_multiple = {
    std::make_tuple("r", true),     std::make_tuple("a", false),
    std::make_tuple("w", false),    std::make_tuple("d", false),
    std::make_tuple("D", false),    std::make_tuple("x", false),
    std::make_tuple("t", false),    std::make_tuple("rwa", false),
    std::make_tuple("rwad", false), std::make_tuple("arwdDxt", false)};
std::vector<const char*> test_pattern_list_invalid = {
    "X", "U", "C", "c", "T", "*", "arwdDxtXUCcT"};

INSTANTIATE_TEST_CASE_P(ParamtererizedTest, ApiTestTablePrivilegesSingle,
                        ::testing::Values(test_pattern_list_single));

INSTANTIATE_TEST_CASE_P(ParamtererizedTest, ApiTestTablePrivilegesMultiple,
                        ::testing::Values(test_pattern_list_multiple));

INSTANTIATE_TEST_CASE_P(ParamtererizedTest, ApiTestTablePrivilegesInvalid,
                        ::testing::Values(test_pattern_list_invalid));

/**
 * @brief setup the data for testing.
 */
void ApiTestForeignTablePrivileges::test_setup() {
  boost::format statement;

  if (global->is_open()) {
    // db connection.
    connection = storage::DbcUtils::make_connection_sptr(
        PQconnectdb(db::Config::get_connection_string().c_str()));

    // create an instance of the Tables class.
    tables = std::make_unique<Tables>(GlobalTestEnvironment::TEST_DB);
    tables->init();

    // create dummy data for ROLE.
    statement = boost::format("CREATE ROLE %s") % role_name;
    PGresult* res = PQexec(connection.get(), statement.str().c_str());
    PQclear(res);
    // get dummy data for ROLE.
    statement = boost::format("SELECT oid FROM pg_authid WHERE rolname='%s'") %
                role_name;
    res = PQexec(connection.get(), statement.str().c_str());
    role_id = str_to_oid(PQgetvalue(res, 0, 0));
    PQclear(res);

    // create dummy data for TABLE.
    create_table(foreign_table_name_ro, "SELECT", table_id_ro);
    create_table(foreign_table_name_rw, "SELECT,INSERT,UPDATE,DELETE",
                 table_id_rw);

    // create dummy data for pg_foreign_table.
    create_foreign_table(foreign_table_name_ro, foreign_table_id_ro);
    create_foreign_table(foreign_table_name_rw, foreign_table_id_rw);
  } else {
    GTEST_SKIP_("metadata repository is not started.");
  }
}

/**
 * @brief discard the data for testing.
 */
void ApiTestForeignTablePrivileges::test_teardown() {
  if (global->is_open()) {
    boost::format statement;
    PGresult* res = nullptr;

    // remove dummy data for pg_foreign_table.
    statement =
        boost::format("DELETE FROM pg_foreign_table where ftrelid IN (%s,%s)") %
        foreign_table_id_ro % foreign_table_id_rw;
    res = PQexec(connection.get(), statement.str().c_str());
    PQclear(res);

    // remove dummy data for TABLE.
    statement =
        boost::format("DROP TABLE tsurugi_catalog.%s") % foreign_table_name_ro;
    res = PQexec(connection.get(), statement.str().c_str());
    PQclear(res);

    statement =
        boost::format("DROP TABLE tsurugi_catalog.%s") % foreign_table_name_rw;
    res = PQexec(connection.get(), statement.str().c_str());
    PQclear(res);

    // remove dummy data for ROLE.
    statement = boost::format("DROP ROLE %s") % role_name;
    res = PQexec(connection.get(), statement.str().c_str());
    PQclear(res);
  }
}

/**
 * @brief create a table metadata for testing.
 */
void ApiTestForeignTablePrivileges::create_table(std::string_view table_name,
                                                 std::string_view privileges,
                                                 Oid& table_id) {
  boost::format statement;
  PGresult* res = nullptr;

  // create dummy data for TABLE.
  statement =
      boost::format("CREATE TABLE tsurugi_catalog.%s (id bigint, name text)") %
      table_name;
  res = PQexec(connection.get(), statement.str().c_str());
  PQclear(res);

  // get dummy data for TABLE.
  statement =
      boost::format("SELECT oid FROM pg_class WHERE relname='%s'") % table_name;
  res = PQexec(connection.get(), statement.str().c_str());
  table_id = str_to_oid(PQgetvalue(res, 0, 0));
  PQclear(res);

  // set dummy data for privileges.
  if (!privileges.empty()) {
    statement = boost::format("GRANT %s ON tsurugi_catalog.%s TO %s") %
                privileges % table_name % role_name;
  } else {
    statement = boost::format("REVOKE ALL ON tsurugi_catalog.%s TO %s") %
                table_name % role_name;
  }
  res = PQexec(connection.get(), statement.str().c_str());
  PQclear(res);
}

/**
 * @brief create a foreign table for testing.
 */
void ApiTestForeignTablePrivileges::create_foreign_table(
    std::string_view table_name, Oid& table_id) {
  std::string ft_statement_sub =
      "SELECT CAST(MAX(ftrelid) AS INTEGER) num FROM pg_foreign_table";
  boost::format statement =
      boost::format(
          "INSERT into pg_foreign_table VALUES"
          " ((%s) + 1, 1"
          " , '{schema_name=tsurugi_catalog,table_name=%s}')"
          " RETURNING ftrelid") %
      ft_statement_sub % table_name;
  PGresult* res = PQexec(connection.get(), statement.str().c_str());
  table_id = str_to_oid(PQgetvalue(res, 0, 0));
  PQclear(res);
}

/**
 * @brief Convert string to Oid.
 * @param (source)  [in]   C-string containing the representation of
 *   decimal integer literal (base 10).
 * @return the converted integral number.
 */
Oid ApiTestForeignTablePrivileges::str_to_oid(const char* source) {
  std::int64_t res_val = 0;
  storage::DbcUtils::str_to_integral(source, res_val);
  return static_cast<Oid>(res_val);
}

/**
 * @brief test for confirm permissions.
 */
TEST_P(ApiTestTablePrivilegesSingle, confirm_tables_permission) {
  auto params = GetParam();

  for (auto param : params) {
    const char* permission = std::get<0>(param);
    bool expected = std::get<1>(param);

    ErrorCode error = ErrorCode::UNKNOWN;
    bool actual = false;

    UTUtils::print("  Test pattern: [", permission, "]");

    // check the table permissions by role id.
    error = tables->confirm_permission_in_acls(role_id, permission, actual);
    EXPECT_EQ(ErrorCode::OK, error);
    EXPECT_EQ(expected, actual);

    // check the table permissions by role name.
    error = tables->confirm_permission_in_acls(role_name, permission, actual);
    EXPECT_EQ(ErrorCode::OK, error);
    EXPECT_EQ(expected, actual);
  }
}

/**
 * @brief test for confirm permissions.
 */
TEST_P(ApiTestTablePrivilegesMultiple, confirm_tables_permission) {
  auto params = GetParam();

  for (auto param : params) {
    const char* permission = std::get<0>(param);
    bool expected = std::get<1>(param);

    ErrorCode error = ErrorCode::UNKNOWN;
    bool actual = false;

    UTUtils::print("  Test pattern: [", permission, "]");

    // check the table permissions by role id.
    error = tables->confirm_permission_in_acls(role_id, permission, actual);
    EXPECT_EQ(ErrorCode::OK, error);
    EXPECT_EQ(expected, actual);

    // check the table permissions by role name.
    error = tables->confirm_permission_in_acls(role_name, permission, actual);
    EXPECT_EQ(ErrorCode::OK, error);
    EXPECT_EQ(expected, actual);
  }
}

/**
 * @brief test for the case where table metadata does not exists.
 */
TEST_F(ApiTestForeignTableNotExists, table_metadata_does_not_exist) {
  ErrorCode error = ErrorCode::UNKNOWN;
  bool res_permission = false;

  // test by role id.
  error = tables->confirm_permission_in_acls(role_id, "r", res_permission);
  EXPECT_EQ(ErrorCode::NOT_FOUND, error);

  // test by role name.
  error = tables->confirm_permission_in_acls(role_name, "r", res_permission);
  EXPECT_EQ(ErrorCode::NOT_FOUND, error);
}

/**
 * @brief test for the case where foreign table does not exists.
 */
TEST_F(ApiTestForeignTableNotExists, foreign_table_does_not_exist) {
  ErrorCode error = ErrorCode::UNKNOWN;
  bool res_permission = false;

  // add read-write table metadata.
  ApiTestTableMetadata::add_table(foreign_table_name_none);

  // test by role id.
  error = tables->confirm_permission_in_acls(role_id, "r", res_permission);
  EXPECT_EQ(ErrorCode::NOT_FOUND, error);

  // test by role name.
  error = tables->confirm_permission_in_acls(role_name, "r", res_permission);
  EXPECT_EQ(ErrorCode::NOT_FOUND, error);

  // remove table metadata.
  ApiTestTableMetadata::remove_table(foreign_table_name_none);
}

/**
 * @brief test for the case where role id or name does not exists.
 */
TEST_F(ApiTestForeignTableNotExists, role_does_not_exist) {
  ErrorCode error = ErrorCode::UNKNOWN;
  bool res_permission = false;

  // add read-write table metadata.
  ApiTestTableMetadata::add_table(foreign_table_name_rw);

  ObjectIdType role_id;
  std::string role_name;

  // the role id (0) does not exist.
  role_id = 0;
  UTUtils::print("  Test pattern: [", role_id, "]");
  error = tables->confirm_permission_in_acls(0, "r", res_permission);
  EXPECT_EQ(ErrorCode::ID_NOT_FOUND, error);

  // the role id (9999999) does not exist.
  role_id = 9999999L;
  UTUtils::print("  Test pattern: [", role_id, "]");
  error = tables->confirm_permission_in_acls(role_id, "r", res_permission);
  EXPECT_EQ(ErrorCode::ID_NOT_FOUND, error);

  // the role name is empty.
  role_name = "";
  UTUtils::print("  Test pattern: [", role_name, "]");
  error = tables->confirm_permission_in_acls(role_name, "r", res_permission);
  EXPECT_EQ(ErrorCode::NAME_NOT_FOUND, error);

  // the role name does not exist.
  role_name = "undefined-name";
  UTUtils::print("  Test pattern: [", role_name, "]");
  error = tables->confirm_permission_in_acls(role_name, "r", res_permission);
  EXPECT_EQ(ErrorCode::NAME_NOT_FOUND, error);

  // remove table metadata.
  ApiTestTableMetadata::remove_table(foreign_table_name_rw);
}

/**
 * @brief test for confirm permissions with invalid parameter.
 */
TEST_P(ApiTestTablePrivilegesInvalid, confirm_tables_permission) {
  auto params = GetParam();

  for (auto permission : params) {
    ErrorCode error = ErrorCode::UNKNOWN;
    bool actual = false;

    UTUtils::print("  Test pattern: [", permission, "]");

    // check the table permissions by role id.
    error = tables->confirm_permission_in_acls(role_id, permission, actual);
    EXPECT_EQ(ErrorCode::INVALID_PARAMETER, error);

    // check the table permissions by role name.
    error = tables->confirm_permission_in_acls(role_name, permission, actual);
    EXPECT_EQ(ErrorCode::INVALID_PARAMETER, error);
  }
}

}  // namespace manager::metadata::testing
