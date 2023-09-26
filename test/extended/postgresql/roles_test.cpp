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
#include <iostream>
#include <regex>
#include <string>
#include <string_view>

#include <boost/foreach.hpp>
#include <boost/format.hpp>
#include <boost/property_tree/ptree.hpp>

#include "manager/metadata/common/config.h"
#include "manager/metadata/dao/postgresql/pg_common.h"
#include "manager/metadata/metadata_factory.h"

namespace {

using boost::property_tree::ptree;

using manager::metadata::ErrorCode;
using manager::metadata::FormatVersionType;
using manager::metadata::GenerationType;
using manager::metadata::ObjectIdType;
using manager::metadata::Roles;
using manager::metadata::Table;
using manager::metadata::Tables;
using manager::metadata::db::PgConnectionPtr;

static constexpr const char* const TEST_DB   = "test";
static constexpr const char* const ROLE_NAME = "tsurugi_ut_role_user_1";

PgConnectionPtr connection;
bool test_succeed = true;

#define EXPECT_EQ(expected, actual) \
  func_expect_eq(expected, actual, __FILE__, __LINE__)
#define EXPECT_GT(actual, value) \
  func_expect_gt(actual, value, __FILE__, __LINE__)

void func_expect_eq(ErrorCode expected, ErrorCode actual, std::string_view file,
                    std::int32_t line) {
  if (expected != actual) {
    std::cout << std::endl
              << file << ": " << line << ": Failure" << std::endl
              << "  Expected value: " << static_cast<int32_t>(expected)
              << std::endl
              << "  Actual value: " << static_cast<int32_t>(actual)
              << std::endl;
    test_succeed = false;
  }
}

template <typename T>
void func_expect_eq(T expected, T actual, std::string_view file,
                    std::int32_t line) {
  if (expected != actual) {
    std::cout << std::endl
              << file << ": " << line << ": Failure" << std::endl
              << "  Expected value: " << expected << std::endl
              << "  Actual value: " << actual << std::endl;
    test_succeed = false;
  }
}

template <typename T1, typename T2>
void func_expect_gt(T1 actual, T2 value, std::string_view file,
                    std::int32_t line) {
  if (actual <= static_cast<T1>(value)) {
    std::cout << std::endl
              << file << ": " << line << ": Failure" << std::endl
              << "  Expected value: > " << value << std::endl
              << "  Actual value: " << actual << std::endl;
    test_succeed = false;
  }
}

/**
 */
std::string indent(int level) {
  std::string s;
  for (int i = 0; i < level; i++) s += "  ";
  return s;
}

/**
 * @brief internal function used in get_tree_string, print_tree.
 * get string converted from ptree.
 * @param (pt)                   [in]  ptree to be converted to string.
 * @param (level)                [in]  indent level.
 * @param (output_string)        [out] string converted from ptree.
 * @param (print_tree_enabled)   [in]  enable/disable to print output_string.
 */
void get_tree_string_internal(const boost::property_tree::ptree& pt, int level,
                              std::string& output_string,
                              bool print_tree_enabled) {
  if (pt.empty()) {
    output_string.append("\"");
    output_string.append(pt.data());
    output_string.append("\"");

    if (print_tree_enabled) std::cerr << "\"" << pt.data() << "\"";
  } else {
    if (level && print_tree_enabled) std::cerr << std::endl;

    if (print_tree_enabled) std::cerr << indent(level) << "{" << std::endl;
    output_string.append("{");

    for (auto pos = pt.begin(); pos != pt.end();) {
      if (print_tree_enabled)
        std::cerr << indent(level + 1) << "\"" << pos->first << "\": ";
      output_string.append("\"");
      output_string.append(pos->first);
      output_string.append("\": ");

      get_tree_string_internal(pos->second, level + 1, output_string,
                               print_tree_enabled);
      ++pos;
      if (pos != pt.end()) {
        if (print_tree_enabled) std::cerr << ",";
        output_string.append(",");
      }
      if (print_tree_enabled) std::cerr << std::endl;
    }

    if (print_tree_enabled) std::cerr << indent(level) << " }";
    output_string.append(" }");
  }

  return;
}

/**
 * @brief Get string converted from ptree. (not print string)
 * @param (pt)                   [in]  ptree to be converted to string.
 */
std::string get_tree_string(const boost::property_tree::ptree& pt) {
  std::string output_string;
  int level = 0;
  get_tree_string_internal(pt, level, output_string, false);
  return output_string;
}

}  // namespace

namespace helper {

namespace db = manager::metadata::db;
using manager::metadata::Config;

/**
 * @brief Connect to the database.
 */
void db_connection() {
  if (PQstatus(connection.get()) != CONNECTION_OK) {
    // db connection.
    PGconn* pgconn = PQconnectdb(Config::get_connection_string().c_str());
    PgConnectionPtr conn(pgconn, [](PGconn* c) { ::PQfinish(c); });
    connection = conn;
  }
}

/**
 * @brief create a role for testing.
 * @param (role_name)  [in]   role name.
 * @param (options)    [in]   options to pass to CREATE ROLE.
 * @return role id.
 */
ObjectIdType create_role(std::string_view role_name, std::string_view options) {
  std::int64_t role_id = 0;
  boost::format statement;

  // db connection.
  db_connection();

  statement     = boost::format("CREATE ROLE %s %s") % role_name % options;
  PGresult* res = PQexec(connection.get(), statement.str().c_str());
  PQclear(res);

  statement =
      boost::format("SELECT oid FROM pg_authid WHERE rolname='%s'") % role_name;
  res     = PQexec(connection.get(), statement.str().c_str());
  role_id = std::stol(PQgetvalue(res, 0, 0));
  PQclear(res);

  return role_id;
}

/**
 * @brief remove a role for testing.
 * @param (role_name)  [in]   role name.
 */
void drop_role(std::string_view role_name) {
  // db connection.
  db_connection();

  // remove dummy data for ROLE.
  boost::format statement = boost::format("DROP ROLE %s") % role_name;
  PGresult* res           = PQexec(connection.get(), statement.str().c_str());
  PQclear(res);
}

/**
 * @brief Verifies that returned role metadata equals expected one.
 * @param (expected)  [in] Expected role metadata.
 * @param (actual)    [in] role metadata returned from api to get
 *   role metadata
 */
void check_roles_expected(const boost::property_tree::ptree& expected,
                          const boost::property_tree::ptree& actual) {
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

}  // namespace helper

namespace test {

/**
 * @brief Test for Roles class object.
 */
void roles_test() {
  ErrorCode result = ErrorCode::UNKNOWN;

  // create dummy data for ROLE.
  ObjectIdType role_id = helper::create_role(
      ROLE_NAME,
      "NOINHERIT CREATEROLE CREATEDB REPLICATION CONNECTION LIMIT 10");

  auto roles = manager::metadata::get_roles_ptr(TEST_DB);
  result     = roles->init();
  EXPECT_EQ(ErrorCode::OK, result);

  ptree role_metadata;
  ptree expect_metadata;
  expect_metadata.put(Roles::FORMAT_VERSION, Roles::format_version());
  expect_metadata.put(Roles::GENERATION, Roles::generation());
  expect_metadata.put(Roles::ROLE_ROLNAME, ROLE_NAME);
  expect_metadata.put(Roles::ROLE_ROLSUPER, "false");       // false
  expect_metadata.put(Roles::ROLE_ROLINHERIT, "false");     // false
  expect_metadata.put(Roles::ROLE_ROLCREATEROLE, "true");   // true
  expect_metadata.put(Roles::ROLE_ROLCREATEDB, "true");     // true
  expect_metadata.put(Roles::ROLE_ROLCANLOGIN, "false");    // false
  expect_metadata.put(Roles::ROLE_ROLREPLICATION, "true");  // true
  expect_metadata.put(Roles::ROLE_ROLBYPASSRLS, "false");   // false
  expect_metadata.put(Roles::ROLE_ROLCONNLIMIT, "10");      // 10
  expect_metadata.put(Roles::ROLE_ROLPASSWORD, "");         // empty
  expect_metadata.put(Roles::ROLE_ROLVALIDUNTIL, "");       // empty

  // test getting by role id.
  result = roles->get(role_id, role_metadata);
  EXPECT_EQ(ErrorCode::OK, result);

  std::cout << "-- get role metadata by role id --" << std::endl;
  std::cout << "  " << get_tree_string(role_metadata) << std::endl;

  // verifies that returned role metadata equals expected one.
  helper::check_roles_expected(expect_metadata, role_metadata);

  // clear property_tree.
  role_metadata.clear();

  // test getting by role name.
  result = roles->get(ROLE_NAME, role_metadata);
  EXPECT_EQ(ErrorCode::OK, result);

  std::cout << "-- get role metadata by role name --" << std::endl;
  std::cout << "  " << get_tree_string(role_metadata) << std::endl;

  // verifies that returned role metadata equals expected one.
  helper::check_roles_expected(expect_metadata, role_metadata);

  // remove dummy data for ROLE.
  helper::drop_role(ROLE_NAME);
}

/**
 * @brief Retrieve and display the Roles metadata.
 * @param (role_name)  [in]  role name.
 */
void get_role_metadata(std::string_view role_name) {
  ErrorCode result = ErrorCode::UNKNOWN;

  auto roles = manager::metadata::get_roles_ptr(TEST_DB);
  result     = roles->init();
  if (result != ErrorCode::OK) {
    std::cout << "Failed to initialize the metadata management object."
              << std::endl
              << "  error code: " << static_cast<int32_t>(result) << std::endl
              << std::endl;
    return;
  }

  ptree role_metadata;
  result = roles->get(role_name, role_metadata);
  if (result == ErrorCode::OK) {
    std::cout << get_tree_string(role_metadata) << std::endl;
  } else {
    std::cout << "Failed to get role metadata." << std::endl
              << "  error code: " << static_cast<int32_t>(result) << std::endl
              << std::endl;
  }
}

/**
 * @brief Retrieve and display the Tables metadata.
 * @param (role_name)   [in]  role name.
 * @param (table_name)  [in]  table name.
 */
void get_table_metadata(std::string_view role_name,
                        std::string_view table_name) {
  ErrorCode result = ErrorCode::UNKNOWN;

  auto tables = manager::metadata::get_tables_ptr(TEST_DB);
  result      = tables->init();
  if (result != ErrorCode::OK) {
    std::cout << "ERR: Failed to initialize the metadata management object."
              << std::endl
              << "  error code: " << static_cast<int32_t>(result) << std::endl
              << std::endl;
    return;
  }

  ptree table_metadata;
  result = tables->get(table_name, table_metadata);
  if (result == ErrorCode::OK) {
    auto acls = table_metadata.get_child_optional(Table::ACL);
    if (!acls) {
      std::cout << "ERR: Failed to get table metadata." << std::endl
                << "  There is no " << Table::ACL << " in the metadata."
                << std::endl
                << std::endl;
      return;
    }

    // Conversion of parameter value (\ -> \\).
    std::string role_name_esc = std::regex_replace(
        std::string(role_name), std::regex(R"(\\)"), R"(\\)");

    std::string acl_value  = "";
    std::string permission = "";
    BOOST_FOREACH (const ptree::value_type& node, acls.get()) {
      std::string acl_data =
          std::regex_replace(node.second.data(), std::regex(R"((^"|"$))"), "");

      std::smatch results;
      std::regex_match(
          acl_data, results,
          std::regex(R"((\\".+\\"|[^\\"]*)=([arwdDxt]+)/(\\".+\\"|.+))"));
      if (results.empty()) {
        std::cout << "ERR: ACL format is invalid. [" << acl_data << "]"
                  << std::endl;
        continue;
      }
      std::string acl_role_name  = results[1].str();
      std::string acl_permission = results[2].str();

      // Conversion of record value (\\ -> \).
      acl_role_name =
          std::regex_replace(acl_role_name, std::regex(R"(\\\\)"), R"(\)");
      // Conversion of record value (\" or \"\" -> ").
      acl_role_name = std::regex_replace(acl_role_name,
                                         std::regex(R"((\\"){1,2})"), R"(")");
      // Conversion of record value (^" or "$ -> ).
      acl_role_name =
          std::regex_replace(acl_role_name, std::regex(R"((^"|"$))"), "");

      if ((acl_role_name.empty()) ||
          (std::regex_match(acl_role_name, std::regex(role_name_esc)))) {
        acl_value  = acl_data;
        permission = acl_permission;
        break;
      }
    }

    std::cout << "  Role name: " << role_name_esc << std::endl;
    std::cout << "  Table name: " << table_name << std::endl;
    std::cout << "  Permission: " << permission;
    if (!acl_value.empty()) {
      std::cout << " (" << acl_value << ")";
    }
    std::cout << std::endl;
  } else {
    std::cout << "ERR: Failed to get table metadata." << std::endl
              << "  error code: " << static_cast<int32_t>(result) << std::endl
              << std::endl;
  }
}

/**
 * @brief Retrieve and display the Roles metadata.
 * @param (role_name)     [in]  role name.
 * @param (permission)    [in]  permissions.
 * @param (check_result)  [out] presence or absence of the specified
 *   permissions.
 */
void confirm_permission_in_acls(std::string_view role_name,
                                const char* permission) {
  ErrorCode result = ErrorCode::UNKNOWN;

  // TODO(future): Change when changing Metadata class.
  auto tables_tmp = manager::metadata::get_tables_ptr(TEST_DB);
  auto tables = static_cast<Tables*>(tables_tmp.get());

  result      = tables->init();
  if (result != ErrorCode::OK) {
    std::cout << "Failed to initialize the metadata management object."
              << std::endl
              << "  error code: " << static_cast<int32_t>(result) << std::endl
              << std::endl;
    return;
  }

  bool check_result;
  result =
      tables->confirm_permission_in_acls(role_name, permission, check_result);
  if (result == ErrorCode::OK) {
    std::cout << "  Role name: " << role_name << std::endl;
    std::cout << "  Permission: " << permission << std::endl;
    std::cout << "  Result: " << std::boolalpha << check_result << std::endl;
  } else {
    std::cout << "Failed to confirm permission." << std::endl
              << "  error code: " << static_cast<int32_t>(result) << std::endl
              << std::endl;
  }
}

}  // namespace test

/**
 * @brief  main function.
 */
int main(int argc, char* argv[]) {
  if (argc == 2) {
    test::get_role_metadata(argv[1]);
  } else if (argc == 3) {
    if (std::regex_match(argv[2], std::regex("[arwdDxt]+"))) {
      test::confirm_permission_in_acls(argv[1], argv[2]);
    } else {
      test::get_table_metadata(argv[1], argv[2]);
    }
  } else {
    std::cout << "*** RolesMetadata test start. ***" << std::endl << std::endl;

    std::cout << "=== class object test start. ===" << std::endl;
    test::roles_test();
    std::cout << "=== class object test done. ===" << std::endl;
    std::cout << std::endl;

    std::cout << "RolesMetadata test : ";
    if (test_succeed) {
      std::cout << "Success" << std::endl;
    } else {
      std::cout << "*** Failure ***" << std::endl;
    }

    std::cout << std::endl;

    std::cout << "*** RolesMetadata test completed. ***" << std::endl;
  }

  return EXIT_SUCCESS;
}
