/*
 * Copyright 2022-2023 tsurugi project.
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
#include "test/metadata/ut_role_metadata.h"

#include "manager/metadata/role.h"
#include "manager/metadata/roles.h"
#include "test/common/ut_utils.h"
#include "test/helper/role_metadata_helper.h"

namespace manager::metadata::testing {

using boost::property_tree::ptree;

/**
 * @brief Verifies that the actual table metadata equals expected one.
 * @param expected  [in]  expected table metadata.
 * @param actual    [in]  actual table metadata.
 * @param file      [in]  file name of the caller.
 * @param line      [in]  line number of the caller.
 * @return none.
 */
void UtRoleMetadata::check_metadata_expected(
    const boost::property_tree::ptree& expected,
    const boost::property_tree::ptree& actual, const char* file,
    const int64_t line) const {
  // role metadata id
  auto opt_id_expected = expected.get_optional<ObjectId>(Role::ROLE_OID);
  if (opt_id_expected) {
    check_expected<ObjectId>(expected, actual, Role::ROLE_OID, file, line);
  } else {
    auto opt_id_actual = actual.get_optional<ObjectId>(Role::ROLE_OID);
    EXPECT_GT_EX(opt_id_actual.get_value_or(INVALID_OBJECT_ID), 0, file, line);
  }

  // role metadata rolname.
  check_expected<std::string>(expected, actual, Role::ROLE_ROLNAME, file,
                              line);
  // role metadata rolsuper.
  check_expected<std::string>(expected, actual, Role::ROLE_ROLSUPER, file,
                              line);
  // role metadata rolinherit.
  check_expected<std::string>(expected, actual, Role::ROLE_ROLINHERIT, file,
                              line);
  // role metadata rolcreaterole.
  check_expected<std::string>(expected, actual, Role::ROLE_ROLCREATEROLE, file,
                              line);
  // role metadata rolcreatedb.
  check_expected<std::string>(expected, actual, Role::ROLE_ROLCREATEDB, file,
                              line);
  // role metadata rolcanlogin.
  check_expected<std::string>(expected, actual, Role::ROLE_ROLCANLOGIN, file,
                              line);
  // role metadata rolreplication.
  check_expected<std::string>(expected, actual, Role::ROLE_ROLREPLICATION,
                              file, line);
  // role metadata rolbypassrls.
  check_expected<std::string>(expected, actual, Role::ROLE_ROLBYPASSRLS, file,
                              line);
  // role metadata rolconnlimit.
  check_expected<std::int32_t>(expected, actual, Role::ROLE_ROLCONNLIMIT, file,
                               line);
  // role metadata rolpassword.
  check_expected<std::string>(expected, actual, Role::ROLE_ROLPASSWORD, file,
                              line);
  // role metadata rolvaliduntil.
  check_expected<std::string>(expected, actual, Role::ROLE_ROLVALIDUNTIL, file,
                              line);
}

/**
 * @brief Generate metadata for testing.
 */
void UtRoleMetadata::generate_test_metadata() {
  metadata_ptree_.put(Role::FORMAT_VERSION, Role::DEFAULT_FORMAT_VERSION);
  metadata_ptree_.put(Role::GENERATION, Role::DEFAULT_GENERATION);
  metadata_ptree_.put(Role::ROLE_OID, role_id_);
  metadata_ptree_.put(Role::ROLE_ROLNAME, kRoleName);
  metadata_ptree_.put(Role::ROLE_ROLSUPER, "false");       // false
  metadata_ptree_.put(Role::ROLE_ROLINHERIT, "false");     // false
  metadata_ptree_.put(Role::ROLE_ROLCREATEROLE, "true");   // true
  metadata_ptree_.put(Role::ROLE_ROLCREATEDB, "true");     // true
  metadata_ptree_.put(Role::ROLE_ROLCANLOGIN, "false");    // false
  metadata_ptree_.put(Role::ROLE_ROLREPLICATION, "true");  // true
  metadata_ptree_.put(Role::ROLE_ROLBYPASSRLS, "false");   // false
  metadata_ptree_.put(Role::ROLE_ROLCONNLIMIT, "10");      // 10
  metadata_ptree_.put(Role::ROLE_ROLPASSWORD, "");         // empty
  metadata_ptree_.put(Role::ROLE_ROLVALIDUNTIL, "");       // empty
}

}  // namespace manager::metadata::testing
