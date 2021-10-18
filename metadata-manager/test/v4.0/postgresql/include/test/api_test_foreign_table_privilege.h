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
#ifndef API_TEST_FOREIGH_TABLE_PRIVILEGES_H_
#define API_TEST_FOREIGH_TABLE_PRIVILEGES_H_

#include <gtest/gtest.h>
#include <libpq-fe.h>
#include <memory>
#include <string_view>

namespace manager::metadata::testing {

class ApiTestForeignTablePrivileges {
 public:
  static void test_setup();
  static void test_teardown();

  template <typename T>
  void check_permission(T object_value, const char* permissions,
                        const bool expect);
  template <typename T>
  void check_invalid_parameter(T object_value, const char* permissions);

 private:
  static void create_table(std::string_view table_name,
                           std::string_view privileges, Oid& table_id);
  static void create_foreign_table(std::string_view table_name, Oid& table_id);
  static Oid str_to_oid(const char* source);
};

}  // namespace manager::metadata::testing

#endif  // API_TEST_FOREIGH_TABLE_PRIVILEGES_H_
