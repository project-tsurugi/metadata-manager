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
#ifndef MANAGER_METADATA_MANAGER_TEST_V4_0_POSTGRESQL_INCLUDE_TEST_HELPER_FOREIGN_TABLE_HELPER_H_
#define MANAGER_METADATA_MANAGER_TEST_V4_0_POSTGRESQL_INCLUDE_TEST_HELPER_FOREIGN_TABLE_HELPER_H_

#include <string_view>

#include "manager/metadata/metadata.h"

namespace manager::metadata::testing {

class ForeignTableHelper {
 public:
  static ObjectIdType create_table(std::string_view table_name,
                                   std::string_view role_name,
                                   std::string_view privileges);
  static void drop_table(std::string_view table_name);

  static ObjectIdType insert_foreign_table(std::string_view table_name);
  static void delete_foreign_table(ObjectIdType foreign_table_id);

 private:
  static void db_connection();
};

}  // namespace manager::metadata::testing

#endif  // MANAGER_METADATA_MANAGER_TEST_V4_0_POSTGRESQL_INCLUDE_TEST_HELPER_FOREIGN_TABLE_HELPER_H_
