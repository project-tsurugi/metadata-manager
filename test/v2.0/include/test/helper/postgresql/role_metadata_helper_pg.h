/*
 * Copyright 2021-2023 tsurugi project.
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
#ifndef TEST_INCLUDE_TEST_HELPER_POSTGRESQL_ROLE_METADATA_HELPER_PG_H_
#define TEST_INCLUDE_TEST_HELPER_POSTGRESQL_ROLE_METADATA_HELPER_PG_H_

#include <string_view>

#include <boost/property_tree/ptree.hpp>

#include "manager/metadata/common/constants.h"

namespace manager::metadata::testing {

class RoleMetadataHelperPg {
 public:
  static ObjectIdType create_role(std::string_view role_name,
                                  std::string_view options);
  static void drop_role(std::string_view role_name);

 private:
  static void db_connection();
};

}  // namespace manager::metadata::testing

#endif  // TEST_INCLUDE_TEST_HELPER_POSTGRESQL_ROLE_METADATA_HELPER_PG_H_
