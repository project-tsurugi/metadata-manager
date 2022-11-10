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
#ifndef TEST_INCLUDE_TEST_HELPER_ROLE_METADATA_HELPER_H_
#define TEST_INCLUDE_TEST_HELPER_ROLE_METADATA_HELPER_H_

#include <string_view>

#include <boost/property_tree/ptree.hpp>

#include "manager/metadata/metadata.h"
#include "test/helper/metadata_helper.h"

#if defined(STORAGE_POSTGRESQL)
#include "test/helper/postgresql/role_metadata_helper_pg.h"
#endif

namespace manager::metadata::testing {

class RoleMetadataHelper : public MetadataHelper {
 public:
  int64_t get_record_count() const override { return 0L; }

  static ObjectId create_role([[maybe_unused]] std::string_view role_name,
                              [[maybe_unused]] std::string_view options) {
#if defined(STORAGE_POSTGRESQL)
    return RoleMetadataHelperPg::create_role(role_name, options);
#elif defined(STORAGE_JSON)
    return INVALID_OBJECT_ID;
#endif
  }

  static void drop_role([[maybe_unused]] std::string_view role_name) {
#if defined(STORAGE_POSTGRESQL)
    RoleMetadataHelperPg::drop_role(role_name);
#endif
  }
};

}  // namespace manager::metadata::testing

#endif  // TEST_INCLUDE_TEST_HELPER_ROLE_METADATA_HELPER_H_
