/*
 * Copyright 2021 Project Tsurugi.
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
#ifndef TEST_INCLUDE_TEST_HELPER_FOREIGN_TABLE_HELPER_H_
#define TEST_INCLUDE_TEST_HELPER_FOREIGN_TABLE_HELPER_H_

#include <string_view>

#include <boost/property_tree/ptree.hpp>

#include "manager/metadata/metadata.h"
#include "test/helper/metadata_helper.h"

#if defined(STORAGE_POSTGRESQL)
#include "test/helper/postgresql/foreign_table_helper_pg.h"
#endif

namespace manager::metadata::testing {

class ForeignTableHelper : public MetadataHelper {
 public:
  int64_t get_record_count() const override { return 0L; }

  static ObjectIdType create_table(
      [[maybe_unused]] std::string_view table_name,
      [[maybe_unused]] std::string_view role_name,
      [[maybe_unused]] std::string_view privileges) {
#if defined(STORAGE_POSTGRESQL)
    return ForeignTableHelperPg::create_table(table_name, role_name,
                                              privileges);
#elif defined(STORAGE_JSON)
    return INVALID_OBJECT_ID;
#endif
  }

  static void drop_table([[maybe_unused]] std::string_view table_name) {
#if defined(STORAGE_POSTGRESQL)
    ForeignTableHelperPg::drop_table(table_name);
#endif
  }

  static void grant_table([[maybe_unused]] std::string_view table_name,
                          [[maybe_unused]] std::string_view role_name,
                          [[maybe_unused]] std::string_view privileges) {
#if defined(STORAGE_POSTGRESQL)
    ForeignTableHelperPg::grant_table(table_name, role_name, privileges);
#endif
  }

  static ObjectIdType insert_foreign_table(
      [[maybe_unused]] std::string_view table_name) {
#if defined(STORAGE_POSTGRESQL)
    return ForeignTableHelperPg::insert_foreign_table(table_name);
#elif defined(STORAGE_JSON)
    return INVALID_OBJECT_ID;
#endif
  }

  static void delete_foreign_table([[maybe_unused]] ObjectId foreign_table_id) {
#if defined(STORAGE_POSTGRESQL)
    ForeignTableHelperPg::delete_foreign_table(foreign_table_id);
#endif
  }
};

}  // namespace manager::metadata::testing

#endif  // TEST_INCLUDE_TEST_HELPER_FOREIGN_TABLE_HELPER_H_
