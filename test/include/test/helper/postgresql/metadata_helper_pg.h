/*
 * Copyright 2022 Project Tsurugi.
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
#ifndef TEST_INCLUDE_TEST_HELPER_POSTGRESQL_METADATA_HELPER_PG_H_
#define TEST_INCLUDE_TEST_HELPER_POSTGRESQL_METADATA_HELPER_PG_H_

#include <string>
#include <string_view>

#include "test/helper/metadata_helper.h"

namespace manager::metadata::testing {

class MetadataHelperPg : public MetadataHelper {
 public:
  /**
   * @param table_name metadata table name.
   */
  explicit MetadataHelperPg(std::string_view table_name)
      : table_name_(table_name) {}
  MetadataHelperPg() = delete;

  int64_t get_record_count() const override;

 private:
  std::string table_name_;
};

}  // namespace manager::metadata::testing

#endif  // TEST_INCLUDE_TEST_HELPER_POSTGRESQL_METADATA_HELPER_PG_H_
