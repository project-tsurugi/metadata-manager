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
#ifndef TEST_INCLUDE_TEST_HELPER_CONSTRAINT_METADATA_HELPER_H_
#define TEST_INCLUDE_TEST_HELPER_CONSTRAINT_METADATA_HELPER_H_

#include <memory>

#include "test/helper/metadata_helper.h"

#if defined(STORAGE_POSTGRESQL)
#include "test/helper/postgresql/metadata_helper_pg.h"
#elif defined(STORAGE_JSON)
#include "test/helper/json/metadata_helper_json.h"
#endif

namespace manager::metadata::testing {

class ConstraintMetadataHelper : public MetadataHelper {
 public:
#if defined(STORAGE_POSTGRESQL)
  ConstraintMetadataHelper()
      : helper_(std::make_unique<MetadataHelperPg>(kTableName)) {}
#elif defined(STORAGE_JSON)
  ConstraintMetadataHelper()
      : helper_(std::make_unique<MetadataHelperJson>(kMetadataName, kRootNode,
                                                     kSubNode)) {}
#endif

  int64_t get_record_count() const override {
    return helper_->get_record_count();
  }

 private:
#if defined(STORAGE_POSTGRESQL)
  static constexpr const char* const kTableName = "constraints";
#elif defined(STORAGE_JSON)
  static constexpr const char* const kMetadataName = "tables";
  static constexpr const char* const kRootNode     = "tables";
  static constexpr const char* const kSubNode      = "constraints";
#endif

#if defined(STORAGE_POSTGRESQL)
  std::unique_ptr<MetadataHelperPg> helper_;
#elif defined(STORAGE_JSON)
  std::unique_ptr<MetadataHelperJson> helper_;
#endif
};

}  // namespace manager::metadata::testing

#endif  // TEST_INCLUDE_TEST_HELPER_CONSTRAINT_METADATA_HELPER_H_
