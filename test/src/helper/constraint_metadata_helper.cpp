/*
 * Copyright 2022 tsurugi project.
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
#include "test/helper/constraint_metadata_helper.h"

#include <memory>

#if defined(STORAGE_POSTGRESQL)
#include "test/helper/postgresql/metadata_helper_pg.h"
#elif defined(STORAGE_JSON)
#include "test/helper/json/metadata_helper_json.h"
#endif

namespace {

#if defined(STORAGE_POSTGRESQL)
static constexpr const char* const kTableName = "tsurugi_constraint";
#elif defined(STORAGE_JSON)
static constexpr const char* const kMetadataName = "tables";
static constexpr const char* const kRootNode     = "tables";
static constexpr const char* const kSubNode      = "constraints";
#endif

}  // namespace

namespace manager::metadata::testing {

int64_t ConstraintMetadataHelper::get_record_count() {
#if defined(STORAGE_POSTGRESQL)
  auto helper = std::make_unique<MetadataHelperPg>(kTableName);
#elif defined(STORAGE_JSON)
  auto helper =
      std::make_unique<MetadataHelperJson>(kMetadataName, kRootNode, kSubNode);
#endif

  return helper->get_record_count();
}

}  // namespace manager::metadata::testing
