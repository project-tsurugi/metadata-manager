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
#ifndef TEST_INCLUDE_TEST_HELPER_COLUMN_STATISTICS_HELPER_H_
#define TEST_INCLUDE_TEST_HELPER_COLUMN_STATISTICS_HELPER_H_

#include <memory>
#include <vector>

#include <boost/property_tree/ptree.hpp>

#include "manager/metadata/common/constants.h"
#include "test/helper/metadata_helper.h"

#if defined(STORAGE_POSTGRESQL)
#include "test/helper/postgresql/metadata_helper_pg.h"
#endif

namespace manager::metadata::testing {

class ColumnStatisticsHelper : public MetadataHelper {
 public:
#if defined(STORAGE_POSTGRESQL)
  ColumnStatisticsHelper()
      : helper_(std::make_unique<MetadataHelperPg>(kTableName)) {}
#elif defined(STORAGE_JSON)
  ColumnStatisticsHelper() {}
#endif

  int64_t get_record_count() const override {
#if defined(STORAGE_POSTGRESQL)
    return helper_->get_record_count();
#elif defined(STORAGE_JSON)
    return 0L;
#endif
  }

 private:
#if defined(STORAGE_POSTGRESQL)
  static constexpr const char* const kTableName = "statistics";
#endif

#if defined(STORAGE_POSTGRESQL)
  std::unique_ptr<MetadataHelperPg> helper_;
#endif
};

}  // namespace manager::metadata::testing

#endif  // TEST_INCLUDE_TEST_HELPER_COLUMN_STATISTICS_HELPER_H_
