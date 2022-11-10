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
#ifndef TEST_INCLUDE_TEST_HELPER_METADATA_HELPER_H_
#define TEST_INCLUDE_TEST_HELPER_METADATA_HELPER_H_

#include <cstdint>

namespace manager::metadata::testing {

class MetadataHelper {
 public:
  virtual int64_t get_record_count() const { return 0L; }
};

}  // namespace manager::metadata::testing

#endif  // TEST_INCLUDE_TEST_HELPER_METADATA_HELPER_H_