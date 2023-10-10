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
#ifndef TEST_INCLUDE_TEST_METADATA_UT_ROLE_METADATA_H_
#define TEST_INCLUDE_TEST_METADATA_UT_ROLE_METADATA_H_

#include <boost/property_tree/ptree.hpp>

#include "manager/metadata/roles.h"
#include "test/common/dummy_object.h"
#include "test/metadata/ut_metadata.h"

namespace manager::metadata::testing {

class UtRoleMetadata : public UtMetadata<DummyObject> {
 public:
  static constexpr const char* const kRoleName = "tsurugi_ut_role_user_1";

  using UtMetadata::UtMetadata;
  UtRoleMetadata() { this->generate_test_metadata(); }
  explicit UtRoleMetadata(ObjectId role_id) : role_id_(role_id) {
    this->generate_test_metadata();
  }

  void check_metadata_expected(const boost::property_tree::ptree& expected,
                               const boost::property_tree::ptree& actual,
                               const char* file,
                               const int64_t line) const override;

  /**
   * @brief Verifies that the actual metadata equals expected one.
   * @param actual    [in]  actual metadata.
   * @param file      [in]  file name of the caller.
   * @param line      [in]  line number of the caller.
   * @return none.
   */
  void check_metadata_expected(const boost::property_tree::ptree& actual,
                               const char* file, const int64_t line) const {
    check_metadata_expected(metadata_ptree_, actual, file, line);
  }

 private:
  ObjectId role_id_ = NOT_INITIALIZED;

  void generate_test_metadata();
};

}  // namespace manager::metadata::testing

#endif  // TEST_INCLUDE_TEST_METADATA_UT_ROLE_METADATA_H_
