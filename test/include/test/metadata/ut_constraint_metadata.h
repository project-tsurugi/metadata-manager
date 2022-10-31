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
#ifndef TEST_INCLUDE_TEST_METADATA_UT_CONSTRAINT_METADATA_H_
#define TEST_INCLUDE_TEST_METADATA_UT_CONSTRAINT_METADATA_H_

#include <memory>
#include <string>

#include <boost/property_tree/ptree.hpp>

#include "manager/metadata/constraints.h"
#include "test/metadata/ut_metadata.h"

namespace manager::metadata::testing {

class UTConstraintMetadata : public UtMetadata {
 public:
  explicit UTConstraintMetadata(const Constraint& metadata)
      : metadata_ptree_(metadata.convert_to_ptree()),
        metadata_struct_(metadata) {}
  explicit UTConstraintMetadata(const boost::property_tree::ptree& metadata)
      : metadata_ptree_(metadata) {
    metadata_struct_.convert_from_ptree(metadata_ptree_);
  }
  explicit UTConstraintMetadata(ObjectId table_id = NOT_INITIALIZED)
      : table_id_(table_id) {}

  void generate_test_metadata() override;

  const manager::metadata::Constraint* get_metadata_struct() const override {
    return &metadata_struct_;
  }
  boost::property_tree::ptree get_metadata_ptree() const override {
    return metadata_ptree_;
  }

  void check_metadata_expected(const boost::property_tree::ptree& expected,
                               const boost::property_tree::ptree& actual,
                               const char* file,
                               const int64_t line) const override;

 private:
  boost::property_tree::ptree metadata_ptree_;
  manager::metadata::Constraint metadata_struct_;

  ObjectId table_id_ = NOT_INITIALIZED;
};

}  // namespace manager::metadata::testing

#endif  // TEST_INCLUDE_TEST_METADATA_UT_CONSTRAINT_METADATA_H_
