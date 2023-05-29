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
#ifndef TEST_JSON_INCLUDE_TEST_HELPER_CONSTRAINT_METADATA_HELPER_H_
#define TEST_JSON_INCLUDE_TEST_HELPER_CONSTRAINT_METADATA_HELPER_H_

#include <map>
#include <memory>
#include <string_view>

#include <boost/property_tree/ptree.hpp>

#include "manager/metadata/constraints.h"
#include "test/utility/ut_constraint_metadata.h"

namespace manager::metadata::testing {

class ConstraintMetadataHelper {
 public:
  static std::int64_t get_record_count();

  static void generate_test_metadata(const ObjectId& table_id,
                       std::unique_ptr<UTConstraintMetadata>& constraint_metadata);

  static void add(const Metadata* constraints,
                  const boost::property_tree::ptree& constraint_metadata,
                  ObjectIdType* constraint_id = nullptr);
  static void add(const Metadata* constraints,
                  const Constraint& constraint_metadata,
                  ObjectIdType* constraint_id = nullptr);

  static void remove(const Metadata* constraints, const ObjectIdType constraint_id);

  static void check_metadata_expected(const boost::property_tree::ptree& expected,
                             const boost::property_tree::ptree& actual);

 private:
  static void check_child_expected(const boost::property_tree::ptree& expected,
                                   const boost::property_tree::ptree& actual,
                                   const char* meta_name);
  template <typename T>
  static void check_expected(const boost::property_tree::ptree& expected,
                             const boost::property_tree::ptree& actual, const char* meta_name);
};

}  // namespace manager::metadata::testing

#endif  // TEST_JSON_INCLUDE_TEST_HELPER_CONSTRAINT_METADATA_HELPER_H_
