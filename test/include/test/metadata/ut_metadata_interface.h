/*
 * Copyright 2020-2021 tsurugi project.
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
#ifndef TEST_INCLUDE_TEST_METADATA_UT_METADATA_INTERFACE_H_
#define TEST_INCLUDE_TEST_METADATA_UT_METADATA_INTERFACE_H_

#include <boost/property_tree/ptree.hpp>

#include "manager/metadata/metadata.h"

namespace manager::metadata::testing {

class UtMetadataInterface {
 public:
  virtual ~UtMetadataInterface() {}

  virtual const manager::metadata::Object* get_metadata_struct() const = 0;
  virtual boost::property_tree::ptree get_metadata_ptree() const       = 0;
  virtual void generate_test_metadata()                                = 0;
  virtual void check_metadata_expected(
      const boost::property_tree::ptree& expected,
      const boost::property_tree::ptree& actual, const char* file,
      const int64_t line) const = 0;

  virtual void check_metadata_expected(
      const boost::property_tree::ptree& expected,
      const ::manager::metadata::Object& actual, const char* file,
      const int64_t line) const = 0;
};

}  // namespace manager::metadata::testing

#endif  // TEST_INCLUDE_TEST_METADATA_UT_METADATA_INTERFACE_H_
