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
#ifndef TEST_INCLUDE_TEST_COMMON_DUMMY_OBJECT_H_
#define TEST_INCLUDE_TEST_COMMON_DUMMY_OBJECT_H_

#include "manager/metadata/object.h"

namespace manager::metadata::testing {

/**
 * @brief Dummy metadata object.
 */
struct DummyObject : public manager::metadata::Object {
 public:
  boost::property_tree::ptree convert_to_ptree() const override {
    boost::property_tree::ptree dummy;
    return dummy;
  }

  void convert_from_ptree(
      [[maybe_unused]] const boost::property_tree::ptree& pt) override {}
};

}  // namespace manager::metadata::testing

#endif  // TEST_INCLUDE_TEST_COMMON_DUMMY_OBJECT_H_
