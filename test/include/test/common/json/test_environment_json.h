/*
 * Copyright 2020-2021 Project Tsurugi.
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
#ifndef TEST_INCLUDE_TEST_COMMON_JSON_TEST_ENVIRONMENT_JSON_H_
#define TEST_INCLUDE_TEST_COMMON_JSON_TEST_ENVIRONMENT_JSON_H_

#include "boost/format.hpp"

#include "manager/metadata/common/config.h"
#include "test/common/test_environment.h"

namespace manager::metadata::testing {

class TestEnvironmentJson : public TestEnvironment {
 public:
  ~TestEnvironmentJson() override {}

  void SetUp() override {
    TestEnvironment::SetUp();

    // initialize json file.
    boost::format filename = boost::format("%s/%s.json") %
                             manager::metadata::Config::get_storage_dir_path() %
                             "tables";
    std::remove(filename.str().c_str());
  }

  void TearDown() override { TestEnvironment::TearDown(); }
};

}  // namespace manager::metadata::testing

#endif  // TEST_INCLUDE_TEST_COMMON_JSON_TEST_ENVIRONMENT_JSON_H_
