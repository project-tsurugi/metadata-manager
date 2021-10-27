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
#include <gtest/gtest.h>

#include <boost/filesystem.hpp>
#include <iostream>

#include "test/global_test_environment.h"

namespace manager::metadata::testing {

GlobalTestEnvironment* const global = reinterpret_cast<GlobalTestEnvironment*>(
    ::testing::AddGlobalTestEnvironment(new GlobalTestEnvironment));

}  // namespace manager::metadata::testing

int main(int argc, char** argv) {
  printf("Running main() from %s\n", __FILE__);
  ::testing::InitGoogleTest(&argc, argv);

  if (argc == 2 && boost::filesystem::exists(argv[1])) {
    manager::metadata::testing::global->set_json_schema_file_name(argv[1]);
  }

  return RUN_ALL_TESTS();
}
