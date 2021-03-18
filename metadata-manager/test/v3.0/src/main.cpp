/*
 * Copyright 2020 tsurugi project.
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

#include "test/api_test_environment.h"

namespace manager::metadata::testing {

ApiTestEnvironment *const api_test_env = reinterpret_cast<ApiTestEnvironment *>(
    ::testing::AddGlobalTestEnvironment(new ApiTestEnvironment));

int main(int argc, char **argv) {
    ::testing::InitGoogleTest(&argc, argv);

    ::testing::AddGlobalTestEnvironment(api_test_env);
    int ret_val = RUN_ALL_TESTS();
    delete api_test_env;

    return ret_val;
}

}  // namespace manager::metadata::testing
