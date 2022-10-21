/*
 * Copyright 2021 tsurugi project.
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
#if !defined(STORAGE_POSTGRESQL) && !defined(STORAGE_JSON)
#define STORAGE_POSTGRESQL
#endif

#include "test/common/global_test_environment.h"

namespace manager::metadata::testing {

#if defined(STORAGE_POSTGRESQL)
TestEnvironmentPg* const global = reinterpret_cast<TestEnvironmentPg*>(
    ::testing::AddGlobalTestEnvironment(new TestEnvironmentPg));
#elif defined(STORAGE_JSON)
TestEnvironmentJson* const global = reinterpret_cast<TestEnvironmentJson*>(
    ::testing::AddGlobalTestEnvironment(new TestEnvironmentJson));
#endif

}  // namespace manager::metadata::testing
