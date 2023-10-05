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
#ifndef TEST_INCLUDE_TEST_COMMON_GLOBAL_TEST_ENVIRONMENT_H_
#define TEST_INCLUDE_TEST_COMMON_GLOBAL_TEST_ENVIRONMENT_H_

#if defined(STORAGE_POSTGRESQL)
#include "test/common/postgresql/test_environment_pg.h"
#elif defined(STORAGE_JSON)
#include "test/common/json/test_environment_json.h"
#endif

namespace manager::metadata::testing {

#define CALL_TRACE SCOPED_TRACE("")

/**
 * @brief GlobalTestEnvironment instance that is a global variable.
 */
#if defined(STORAGE_POSTGRESQL)
extern TestEnvironmentPg* const g_environment_;
#elif defined(STORAGE_JSON)
extern TestEnvironmentJson* const g_environment_;
#endif

class GlobalTestEnvironment : public ::testing::Environment {
 public:
  /**
   * @brief database name assigned to each API constructor argument.
   */
  static constexpr const char* const TEST_DB = "test";
};

}  // namespace manager::metadata::testing

#endif  // TEST_INCLUDE_TEST_COMMON_GLOBAL_TEST_ENVIRONMENT_H_
