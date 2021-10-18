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
#include <gtest/gtest.h>

#include "manager/metadata/dao/common/config.h"
#include "test/utility/ut_utils.h"

namespace manager::metadata::testing {

using namespace manager::metadata::db;

class DaoTestCommon : public ::testing::Test {};

/**
 * @brief Gets Connection Strings from OS environment variable.
 */
TEST_F(DaoTestCommon, get_storage_dir_path) {
  const char* tmp_dir = std::getenv("TSURUGI_METADATA_DIR");

  if (tmp_dir == nullptr) {
    std::string default_dir =
        std::string(getenv("HOME")) + "/.local/tsurugi/metadata";
    EXPECT_EQ(default_dir, Config::get_storage_dir_path());
    UTUtils::print("Metadata storage directory:",
                   Config::get_storage_dir_path());
  } else {
    EXPECT_EQ(tmp_dir, Config::get_storage_dir_path());
    UTUtils::print("Metadata storage directory:",
                   Config::get_storage_dir_path());
  }
}

}  // namespace manager::metadata::testing
