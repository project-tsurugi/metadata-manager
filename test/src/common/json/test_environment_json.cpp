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
#include "test/common/json/test_environment_json.h"

#include <boost/format.hpp>

#include "manager/metadata/common/config.h"
#include "test/helper/table_metadata_helper.h"

namespace manager::metadata::testing {

void TestEnvironmentJson::SetUp() {
  // generate table metadata as test data.
  testdata_table_metadata = std::make_unique<UTTableMetadata>("");
  testdata_table_metadata->generate_test_metadata();

  // initialize json file.
  boost::format filename = boost::format("%s/%s.json") %
                           manager::metadata::Config::get_storage_dir_path() %
                           "tables";
  std::remove(filename.str().c_str());
}

void TestEnvironmentJson::TearDown() {}

}  // namespace manager::metadata::testing
