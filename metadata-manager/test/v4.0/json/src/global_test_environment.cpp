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
#include "test/global_test_environment.h"

#include <boost/format.hpp>
#include <limits>

#include "manager/metadata/dao/common/config.h"
#include "manager/metadata/dao/json/tables_dao.h"
#include "test/utility/ut_utils.h"

namespace manager::metadata::testing {

using namespace manager::metadata;
using namespace manager::metadata::db;
using namespace manager::metadata::db::json;

void GlobalTestEnvironment::SetUp() {
  // generate table metadata as test data.
  UTUtils::generate_table_metadata(testdata_table_metadata);

  // generate column statistics as test data.
  for (auto column : testdata_table_metadata->columns) {
    column_statistics.push_back(UTUtils::generate_column_statistic());
  }

  // initialize non-existing table id.
  table_id_not_exists = {-1,
                         0,
                         INT64_MAX - 1,
                         INT64_MAX,
                         std::numeric_limits<ObjectIdType>::infinity(),
                         -std::numeric_limits<ObjectIdType>::infinity(),
                         std::numeric_limits<ObjectIdType>::quiet_NaN()};

  // initialize non-existing ordinal positions.
  ordinal_position_not_exists = {
      -1,
      0,
      INT64_MAX - 1,
      INT64_MAX,
      4,
      std::numeric_limits<ObjectIdType>::infinity(),
      -std::numeric_limits<ObjectIdType>::infinity(),
      std::numeric_limits<ObjectIdType>::quiet_NaN()};

  // initialize json file.
  boost::format filename =
      boost::format("%s/%s.json") % Config::get_storage_dir_path() % "tables";
  std::remove(filename.str().c_str());
}

void GlobalTestEnvironment::TearDown() {}

}  // namespace manager::metadata::testing
