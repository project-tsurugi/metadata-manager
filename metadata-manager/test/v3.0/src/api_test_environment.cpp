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
#include "test/api_test_environment.h"

extern "C" {
#include <libpq-fe.h>
}

#include <limits>

#include "manager/metadata/dao/common/config.h"
#include "manager/metadata/dao/common/dbc_utils.h"

#include "test/utility/ut_utils.h"

namespace manager::metadata::testing {

using namespace manager::metadata::db;
using namespace manager::metadata;

void ApiTestEnvironment::SetUp() {
    UTUtils::generate_table_metadata(testdata_table_metadata, true);
    UTUtils::generate_table_metadata(
        testdata_table_metadata_without_primary_keys, false);

    for (auto column : testdata_table_metadata->columns) {
        column_statistics.push_back(UTUtils::generate_column_statistic());
    }

    boost::property_tree::ptree empty_column;

    for (auto column : testdata_table_metadata->columns) {
        empty_columns.push_back(empty_column);
    }

    table_id_not_exists = {-1,
                           0,
                           INT64_MAX - 1,
                           INT64_MAX,
                           std::numeric_limits<ObjectIdType>::infinity(),
                           -std::numeric_limits<ObjectIdType>::infinity(),
                           std::numeric_limits<ObjectIdType>::quiet_NaN()};
    ordinal_position_not_exists = {
        -1,
        0,
        INT64_MAX - 1,
        INT64_MAX,
        4,
        std::numeric_limits<ObjectIdType>::infinity(),
        -std::numeric_limits<ObjectIdType>::infinity(),
        std::numeric_limits<ObjectIdType>::quiet_NaN()};

    ConnectionSPtr connection = DbcUtils::make_connection_sptr(
        PQconnectdb(Config::get_connection_string().c_str()));

    if (DbcUtils::is_open(connection)) {
        is_open_ = true;
    } else {
        is_open_ = false;
    }

    UTUtils::print("global Setup()");
}

void ApiTestEnvironment::TearDown() { UTUtils::print("global TearDown()"); }

}  // namespace manager::metadata::testing
