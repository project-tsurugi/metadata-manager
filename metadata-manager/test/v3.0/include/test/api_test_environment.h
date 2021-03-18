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

#ifndef API_TEST_ENVIRONMENT_H_
#define API_TEST_ENVIRONMENT_H_

#include <gtest/gtest.h>
#include <boost/property_tree/ptree.hpp>
#include <memory>
#include <vector>

#include "manager/metadata/metadata.h"
#include "test/utility/ut_table_metadata.h"

namespace manager::metadata::testing {

class ApiTestEnvironment : public ::testing::Environment {
   public:
    ~ApiTestEnvironment() override {}
    void SetUp() override;
    void TearDown() override;

    std::unique_ptr<UTTableMetadata> testdata_table_metadata;
    std::unique_ptr<UTTableMetadata>
        testdata_table_metadata_without_primary_keys;
    std::vector<boost::property_tree::ptree> column_statistics;
    std::vector<boost::property_tree::ptree> empty_columns;
    static constexpr char TEST_DB[] = "test";
    std::vector<ObjectIdType> table_id_not_exists;
    std::vector<ObjectIdType> ordinal_position_not_exists;
    bool is_open() { return is_open_; }

   private:
    bool is_open_ = true;
};

extern ApiTestEnvironment *const api_test_env;

}  // namespace manager::metadata::testing

#endif  // API_TEST_ENVIRONMENT_H_
