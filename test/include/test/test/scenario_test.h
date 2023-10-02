/*
 * Copyright 2022 tsurugi project.
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
#ifndef TEST_INCLUDE_TEST_TEST_SCENARIO_TEST_H_
#define TEST_INCLUDE_TEST_TEST_SCENARIO_TEST_H_

#include <memory>
#include <vector>

#include "test/test/column_statistics_test.h"
#include "test/test/constraint_metadata_test.h"
#include "test/test/index_metadata_test.h"
#include "test/test/table_metadata_test.h"

namespace manager::metadata::testing {

namespace scenario_test {

using ScenarioTestParam = std::shared_ptr<MetadataTest>;

/**
 * @brief Specify the metadata management to be tested in the scenario test
 *   of the get API by ID.
 *   This test executes add(ptree/structure), exists(), get(ptree[/structure]),
 *   remove().
 */
static std::vector<ScenarioTestParam> get_test_by_id = {
    std::make_shared<TableMetadataTest>(),
    std::make_shared<ConstraintMetadataTest>(),
    std::make_shared<IndexMetadataTest>(),
    std::make_shared<StatisticsMetadataTest>(),
};

/**
 * @brief Specify the metadata management to be tested in the scenario test
 *   of the get API by name.
 *   This test executes add(ptree/structure), exists(), get(ptree[/structure]),
 *   remove().
 */
static std::vector<ScenarioTestParam> get_test_by_name = {
    std::make_shared<TableMetadataTest>(),
    std::make_shared<IndexMetadataTest>(),
    std::make_shared<StatisticsMetadataTest>(),
};

/**
 * @brief Specify the metadata management to be tested in the scenario test
 *   of the update API by .
 *   This test executes add(ptree), get(ptree), update(), remove().
 */
static std::vector<ScenarioTestParam> getall_test = {
    std::make_shared<TableMetadataTest>(),
    std::make_shared<ConstraintMetadataTest>(),
    std::make_shared<IndexMetadataTest>(),
    std::make_shared<StatisticsMetadataTest>(),
};

/**
 * @brief Specify the metadata management to be tested in the scenario test
 *   of the update API by ID.
 *   This test executes add(ptree), exists(), get(ptree[/structure]),
 *   remove().
 */
static std::vector<ScenarioTestParam> update_test = {
    std::make_shared<TableMetadataTest>(),
    std::make_shared<IndexMetadataTest>(),
};

}  // namespace scenario_test

}  // namespace manager::metadata::testing

#endif  // TEST_INCLUDE_TEST_TEST_SCENARIO_TEST_H_
