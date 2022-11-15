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
#ifndef TEST_INCLUDE_TEST_TEST_INVALID_KEY_TEST_H_
#define TEST_INCLUDE_TEST_TEST_INVALID_KEY_TEST_H_

#include <memory>
#include <tuple>
#include <vector>

#include "test/test/column_statistics_test.h"
#include "test/test/constraint_metadata_test.h"
#include "test/test/index_metadata_test.h"
#include "test/test/table_metadata_test.h"

namespace manager::metadata::testing {

namespace invalid_test {

using InvalidTestParamId =
    std::tuple<std::shared_ptr<MetadataTest>, ErrorCode, ErrorCode, ErrorCode>;
using InvalidTestParamName =
    std::tuple<std::shared_ptr<MetadataTest>, ErrorCode, ErrorCode>;

/**
 * @brief Specify the metadata management to be tested in the invalid key test
 *   of the APIs by ID.
 *   [MetadataTest object, get, update, remove]
 */
static std::vector<InvalidTestParamId> test_by_id = {
    std::make_tuple(std::make_shared<TableMetadataTest>(),
                    ErrorCode::ID_NOT_FOUND, ErrorCode::ID_NOT_FOUND,
                    ErrorCode::ID_NOT_FOUND),
    std::make_tuple(std::make_shared<ConstraintMetadataTest>(),
                    ErrorCode::ID_NOT_FOUND, ErrorCode::UNKNOWN,
                    ErrorCode::ID_NOT_FOUND),
    std::make_tuple(std::make_shared<IndexMetadataTest>(),
                    ErrorCode::INVALID_PARAMETER, ErrorCode::INVALID_PARAMETER,
                    ErrorCode::INVALID_PARAMETER),
    std::make_tuple(std::make_shared<StatisticsMetadataTest>(),
                    ErrorCode::ID_NOT_FOUND, ErrorCode::UNKNOWN,
                    ErrorCode::ID_NOT_FOUND),
};

/**
 * @brief Specify the metadata management to be tested in the invalid key test
 *   of the APIs by name.
 *   [MetadataTest object, get, remove]
 */
static std::vector<InvalidTestParamName> test_by_name = {
    std::make_tuple(std::make_shared<TableMetadataTest>(),
                    ErrorCode::NAME_NOT_FOUND, ErrorCode::NAME_NOT_FOUND),
    std::make_tuple(std::make_shared<ConstraintMetadataTest>(),
                    ErrorCode::UNKNOWN, ErrorCode::UNKNOWN),
    std::make_tuple(std::make_shared<IndexMetadataTest>(),
                    ErrorCode::NAME_NOT_FOUND, ErrorCode::NAME_NOT_FOUND),
    std::make_tuple(std::make_shared<StatisticsMetadataTest>(),
                    ErrorCode::NAME_NOT_FOUND, ErrorCode::NAME_NOT_FOUND),
};

/**
 * @brief Specify the metadata management to be tested in the invalid key test
 *   of the APIs by ID.
 *   [MetadataTest object, get, update, remove]
 */
static std::vector<InvalidTestParamId> test_by_invalid_id = {
    std::make_tuple(std::make_shared<TableMetadataTest>(),
                    ErrorCode::ID_NOT_FOUND, ErrorCode::ID_NOT_FOUND,
                    ErrorCode::ID_NOT_FOUND),
    std::make_tuple(std::make_shared<ConstraintMetadataTest>(),
                    ErrorCode::ID_NOT_FOUND, ErrorCode::UNKNOWN,
                    ErrorCode::ID_NOT_FOUND),
    std::make_tuple(std::make_shared<IndexMetadataTest>(),
                    ErrorCode::INVALID_PARAMETER, ErrorCode::INVALID_PARAMETER,
                    ErrorCode::INVALID_PARAMETER),
    std::make_tuple(std::make_shared<StatisticsMetadataTest>(),
                    ErrorCode::ID_NOT_FOUND, ErrorCode::UNKNOWN,
                    ErrorCode::ID_NOT_FOUND),
};

/**
 * @brief Specify the metadata management to be tested in the invalid key test
 *   of the APIs by name.
 *   [MetadataTest object, get, remove]
 */
static std::vector<InvalidTestParamName> test_by_invalid_name = {
    std::make_tuple(std::make_shared<TableMetadataTest>(),
                    ErrorCode::NAME_NOT_FOUND, ErrorCode::NAME_NOT_FOUND),
    std::make_tuple(std::make_shared<ConstraintMetadataTest>(),
                    ErrorCode::UNKNOWN, ErrorCode::UNKNOWN),
    std::make_tuple(std::make_shared<IndexMetadataTest>(),
                    ErrorCode::INVALID_PARAMETER, ErrorCode::INVALID_PARAMETER),
    std::make_tuple(std::make_shared<StatisticsMetadataTest>(),
                    ErrorCode::NAME_NOT_FOUND, ErrorCode::NAME_NOT_FOUND),
};

}  // namespace invalid_test

}  // namespace manager::metadata::testing

#endif  // TEST_INCLUDE_TEST_TEST_INVALID_KEY_TEST_H_
