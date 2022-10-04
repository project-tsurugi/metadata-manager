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
#include <gtest/gtest.h>

#include <memory>
#include <string>

#include <boost/foreach.hpp>
#include <boost/property_tree/json_parser.hpp>

#include "manager/metadata/common/config.h"
#include "manager/metadata/error_code.h"
#include "manager/metadata/indexes.h"
#include "manager/metadata/tables.h"
#include "manager/metadata/metadata_factory.h"

#include "test/global_test_environment.h"
#include "test/helper/index_metadata_helper.h"
#include "test/helper/table_metadata_helper.h"
#include "test/utility/ut_table_metadata.h"
#include "test/utility/ut_utils.h"

namespace {

manager::metadata::ObjectId table_id = 0;

}  // namespace

namespace manager::metadata::testing {

using boost::property_tree::ptree;

class ApiTestIndexMetadata : public ::testing::Test {
  void SetUp() override {
    UTUtils::print(">> gtest::SetUp");
    // Get table metadata for testing.
    UTTableMetadata testdata_table_metadata = *(global->testdata_table_metadata.get());
    // Copy table metadata.
    ptree new_table = testdata_table_metadata.tables;
    // Change to a unique table name.
    std::string new_table_name = new_table.get<std::string>(Tables::NAME) +
                                 "_ApiTestIndexMetadata" + "_" + std::to_string(__LINE__);
    new_table.put(Tables::NAME, new_table_name);

    // Add table metadata.
    TableMetadataHelper::add_table(new_table, &table_id);
    UTUtils::print("<< gtest::SetUp\n");
  }

  void TearDown() override {
    UTUtils::print(">> gtest::TearDown");
    // Remove table metadata.
    UTUtils::print("-- remove table metadata --");
    UTUtils::print(" table id: ", table_id);

    auto tables = std::make_unique<Tables>(GlobalTestEnvironment::TEST_DB);
    tables->remove(table_id);
    UTUtils::print("<< gtest::TearDown\n");
  }
};

/**
 * @brief Test that adds metadata for a new index and retrieves it using the index id as
 * the key with the ptree type.
 * - add:
 *     patterns that obtain a index id.
 * - get:
 *     index id as a key.
 * - remove:
 *     index id as a key.
 */
TEST_F(ApiTestIndexMetadata, add_get_index_metadata_by_id) {
  std::unique_ptr<UTIndexMetadata> index_metadata;

  // generate metadata.
  IndexMetadataHelper::generate_test_metadata(table_id, index_metadata);
  ptree new_index = index_metadata->indexes_metadata;
  // change to a unique index name.
  new_index.put(Index::NAME,
                  new_index.get<std::string>(Index::NAME) + "_" + std::to_string(__LINE__));
  // set table id.
  new_index.put(Index::TABLE_ID, table_id);

  // generate index metadata manager.
  auto indexes = std::make_unique<Indexes>(GlobalTestEnvironment::TEST_DB);
  ErrorCode error = indexes->init();
  ASSERT_EQ(ErrorCode::OK, error);

  ObjectId inserted_id = INVALID_OBJECT_ID;
  // add index metadata.
  IndexMetadataHelper::add(indexes.get(), new_index, &inserted_id);
  // set index id.
  new_index.put(Index::ID, inserted_id);

  UTUtils::print("-- get index metadata by id --");
  {
    ptree index_metadata_inserted;
    // get index metadata by index id.
    error = indexes->get(inserted_id, index_metadata_inserted);
    EXPECT_EQ(ErrorCode::OK, error);

    UTUtils::print(UTUtils::get_tree_string(index_metadata_inserted));

    // verifies that the returned index metadata is expected one.
    IndexMetadataHelper::check_metadata_expected(new_index, index_metadata_inserted);
  }

  // remove index metadata by index id.
  UTUtils::print("-- remove index metadata by id --");
  {
    // remove table metadata.
    error = indexes->remove(inserted_id);
    EXPECT_EQ(ErrorCode::OK, error);
  }
}

/**
 * @brief Test that adds metadata for a new index and retrieves it using the index name as
 * the key with the ptree type.
 * - add:
 *     patterns that obtain a index name.
 * - get:
 *     index name as a key.
 * - remove:
 *     index name as a key.
 */
TEST_F(ApiTestIndexMetadata, add_get_index_metadata_by_name) {
  std::unique_ptr<UTIndexMetadata> index_metadata;

  // generate metadata.
  IndexMetadataHelper::generate_test_metadata(table_id, index_metadata);
  ptree new_index = index_metadata->indexes_metadata;
  // change to a unique index name.
  std::string index_name =
      new_index.get<std::string>(Index::NAME) + "_" + std::to_string(__LINE__);
  new_index.put(Index::NAME, index_name);
  // set table id.
  new_index.put(Index::TABLE_ID, table_id);

  // generate index metadata manager.
  auto indexes  = std::make_unique<Indexes>(GlobalTestEnvironment::TEST_DB);
  ErrorCode error = indexes->init();
  ASSERT_EQ(ErrorCode::OK, error);

  ObjectId inserted_id = INVALID_OBJECT_ID;
  // add index metadata.
  IndexMetadataHelper::add(indexes.get(), new_index, &inserted_id);
  // set index id.
  new_index.put(Index::ID, inserted_id);

  UTUtils::print("-- get index metadata by name --");
  {
    ptree index_metadata_inserted;
    // get index metadata by index name.
    error = indexes->get(index_name, index_metadata_inserted);
    EXPECT_EQ(ErrorCode::OK, error);

    UTUtils::print(UTUtils::get_tree_string(index_metadata_inserted));

    // verifies that the returned index metadata is expected one.
    IndexMetadataHelper::check_metadata_expected(new_index, index_metadata_inserted);
  }

  UTUtils::print("-- remove index metadata by name --");
  {
    ObjectId removed_id = -1;

    // remove index metadata by index name.
    error = indexes->remove(index_name, &removed_id);
    EXPECT_EQ(ErrorCode::OK, error);
    EXPECT_EQ(inserted_id, removed_id);
  }
}

/**
 * @brief Test that adds metadata for a new index and retrieves it using the index id as
 * the key with the ptree type.
 * - add:
 *     patterns that do not obtain a index id.
 * - get_all:
 * - remove:
 *     index id as a key.
 */
TEST_F(ApiTestIndexMetadata, add_get_all_index_metadata) {
  static constexpr const int32_t kTestIndexCount = 5;

  std::unique_ptr<UTIndexMetadata> index_metadata;
  auto base_index_count = IndexMetadataHelper::get_record_count();

  // generate index metadata manager.
  auto indexes    = std::make_unique<Indexes>(GlobalTestEnvironment::TEST_DB);
  ErrorCode error = indexes->init();
  ASSERT_EQ(ErrorCode::OK, error);

  // generate metadata.
  IndexMetadataHelper::generate_test_metadata(table_id, index_metadata);
  ptree new_indexes = index_metadata->indexes_metadata;
  // get name.
  auto index_name = new_indexes.get<std::string>(Index::NAME);
  // set table id.
  new_indexes.put(Index::TABLE_ID, table_id);

  // add index metadata.
  ObjectId index_ids[kTestIndexCount];
  int32_t name_index = 0;
  for (auto& inserted_id : index_ids) {
    new_indexes.put(Index::NAME, index_name + std::to_string(++name_index));

    inserted_id = 0;
    IndexMetadataHelper::add(indexes.get(), new_indexes, &inserted_id);
  }

  std::vector<boost::property_tree::ptree> container = {};
  // get index metadata.
  error = indexes->get_all(container);
  EXPECT_EQ(ErrorCode::OK, error);
  EXPECT_EQ(base_index_count + kTestIndexCount, container.size());

  UTUtils::print("-- get all index metadata --");
  {
    ptree expected_indexes = new_indexes;
    for (int32_t index = 0; index < kTestIndexCount; index++) {
      ptree actual_indexes = container[base_index_count + index];
      UTUtils::print(UTUtils::get_tree_string(actual_indexes));

      // set index name.
      expected_indexes.put(Index::NAME, index_name + std::to_string(index + 1));
      // set index id.
      expected_indexes.put(Index::ID, index_ids[index]);
      // verifies that the returned table metadata is expected one.
      IndexMetadataHelper::check_metadata_expected(expected_indexes, actual_indexes);
    }
  }

  // cleanup
  UTUtils::print("-- remove index metadata --");
  {
    for (auto& index_id : index_ids) {
      UTUtils::print(" index_id: ", index_id);
      indexes->remove(index_id);
      EXPECT_EQ(ErrorCode::OK, error);
    }
  }
}

/**
 * @brief Test removes index metadata by id.
 * - add:
 *     patterns that do not obtain a index id.
 * - remove:
 *     index id as a key.
 */
TEST_F(ApiTestIndexMetadata, remove_index_metadata_by_id) {
  std::unique_ptr<UTIndexMetadata> index_metadata;

  // generate metadata.
  IndexMetadataHelper::generate_test_metadata(table_id, index_metadata);
  ptree new_indexes = index_metadata->indexes_metadata;
  // change to a unique index name.
  new_indexes.put(Index::NAME,
                  new_indexes.get<std::string>(Index::NAME) + "_" + std::to_string(__LINE__));
  // set table id.
  new_indexes.put(Index::TABLE_ID, table_id);

  // generate index metadata manager.
  auto indexes    = std::make_unique<Indexes>(GlobalTestEnvironment::TEST_DB);
  ErrorCode error = indexes->init();
  ASSERT_EQ(ErrorCode::OK, error);

  ObjectId inserted_id = -1;
  // add index metadata.
  IndexMetadataHelper::add(indexes.get(), new_indexes, &inserted_id);

  UTUtils::print("-- remove index metadata by id --");
  {
    // remove table metadata by index id.
    ErrorCode error = indexes->remove(inserted_id);
    EXPECT_EQ(ErrorCode::OK, error);
  }

  UTUtils::print("-- get index metadata --");
  {
    ptree index_metadata_removed;
    // get index metadata by index id.
    error = indexes->get(inserted_id, index_metadata_removed);
    EXPECT_EQ(ErrorCode::ID_NOT_FOUND, error);

    UTUtils::print(UTUtils::get_tree_string(index_metadata_removed));
  }

  UTUtils::print("-- re-remove index metadata --");
  {
    // get index metadata by index id.
    error = indexes->remove(inserted_id);
    EXPECT_EQ(ErrorCode::ID_NOT_FOUND, error);
  }
}

/**
 * @brief Test removes index metadata by id.
 * - add:
 *     patterns that do not obtain a index id.
 * - remove:
 *     index id as a key.
 */
TEST_F(ApiTestIndexMetadata, remove_index_metadata_by_name) {
  std::unique_ptr<UTIndexMetadata> index_metadata;

  // generate metadata.
  IndexMetadataHelper::generate_test_metadata(table_id, index_metadata);
  ptree new_indexes = index_metadata->indexes_metadata;
  // change to a unique index name.
  std::string index_name =
      new_indexes.get<std::string>(Index::NAME) + "_" + std::to_string(__LINE__);
  new_indexes.put(Index::NAME, index_name);
  // set table id.
  new_indexes.put(Index::TABLE_ID, table_id);

  // generate index metadata manager.
  auto indexes    = std::make_unique<Indexes>(GlobalTestEnvironment::TEST_DB);
  ErrorCode error = indexes->init();
  ASSERT_EQ(ErrorCode::OK, error);

  ObjectId inserted_id = -1;
  // add index metadata.
  IndexMetadataHelper::add(indexes.get(), new_indexes, &inserted_id);

  UTUtils::print("-- remove index metadata by name --");
  {
    ObjectId removed_id = -1;

    // remove index metadata by index name.
    error = indexes->remove(index_name, &removed_id);
    EXPECT_EQ(ErrorCode::OK, error);
    EXPECT_EQ(inserted_id, removed_id);
  }

  UTUtils::print("-- get index metadata --");
  {
    ptree index_metadata_removed;
    // get index metadata by index id.
    error = indexes->get(inserted_id, index_metadata_removed);
    EXPECT_EQ(ErrorCode::ID_NOT_FOUND, error);

    UTUtils::print(UTUtils::get_tree_string(index_metadata_removed));
  }

  UTUtils::print("-- re-remove index metadata by name --");
  {
    ObjectId removed_id = -1;

    // remove index metadata by index name.
    error = indexes->remove(index_name, &removed_id);
    EXPECT_EQ(ErrorCode::NAME_NOT_FOUND, error);
    EXPECT_EQ(-1, removed_id);
  }
}

/**
 * @brief This test adds metadata with the same index name.
 * - add:
 *     patterns that obtain a index id.
 */
TEST_F(ApiTestIndexMetadata, add_name_duplicate) {
  std::unique_ptr<UTIndexMetadata> index_metadata;

  // generate metadata.
  IndexMetadataHelper::generate_test_metadata(table_id, index_metadata);
  ptree new_indexes = index_metadata->indexes_metadata;
  // change to a unique index name.
  new_indexes.put(Index::NAME,
                  new_indexes.get<std::string>(Index::NAME) + "_" + std::to_string(__LINE__));
  // set table id.
  new_indexes.put(Index::TABLE_ID, table_id);

  // generate index metadata manager.
  auto indexes    = std::make_unique<Indexes>(GlobalTestEnvironment::TEST_DB);
  ErrorCode error = indexes->init();
  ASSERT_EQ(ErrorCode::OK, error);

  ObjectId object_id_1 = -1;
  ObjectId object_id_2 = -1;

  // add first index metadata.
  UTUtils::print("-- add first index metadata --");
  error = indexes->add(new_indexes, &object_id_1);
  ASSERT_EQ(ErrorCode::OK, error);
  ASSERT_GT(object_id_1, 0);
  UTUtils::print(" >> index_id: ", object_id_1);

  // add first index metadata.
  UTUtils::print("-- add second index metadata --");
  error = indexes->add(new_indexes, &object_id_2);
  ASSERT_EQ(ErrorCode::ALREADY_EXISTS, error);
  ASSERT_EQ(object_id_2, -1);
  UTUtils::print(" >> index_id: ", object_id_2);

  // remove index metadata by index id.
  IndexMetadataHelper::remove(indexes.get(), object_id_1);
}

/**
 * @brief Test for incorrect index IDs.
 */
TEST_F(ApiTestIndexMetadata, all_invalid_parameter) {
  std::unique_ptr<UTIndexMetadata> index_metadata;
  // generate metadata.
  IndexMetadataHelper::generate_test_metadata(table_id, index_metadata);

  // generate index metadata manager.
  auto indexes    = std::make_unique<Indexes>(GlobalTestEnvironment::TEST_DB);
  ErrorCode error = indexes->init();
  ASSERT_EQ(ErrorCode::OK, error);

#if 0
  {
    ptree test_metadata;
    UTUtils::print("-- add index metadata without name --");
    test_metadata = index_metadata->indexes_metadata;
    test_metadata.erase(Index::NAME);
    test_metadata.put(Index::TABLE_ID, table_id);
    // Execute the API.
    error = indexes->add(test_metadata);
    EXPECT_EQ(ErrorCode::INVALID_PARAMETER, error);

    // Test the case where the table ID is not set.
    test_metadata = index_metadata->indexes_metadata;
    // Execute the API.
    error = indexes->add(test_metadata);
    EXPECT_EQ(ErrorCode::INVALID_PARAMETER, error);
  }
#endif
  {
    ptree index_metadata;

    UTUtils::print("-- get index metadata with invalid ID --");
    ObjectId index_id = INVALID_OBJECT_ID;
    // Execute the API.
    error = indexes->get(index_id, index_metadata);
    EXPECT_EQ(ErrorCode::INVALID_PARAMETER, error);

    UTUtils::print("-- get index metadata with invalid name --");
    std::string index_name = "";
    // Execute the API.
    error = indexes->get(index_name, index_metadata);
    EXPECT_EQ(ErrorCode::INVALID_PARAMETER, error);
  }

  // remove index metadata.
  {
    UTUtils::print("-- remove index metadata with invalid ID --");
    ObjectId index_id = INVALID_OBJECT_ID;
    // Execute the API.
    error = indexes->remove(index_id);
    EXPECT_EQ(ErrorCode::INVALID_PARAMETER, error);

    UTUtils::print("-- remove index metadata with invalid name --");
    std::string index_name = "";
    ObjectId ret_index_id  =INVALID_OBJECT_ID;
    // Execute the API.
    error = indexes->remove(index_name, &ret_index_id);
    EXPECT_EQ(ErrorCode::INVALID_PARAMETER, error);
    EXPECT_EQ(-1, ret_index_id);
  }
}

/**
 * @brief happy test for all index metadata getting.
 */
TEST_F(ApiTestIndexMetadata, get_all_index_metadata_empty) {
  // get base count
  std::int64_t base_table_count = IndexMetadataHelper::get_record_count();

  // generate index metadata manager.
  auto indexes = std::make_unique<Indexes>(GlobalTestEnvironment::TEST_DB);
  ErrorCode error = indexes->init();
  EXPECT_EQ(ErrorCode::OK, error);

  std::vector<boost::property_tree::ptree> container = {};
  // get index metadata.
  error = indexes->get_all(container);
  EXPECT_EQ(ErrorCode::OK, error);
  EXPECT_EQ(base_table_count, container.size());
}

/**
 * @brief happy test for adding, getting and removing
 *   one new table metadata without initialization of all api.
 */
TEST_F(ApiTestIndexMetadata, add_get_remove_without_initialized) {
  std::unique_ptr<UTIndexMetadata> index_metadata;

  // generate metadata.
  IndexMetadataHelper::generate_test_metadata(table_id, index_metadata);
  ptree new_indexes = index_metadata->indexes_metadata;
  // change to a unique index name.
  new_indexes.put(Index::NAME,
                  new_indexes.get<std::string>(Index::NAME) + "_" + std::to_string(__LINE__));
  // set table id.
  new_indexes.put(Index::TABLE_ID, table_id);

  ObjectId object_id = -1;
  UTUtils::print("-- add index metadata --");
  {
    // generate index metadata manager.
    auto indexes = std::make_unique<Indexes>(GlobalTestEnvironment::TEST_DB);
    // add index metadata.
    ErrorCode error = indexes->add(new_indexes, &object_id);
    EXPECT_EQ(ErrorCode::OK, error);
  }

  UTUtils::print("-- get index metadata --");
  {
    ptree index_metadata_inserted;
    // generate index metadata manager.
    auto indexes = std::make_unique<Indexes>(GlobalTestEnvironment::TEST_DB);
    // get index metadata by index id.
    ErrorCode error = indexes->get(object_id, index_metadata_inserted);
    EXPECT_EQ(ErrorCode::OK, error);
  }

  UTUtils::print("-- get_all index metadata --");
  {
    std::vector<boost::property_tree::ptree> container = {};
    // generate index metadata manager.
    auto indexes = std::make_unique<Indexes>(GlobalTestEnvironment::TEST_DB);
    // get index metadata by index id.
    ErrorCode error = indexes->get_all(container);
    EXPECT_EQ(ErrorCode::OK, error);
  }

  UTUtils::print("-- remove index metadata --");
  {
    // generate index metadata manager.
    auto indexes = std::make_unique<Indexes>(GlobalTestEnvironment::TEST_DB);
    // remove index metadata by index id.
    ErrorCode error = indexes->remove(object_id);
    EXPECT_EQ(ErrorCode::OK, error);
  }
}

/**
 * @brief happy test for removing one new table metadata by table name.
 */
TEST_F(ApiTestIndexMetadata, unsupported_apis) {
  // generate index metadata manager.
  auto indexes    = std::make_unique<Indexes>(GlobalTestEnvironment::TEST_DB);
  ErrorCode error = indexes->init();
  EXPECT_EQ(ErrorCode::OK, error);

  boost::property_tree::ptree object;
  ObjectId object_id = 9999;

  // update().
  error = indexes->update(object_id, object);
  EXPECT_EQ(ErrorCode::UNKNOWN, error);
}

/**
 * @brief Test that adds metadata for a new index and retrieves it using the index id as
 * the key with the ptree type.
 * - add:
 *     struct: patterns that obtain a index id.
 * - get:
 *     struct: index id as a key.
 *     ptree : index id as a key.
 * - remove:
 *     index id as a key.
 */
TEST_F(ApiTestIndexMetadata, add_get_index_metadata_object_ptree) {
  std::unique_ptr<UTIndexMetadata> index_metadata;

  // generate metadata.
  IndexMetadataHelper::generate_test_metadata(table_id, index_metadata);
  Index new_indexes;
  new_indexes.convert_from_ptree(index_metadata->indexes_metadata);
  // change to a unique index name.
  new_indexes.name = new_indexes.name + "_" + std::to_string(__LINE__);
  // set table id.
  new_indexes.table_id = table_id;

  // generate index metadata manager.
  auto indexes    = std::make_unique<Indexes>(GlobalTestEnvironment::TEST_DB);
  ErrorCode error = indexes->init();
  ASSERT_EQ(ErrorCode::OK, error);

  auto indexes2 = get_indexes(GlobalTestEnvironment::TEST_DB);
  error = indexes2->init();
  ASSERT_EQ(ErrorCode::OK, error);

  ObjectId inserted_id = -1;
  // add index metadata.
  IndexMetadataHelper::add(indexes.get(), new_indexes, &inserted_id);
  // set index id.
  new_indexes.id = inserted_id;

  UTUtils::print("-- get index metadata in ptree --");
  {
    ptree get_index_metadata;
    // get index metadata by index id.
    error = indexes->get(inserted_id, get_index_metadata);
    EXPECT_EQ(ErrorCode::OK, error);

    UTUtils::print(UTUtils::get_tree_string(get_index_metadata));

    // verifies that the returned index metadata is expected one.
    IndexMetadataHelper::check_metadata_expected(new_indexes.convert_to_ptree(),
                                                 get_index_metadata);
  }

  UTUtils::print("-- get index metadata in struct --");
  {
    Index get_index_metadata;
    // get index metadata by index id.
    error = indexes2->get(inserted_id, &get_index_metadata); 
    EXPECT_EQ(ErrorCode::OK, error);

    UTUtils::print(UTUtils::get_tree_string(get_index_metadata.convert_to_ptree()));

    // verifies that the returned index metadata is expected one.
    IndexMetadataHelper::check_metadata_expected(new_indexes.convert_to_ptree(),
                                                 get_index_metadata.convert_to_ptree());
  }

  // remove index metadata by index id.
  IndexMetadataHelper::remove(indexes.get(), inserted_id);

}

/**
 * @brief Test that adds metadata for a new index and retrieves it using the index id as
 * the key with the ptree type.
 * @brief Test that adds metadata for a new index and retrieves it using the index id as
 * the key with the ptree type.
 * - add:
 *     ptree: patterns that obtain a index id.
 * - get:
 *     struct: index id as a key.
 *     ptree : index id as a key.
 * - remove:
 *     index id as a key.
 */
TEST_F(ApiTestIndexMetadata, add_get_index_metadata_ptree_object) {
  std::unique_ptr<UTIndexMetadata> index_metadata;

  // generate metadata.
  IndexMetadataHelper::generate_test_metadata(table_id, index_metadata);
  ptree new_index = index_metadata->indexes_metadata;
  // change to a unique index name.
  new_index.put(Index::NAME,
                  new_index.get<std::string>(Index::NAME) + "_" + std::to_string(__LINE__));
  // set table id.
  new_index.put(Index::TABLE_ID, table_id);

  // generate index metadata manager.
  auto indexes  = std::make_unique<Indexes>(GlobalTestEnvironment::TEST_DB);
  ErrorCode error = indexes->init();
  ASSERT_EQ(ErrorCode::OK, error);

  auto indexes2 = get_indexes(GlobalTestEnvironment::TEST_DB);
  error = indexes2->init();
  ASSERT_EQ(ErrorCode::OK, error);

  ObjectId inserted_id = INVALID_OBJECT_ID;
  // add index metadata.
  IndexMetadataHelper::add(indexes.get(), new_index, &inserted_id);
  // set index id.
  new_index.put(Index::ID, inserted_id);

  UTUtils::print("-- get index metadata in ptree --");
  {
    ptree get_index_metadata;
    // get index metadata by index id.
    error = indexes->get(inserted_id, get_index_metadata);
    EXPECT_EQ(ErrorCode::OK, error);

    UTUtils::print(UTUtils::get_tree_string(get_index_metadata));

    // verifies that the returned index metadata is expected one.
    IndexMetadataHelper::check_metadata_expected(new_index, get_index_metadata);
  }

  UTUtils::print("-- get index metadata in struct --");
  {
    Index index;
    // get index metadata by index id.
    error = indexes2->get(inserted_id, &index);
    EXPECT_EQ(ErrorCode::OK, error);

    UTUtils::print(UTUtils::get_tree_string(index.convert_to_ptree()));

    // verifies that the returned index metadata is expected one.
    IndexMetadataHelper::check_metadata_expected(new_index,
                                                 index.convert_to_ptree());

  }

  // remove index metadata by index id.
  IndexMetadataHelper::remove(indexes.get(), inserted_id);
}

}  // namespace manager::metadata::testing
