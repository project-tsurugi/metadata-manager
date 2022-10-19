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

#include <boost/property_tree/json_parser.hpp>

#include "manager/metadata/metadata_factory.h"
#include "manager/metadata/tables.h"
#include "test/common/postgresql/global_test_environment_pg.h"
#include "test/common/postgresql/ut_utils_pg.h"
#include "test/helper/postgresql/index_metadata_helper_pg.h"
#include "test/helper/postgresql/table_metadata_helper_pg.h"

namespace manager::metadata::testing {

using boost::property_tree::ptree;

class ApiTestIndexMetadata : public ::testing::Test {
 public:
  manager::metadata::ObjectId table_id = 0;

  void SetUp() override {
    if (!global->is_open()) {
      GTEST_SKIP_("metadata repository is not started.");
    }

    UTUtils::print(">> gtest::SetUp()");

    // Get table metadata for testing.
    UTTableMetadata testdata_table_metadata =
        *(global->testdata_table_metadata.get());
    // Copy table metadata.
    ptree new_metadata = testdata_table_metadata.tables;
    // Change to a unique table name.
    std::string table_name = new_metadata.get<std::string>(Tables::NAME) +
                             "_ApiTestIndexMetadata" + std::to_string(__LINE__);
    new_metadata.put(Tables::NAME, table_name);

    // Add table metadata.
    TableMetadataHelper::add_table(new_metadata, &table_id);

    UTUtils::print("<< gtest::SetUp()\n");
  }

  void TearDown() override {
    if (global->is_open()) {
      UTUtils::print(">> gtest::TearDown()");

      // Remove table metadata.
      TableMetadataHelper::remove_table(table_id);

      UTUtils::print("<< gtest::TearDown()\n");
    }
  }
};

/**
 * @brief Test that adds metadata for a new index and retrieves it using the
 * index id as the key with the ptree type.
 * - add:
 *     patterns that obtain a index id.
 * - get:
 *     index id as a key.
 * - remove:
 *     index id as a key.
 */
TEST_F(ApiTestIndexMetadata, add_get_index_metadata_by_id) {
  std::unique_ptr<UTIndexMetadata> testdata_index_metadata;

  // generate metadata.
  IndexMetadataHelper::generate_test_metadata(table_id,
                                              testdata_index_metadata);
  ptree new_metadata = testdata_index_metadata->indexes_metadata;

  // change to a unique index name.
  new_metadata.put(Index::NAME, new_metadata.get<std::string>(Index::NAME) +
                                    "_" + std::to_string(__LINE__));

  // generate index metadata manager.
  auto indexes    = get_index_metadata(GlobalTestEnvironment::TEST_DB);
  ErrorCode error = indexes->init();
  ASSERT_EQ(ErrorCode::OK, error);

  ObjectId inserted_id = INVALID_OBJECT_ID;
  // add index metadata.
  IndexMetadataHelper::add(indexes.get(), new_metadata, &inserted_id);

  UTUtils::print("-- get index metadata by id --");
  {
    ptree metadata_inserted;

    // get index metadata by index id.
    error = indexes->get(inserted_id, metadata_inserted);
    EXPECT_EQ(ErrorCode::OK, error);

    UTUtils::print(UTUtils::get_tree_string(metadata_inserted));

    // set index id.
    new_metadata.put(Index::ID, inserted_id);
    // verifies that the returned index metadata is expected one.
    IndexMetadataHelper::check_metadata_expected(new_metadata,
                                                 metadata_inserted);
  }

  // remove index metadata by index id.
  IndexMetadataHelper::remove(indexes.get(), inserted_id);
}

/**
 * @brief Test that adds metadata for a new index and retrieves it using the
 * index name as the key with the ptree type.
 * - add:
 *     patterns that obtain a index name.
 * - get:
 *     index name as a key.
 * - remove:
 *     index name as a key.
 */
TEST_F(ApiTestIndexMetadata, add_get_index_metadata_by_name) {
  std::unique_ptr<UTIndexMetadata> testdata_index_metadata;

  // generate metadata.
  IndexMetadataHelper::generate_test_metadata(table_id,
                                              testdata_index_metadata);
  ptree new_metadata = testdata_index_metadata->indexes_metadata;

  // change to a unique index name.
  std::string index_name = new_metadata.get<std::string>(Index::NAME) + "_" +
                           std::to_string(__LINE__);
  new_metadata.put(Index::NAME, index_name);

  // generate index metadata manager.
  auto indexes    = get_index_metadata(GlobalTestEnvironment::TEST_DB);
  ErrorCode error = indexes->init();
  ASSERT_EQ(ErrorCode::OK, error);

  ObjectId inserted_id = INVALID_OBJECT_ID;
  // add index metadata.
  IndexMetadataHelper::add(indexes.get(), new_metadata, &inserted_id);

  UTUtils::print("-- get index metadata by name --");
  {
    ptree metadata_inserted;

    // get index metadata by index name.
    error = indexes->get(index_name, metadata_inserted);
    EXPECT_EQ(ErrorCode::OK, error);

    UTUtils::print(UTUtils::get_tree_string(metadata_inserted));

    // set index id.
    new_metadata.put(Index::ID, inserted_id);
    // verifies that the returned index metadata is expected one.
    IndexMetadataHelper::check_metadata_expected(new_metadata,
                                                 metadata_inserted);
  }

  ObjectId removed_id = INVALID_OBJECT_ID;
  // remove index metadata by index name.
  IndexMetadataHelper::remove(indexes.get(), index_name, &removed_id);
  EXPECT_EQ(inserted_id, removed_id);
}

/**
 * @brief Test that adds metadata for a new index and retrieves it using the
 * index id as the key with the ptree type.
 * - add:
 *     patterns that do not obtain a index id.
 * - get_all:
 * - remove:
 *     index id as a key.
 */
TEST_F(ApiTestIndexMetadata, add_get_all_index_metadata) {
  static constexpr const int32_t kTestIndexCount = 5;

  std::unique_ptr<UTIndexMetadata> testdata_index_metadata;
  auto base_index_count = IndexMetadataHelper::get_record_count();

  // generate index metadata manager.
  auto indexes    = get_index_metadata(GlobalTestEnvironment::TEST_DB);
  ErrorCode error = indexes->init();
  ASSERT_EQ(ErrorCode::OK, error);

  // generate metadata.
  IndexMetadataHelper::generate_test_metadata(table_id,
                                              testdata_index_metadata);
  ptree new_metadata = testdata_index_metadata->indexes_metadata;
  // get name.
  auto index_name = new_metadata.get<std::string>(Index::NAME);

  // add index metadata.
  ObjectId index_ids[kTestIndexCount];
  int32_t name_index = 0;
  for (auto& inserted_id : index_ids) {
    new_metadata.put(Index::NAME, index_name + std::to_string(++name_index));

    inserted_id = INVALID_OBJECT_ID;
    IndexMetadataHelper::add(indexes.get(), new_metadata, &inserted_id);
  }

  UTUtils::print("-- get all index metadata --");
  {
    std::vector<boost::property_tree::ptree> container = {};
    // get index metadata.
    error = indexes->get_all(container);
    EXPECT_EQ(ErrorCode::OK, error);
    EXPECT_EQ(base_index_count + kTestIndexCount, container.size());

    ptree expected_metadata = new_metadata;
    for (int32_t index = 0; index < kTestIndexCount; index++) {
      ptree actual_metadata = container[base_index_count + index];
      UTUtils::print(UTUtils::get_tree_string(actual_metadata));

      // set index name.
      expected_metadata.put(Index::NAME,
                            index_name + std::to_string(index + 1));
      // set index id.
      expected_metadata.put(Tables::ID, index_ids[index]);

      // verifies that the returned index metadata is expected one.
      IndexMetadataHelper::check_metadata_expected(expected_metadata,
                                                   actual_metadata);
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
 * @brief This is a test to update index metadata.
 * - add:
 *     patterns that obtain a index id.
 * - update:
 *     index id as a key.
 * - get:
 *     index id as a key.
 * - remove:
 *     index id as a key.
 */
TEST_F(ApiTestIndexMetadata, add_update_index_metadata) {
  std::unique_ptr<UTIndexMetadata> testdata_index_metadata;

  // generate metadata.
  IndexMetadataHelper::generate_test_metadata(table_id,
                                              testdata_index_metadata);
  ptree new_metadata = testdata_index_metadata->indexes_metadata;

  // change to a unique index name.
  new_metadata.put(Index::NAME, new_metadata.get<std::string>(Index::NAME) +
                                    "_" + std::to_string(__LINE__));

  // generate index metadata manager.
  auto indexes    = get_index_metadata(GlobalTestEnvironment::TEST_DB);
  ErrorCode error = indexes->init();
  ASSERT_EQ(ErrorCode::OK, error);

  ObjectId inserted_id = INVALID_OBJECT_ID;
  // add index metadata.
  IndexMetadataHelper::add(indexes.get(), new_metadata, &inserted_id);

  ptree metadata_inserted;
  UTUtils::print("-- get inserted index metadata --");
  {
    // get index metadata by index id.
    error = indexes->get(inserted_id, metadata_inserted);
    EXPECT_EQ(ErrorCode::OK, error);

    UTUtils::print(UTUtils::get_tree_string(metadata_inserted));
  }

  ptree metadata = metadata_inserted;
  UTUtils::print("-- update index metadata --");
  {
    metadata = metadata_inserted;

    // name
    metadata.put(Index::NAME,
                 metadata_inserted.get<std::string>(Index::NAME) + "-update");
    // namespace
    metadata.put(
        Index::NAMESPACE,
        metadata_inserted.get<std::string>(Index::NAMESPACE) + "-update");
    // access_method
    metadata.put(Index::ACCESS_METHOD,
                 static_cast<int64_t>(Index::AccessMethod::MASS_TREE_METHOD));
    // is_primary
    metadata.put(Index::IS_PRIMARY, true);
    // columns
    {
      ptree elements;
      ptree element;
      element.put("", 11);
      elements.push_back(std::make_pair("", element));
      element.put("", 12);
      elements.push_back(std::make_pair("", element));

      metadata.erase(Index::KEYS);
      metadata.add_child(Index::KEYS, elements);
    }
    // columns id.
    {
      ptree elements;
      ptree element;
      element.put("", 2011);
      elements.push_back(std::make_pair("", element));
      element.put("", 2012);
      elements.push_back(std::make_pair("", element));

      metadata.erase(Index::KEYS_ID);
      metadata.add_child(Index::KEYS_ID, elements);
    }

    UTUtils::print(" >> update index_id: ", inserted_id);
    // update index metadata by index id.
    error = indexes->update(inserted_id, metadata);
    ASSERT_EQ(ErrorCode::OK, error);
  }

  ptree metadata_updated;
  UTUtils::print("-- get updated index metadata --");
  {
    // get index metadata by index id.
    error = indexes->get(inserted_id, metadata_updated);
    EXPECT_EQ(ErrorCode::OK, error);

    UTUtils::print(UTUtils::get_tree_string(metadata_updated));
  }

  // verifies that the returned index metadata is expected one.
  IndexMetadataHelper::check_metadata_expected(metadata, metadata_updated);

  // remove index metadata by index id.
  IndexMetadataHelper::remove(indexes.get(), inserted_id);
}

/**
 * @brief Test removes index metadata by id.
 * - add:
 *     patterns that do not obtain a index id.
 * - remove:
 *     index id as a key.
 */
TEST_F(ApiTestIndexMetadata, remove_index_metadata_by_id) {
  std::unique_ptr<UTIndexMetadata> testdata_index_metadata;

  // generate metadata.
  IndexMetadataHelper::generate_test_metadata(table_id,
                                              testdata_index_metadata);
  ptree new_metadata = testdata_index_metadata->indexes_metadata;
  // change to a unique index name.
  new_metadata.put(Index::NAME, new_metadata.get<std::string>(Index::NAME) +
                                    "_" + std::to_string(__LINE__));

  // generate index metadata manager.
  auto indexes    = get_index_metadata(GlobalTestEnvironment::TEST_DB);
  ErrorCode error = indexes->init();
  ASSERT_EQ(ErrorCode::OK, error);

  ObjectId inserted_id = INVALID_OBJECT_ID;
  // add index metadata.
  IndexMetadataHelper::add(indexes.get(), new_metadata, &inserted_id);

  // remove index metadata by index id.
  IndexMetadataHelper::remove(indexes.get(), inserted_id);

  UTUtils::print("-- get index metadata --");
  {
    ptree metadata_removed;
    // get index metadata by index id.
    error = indexes->get(inserted_id, metadata_removed);
    EXPECT_EQ(ErrorCode::ID_NOT_FOUND, error);
  }

  UTUtils::print("-- re-remove index metadata --");
  {
    // get index metadata by index id.
    error = indexes->remove(inserted_id);
    EXPECT_EQ(ErrorCode::ID_NOT_FOUND, error);
  }
}

/**
 * @brief Test removes index metadata by name.
 * - add:
 *     patterns that do not obtain a index name.
 * - remove:
 *     index name as a key.
 */
TEST_F(ApiTestIndexMetadata, remove_index_metadata_by_name) {
  std::unique_ptr<UTIndexMetadata> testdata_index_metadata;

  // generate metadata.
  IndexMetadataHelper::generate_test_metadata(table_id,
                                              testdata_index_metadata);
  ptree new_metadata = testdata_index_metadata->indexes_metadata;
  // change to a unique index name.
  std::string index_name = new_metadata.get<std::string>(Index::NAME) + "_" +
                           std::to_string(__LINE__);
  new_metadata.put(Index::NAME, index_name);

  // generate index metadata manager.
  auto indexes    = get_index_metadata(GlobalTestEnvironment::TEST_DB);
  ErrorCode error = indexes->init();
  ASSERT_EQ(ErrorCode::OK, error);

  ObjectId inserted_id = INVALID_OBJECT_ID;
  // add index metadata.
  IndexMetadataHelper::add(indexes.get(), new_metadata, &inserted_id);

  ObjectId removed_id = INVALID_OBJECT_ID;
  // remove index metadata by index name.
  IndexMetadataHelper::remove(indexes.get(), index_name, &removed_id);
  EXPECT_EQ(inserted_id, removed_id);

  UTUtils::print("-- get index metadata --");
  {
    ptree metadata_removed;
    // get index metadata by index id.
    error = indexes->get(inserted_id, metadata_removed);
    EXPECT_EQ(ErrorCode::ID_NOT_FOUND, error);
  }

  UTUtils::print("-- re-remove index metadata by name --");
  {
    ObjectId removed_id = INVALID_OBJECT_ID;

    // remove index metadata by index name.
    error = indexes->remove(index_name, &removed_id);
    EXPECT_EQ(ErrorCode::NAME_NOT_FOUND, error);
    EXPECT_EQ(INVALID_OBJECT_ID, removed_id);
  }
}

/**
 * @brief This test adds metadata with the same index name.
 * - add:
 *     patterns that obtain a index id.
 */
TEST_F(ApiTestIndexMetadata, add_name_duplicate) {
  std::unique_ptr<UTIndexMetadata> testdata_index_metadata;

  // generate metadata.
  IndexMetadataHelper::generate_test_metadata(table_id,
                                              testdata_index_metadata);
  ptree new_metadata = testdata_index_metadata->indexes_metadata;
  // change to a unique index name.
  new_metadata.put(Index::NAME, new_metadata.get<std::string>(Index::NAME) +
                                    "_" + std::to_string(__LINE__));

  // generate index metadata manager.
  auto indexes    = get_index_metadata(GlobalTestEnvironment::TEST_DB);
  ErrorCode error = indexes->init();
  ASSERT_EQ(ErrorCode::OK, error);

  ObjectId inserted_id_1st = INVALID_OBJECT_ID;
  ObjectId inserted_id_2nd = INVALID_OBJECT_ID;

  // add first index metadata.
  UTUtils::print("-- add first index metadata --");
  error = indexes->add(new_metadata, &inserted_id_1st);
  ASSERT_EQ(ErrorCode::OK, error);
  ASSERT_GT(inserted_id_1st, 0);
  UTUtils::print(" >> index_id: ", inserted_id_1st);

  // add second index metadata.
  UTUtils::print("-- add second index metadata --");
  error = indexes->add(new_metadata, &inserted_id_2nd);
  ASSERT_EQ(ErrorCode::ALREADY_EXISTS, error);
  ASSERT_EQ(INVALID_OBJECT_ID, inserted_id_2nd);
  UTUtils::print(" >> index_id: ", inserted_id_2nd);

  // remove index metadata by index id.
  IndexMetadataHelper::remove(indexes.get(), inserted_id_1st);
}

/**
 * @brief Test for incorrect index IDs.
 */
TEST_F(ApiTestIndexMetadata, all_invalid_parameter) {
  // generate index metadata manager.
  auto indexes    = get_index_metadata(GlobalTestEnvironment::TEST_DB);
  ErrorCode error = indexes->init();
  ASSERT_EQ(ErrorCode::OK, error);

#if 0
  {
    std::unique_ptr<UTIndexMetadata> testdata_index_metadata;
    // generate metadata.
    IndexMetadataHelper::generate_test_metadata(table_id, testdata_index_metadata);

    ptree test_metadata;
    UTUtils::print("-- add index metadata without name --");
    test_metadata = testdata_index_metadata->indexes_metadata;
    test_metadata.erase(Index::NAME);
    test_metadata.put(Index::TABLE_ID, table_id);
    // Execute the API.
    error = indexes->add(test_metadata);
    EXPECT_EQ(ErrorCode::INVALID_PARAMETER, error);

    // Test the case where the table ID is not set.
    test_metadata = testdata_index_metadata->indexes_metadata;
    // Execute the API.
    error = indexes->add(test_metadata);
    EXPECT_EQ(ErrorCode::INVALID_PARAMETER, error);
  }
#endif
  {
    ptree test_metadata;

    UTUtils::print("-- get index metadata with invalid ID --");
    ObjectId index_id = INVALID_OBJECT_ID;
    // Execute the API.
    error = indexes->get(index_id, test_metadata);
    EXPECT_EQ(ErrorCode::INVALID_PARAMETER, error);

    UTUtils::print("-- get index metadata with invalid name --");
    std::string index_name = "";
    // Execute the API.
    error = indexes->get(index_name, test_metadata);
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
    ObjectId removed_id    = INVALID_OBJECT_ID;
    // Execute the API.
    error = indexes->remove(index_name, &removed_id);
    EXPECT_EQ(ErrorCode::INVALID_PARAMETER, error);
    EXPECT_EQ(INVALID_OBJECT_ID, removed_id);
  }
}

/**
 * @brief happy test for all index metadata getting.
 */
TEST_F(ApiTestIndexMetadata, get_all_index_metadata_empty) {
  // get base count
  std::int64_t base_index_count = IndexMetadataHelper::get_record_count();

  // generate index metadata manager.
  auto indexes    = get_index_metadata(GlobalTestEnvironment::TEST_DB);
  ErrorCode error = indexes->init();
  EXPECT_EQ(ErrorCode::OK, error);

  std::vector<boost::property_tree::ptree> container = {};
  // get index metadata.
  error = indexes->get_all(container);
  EXPECT_EQ(ErrorCode::OK, error);
  EXPECT_EQ(base_index_count, container.size());
}

/**
 * @brief happy test for adding, getting and removing
 *   one new index metadata without initialization of all api.
 */
TEST_F(ApiTestIndexMetadata, add_get_remove_without_initialized) {
  std::unique_ptr<UTIndexMetadata> testdata_index_metadata;

  // generate metadata.
  IndexMetadataHelper::generate_test_metadata(table_id,
                                              testdata_index_metadata);
  ptree new_metadata = testdata_index_metadata->indexes_metadata;
  // change to a unique index name.
  new_metadata.put(Index::NAME, new_metadata.get<std::string>(Index::NAME) +
                                    "_" + std::to_string(__LINE__));

  ObjectId inserted_id = INVALID_OBJECT_ID;
  UTUtils::print("-- add index metadata --");
  {
    // generate index metadata manager.
    auto indexes = get_index_metadata(GlobalTestEnvironment::TEST_DB);
    // add index metadata.
    ErrorCode error = indexes->add(new_metadata, &inserted_id);
    EXPECT_EQ(ErrorCode::OK, error);
  }

  UTUtils::print("-- get index metadata --");
  {
    ptree metadata_inserted;
    // generate index metadata manager.
    auto indexes = get_index_metadata(GlobalTestEnvironment::TEST_DB);
    // get index metadata by index id.
    ErrorCode error = indexes->get(inserted_id, metadata_inserted);
    EXPECT_EQ(ErrorCode::OK, error);
  }

  UTUtils::print("-- get_all index metadata --");
  {
    std::vector<boost::property_tree::ptree> container = {};
    // generate index metadata manager.
    auto indexes = get_index_metadata(GlobalTestEnvironment::TEST_DB);
    // get index metadata by index id.
    ErrorCode error = indexes->get_all(container);
    EXPECT_EQ(ErrorCode::OK, error);
  }

  UTUtils::print("-- remove index metadata --");
  {
    // generate index metadata manager.
    auto indexes = get_index_metadata(GlobalTestEnvironment::TEST_DB);
    // remove index metadata by index id.
    ErrorCode error = indexes->remove(inserted_id);
    EXPECT_EQ(ErrorCode::OK, error);
  }
}

/**
 * @brief Test that adds metadata for a new index and retrieves it using the
 * index id as the key with the ptree type.
 * - add:
 *     struct: patterns that obtain a index id.
 * - get:
 *     struct: index id as a key.
 *     ptree : index id as a key.
 * - remove:
 *     index id as a key.
 */
TEST_F(ApiTestIndexMetadata, add_get_index_metadata_object_ptree) {
  std::unique_ptr<UTIndexMetadata> testdata_index_metadata;

  // generate metadata.
  IndexMetadataHelper::generate_test_metadata(table_id,
                                              testdata_index_metadata);
  Index new_metadata;
  new_metadata.convert_from_ptree(testdata_index_metadata->indexes_metadata);
  // change to a unique index name.
  new_metadata.name = new_metadata.name + "_" + std::to_string(__LINE__);

  // generate index metadata manager.
  auto indexes    = get_index_metadata(GlobalTestEnvironment::TEST_DB);
  ErrorCode error = indexes->init();
  ASSERT_EQ(ErrorCode::OK, error);

  ObjectId inserted_id = INVALID_OBJECT_ID;
  // add index metadata.
  IndexMetadataHelper::add(indexes.get(), new_metadata, &inserted_id);
  // set index id.
  new_metadata.id = inserted_id;

  UTUtils::print("-- get index metadata in ptree --");
  {
    ptree metadata_retrieved;

    // get index metadata by index id.
    error = indexes->get(inserted_id, metadata_retrieved);
    EXPECT_EQ(ErrorCode::OK, error);

    UTUtils::print(UTUtils::get_tree_string(metadata_retrieved));

    // verifies that the returned index metadata is expected one.
    IndexMetadataHelper::check_metadata_expected(
        new_metadata.convert_to_ptree(), metadata_retrieved);
  }

  UTUtils::print("-- get index metadata in struct --");
  {
    Index metadata_retrieved;

    // get index metadata by index id.
    error = indexes->get(inserted_id, metadata_retrieved);
    EXPECT_EQ(ErrorCode::OK, error);

    UTUtils::print(
        UTUtils::get_tree_string(metadata_retrieved.convert_to_ptree()));

    // verifies that the returned index metadata is expected one.
    IndexMetadataHelper::check_metadata_expected(
        new_metadata.convert_to_ptree(), metadata_retrieved.convert_to_ptree());
  }

  // remove index metadata by index id.
  IndexMetadataHelper::remove(indexes.get(), inserted_id);
}

/**
 * @brief Test that adds metadata for a new index and retrieves it using the
 * index id as the key with the ptree type.
 * @brief Test that adds metadata for a new index and retrieves it using the
 * index id as the key with the ptree type.
 * - add:
 *     ptree: patterns that obtain a index id.
 * - get:
 *     struct: index id as a key.
 *     ptree : index id as a key.
 * - remove:
 *     index id as a key.
 */
TEST_F(ApiTestIndexMetadata, add_get_index_metadata_ptree_object) {
  std::unique_ptr<UTIndexMetadata> testdata_index_metadata;

  // generate metadata.
  IndexMetadataHelper::generate_test_metadata(table_id,
                                              testdata_index_metadata);
  ptree new_metadata = testdata_index_metadata->indexes_metadata;
  // change to a unique index name.
  new_metadata.put(Index::NAME, new_metadata.get<std::string>(Index::NAME) +
                                    "_" + std::to_string(__LINE__));

  // generate index metadata manager.
  auto indexes    = get_index_metadata(GlobalTestEnvironment::TEST_DB);
  ErrorCode error = indexes->init();
  ASSERT_EQ(ErrorCode::OK, error);

  ObjectId inserted_id = INVALID_OBJECT_ID;
  // add index metadata.
  IndexMetadataHelper::add(indexes.get(), new_metadata, &inserted_id);
  // set index id.
  new_metadata.put(Index::ID, inserted_id);

  UTUtils::print("-- get index metadata in ptree --");
  {
    ptree get_index_metadata;
    // get index metadata by index id.
    error = indexes->get(inserted_id, get_index_metadata);
    EXPECT_EQ(ErrorCode::OK, error);

    UTUtils::print(UTUtils::get_tree_string(get_index_metadata));

    // verifies that the returned index metadata is expected one.
    IndexMetadataHelper::check_metadata_expected(new_metadata,
                                                 get_index_metadata);
  }

  UTUtils::print("-- get index metadata in struct --");
  {
    Index metadata_retrieved;
    // get index metadata by index id.
    error = indexes->get(inserted_id, metadata_retrieved);
    EXPECT_EQ(ErrorCode::OK, error);

    UTUtils::print(
        UTUtils::get_tree_string(metadata_retrieved.convert_to_ptree()));

    // verifies that the returned index metadata is expected one.
    IndexMetadataHelper::check_metadata_expected(
        new_metadata, metadata_retrieved.convert_to_ptree());
  }

  // remove index metadata by index id.
  IndexMetadataHelper::remove(indexes.get(), inserted_id);
}

}  // namespace manager::metadata::testing
