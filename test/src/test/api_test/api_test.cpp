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
#include "test/test/api_test.h"

#include <gtest/gtest.h>

#include "manager/metadata/metadata.h"
#include "test/common/global_test_environment.h"
#include "test/common/ut_utils.h"

namespace manager::metadata::testing {

using boost::property_tree::ptree;

/**
 * @brief This is a test of the basic paths of metadata management (add, get,
 *   remove).
 *   add: metadata is ptree type.
 *   exists: object ID as a key.
 *   get: object ID as a key.
 *   remove: object ID as a key.
 * @param ut_metadata  [in]  test metadata.
 */
void ApiTest::test_flow_get_by_id(
    const UtMetadataInterface* ut_metadata) const {
  CALL_TRACE;
  ASSERT_TRUE(managers_);

  // Test to initialize the manager.
  ASSERT_NO_FATAL_FAILURE(this->test_init(managers_.get(), ErrorCode::OK));

  auto inserted_metadata = ut_metadata->get_metadata_ptree();
  ObjectId new_object_id = INVALID_OBJECT_ID;
  // Add metadata.
  {
    CALL_TRACE;
    // Test to add metadata.
    EXPECT_NO_FATAL_FAILURE({
      new_object_id =
          this->test_add(managers_.get(), inserted_metadata, ErrorCode::OK);
    });
    // If there is an error in the test, it terminates.
    if (::testing::Test::HasFailure()) {
      FAIL();
    }

    // Set object ID.
    inserted_metadata.put(Object::ID, new_object_id);
  }

  {
    CALL_TRACE;
    // Test to exists metadata.
    this->test_exists(managers_.get(), new_object_id, true);
  }

  ptree retrieved_ptree;
  // Get metadata with ptree.
  {
    CALL_TRACE;
    // Test to get metadata with ptree.
    this->test_get(managers_.get(), new_object_id, ErrorCode::OK,
                   retrieved_ptree);
    // Verifies that the returned metadata is expected one.
    ut_metadata->CHECK_METADATA_EXPECTED(inserted_metadata, retrieved_ptree);
  }

  auto retrieved_struct = this->get_structure();
  // Get metadata with structure.
  if (retrieved_struct != nullptr) {
    CALL_TRACE;
    // Test to get metadata with structure.
    this->test_get(managers_.get(), new_object_id, ErrorCode::OK,
                   *retrieved_struct);
    // Verifies that the returned metadata is expected one.
    ut_metadata->CHECK_METADATA_EXPECTED(inserted_metadata, *retrieved_struct);
  }

  {
    CALL_TRACE;
    // Test to remove metadata.
    this->test_remove(managers_.get(), new_object_id, ErrorCode::OK);
  }

  {
    CALL_TRACE;
    // Test to see if data has been removed.
    this->test_exists(managers_.get(), new_object_id, false);
    this->test_get(managers_.get(), new_object_id, ErrorCode::ID_NOT_FOUND,
                   retrieved_ptree);
  }
}

/**
 * @brief This is a test of the basic paths of metadata management (add, get,
 *   remove).
 *   add: metadata is structure type.
 *   exists: object ID as a key.
 *   get: object ID as a key.
 *   remove: object ID as a key.
 * @param ut_metadata  [in]  test metadata.
 */
void ApiTest::test_flow_get_by_id_with_struct(
    const UtMetadataInterface* ut_metadata) const {
  CALL_TRACE;
  ASSERT_TRUE(managers_);

  // Test to initialize the manager.
  ASSERT_NO_FATAL_FAILURE(this->test_init(managers_.get(), ErrorCode::OK));

  auto inserted_metadata = ut_metadata->get_metadata_ptree();
  auto inserted_metadata_struct =
      const_cast<Object*>(ut_metadata->get_metadata_struct());
  ObjectId new_object_id = INVALID_OBJECT_ID;
  // Add metadata.
  {
    CALL_TRACE;
    // Test to add metadata with structure.
    EXPECT_NO_FATAL_FAILURE({
      new_object_id = this->test_add(managers_.get(), *inserted_metadata_struct,
                                     ErrorCode::OK);
    });
    // If there is an error in the test, it terminates.
    if (::testing::Test::HasFailure()) {
      FAIL();
    }

    // Set object ID.
    inserted_metadata.put(Object::ID, new_object_id);
  }

  {
    CALL_TRACE;
    // Test to exists metadata.
    this->test_exists(managers_.get(), new_object_id, true);
  }

  ptree retrieved_ptree;
  // Get metadata with ptree.
  {
    CALL_TRACE;
    // Test to get metadata with ptree.
    this->test_get(managers_.get(), new_object_id, ErrorCode::OK,
                   retrieved_ptree);
    // Verifies that the returned metadata is expected one.
    ut_metadata->CHECK_METADATA_EXPECTED(inserted_metadata, retrieved_ptree);
  }

  auto retrieved_struct = this->get_structure();
  // Get metadata with structure.
  if (retrieved_struct != nullptr) {
    CALL_TRACE;
    // Test to get metadata with structure.
    this->test_get(managers_.get(), new_object_id, ErrorCode::OK,
                   *retrieved_struct);
    // Verifies that the returned metadata is expected one.
    ut_metadata->CHECK_METADATA_EXPECTED(inserted_metadata, *retrieved_struct);
  }

  {
    CALL_TRACE;
    // Test to remove metadata.
    this->test_remove(managers_.get(), new_object_id, ErrorCode::OK);
  }

  {
    CALL_TRACE;
    // Test to see if data has been removed.
    this->test_exists(managers_.get(), new_object_id, false);
    this->test_get(managers_.get(), new_object_id, ErrorCode::ID_NOT_FOUND,
                   retrieved_ptree);
  }
}

/**
 * @brief This is a test of the basic paths of metadata management (add, get,
 *   remove).
 *   add: metadata is ptree type.
 *   exists: object name as a key.
 *   get: object name as a key.
 *   remove: object name as a key.
 * @param ut_metadata  [in]  test metadata.
 */
void ApiTest::test_flow_get_by_name(
    const UtMetadataInterface* ut_metadata) const {
  CALL_TRACE;
  ASSERT_TRUE(managers_);

  // Test to initialize the manager.
  ASSERT_NO_FATAL_FAILURE(this->test_init(managers_.get(), ErrorCode::OK));

  auto inserted_metadata = ut_metadata->get_metadata_ptree();
  auto object_name       = ut_metadata->get_metadata_struct()->name;

  ObjectId new_object_id = INVALID_OBJECT_ID;
  // Add metadata.
  {
    CALL_TRACE;
    // Test to add metadata.
    EXPECT_NO_FATAL_FAILURE({
      new_object_id =
          this->test_add(managers_.get(), inserted_metadata, ErrorCode::OK);
    });
    // If there is an error in the test, it terminates.
    if (::testing::Test::HasFailure()) {
      FAIL();
    }

    // Set object ID.
    inserted_metadata.put(Object::ID, new_object_id);
  }

  {
    CALL_TRACE;
    // Test to exists metadata.
    this->test_exists(managers_.get(), object_name, true);
  }

  ptree retrieved_ptree;
  // Get metadata with ptree.
  {
    CALL_TRACE;
    // Test to get metadata with ptree.
    this->test_get(managers_.get(), object_name, ErrorCode::OK,
                   retrieved_ptree);
    // Verifies that the returned metadata is expected one.
    ut_metadata->CHECK_METADATA_EXPECTED(inserted_metadata, retrieved_ptree);
  }

  auto retrieved_struct = this->get_structure();
  // Get metadata with structure.
  if (retrieved_struct != nullptr) {
    CALL_TRACE;
    // Test to get metadata with structure.
    this->test_get(managers_.get(), object_name, ErrorCode::OK,
                   *retrieved_struct);
    // Verifies that the returned metadata is expected one.
    ut_metadata->CHECK_METADATA_EXPECTED(inserted_metadata, *retrieved_struct);
  }

  {
    CALL_TRACE;
    // Test to remove metadata.
    this->test_remove(managers_.get(), object_name, ErrorCode::OK);
  }

  {
    CALL_TRACE;
    // Test to see if data has been removed.
    this->test_exists(managers_.get(), object_name, false);
    this->test_get(managers_.get(), object_name, ErrorCode::NAME_NOT_FOUND,
                   retrieved_ptree);
  }
}

/**
 * @brief This is a test of the basic paths of metadata management (add, get,
 *   remove).
 *   add: metadata is structure type.
 *   exists: object name as a key.
 *   get: object name as a key.
 *   remove: object name as a key.
 * @param ut_metadata  [in]  test metadata.
 */
void ApiTest::test_flow_get_by_name_with_struct(
    const UtMetadataInterface* ut_metadata) const {
  CALL_TRACE;
  ASSERT_TRUE(managers_);

  // Test to initialize the manager.
  ASSERT_NO_FATAL_FAILURE(this->test_init(managers_.get(), ErrorCode::OK));

  auto inserted_metadata = ut_metadata->get_metadata_ptree();
  auto inserted_metadata_struct =
      const_cast<Object*>(ut_metadata->get_metadata_struct());
  auto object_name = inserted_metadata_struct->name;

  ObjectId new_object_id = INVALID_OBJECT_ID;
  // Add metadata.
  {
    CALL_TRACE;
    // Test to add metadata with structure.
    EXPECT_NO_FATAL_FAILURE({
      new_object_id = this->test_add(managers_.get(), *inserted_metadata_struct,
                                     ErrorCode::OK);
    });
    // If there is an error in the test, it terminates.
    if (::testing::Test::HasFailure()) {
      FAIL();
    }

    // Set object ID.
    inserted_metadata.put(Object::ID, new_object_id);
  }

  {
    CALL_TRACE;
    // Test to exists metadata.
    this->test_exists(managers_.get(), object_name, true);
  }

  ptree retrieved_ptree;
  // Get metadata with ptree.
  {
    CALL_TRACE;
    // Test to get metadata with ptree.
    this->test_get(managers_.get(), object_name, ErrorCode::OK,
                   retrieved_ptree);
    // Verifies that the returned metadata is expected one.
    ut_metadata->CHECK_METADATA_EXPECTED(inserted_metadata, retrieved_ptree);
  }

  auto retrieved_struct = this->get_structure();
  // Get metadata with structure.
  if (retrieved_struct != nullptr) {
    CALL_TRACE;
    // Test to get metadata with structure.
    this->test_get(managers_.get(), object_name, ErrorCode::OK,
                   *retrieved_struct);
    // Verifies that the returned metadata is expected one.
    ut_metadata->CHECK_METADATA_EXPECTED(inserted_metadata, *retrieved_struct);
  }

  {
    CALL_TRACE;
    // Test to remove metadata.
    this->test_remove(managers_.get(), object_name, ErrorCode::OK);
  }

  {
    CALL_TRACE;
    // Test to see if data has been removed.
    this->test_exists(managers_.get(), object_name, false);
    this->test_get(managers_.get(), object_name, ErrorCode::NAME_NOT_FOUND,
                   retrieved_ptree);
  }
}

/**
 * @brief This is a test of the basic paths of metadata management (add,
 *   get_all, remove).
 *   add: metadata is ptree type.
 *   get_all: all object.
 *   remove: object ID as a key.
 * @param ut_metadata          [in]  test metadata.
 * @param unique_data_creator  [in]  callback function to create unique data.
 * @param create_data_max      [in]  number of data to be created.
 */
void ApiTest::test_flow_getall(const UtMetadataInterface* ut_metadata,
                               UniqueDataCreator unique_data_creator,
                               const int32_t create_data_max) const {
  CALL_TRACE;
  ASSERT_TRUE(managers_);

  // Get the current number of records.
  auto base_record_count = this->get_record_count();

  {
    CALL_TRACE;
    // Test to initialize the manager.
    ASSERT_NO_FATAL_FAILURE(this->test_init(managers_.get(), ErrorCode::OK));
  }

  std::vector<ptree> metadata_container{};
  {
    CALL_TRACE;
    // Testing in the pre-addition state.
    this->test_getall(managers_.get(), ErrorCode::OK, metadata_container);
    EXPECT_EQ(base_record_count, metadata_container.size());
  }

  std::vector<ptree> test_metadata_list{};
  // Generate test metadata.
  {
    CALL_TRACE;
    EXPECT_NO_FATAL_FAILURE({
      test_metadata_list =
          this->metadata_add(managers_.get(), ut_metadata->get_metadata_ptree(),
                             unique_data_creator, create_data_max);
    });
    // If there is an error in the test, it terminates.
    if (::testing::Test::HasFailure()) {
      FAIL();
    }
  }

  // Get metadata with structure.
  {
    CALL_TRACE;
    // Test to all get metadata with structure.
    this->test_getall(managers_.get(), ErrorCode::OK, metadata_container);

    int32_t expect_count = base_record_count + create_data_max;
    int32_t actual_count = metadata_container.size();
    ASSERT_EQ(expect_count, actual_count);

    // Inspect the returned metadata.
    this->metadata_compare_all(ut_metadata, test_metadata_list,
                               metadata_container);
  }

  // Remove metadata.
  this->metadata_remove(managers_.get(), test_metadata_list);
}

/**
 * @brief This is a test of the basic paths of metadata management (add,
 *   get_all, remove).
 *   add: metadata is ptree type.
 *   get_all: all object.
 *   next: all object with ptree type.
 *   remove: object ID as a key.
 * @param ut_metadata          [in]  test metadata.
 * @param unique_data_creator  [in]  callback function to create unique data.
 * @param create_data_max      [in]  number of data to be created.
 */
void ApiTest::test_flow_getall_next(const UtMetadataInterface* ut_metadata,
                                    UniqueDataCreator unique_data_creator,
                                    const int32_t create_data_max) const {
  CALL_TRACE;
  ASSERT_TRUE(managers_);

  // Get the current number of records.
  auto base_record_count = this->get_record_count();

  {
    CALL_TRACE;
    // Test to initialize the manager.
    ASSERT_NO_FATAL_FAILURE(this->test_init(managers_.get(), ErrorCode::OK));
  }

  std::vector<ptree> test_metadata_list{};
  // Generate test metadata.
  {
    CALL_TRACE;
    EXPECT_NO_FATAL_FAILURE({
      test_metadata_list =
          this->metadata_add(managers_.get(), ut_metadata->get_metadata_ptree(),
                             unique_data_creator, create_data_max);
    });
    // If there is an error in the test, it terminates.
    if (::testing::Test::HasFailure()) {
      FAIL();
    }
  }

  // Get metadata with structure.
  {
    CALL_TRACE;

    std::vector<ptree> metadata_container{};
    // Test to all get metadata with structure.
    this->test_getall_next(managers_.get(), ErrorCode::OK, metadata_container);

    int32_t expect_count = base_record_count + test_metadata_list.size();
    int32_t actual_count = metadata_container.size();
    ASSERT_EQ(expect_count, actual_count);

    // Inspect the returned metadata.
    this->metadata_compare_all(ut_metadata, test_metadata_list,
                               metadata_container);
  }

  // Remove metadata.
  this->metadata_remove(managers_.get(), test_metadata_list);
}

/**
 * @brief This is a test of the basic paths of metadata management (add, get,
 *   update, remove).
 *   add: metadata is ptree type.
 *   get: object ID as a key.
 *   update: object ID as a key.
 *   remove: object ID as a key.
 * @param ut_metadata          [in]  test metadata.
 * @param update_data_creator  [in]  callback function to create update test
 * metadata.
 */
void ApiTest::test_flow_update(const UtMetadataInterface* ut_metadata,
                               UpdateDataCreator update_data_creator) const {
  CALL_TRACE;
  ASSERT_TRUE(managers_);

  // Test to initialize the manager.
  ASSERT_NO_FATAL_FAILURE(this->test_init(managers_.get(), ErrorCode::OK));

  auto inserted_metadata = ut_metadata->get_metadata_ptree();
  ObjectId new_object_id = INVALID_OBJECT_ID;
  // Add metadata.
  {
    CALL_TRACE;
    EXPECT_NO_FATAL_FAILURE({
      // Test to add metadata.
      new_object_id =
          this->test_add(managers_.get(), inserted_metadata, ErrorCode::OK);
    });
    // If there is an error in the test, it terminates.
    if (::testing::Test::HasFailure()) {
      FAIL();
    }
    // Set object ID.
    inserted_metadata.put(Object::ID, new_object_id);
  }

  ptree before_ptree;
  // Get metadata with ptree.
  {
    CALL_TRACE;
    EXPECT_NO_FATAL_FAILURE({
      // Test to get metadata with ptree.
      this->test_get(managers_.get(), new_object_id, ErrorCode::OK,
                     before_ptree);

      // Verifies that the returned metadata is expected one.
      ut_metadata->CHECK_METADATA_EXPECTED(inserted_metadata, before_ptree);
    });
    // If there is an error in the test, it terminates.
    if (::testing::Test::HasFailure()) {
      FAIL();
    }
  }

  // Generate data for updating.
  auto ut_metadata_update = std::move(update_data_creator(before_ptree));

  auto updated_metadata = ut_metadata_update->get_metadata_ptree();
  // Update metadata.
  {
    CALL_TRACE;
    // Set object ID.
    updated_metadata.put(Object::ID, new_object_id);

    // Test to update metadata.
    this->test_update(managers_.get(), new_object_id, updated_metadata,
                      ErrorCode::OK);
  }

  ptree after_ptree;
  // Get metadata with ptree.
  {
    CALL_TRACE;
    // Test to get metadata with ptree.
    this->test_get(managers_.get(), new_object_id, ErrorCode::OK, after_ptree);
    // Verifies that the returned metadata is expected one.
    ut_metadata->CHECK_METADATA_EXPECTED(updated_metadata, after_ptree);
  }

  {
    CALL_TRACE;
    // Test to remove metadata.
    this->test_remove(managers_.get(), new_object_id, ErrorCode::OK);
  }
}

/**
 * @brief This is a test to init metadata.
 * @param metadata_manager  [in]  metadata management class object.
 * @param expect_code       [in]  expected result code.
 */
void ApiTest::test_init(const Metadata* metadata_manager,
                        ErrorCode expect_code) const {
  const auto managers =
      (metadata_manager == nullptr ? managers_.get() : metadata_manager);

  UTUtils::print("-- init test metadata --");

  ErrorCode error = managers->init();
  ASSERT_EQ(expect_code, error);
}

/**
 * @brief This is a test to add metadata.
 * @param metadata_manager  [in]  metadata management class object.
 * @param metadata_object   [in]  metadata object.
 * @param expect_code       [in]  expected result code.
 * @return ObjectId - added object ID.
 */
ObjectId ApiTest::test_add(const Metadata* metadata_manager,
                           boost::property_tree::ptree& metadata_object,
                           ErrorCode expect_code) const {
  UTUtils::print("-- add test metadata with ptree --");
  return metadata_add(metadata_manager, metadata_object, expect_code);
}

/**
 * @brief This is a test to add metadata.
 * @param metadata_manager  [in]  metadata management class object.
 * @param metadata_object   [in]  metadata object.
 * @param expect_code       [in]  expected result code.
 * @return ObjectId - added object ID.
 */
ObjectId ApiTest::test_add(const Metadata* metadata_manager,
                           Object& metadata_object,
                           ErrorCode expect_code) const {
  UTUtils::print("-- add test metadata with structure --");
  return metadata_add(metadata_manager, metadata_object, expect_code);
}

/**
 * @brief This is a test to get metadata.
 * @param metadata_manager  [in]  metadata management class object.
 * @param object_id         [in]  object ID of the metadata to be retrieved.
 * @param metadata_object   [out] retrieved metadata object.
 * @param expect_code       [in]  expected result code.
 * @return boost::property_tree::ptree - retrieved metadata.
 */
void ApiTest::test_get(const Metadata* metadata_manager, ObjectId object_id,
                       ErrorCode expect_code,
                       boost::property_tree::ptree& metadata_object) const {
  UTUtils::print("-- get test metadata by object ID with ptree --");
  metadata_get(metadata_manager, object_id, expect_code, metadata_object);
}

/**
 * @brief This is a test to get metadata.
 * @param metadata_manager  [in]  metadata management class object.
 * @param object_id         [in]  object ID of the metadata to be retrieved.
 * @param expect_code       [in]  expected result code.
 * @param metadata_object   [out] retrieved metadata object.
 */
void ApiTest::test_get(const Metadata* metadata_manager, ObjectId object_id,
                       ErrorCode expect_code, Object& metadata_object) const {
  UTUtils::print("-- get test metadata by object ID with structure --");
  metadata_get(metadata_manager, object_id, expect_code, metadata_object);
}

/**
 * @brief This is a test to get metadata.
 * @param metadata_manager  [in]  metadata management class object.
 * @param object_name       [in]  object name of the metadata to be retrieved.
 * @param expect_code       [in]  expected result code.
 * @param metadata_object   [out] retrieved metadata object.
 */
void ApiTest::test_get(const Metadata* metadata_manager,
                       std::string_view object_name, ErrorCode expect_code,
                       boost::property_tree::ptree& metadata_object) const {
  UTUtils::print("-- get test metadata by object name with ptree --");
  metadata_get(metadata_manager, object_name, expect_code, metadata_object);
}

/**
 * @brief This is a test to get metadata.
 * @param metadata_manager  [in]  metadata management class object.
 * @param object_name       [in]  object name of the metadata to be retrieved.
 * @param expect_code       [in]  expected result code.
 * @param metadata_object   [out] retrieved metadata object.
 */
void ApiTest::test_get(const Metadata* metadata_manager,
                       std::string_view object_name, ErrorCode expect_code,
                       Object& metadata_object) const {
  UTUtils::print("-- get test metadata by object name with structure --");
  metadata_get(metadata_manager, object_name, expect_code, metadata_object);
}

/**
 * @brief This is a test to get metadata.
 * @param metadata_manager  [in]  metadata management class object.
 * @param expect_code       [in]  expected result code.
 * @param container         [out] retrieved metadata.
 */
void ApiTest::test_getall(
    const Metadata* metadata_manager, ErrorCode expect_code,
    std::vector<boost::property_tree::ptree>& container) const {
  const auto managers =
      (metadata_manager == nullptr ? managers_.get() : metadata_manager);
  assert(managers != nullptr);

  UTUtils::print("-- get_all test metadata with ptree --");

  container.clear();
  // get metadata by object ID.
  ErrorCode actual = managers->get_all(container);
  EXPECT_EQ(expect_code, actual);

  UTUtils::print(" >> Count: ", container.size());
  for (auto& metadata : container) {
    UTUtils::print(" " + UTUtils::get_tree_string(metadata));
  }
}

/**
 * @brief This is a test to get metadata.
 * @param managers     [in]  metadata management class object.
 * @param expect_code  [in]  expected result code.
 * @param container    [out] retrieved metadata.
 */
void ApiTest::test_getall_next(const Metadata* metadata_manager,
                               ErrorCode expect_code,
                               std::vector<ptree>& container) const {
  manager::metadata::Metadata* managers =
      const_cast<manager::metadata::Metadata*>(
          metadata_manager == nullptr ? managers_.get() : metadata_manager);
  assert(managers != nullptr);

  UTUtils::print("-- get_all-next test metadata with ptree --");

  container.clear();

  // get all metadata.
  ErrorCode actual_code = managers->get_all();
  EXPECT_EQ(expect_code, actual_code);

  ptree metadata_object;
  while ((actual_code = managers->next(metadata_object)) == ErrorCode::OK) {
    container.push_back(metadata_object);

    UTUtils::print(" >> Next: ", (container.size()));
    UTUtils::print("  " + UTUtils::get_tree_string(metadata_object));
  }
  ASSERT_EQ(ErrorCode::END_OF_ROW, actual_code);
}

/**
 * @brief This is a test to exists metadata.
 * @param metadata_manager  [in]  metadata management class object.
 * @param object_id         [in]  object ID of the metadata to be retrieved.
 * @param expected          [in]  expected result.
 */
void ApiTest::test_exists(const Metadata* metadata_manager, ObjectId object_id,
                          bool expected) const {
  const auto managers =
      (metadata_manager == nullptr ? managers_.get() : metadata_manager);

  UTUtils::print("-- exists test metadata --");

  bool actual = managers->exists(object_id);
  EXPECT_EQ(expected, actual);
}

/**
 * @brief This is a test to exists metadata.
 * @param metadata_manager  [in]  metadata management class object.
 * @param object_name       [in]  object name of the metadata to be retrieved.
 * @param expected          [in]  expected result.
 */
void ApiTest::test_exists(const Metadata* metadata_manager,
                          std::string_view object_name, bool expected) const {
  const auto managers =
      (metadata_manager == nullptr ? managers_.get() : metadata_manager);

  UTUtils::print("-- exists test metadata --");

  bool actual = managers->exists(object_name);
  EXPECT_EQ(expected, actual);
}

/**
 * @brief This is a test to update metadata.
 * @param metadata_manager  [in]  metadata management class object.
 * @param object_id         [in]  object ID of the metadata to be retrieved.
 * @param metadata_object   [in]  metadata object.
 * @param expect_code       [in]  expected result code.
 */
void ApiTest::test_update(const Metadata* metadata_manager, ObjectId object_id,
                          boost::property_tree::ptree& metadata_object,
                          ErrorCode expect_code) const {
  UTUtils::print("-- update test metadata by object id with ptree --");
  metadata_update(metadata_manager, object_id, metadata_object, expect_code);
}

#if 0
/**
 * @brief This is a test to update metadata.
 * @param metadata_manager  [in]  metadata management class object.
 * @param object_id         [in]  object ID of the metadata to be retrieved.
 * @param metadata_object   [in]  metadata object.
 * @param expect_code       [in]  expected result code.
 */
void ApiTest::test_update(const Metadata* metadata_manager, ObjectId object_id,
                           Object& metadata_object,
                           ErrorCode expect_code) const {
  UTUtils::print("-- update test metadata by object id with structure --");
  metadata_update(metadata_manager, object_id, metadata_object, expect_code);
}
#endif

/**
 * @brief This is a test to remove metadata.
 * @param metadata_manager  [in]  metadata management class object.
 * @param object_id         [in]  object ID of the metadata to be removed.
 * @param expect_code       [in]  expected result code.
 */
void ApiTest::test_remove(const Metadata* metadata_manager, ObjectId object_id,
                          ErrorCode expect_code) const {
  const auto managers =
      (metadata_manager == nullptr ? managers_.get() : metadata_manager);
  assert(managers != nullptr);

  UTUtils::print("-- remove test metadata by object ID --");
  UTUtils::print(" >> object ID: ", object_id);

  // remove metadata by object ID.
  ErrorCode actual = managers->remove(object_id);
  ASSERT_EQ(expect_code, actual);
}

/**
 * @brief This is a test to remove metadata.
 * @param metadata_manager  [in]  metadata management class object.
 * @param object_name       [in]  object name of the metadata to be removed.
 * @param expect_code       [in]  expected result code.
 */
void ApiTest::test_remove(const Metadata* metadata_manager,
                          std::string_view object_name,
                          ErrorCode expect_code) const {
  const auto managers =
      (metadata_manager == nullptr ? managers_.get() : metadata_manager);
  assert(managers != nullptr);

  UTUtils::print("-- remove test metadata by object name --");
  UTUtils::print(" >> object name: ", object_name);

  ObjectIdType object_id = INVALID_VALUE;
  // remove metadata by object name.
  ErrorCode actual = managers->remove(object_name, &object_id);
  ASSERT_EQ(expect_code, actual);
  if (expect_code == ErrorCode::OK) {
    ASSERT_GT(object_id, 0);
  } else {
    ASSERT_EQ(object_id, INVALID_VALUE);
  }

  UTUtils::print(" object ID: ", object_id);
}

/**
 * @brief This is a test to add metadata.
 * @param metadata_manager  [in]  metadata management class object.
 * @param metadata_object   [in]  metadata object.
 * @param expect_code       [in]  expected result code.
 * @return ObjectId - added object ID.
 */
template <typename T>
ObjectId ApiTest::metadata_add(const Metadata* metadata_manager,
                               T& metadata_object,
                               ErrorCode expect_code) const {
  const auto managers =
      (metadata_manager == nullptr ? managers_.get() : metadata_manager);
  assert(managers != nullptr);

  UTUtils::print(" " + UTUtils::get_tree_string(metadata_object));

  ObjectId object_id = INVALID_OBJECT_ID;

  // add metadata.
  ErrorCode actual = managers->add(metadata_object, &object_id);
  EXPECT_EQ(expect_code, actual);
  if (expect_code == ErrorCode::OK) {
    EXPECT_GT(object_id, 0);
  } else {
    EXPECT_EQ(object_id, INVALID_OBJECT_ID);
  }

  // If there is an error in the test, it terminates.
  if (!::testing::Test::HasFailure()) {
    UTUtils::print(" >> new object ID: ", object_id);
  }

  return object_id;
}

/**
 * @brief Adds the specified number of pieces of metadata.
 * @param metadata_manager     [in]  metadata management class object.
 * @param metadata             [in]  test metadata.
 * @param unique_data_creator  [in]  callback function to create unique data.
 * @param create_data_max      [in]  number of data to be created.
 */
std::vector<ptree> ApiTest::metadata_add(const Metadata* metadata_manager,
                                         const ptree& metadata,
                                         UniqueDataCreator unique_data_creator,
                                         const int32_t create_data_max) const {
  // Generate test metadata.
  std::vector<ptree> test_metadata_list{};
  for (int32_t num = 1; num <= create_data_max; num++) {
    auto added_metadata = metadata;
    // Creates unique data using the specified callback function.
    unique_data_creator(added_metadata, num);
    test_metadata_list.push_back(added_metadata);
  }

  // Add metadata.
  {
    CALL_TRACE;
    for (auto& metadata : test_metadata_list) {
      ObjectId object_id = INVALID_OBJECT_ID;
      // Test to add metadata.
      EXPECT_NO_FATAL_FAILURE({
        object_id =
            this->metadata_add(metadata_manager, metadata, ErrorCode::OK);
      });

      // If there is an error in the test, it terminates.
      if (::testing::Test::HasFailure()) {
        break;
      }

      // Set object ID.
      metadata.put(Object::ID, object_id);
    }
  }
  return (test_metadata_list);
}

/**
 * @brief This is a test to get metadata.
 * @param metadata_manager  [in]  metadata management class object.
 * @param object_key        [in]  object ID/name of the metadata to be
     retrieved.
 * @param expect_code       [in]  expected result code.
 * @param metadata_object   [out] retrieved metadata object.
 * @return boost::property_tree::ptree - retrieved metadata.
 */
template <typename KEY, typename T>
void ApiTest::metadata_get(const Metadata* metadata_manager, KEY object_key,
                           ErrorCode expect_code, T& metadata_object) const {
  const auto managers =
      (metadata_manager == nullptr ? managers_.get() : metadata_manager);
  assert(managers != nullptr);

  UTUtils::print(" >> object key: ", object_key);

  // get metadata by object ID.
  ErrorCode actual = managers->get(object_key, metadata_object);
  EXPECT_EQ(expect_code, actual);

  // If there is an error in the test, it terminates.
  if (!::testing::Test::HasFailure()) {
    if (expect_code == ErrorCode::OK) {
      UTUtils::print(" " + UTUtils::get_tree_string(metadata_object));
    } else {
      UTUtils::print(" Does not exist.");
    }
  }
}

/**
 * @brief This is a test to update metadata.
 * @param metadata_manager  [in]  metadata management class object.
 * @param object_key        [in]  object ID/name of the metadata to be
 retrieved.
 * @param metadata_object   [in]  metadata object.
 * @param expect_code       [in]  expected result code.
 */
template <typename KEY, typename T>
void ApiTest::metadata_update(const Metadata* metadata_manager, KEY object_key,
                              T& metadata_object, ErrorCode expect_code) const {
  const auto managers =
      (metadata_manager == nullptr ? managers_.get() : metadata_manager);
  assert(managers != nullptr);

  UTUtils::print(" >> object key: ", object_key);
  UTUtils::print(" " + UTUtils::get_tree_string(metadata_object));

  // add metadata.
  ErrorCode actual = managers->update(object_key, metadata_object);
  EXPECT_EQ(expect_code, actual);
}

/**
 * @brief Removes the metadata.
 * @param metadata_manager  [in]  metadata management class object.
 * @param metadata_list     [in]  list of metadata.
 */
void ApiTest::metadata_remove(const Metadata* metadata_manager,
                              const std::vector<ptree>& metadata_list) const {
  const auto managers =
      (metadata_manager == nullptr ? managers_.get() : metadata_manager);
  assert(managers != nullptr);

  UTUtils::print("-- remove test metadata by object ID --");
  for (auto& metadata : metadata_list) {
    auto object_id = metadata.get<ObjectId>(Object::ID);
    UTUtils::print(" >> object ID: ", object_id);

    // remove metadata by object ID.
    managers->remove(object_id);
  }
}

/**
 * @brief Compare all metadata.
 * @param ut_metadata           [in]  test metadata.
 * @param expect_metadata_list  [in]  expected metadata.
 * @param actual_metadata_list  [in]  actual metadata.
 */
void ApiTest::metadata_compare_all(
    const UtMetadataInterface* ut_metadata,
    const std::vector<ptree>& expect_metadata_list,
    const std::vector<ptree>& actual_metadata_list) const {
  CALL_TRACE;

  // Inspect the returned metadata.
  for (auto& expect_metadata : expect_metadata_list) {
    // Extract object ID.
    auto expect_id = expect_metadata.get<ObjectId>(Object::ID);

    for (auto& actual_metadata : actual_metadata_list) {
      // Extract object ID.
      auto actual_id = actual_metadata.get<ObjectId>(Object::ID);
      if (expect_id == actual_id) {
        // Verifies that the returned metadata is expected one.
        ut_metadata->CHECK_METADATA_EXPECTED(expect_metadata, actual_metadata);
        break;
      }
    }
  }
}
}  // namespace manager::metadata::testing
