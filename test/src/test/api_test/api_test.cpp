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
 *   add: patterns that obtain a object ID.
 *   get: object ID as a key.
 *   remove: object ID as a key.
 * @param ut_metadata  [in]  test metadata.
 */
void ApiTest::test_flow_get_by_id(const UtMetadata* ut_metadata) const {
  SCOPED_TRACE("");
  ASSERT_TRUE(managers_);

  // Test to initialize the manager.
  ASSERT_NO_FATAL_FAILURE(this->test_init(managers_.get()));

  auto inserted_metadata = ut_metadata->get_metadata_ptree();
  ObjectId new_object_id = INVALID_OBJECT_ID;
  // Add metadata.
  {
    // Test to add metadata.
    EXPECT_NO_FATAL_FAILURE({
      new_object_id = this->test_add(managers_.get(), inserted_metadata);
    });
    // If there is an error in the test, it terminates.
    if (::testing::Test::HasFailure()) {
      FAIL();
    }

    // Set object ID.
    inserted_metadata.put(Object::ID, new_object_id);
  }

  ptree retrieved_ptree;
  // Get metadata with ptree.
  {
    // Test to get metadata with ptree.
    this->test_get(managers_.get(), new_object_id, retrieved_ptree);
    // Verifies that the returned metadata is expected one.
    ut_metadata->CHECK_METADATA_EXPECTED(inserted_metadata, retrieved_ptree);
  }

  auto retrieved_struct = this->get_structure();
  // Get metadata with structure.
  {
    // Test to get metadata with structure.
    this->test_get(managers_.get(), new_object_id, *retrieved_struct);
    // Verifies that the returned metadata is expected one.
    ut_metadata->CHECK_METADATA_EXPECTED(inserted_metadata, *retrieved_struct);
  }

  // Test to remove metadata.
  this->test_remove(managers_.get(), new_object_id);

  // Test whether metadata has been removed.
  this->test_get(managers_.get(), new_object_id, retrieved_ptree,
                 ErrorCode::ID_NOT_FOUND);
}

/**
 * @brief This is a test of the basic paths of metadata management (add, get,
 *   remove).
 *   add: patterns that obtain a object ID.
 *   get: object ID as a key.
 *   remove: object ID as a key.
 * @param ut_metadata  [in]  test metadata.
 */
void ApiTest::test_flow_get_by_id_with_struct(
    const UtMetadata* ut_metadata) const {
  SCOPED_TRACE("");
  ASSERT_TRUE(managers_);

  // Test to initialize the manager.
  ASSERT_NO_FATAL_FAILURE(this->test_init(managers_.get()));

  auto inserted_metadata = ut_metadata->get_metadata_ptree();
  auto inserted_metadata_struct =
      const_cast<Object*>(ut_metadata->get_metadata_struct());
  ObjectId new_object_id = INVALID_OBJECT_ID;
  // Add metadata.
  {
    // Test to add metadata with structure.
    EXPECT_NO_FATAL_FAILURE({
      new_object_id =
          this->test_add(managers_.get(), *inserted_metadata_struct);
    });
    // If there is an error in the test, it terminates.
    if (::testing::Test::HasFailure()) {
      FAIL();
    }

    // Set object ID.
    inserted_metadata.put(Object::ID, new_object_id);
  }

  ptree retrieved_ptree;
  // Get metadata with ptree.
  {
    // Test to get metadata with ptree.
    this->test_get(managers_.get(), new_object_id, retrieved_ptree);
    // Verifies that the returned metadata is expected one.
    ut_metadata->CHECK_METADATA_EXPECTED(inserted_metadata, retrieved_ptree);
  }

  auto retrieved_struct = this->get_structure();
  // Get metadata with structure.
  {
    // Test to get metadata with structure.
    this->test_get(managers_.get(), new_object_id, *retrieved_struct);
    // Verifies that the returned metadata is expected one.
    ut_metadata->CHECK_METADATA_EXPECTED(inserted_metadata, *retrieved_struct);
  }

  // Test to remove metadata.
  this->test_remove(managers_.get(), new_object_id);

  // Test whether metadata has been removed.
  this->test_get(managers_.get(), new_object_id, retrieved_ptree,
                 ErrorCode::ID_NOT_FOUND);
}

/**
 * @brief This is a test of the basic paths of metadata management (add, get,
 *   remove).
 *   add: patterns that obtain a object ID.
 *   get: object name as a key.
 *   remove: object name as a key.
 * @param ut_metadata  [in]  test metadata.
 */
void ApiTest::test_flow_get_by_name(const UtMetadata* ut_metadata) const {
  SCOPED_TRACE("");
  ASSERT_TRUE(managers_);

  // Test to initialize the manager.
  ASSERT_NO_FATAL_FAILURE(this->test_init(managers_.get()));

  auto inserted_metadata = ut_metadata->get_metadata_ptree();
  auto object_name       = ut_metadata->get_metadata_struct()->name;

  ObjectId new_object_id = INVALID_OBJECT_ID;
  // Add metadata.
  {
    // Test to add metadata.
    EXPECT_NO_FATAL_FAILURE({
      new_object_id = this->test_add(managers_.get(), inserted_metadata);
    });
    // If there is an error in the test, it terminates.
    if (::testing::Test::HasFailure()) {
      FAIL();
    }

    // Set object ID.
    inserted_metadata.put(Object::ID, new_object_id);
  }

  ptree retrieved_ptree;
  // Get metadata with ptree.
  {
    // Test to get metadata with ptree.
    this->test_get(managers_.get(), object_name, retrieved_ptree);
    // Verifies that the returned metadata is expected one.
    ut_metadata->CHECK_METADATA_EXPECTED(inserted_metadata, retrieved_ptree);
  }

  auto retrieved_struct = this->get_structure();
  // Get metadata with structure.
  {
    // Test to get metadata with structure.
    this->test_get(managers_.get(), object_name, *retrieved_struct);
    // Verifies that the returned metadata is expected one.
    ut_metadata->CHECK_METADATA_EXPECTED(inserted_metadata, *retrieved_struct);
  }

  // Test to remove metadata.
  this->test_remove(managers_.get(), object_name);

  // Test whether metadata has been removed with ptree.
  this->test_get(managers_.get(), object_name, retrieved_ptree,
                 ErrorCode::NAME_NOT_FOUND);
}

/**
 * @brief This is a test of the basic paths of metadata management (add, get,
 *   remove).
 *   add: patterns that obtain a object ID.
 *   get: object name as a key.
 *   remove: object name as a key.
 * @param ut_metadata  [in]  test metadata.
 */
void ApiTest::test_flow_get_by_name_with_struct(
    const UtMetadata* ut_metadata) const {
  SCOPED_TRACE("");
  ASSERT_TRUE(managers_);

  // Test to initialize the manager.
  ASSERT_NO_FATAL_FAILURE(this->test_init(managers_.get()));

  auto inserted_metadata = ut_metadata->get_metadata_ptree();
  auto inserted_metadata_struct =
      const_cast<Object*>(ut_metadata->get_metadata_struct());
  auto object_name = inserted_metadata_struct->name;

  ObjectId new_object_id = INVALID_OBJECT_ID;
  // Add metadata.
  {
    // Test to add metadata with structure.
    EXPECT_NO_FATAL_FAILURE({
      new_object_id =
          this->test_add(managers_.get(), *inserted_metadata_struct);
    });
    // If there is an error in the test, it terminates.
    if (::testing::Test::HasFailure()) {
      FAIL();
    }

    // Set object ID.
    inserted_metadata.put(Object::ID, new_object_id);
  }

  ptree retrieved_ptree;
  // Get metadata with ptree.
  {
    // Test to get metadata with ptree.
    this->test_get(managers_.get(), object_name, retrieved_ptree);
    // Verifies that the returned metadata is expected one.
    ut_metadata->CHECK_METADATA_EXPECTED(inserted_metadata, retrieved_ptree);
  }

  auto retrieved_struct = this->get_structure();
  // Get metadata with structure.
  {
    // Test to get metadata with structure.
    this->test_get(managers_.get(), object_name, *retrieved_struct);
    // Verifies that the returned metadata is expected one.
    ut_metadata->CHECK_METADATA_EXPECTED(inserted_metadata, *retrieved_struct);
  }

  // Test to remove metadata.
  this->test_remove(managers_.get(), object_name);

  // Test whether metadata has been removed.
  this->test_get(managers_.get(), object_name, retrieved_ptree,
                 ErrorCode::NAME_NOT_FOUND);
}

/**
 * @brief This is a test of the basic paths of metadata management (add,
 *   get_all, remove).
 *   add: patterns that obtain a object ID.
 *   get_all: all object.
 *   remove: object ID as a key.
 * @param ut_metadata  [in]  test metadata.
 */
void ApiTest::test_flow_getall(const UtMetadata* ut_metadata) const {
  static constexpr const int32_t kTestConstraintCount = 5;

  SCOPED_TRACE("");
  ASSERT_TRUE(managers_);

  // Get the current number of records.
  auto base_record_count = this->get_record_count();

  // Test to initialize the manager.
  ASSERT_NO_FATAL_FAILURE(this->test_init(managers_.get()));

  std::vector<ptree> metadata_container = {};

  // Testing in the pre-addition state.
  metadata_container = this->test_getall(managers_.get());
  EXPECT_EQ(base_record_count, metadata_container.size());

  // Generate test metadata.
  std::vector<ptree> test_metadata_list{};
  for (int32_t num = 1; num <= kTestConstraintCount; num++) {
    std::string metadata_name = "metadata_name_" +
                                UTUtils::generate_narrow_uid() + "_" +
                                std::to_string(num);

    auto metadata = ut_metadata->get_metadata_ptree();
    metadata.put(Object::NAME, metadata_name);

    test_metadata_list.push_back(metadata);
  }

  // Add metadata.
  for (auto& metadata : test_metadata_list) {
    ObjectId object_id = INVALID_OBJECT_ID;
    // Test to add metadata.
    EXPECT_NO_FATAL_FAILURE(
        { object_id = this->test_add(managers_.get(), metadata); });

    // If there is an error in the test, it terminates.
    if (::testing::Test::HasFailure()) {
      FAIL();
    }

    // Set object ID.
    metadata.put(Object::ID, object_id);
  }

  // Get metadata with structure.
  {
    metadata_container = this->test_getall(managers_.get());

    int32_t expect_count = base_record_count + test_metadata_list.size();
    int32_t actual_count = metadata_container.size();
    ASSERT_EQ(expect_count, actual_count);

    // Inspect the returned metadata.
    for (auto& expect_metadata : test_metadata_list) {
      // Extract object ID.
      auto expect_id = expect_metadata.get<ObjectId>(Object::ID);

      for (auto& actual_metadata : metadata_container) {
        // Extract object ID.
        auto actual_id = actual_metadata.get<ObjectId>(Object::ID);
        if (expect_id == actual_id) {
          // Verifies that the returned metadata is expected one.
          ut_metadata->CHECK_METADATA_EXPECTED(expect_metadata,
                                               actual_metadata);
          break;
        }
      }
    }
  }

  // Remove metadata.
  for (auto& metadata : test_metadata_list) {
    // Extract object ID.
    auto object_id = metadata.get<ObjectId>(Object::ID);

    // Test to remove metadata.
    this->test_remove(managers_.get(), object_id);
  }
}

/**
 * @brief This is a test of the basic paths of metadata management (add, get,
 *   update, remove).
 *   add: patterns that obtain a object ID.
 *   get: object ID as a key.
 *   update: object ID as a key.
 *   remove: object ID as a key.
 * @param ut_metadata         [in]  test metadata.
 * @param ut_metadata_update  [in]  update test metadata.
 */
void ApiTest::test_flow_update(const UtMetadata* ut_metadata,
                               const UtMetadata* ut_metadata_update) const {
  SCOPED_TRACE("");
  ASSERT_TRUE(managers_);

  // Test to initialize the manager.
  ASSERT_NO_FATAL_FAILURE(this->test_init(managers_.get()));

  auto inserted_metadata = ut_metadata->get_metadata_ptree();
  ObjectId new_object_id = INVALID_OBJECT_ID;
  // Add metadata.
  {
    // Test to add metadata.
    EXPECT_NO_FATAL_FAILURE({
      new_object_id = this->test_add(managers_.get(), inserted_metadata);
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
    // Test to get metadata with ptree.
    this->test_get(managers_.get(), new_object_id, before_ptree);
    // Verifies that the returned metadata is expected one.
    ut_metadata->CHECK_METADATA_EXPECTED(inserted_metadata, before_ptree);
  }

  auto updated_metadata = ut_metadata_update->get_metadata_ptree();
  // Update metadata.
  {
    // Set object ID.
    updated_metadata.put(Object::ID, new_object_id);

    // Test to update metadata.
    this->test_update(managers_.get(), new_object_id, updated_metadata);
  }

  ptree after_ptree;
  // Get metadata with ptree.
  {
    // Test to get metadata with ptree.
    this->test_get(managers_.get(), new_object_id, after_ptree);
    // Verifies that the returned metadata is expected one.
    ut_metadata->CHECK_METADATA_EXPECTED(updated_metadata, after_ptree);
  }

  // Test to remove metadata.
  this->test_remove(managers_.get(), new_object_id);
}

/**
 * @brief This is a test to init metadata.
 * @param managers     [in]  metadata management class object.
 * @param expect_code  [in]  expected result code.
 * @return ObjectId - added object ID.
 */
void ApiTest::test_init(const Metadata* managers, ErrorCode expect_code) const {
  assert(managers != nullptr);

  UTUtils::print("-- init test metadata --");

  ErrorCode error = managers_->init();
  ASSERT_EQ(expect_code, error);
}

/**
 * @brief This is a test to add metadata.
 * @param managers         [in]  metadata management class object.
 * @param metadata_object  [in]  metadata object.
 * @param expect_code      [in]  expected result code.
 * @return ObjectId - added object ID.
 */
ObjectId ApiTest::test_add(const Metadata* managers,
                           boost::property_tree::ptree& metadata_object,
                           ErrorCode expect_code) const {
  UTUtils::print("-- add test metadata with ptree --");
  return metadata_add(managers, metadata_object, expect_code);
}

/**
 * @brief This is a test to add metadata.
 * @param managers         [in]  metadata management class object.
 * @param metadata_object  [in]  metadata object.
 * @param expect_code      [in]  expected result code.
 * @return ObjectId - added object ID.
 */
ObjectId ApiTest::test_add(const Metadata* managers, Object& metadata_object,
                           ErrorCode expect_code) const {
  UTUtils::print("-- add test metadata with structure --");
  return metadata_add(managers, metadata_object, expect_code);
}

/**
 * @brief This is a test to get metadata.
 * @param managers         [in]  metadata management class object.
 * @param object_id        [in]  object ID of the metadata to be retrieved.
 * @param metadata_object  [out] retrieved metadata object.
 * @param expect_code      [in]  expected result code.
 * @return boost::property_tree::ptree - retrieved metadata.
 */
void ApiTest::test_get(const Metadata* managers, ObjectId object_id,
                       boost::property_tree::ptree& metadata_object,
                       ErrorCode expect_code) const {
  UTUtils::print("-- get test metadata by object ID with ptree --");
  metadata_get(managers, object_id, metadata_object, expect_code);
}

/**
 * @brief This is a test to get metadata.
 * @param managers         [in]  metadata management class object.
 * @param object_id        [in]  object ID of the metadata to be retrieved.
 * @param metadata_object  [out] retrieved metadata object.
 * @param expect_code      [in]  expected result code.
 */
void ApiTest::test_get(const Metadata* managers, ObjectId object_id,
                       Object& metadata_object, ErrorCode expect_code) const {
  UTUtils::print("-- get test metadata by object ID with structure --");
  metadata_get(managers, object_id, metadata_object, expect_code);
}

/**
 * @brief This is a test to get metadata.
 * @param managers         [in]  metadata management class object.
 * @param object_name      [in]  object name of the metadata to be retrieved.
 * @param metadata_object  [out] retrieved metadata object.
 * @param expect_code      [in]  expected result code.
 */
void ApiTest::test_get(const Metadata* managers, std::string_view object_name,
                       boost::property_tree::ptree& metadata_object,
                       ErrorCode expect_code) const {
  UTUtils::print("-- get test metadata by object name with ptree --");
  metadata_get(managers, object_name, metadata_object, expect_code);
}

/**
 * @brief This is a test to get metadata.
 * @param managers         [in]  metadata management class object.
 * @param object_name      [in]  object name of the metadata to be retrieved.
 * @param metadata_object  [out] retrieved metadata object.
 * @param expect_code      [in]  expected result code.
 */
void ApiTest::test_get(const Metadata* managers, std::string_view object_name,
                       Object& metadata_object, ErrorCode expect_code) const {
  UTUtils::print("-- get test metadata by object name with structure --");
  metadata_get(managers, object_name, metadata_object, expect_code);
}

/**
 * @brief This is a test to get metadata.
 * @param managers     [in]  metadata management class object.
 * @param expect_code  [in]  expected result code.
 * @return std::vector<boost::property_tree::ptree> - retrieved metadata.
 */
std::vector<boost::property_tree::ptree> ApiTest::test_getall(
    const Metadata* managers, ErrorCode expect_code) const {
  assert(managers != nullptr);

  UTUtils::print("-- get_all test metadata --");

  std::vector<ptree> retrieved_metadata = {};
  // get metadata by object ID.
  ErrorCode actual = managers->get_all(retrieved_metadata);
  EXPECT_EQ(expect_code, actual);

  UTUtils::print(" >> Count: ", retrieved_metadata.size());
  for (auto& metadata : retrieved_metadata) {
    UTUtils::print(" " + UTUtils::get_tree_string(metadata));
  }

  return retrieved_metadata;
}

/**
 * @brief This is a test to update metadata.
 * @param managers         [in]  metadata management class object.
 * @param object_id        [in]  object ID of the metadata to be retrieved.
 * @param metadata_object  [in]  metadata object.
 * @param expect_code      [in]  expected result code.
 */
void ApiTest::test_update(const Metadata* managers, ObjectId object_id,
                          boost::property_tree::ptree& metadata_object,
                          ErrorCode expect_code) const {
  UTUtils::print("-- update test metadata by object id with ptree --");
  metadata_update(managers, object_id, metadata_object, expect_code);
}

/**
 * @brief This is a test to remove metadata.
 * @param managers     [in]  metadata management class object.
 * @param object_id    [in]  object ID of the metadata to be removed.
 * @param expect_code  [in]  expected result code.
 */
void ApiTest::test_remove(const Metadata* managers, ObjectId object_id,
                          ErrorCode expect_code) const {
  assert(managers != nullptr);

  UTUtils::print("-- remove test metadata by object ID --");
  UTUtils::print(" >> object ID: ", object_id);

  // remove metadata by object ID.
  ErrorCode actual = managers->remove(object_id);
  ASSERT_EQ(expect_code, actual);
}

/**
 * @brief This is a test to remove metadata.
 * @param managers    [in]  metadata management class object.
 * @param object_name [in]  object name of the metadata to be removed.
 * @param expect_code [in]  expected result code.
 */
void ApiTest::test_remove(const Metadata* managers,
                          std::string_view object_name,
                          ErrorCode expect_code) const {
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
 * @param managers         [in]  metadata management class object.
 * @param metadata_object  [in]  metadata object.
 * @param expect_code      [in]  expected result code.
 * @return ObjectId - added object ID.
 */
template <typename T>
ObjectId ApiTest::metadata_add(const Metadata* managers, T& metadata_object,
                               ErrorCode expect_code) const {
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
 * @brief This is a test to get metadata.
 * @param managers         [in]  metadata management class object.
 * @param object_key       [in]  object ID/name of the metadata to be
 retrieved.
 * @param metadata_object  [out] retrieved metadata object.
 * @param expect_code      [in]  expected result code.
 * @return boost::property_tree::ptree - retrieved metadata.
 */
template <typename KEY, typename T>
void ApiTest::metadata_get(const Metadata* managers, KEY object_key,
                           T& metadata_object, ErrorCode expect_code) const {
  assert(managers != nullptr);

  UTUtils::print(" >> object key: ", object_key);

  // get metadata by object ID.
  ErrorCode actual = managers->get(object_key, metadata_object);
  EXPECT_EQ(expect_code, actual);

  UTUtils::print(" " + UTUtils::get_tree_string(metadata_object));
}

/**
 * @brief This is a test to update metadata.
 * @param managers         [in]  metadata management class object.
 * @param object_key       [in]  object ID/name of the metadata to be
 retrieved.
 * @param metadata_object  [in]  metadata object.
 * @param expect_code      [in]  expected result code.
 */
template <typename KEY, typename T>
void ApiTest::metadata_update(const Metadata* managers, KEY object_key,
                              T& metadata_object, ErrorCode expect_code) const {
  assert(managers != nullptr);

  UTUtils::print(" >> object key: ", object_key);
  UTUtils::print(" " + UTUtils::get_tree_string(metadata_object));

  // add metadata.
  ErrorCode actual = managers->update(object_key, metadata_object);
  EXPECT_EQ(expect_code, actual);
}

}  // namespace manager::metadata::testing
