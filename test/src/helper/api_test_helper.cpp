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
#include "test/helper/api_test_helper.h"

#include <gtest/gtest.h>

#include "test/common/ut_utils.h"

namespace manager::metadata::testing {

using boost::property_tree::ptree;

/**
 * @brief This is a test to init metadata.
 * @param manager      [in]  metadata management class object.
 * @param expect_code  [in]  expected result code.
 */
void ApiTestHelper::test_init(const Metadata* manager, ErrorCode expect_code) {
  UTUtils::print("-- init test metadata --");

  ErrorCode error = manager->init();
  ASSERT_EQ(expect_code, error);
}

/**
 * @brief This is a test to add metadata.
 * @param manager          [in]  metadata management class object.
 * @param metadata_object  [in]  metadata object.
 * @param expect_code      [in]  expected result code.
 * @return ObjectId - added object ID.
 */
ObjectId ApiTestHelper::test_add(const Metadata* manager,
                                 boost::property_tree::ptree& metadata_object,
                                 ErrorCode expect_code) {
  UTUtils::print("-- add test metadata with ptree --");
  return metadata_add(manager, metadata_object, expect_code);
}

/**
 * @brief This is a test to add metadata.
 * @param manager          [in]  metadata management class object.
 * @param metadata_object  [in]  metadata object.
 * @param expect_code      [in]  expected result code.
 * @return ObjectId - added object ID.
 */
ObjectId ApiTestHelper::test_add(const Metadata* manager,
                                 Object& metadata_object,
                                 ErrorCode expect_code) {
  UTUtils::print("-- add test metadata with structure --");
  return metadata_add(manager, metadata_object, expect_code);
}

/**
 * @brief This is a test to get metadata.
 * @param manager          [in]  metadata management class object.
 * @param object_id        [in]  object ID of the metadata to be retrieved.
 * @param metadata_object  [out] retrieved metadata object.
 * @param expect_code      [in]  expected result code.
 * @return boost::property_tree::ptree - retrieved metadata.
 */
void ApiTestHelper::test_get(const Metadata* manager, ObjectId object_id,
                             ErrorCode expect_code,
                             boost::property_tree::ptree& metadata_object) {
  UTUtils::print("-- get test metadata by object ID with ptree --");
  metadata_get(manager, object_id, expect_code, metadata_object);
}

/**
 * @brief This is a test to get metadata.
 * @param manager          [in]  metadata management class object.
 * @param object_id        [in]  object ID of the metadata to be retrieved.
 * @param expect_code      [in]  expected result code.
 * @param metadata_object  [out] retrieved metadata object.
 */
void ApiTestHelper::test_get(const Metadata* manager, ObjectId object_id,
                             ErrorCode expect_code, Object& metadata_object) {
  UTUtils::print("-- get test metadata by object ID with structure --");
  metadata_get(manager, object_id, expect_code, metadata_object);
}

/**
 * @brief This is a test to get metadata.
 * @param manager          [in]  metadata management class object.
 * @param object_name      [in]  object name of the metadata to be retrieved.
 * @param expect_code      [in]  expected result code.
 * @param metadata_object  [out] retrieved metadata object.
 */
void ApiTestHelper::test_get(const Metadata* manager,
                             std::string_view object_name,
                             ErrorCode expect_code,
                             boost::property_tree::ptree& metadata_object) {
  UTUtils::print("-- get test metadata by object name with ptree --");
  metadata_get(manager, object_name, expect_code, metadata_object);
}

/**
 * @brief This is a test to get metadata.
 * @param manager          [in]  metadata management class object.
 * @param object_name      [in]  object name of the metadata to be retrieved.
 * @param expect_code      [in]  expected result code.
 * @param metadata_object  [out] retrieved metadata object.
 */
void ApiTestHelper::test_get(const Metadata* manager,
                             std::string_view object_name,
                             ErrorCode expect_code, Object& metadata_object) {
  UTUtils::print("-- get test metadata by object name with structure --");
  metadata_get(manager, object_name, expect_code, metadata_object);
}

/**
 * @brief This is a test to get metadata.
 * @param manager      [in]  metadata management class object.
 * @param expect_code  [in]  expected result code.
 * @param container    [out] retrieved metadata.
 */
void ApiTestHelper::test_getall(
    const Metadata* manager, ErrorCode expect_code,
    std::vector<boost::property_tree::ptree>& container) {
  assert(manager != nullptr);

  UTUtils::print("-- get_all test metadata with ptree --");

  container.clear();
  // get metadata by object ID.
  ErrorCode actual = manager->get_all(container);
  EXPECT_EQ(expect_code, actual);

  UTUtils::print(" >> Count: ", container.size());
  for (auto& metadata : container) {
    UTUtils::print(" " + UTUtils::get_tree_string(metadata));
  }
}

/**
 * @brief This is a test to get metadata.
 * @param manager      [in]  metadata management class object.
 * @param expect_code  [in]  expected result code.
 * @param container    [out] retrieved metadata.
 */
void ApiTestHelper::test_getall_next(Metadata* manager, ErrorCode expect_code,
                                     std::vector<ptree>& container) {
  assert(manager != nullptr);

  UTUtils::print("-- get_all-next test metadata with ptree --");

  container.clear();

  // get all metadata.
  ErrorCode actual_code = manager->get_all();
  EXPECT_EQ(expect_code, actual_code);

  ptree metadata_object;
  while ((actual_code = manager->next(metadata_object)) == ErrorCode::OK) {
    container.push_back(metadata_object);

    UTUtils::print(" >> Next: ", (container.size()));
    UTUtils::print("  " + UTUtils::get_tree_string(metadata_object));
  }
  ASSERT_EQ(ErrorCode::END_OF_ROW, actual_code);
}

/**
 * @brief This is a test to exists metadata.
 * @param manager    [in]  metadata management class object.
 * @param object_id  [in]  object ID of the metadata to be retrieved.
 * @param expected   [in]  expected result.
 */
void ApiTestHelper::test_exists(const Metadata* manager, ObjectId object_id,
                                bool expected) {
  UTUtils::print("-- exists test metadata --");

  bool actual = manager->exists(object_id);
  EXPECT_EQ(expected, actual);
}

/**
 * @brief This is a test to exists metadata.
 * @param manager      [in]  metadata management class object.
 * @param object_name  [in]  object name of the metadata to be retrieved.
 * @param expected     [in]  expected result.
 */
void ApiTestHelper::test_exists(const Metadata* manager,
                                std::string_view object_name, bool expected) {
  UTUtils::print("-- exists test metadata --");

  bool actual = manager->exists(object_name);
  EXPECT_EQ(expected, actual);
}

/**
 * @brief This is a test to update metadata.
 * @param manager          [in]  metadata management class object.
 * @param object_id        [in]  object ID of the metadata to be retrieved.
 * @param metadata_object  [in]  metadata object.
 * @param expect_code      [in]  expected result code.
 */
void ApiTestHelper::test_update(const Metadata* manager, ObjectId object_id,
                                boost::property_tree::ptree& metadata_object,
                                ErrorCode expect_code) {
  UTUtils::print("-- update test metadata by object id with ptree --");
  metadata_update(manager, object_id, metadata_object, expect_code);
}

#if 0
/**
 * @brief This is a test to update metadata.
 * @param manager          [in]  metadata management class object.
 * @param object_id        [in]  object ID of the metadata to be retrieved.
 * @param metadata_object  [in]  metadata object.
 * @param expect_code      [in]  expected result code.
 */
void ApiTestHelper::test_update(const Metadata* manager, ObjectId object_id,
                           Object& metadata_object,
                           ErrorCode expect_code) {
  UTUtils::print("-- update test metadata by object id with structure --");
  metadata_update(manager, object_id, metadata_object, expect_code);
}
#endif

/**
 * @brief This is a test to remove metadata.
 * @param manager      [in]  metadata management class object.
 * @param object_id    [in]  object ID of the metadata to be removed.
 * @param expect_code  [in]  expected result code.
 */
void ApiTestHelper::test_remove(const Metadata* manager, ObjectId object_id,
                                ErrorCode expect_code) {
  assert(manager != nullptr);

  UTUtils::print("-- remove test metadata by object ID --");
  UTUtils::print(" >> object ID: ", object_id);

  // remove metadata by object ID.
  ErrorCode actual = manager->remove(object_id);
  ASSERT_EQ(expect_code, actual);
}

/**
 * @brief This is a test to remove metadata.
 * @param manager      [in]  metadata management class object.
 * @param object_name  [in]  object name of the metadata to be removed.
 * @param expect_code  [in]  expected result code.
 */
void ApiTestHelper::test_remove(const Metadata* manager,
                                std::string_view object_name,
                                ErrorCode expect_code) {
  assert(manager != nullptr);

  UTUtils::print("-- remove test metadata by object name --");
  UTUtils::print(" >> object name: ", object_name);

  ObjectIdType object_id = INVALID_VALUE;
  // remove metadata by object name.
  ErrorCode actual = manager->remove(object_name, &object_id);
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
 * @param manager          [in]  metadata management class object.
 * @param metadata_object  [in]  metadata object.
 * @param expect_code      [in]  expected result code.
 * @return ObjectId - added object ID.
 */
template <typename T>
ObjectId ApiTestHelper::metadata_add(const Metadata* manager,
                                     T& metadata_object,
                                     ErrorCode expect_code) {
  assert(manager != nullptr);

  UTUtils::print(" " + UTUtils::get_tree_string(metadata_object));

  ObjectId object_id = INVALID_OBJECT_ID;

  // add metadata.
  ErrorCode actual = manager->add(metadata_object, &object_id);
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
 * @param manager          [in]  metadata management class object.
 * @param object_key       [in]  object ID/name of the metadata to be
     retrieved.
 * @param expect_code      [in]  expected result code.
 * @param metadata_object  [out] retrieved metadata object.
 * @return boost::property_tree::ptree - retrieved metadata.
 */
template <typename KEY, typename T>
void ApiTestHelper::metadata_get(const Metadata* manager, KEY object_key,
                                 ErrorCode expect_code, T& metadata_object) {
  assert(manager != nullptr);

  UTUtils::print(" >> object key: ", object_key);

  // get metadata by object ID.
  ErrorCode actual = manager->get(object_key, metadata_object);
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
 * @param manager          [in]  metadata management class object.
 * @param object_key       [in]  object ID/name of the metadata to be
 retrieved.
 * @param metadata_object  [in]  metadata object.
 * @param expect_code      [in]  expected result code.
 */
template <typename KEY, typename T>
void ApiTestHelper::metadata_update(const Metadata* manager, KEY object_key,
                                    T& metadata_object, ErrorCode expect_code) {
  assert(manager != nullptr);

  UTUtils::print(" >> object key: ", object_key);
  UTUtils::print(" " + UTUtils::get_tree_string(metadata_object));

  // add metadata.
  ErrorCode actual = manager->update(object_key, metadata_object);
  EXPECT_EQ(expect_code, actual);
}

}  // namespace manager::metadata::testing
