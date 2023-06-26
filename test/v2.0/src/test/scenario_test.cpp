/*
 * Copyright 2022-2023 tsurugi project.
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
#include "test/test/scenario_test.h"

#include <gtest/gtest.h>

#include "test/common/global_test_environment.h"
#include "test/helper/api_test_helper.h"
#include "test/helper/table_metadata_helper.h"

namespace manager::metadata::testing {

namespace {

manager::metadata::ObjectId test_table_id;

}  // namespace

using boost::property_tree::ptree;

class ScenarioTest
    : public ::testing::TestWithParam<scenario_test::ScenarioTestParam> {
 public:
  static void SetUpTestCase() {
    if (g_environment_->is_open()) {
      UTUtils::print(">> gtest::SetUpTestCase()");

      // Change to a unique table name.
      std::string table_name = "ScenarioTest_" + UTUtils::generate_narrow_uid();

      // Add table metadata.
      TableMetadataHelper::add_table(table_name, &test_table_id);

      UTUtils::print("<< gtest::SetUpTestCase()");
    }
  }

  static void TearDownTestCase() {
    if (g_environment_->is_open()) {
      UTUtils::print(">> gtest::TearDownTestCase()");

      // Remove table metadata.
      TableMetadataHelper::remove_table(test_table_id);

      UTUtils::print("<< gtest::TearDownTestCase()");
    }
  }

  void SetUp() override { UTUtils::skip_if_connection_not_opened(); }
  void TearDown() override {}

 protected:
  /**
   * @brief Adds the specified number of pieces of metadata.
   * @param manager              [in]  metadata management class object.
   * @param metadata             [in]  test metadata.
   * @param unique_data_creator  [in]  callback function to create unique data.
   * @param create_data_max      [in]  number of data to be created.
   */
  std::vector<ptree> metadata_add(const Metadata* manager,
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
              ApiTestHelper::metadata_add(manager, metadata, ErrorCode::OK);
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
   * @brief Removes the metadata.
   * @param manager        [in]  metadata management class object.
   * @param metadata_list  [in]  list of metadata.
   */
  void metadata_remove(const Metadata* manager,
                       const std::vector<ptree>& metadata_list) const {
    assert(manager != nullptr);

    UTUtils::print("-- remove test metadata by object ID --");
    for (auto& metadata : metadata_list) {
      auto object_id = metadata.get<ObjectId>(Object::ID);
      UTUtils::print(" >> object ID: ", object_id);

      // remove metadata by object ID.
      manager->remove(object_id);
    }
  }

  /**
   * @brief Compare all metadata.
   * @param ut_metadata           [in]  test metadata.
   * @param expect_metadata_list  [in]  expected metadata.
   * @param actual_metadata_list  [in]  actual metadata.
   */
  void metadata_compare_all(
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
          ut_metadata->CHECK_METADATA_EXPECTED(expect_metadata,
                                               actual_metadata);
          break;
        }
      }
    }
  }
};

/**
 * @brief Class to test the basic path of metadata management using object ID.
 */
class GetByIdTest : public ScenarioTest {};

/**
 * @brief Class to test the basic path of metadata management using object name.
 */
class GetByNameTest : public ScenarioTest {};

/**
 * @brief Class to test the get-all path of metadata management.
 */
class GetAllTest : public ScenarioTest {};

/**
 * @brief Class to test the update path of metadata management.
 */
class UpdateTest : public ScenarioTest {};

INSTANTIATE_TEST_CASE_P(ScenarioTest, GetByIdTest,
                        ::testing::ValuesIn(scenario_test::get_test_by_id));

INSTANTIATE_TEST_CASE_P(ScenarioTest, GetByNameTest,
                        ::testing::ValuesIn(scenario_test::get_test_by_name));

INSTANTIATE_TEST_CASE_P(ScenarioTest, GetAllTest,
                        ::testing::ValuesIn(scenario_test::getall_test));

INSTANTIATE_TEST_CASE_P(ScenarioTest, UpdateTest,
                        ::testing::ValuesIn(scenario_test::update_test));

/**
 * @brief This is a test of the basic paths of metadata management (add, get,
 *   remove).
 *   add: metadata is ptree type.
 *   exists: object ID as a key.
 *   get: object ID as a key.
 *   remove: object ID as a key.
 */
TEST_P(GetByIdTest, test_by_id_with_ptree) {
  constexpr const char* const kTestTitle =
      "Add(ptree)-Exists(ID)-Get(ID/ptree[, structure])-Remove(ID)";

  CALL_TRACE;
  auto& metadata_test = GetParam();

  UTUtils::print(">> Scenario test: ", kTestTitle);
  UTUtils::print("[", typeid(*metadata_test->get_metadata_manager()).name(),
                 "]");

  if (metadata_test->is_test_skip()) {
    GTEST_SKIP();
  }

  auto manager_sptr       = metadata_test->get_metadata_manager();
  auto test_metadata_sptr = metadata_test->get_test_metadata(test_table_id);

  auto manager = manager_sptr.get();
  ASSERT_TRUE(manager);

  // Test to initialize the manager.
  ASSERT_NO_FATAL_FAILURE(ApiTestHelper::test_init(manager, ErrorCode::OK));

  auto test_metadata     = test_metadata_sptr.get();
  auto inserted_metadata = test_metadata->get_metadata_ptree();
  ObjectId new_object_id = INVALID_OBJECT_ID;
  // Add metadata.
  {
    CALL_TRACE;
    // Test to add metadata.
    EXPECT_NO_FATAL_FAILURE({
      new_object_id =
          ApiTestHelper::test_add(manager, inserted_metadata, ErrorCode::OK);
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
    ApiTestHelper::test_exists(manager, new_object_id, true);
  }

  ptree retrieved_ptree;
  // Get metadata with ptree.
  {
    CALL_TRACE;
    // Test to get metadata with ptree.
    ApiTestHelper::test_get(manager, new_object_id, ErrorCode::OK,
                            retrieved_ptree);
    // Verifies that the returned metadata is expected one.
    test_metadata->CHECK_METADATA_EXPECTED(inserted_metadata, retrieved_ptree);
  }

  // Get metadata with structure.
  auto metadata_struct = metadata_test->get_structure();
  if (metadata_struct != nullptr) {
    CALL_TRACE;
    // Test to get metadata with structure.
    ApiTestHelper::test_get(manager, new_object_id, ErrorCode::OK,
                            *metadata_struct);
    // Verifies that the returned metadata is expected one.
    test_metadata->CHECK_METADATA_EXPECTED(inserted_metadata, *metadata_struct);
  }

  {
    CALL_TRACE;
    // Test to remove metadata.
    ApiTestHelper::test_remove(manager, new_object_id, ErrorCode::OK);
  }

  {
    CALL_TRACE;
    // Test to see if data has been removed.
    ApiTestHelper::test_exists(manager, new_object_id, false);
    ApiTestHelper::test_get(manager, new_object_id, ErrorCode::ID_NOT_FOUND,
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
 */
TEST_P(GetByIdTest, test_by_id_with_struct) {
  constexpr const char* const kTestTitle =
      "Add(structure)-Exists(ID)-Get(ID/ptree[, structure])-Remove(ID)";

  CALL_TRACE;
  auto& metadata_test = GetParam();

  UTUtils::print(">> Scenario test: ", kTestTitle);
  UTUtils::print("[", typeid(*metadata_test->get_metadata_manager()).name(),
                 "]");

  if (metadata_test->is_test_skip()) {
    GTEST_SKIP();
  } else if (metadata_test->get_structure() == nullptr) {
    GTEST_SKIP_("  Skipped: The structure API is not supported.");
  }

  auto manager_sptr       = metadata_test->get_metadata_manager();
  auto test_metadata_sptr = metadata_test->get_test_metadata(test_table_id);

  auto manager = manager_sptr.get();
  ASSERT_TRUE(manager);

  // Test to initialize the manager.
  ASSERT_NO_FATAL_FAILURE(ApiTestHelper::test_init(manager, ErrorCode::OK));

  auto test_metadata     = test_metadata_sptr.get();
  auto inserted_metadata = test_metadata->get_metadata_ptree();
  auto inserted_metadata_struct =
      const_cast<Object*>(test_metadata->get_metadata_struct());
  ObjectId new_object_id = INVALID_OBJECT_ID;
  // Add metadata.
  {
    CALL_TRACE;
    // Test to add metadata with structure.
    EXPECT_NO_FATAL_FAILURE({
      new_object_id = ApiTestHelper::test_add(
          manager, *inserted_metadata_struct, ErrorCode::OK);
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
    ApiTestHelper::test_exists(manager, new_object_id, true);
  }

  ptree retrieved_ptree;
  // Get metadata with ptree.
  {
    CALL_TRACE;
    // Test to get metadata with ptree.
    ApiTestHelper::test_get(manager, new_object_id, ErrorCode::OK,
                            retrieved_ptree);
    // Verifies that the returned metadata is expected one.
    test_metadata->CHECK_METADATA_EXPECTED(inserted_metadata, retrieved_ptree);
  }

  // Get metadata with structure.
  auto metadata_struct = metadata_test->get_structure();
  if (metadata_struct != nullptr) {
    CALL_TRACE;
    // Test to get metadata with structure.
    ApiTestHelper::test_get(manager, new_object_id, ErrorCode::OK,
                            *metadata_struct);
    // Verifies that the returned metadata is expected one.
    test_metadata->CHECK_METADATA_EXPECTED(inserted_metadata, *metadata_struct);
  }

  {
    CALL_TRACE;
    // Test to remove metadata.
    ApiTestHelper::test_remove(manager, new_object_id, ErrorCode::OK);
  }

  {
    CALL_TRACE;
    // Test to see if data has been removed.
    ApiTestHelper::test_exists(manager, new_object_id, false);
    ApiTestHelper::test_get(manager, new_object_id, ErrorCode::ID_NOT_FOUND,
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
 */
TEST_P(GetByNameTest, test_by_name_with_ptree) {
  constexpr const char* const kTestTitle =
      "Add(ptree)-Exists(name)-Get(name/ptree[, structure])-Remove(name)";

  CALL_TRACE;
  auto& metadata_test = GetParam();

  UTUtils::print(">> Scenario test: ", kTestTitle);
  UTUtils::print("[", typeid(*metadata_test->get_metadata_manager()).name(),
                 "]");

  if (metadata_test->is_test_skip()) {
    GTEST_SKIP();
  }

  auto manager_sptr       = metadata_test->get_metadata_manager();
  auto test_metadata_sptr = metadata_test->get_test_metadata(test_table_id);

  auto manager = manager_sptr.get();
  ASSERT_TRUE(manager);

  // Test to initialize the manager.
  ASSERT_NO_FATAL_FAILURE(ApiTestHelper::test_init(manager, ErrorCode::OK));

  auto test_metadata     = test_metadata_sptr.get();
  auto inserted_metadata = test_metadata->get_metadata_ptree();
  auto object_name       = test_metadata->get_metadata_struct()->name;

  ObjectId new_object_id = INVALID_OBJECT_ID;
  // Add metadata.
  {
    CALL_TRACE;
    // Test to add metadata.
    EXPECT_NO_FATAL_FAILURE({
      new_object_id =
          ApiTestHelper::test_add(manager, inserted_metadata, ErrorCode::OK);
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
    ApiTestHelper::test_exists(manager, object_name, true);
  }

  ptree retrieved_ptree;
  // Get metadata with ptree.
  {
    CALL_TRACE;
    // Test to get metadata with ptree.
    ApiTestHelper::test_get(manager, object_name, ErrorCode::OK,
                            retrieved_ptree);
    // Verifies that the returned metadata is expected one.
    test_metadata->CHECK_METADATA_EXPECTED(inserted_metadata, retrieved_ptree);
  }

  // Get metadata with structure.
  auto metadata_struct = metadata_test->get_structure();
  if (metadata_struct != nullptr) {
    CALL_TRACE;
    // Test to get metadata with structure.
    ApiTestHelper::test_get(manager, object_name, ErrorCode::OK,
                            *metadata_struct);
    // Verifies that the returned metadata is expected one.
    test_metadata->CHECK_METADATA_EXPECTED(inserted_metadata, *metadata_struct);
  }

  {
    CALL_TRACE;
    // Test to remove metadata.
    ApiTestHelper::test_remove(manager, object_name, ErrorCode::OK);
  }

  {
    CALL_TRACE;
    // Test to see if data has been removed.
    ApiTestHelper::test_exists(manager, object_name, false);
    ApiTestHelper::test_get(manager, object_name, ErrorCode::NAME_NOT_FOUND,
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
 */
TEST_P(GetByNameTest, test_by_name_with_struct) {
  constexpr const char* const kTestTitle =
      "Add(structure)-Exists(name)-Get(name/ptree[, structure])-Remove(name)";

  CALL_TRACE;
  auto& metadata_test = GetParam();

  UTUtils::print(">> Scenario test: ", kTestTitle);
  UTUtils::print("[", typeid(*metadata_test->get_metadata_manager()).name(),
                 "]");

  if (metadata_test->is_test_skip()) {
    GTEST_SKIP();
  } else if (metadata_test->get_structure() == nullptr) {
    GTEST_SKIP_("  Skipped: The structure API is not supported.");
  }

  auto manager_sptr       = metadata_test->get_metadata_manager();
  auto test_metadata_sptr = metadata_test->get_test_metadata(test_table_id);

  auto manager = manager_sptr.get();
  ASSERT_TRUE(manager);

  // Test to initialize the manager.
  ASSERT_NO_FATAL_FAILURE(ApiTestHelper::test_init(manager, ErrorCode::OK));

  auto test_metadata     = test_metadata_sptr.get();
  auto inserted_metadata = test_metadata->get_metadata_ptree();
  auto inserted_metadata_struct =
      const_cast<Object*>(test_metadata->get_metadata_struct());
  auto object_name = inserted_metadata_struct->name;

  ObjectId new_object_id = INVALID_OBJECT_ID;
  // Add metadata.
  {
    CALL_TRACE;
    // Test to add metadata with structure.
    EXPECT_NO_FATAL_FAILURE({
      new_object_id = ApiTestHelper::test_add(
          manager, *inserted_metadata_struct, ErrorCode::OK);
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
    ApiTestHelper::test_exists(manager, object_name, true);
  }

  ptree retrieved_ptree;
  // Get metadata with ptree.
  {
    CALL_TRACE;
    // Test to get metadata with ptree.
    ApiTestHelper::test_get(manager, object_name, ErrorCode::OK,
                            retrieved_ptree);
    // Verifies that the returned metadata is expected one.
    test_metadata->CHECK_METADATA_EXPECTED(inserted_metadata, retrieved_ptree);
  }

  // Get metadata with structure.
  auto metadata_struct = metadata_test->get_structure();
  if (metadata_struct != nullptr) {
    CALL_TRACE;
    // Test to get metadata with structure.
    ApiTestHelper::test_get(manager, object_name, ErrorCode::OK,
                            *metadata_struct);
    // Verifies that the returned metadata is expected one.
    test_metadata->CHECK_METADATA_EXPECTED(inserted_metadata, *metadata_struct);
  }

  {
    CALL_TRACE;
    // Test to remove metadata.
    ApiTestHelper::test_remove(manager, object_name, ErrorCode::OK);
  }

  {
    CALL_TRACE;
    // Test to see if data has been removed.
    ApiTestHelper::test_exists(manager, object_name, false);
    ApiTestHelper::test_get(manager, object_name, ErrorCode::NAME_NOT_FOUND,
                            retrieved_ptree);
  }
}

/**
 * @brief This is a test of the basic paths of metadata management (add,
 *   get_all, remove).
 *   add: metadata is ptree type.
 *   get_all: all object.
 *   remove: object ID as a key.
 */
TEST_P(GetAllTest, test_getall) {
  constexpr const char* const kTestTitle =
      "Add(ptree)-Getall(ptree)-Remove(ID)";

  CALL_TRACE;
  auto& metadata_test = GetParam();

  UTUtils::print(">> Scenario test: ", kTestTitle);
  UTUtils::print("[", typeid(*metadata_test->get_metadata_manager()).name(),
                 "]");

  if (metadata_test->is_test_skip()) {
    GTEST_SKIP();
  }

  auto manager_sptr       = metadata_test->get_metadata_manager();
  auto test_metadata_sptr = metadata_test->get_test_metadata(test_table_id);

  auto manager = manager_sptr.get();
  ASSERT_TRUE(manager);

  {
    CALL_TRACE;
    // Test to initialize the manager.
    ASSERT_NO_FATAL_FAILURE(ApiTestHelper::test_init(manager, ErrorCode::OK));
  }

  // Get the current number of records.
  auto current_record_count = metadata_test->get_record_count();

  std::vector<ptree> metadata_container{};
  {
    CALL_TRACE;
    // Testing in the pre-addition state.
    ApiTestHelper::test_getall(manager, ErrorCode::OK, metadata_container);
    EXPECT_EQ(current_record_count, metadata_container.size());
  }

  auto test_metadata = test_metadata_sptr.get();
  auto [unique_data_creator, create_data_max] =
      metadata_test->get_unique_data_creator();
  std::vector<ptree> test_metadata_list{};
  // Generate test metadata.
  {
    CALL_TRACE;
    EXPECT_NO_FATAL_FAILURE({
      test_metadata_list =
          this->metadata_add(manager, test_metadata->get_metadata_ptree(),
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
    ApiTestHelper::test_getall(manager, ErrorCode::OK, metadata_container);

    int32_t expect_count = current_record_count + create_data_max;
    int32_t actual_count = metadata_container.size();
    ASSERT_EQ(expect_count, actual_count);

    // Inspect the returned metadata.
    this->metadata_compare_all(test_metadata, test_metadata_list,
                               metadata_container);
  }

  // Remove metadata.
  this->metadata_remove(manager, test_metadata_list);
}

/**
 * @brief This is a test of the basic paths of metadata management (add,
 *   get_all, remove).
 *   add: metadata is ptree type.
 *   get_all: all object.
 *   next: all object with ptree type.
 *   remove: object ID as a key.
 */
TEST_P(GetAllTest, test_getall_next) {
  constexpr const char* const kTestTitle =
      "Add(ptree)-Getall/Next(ptree)-Remove(ID)";

  CALL_TRACE;
  auto& metadata_test = GetParam();

  UTUtils::print(">> Scenario test: ", kTestTitle);
  UTUtils::print("[", typeid(*metadata_test->get_metadata_manager()).name(),
                 "]");

  if (metadata_test->is_test_skip()) {
    GTEST_SKIP();
  }

  auto manager_sptr       = metadata_test->get_metadata_manager();
  auto test_metadata_sptr = metadata_test->get_test_metadata(test_table_id);

  auto manager = manager_sptr.get();
  ASSERT_TRUE(manager);

  {
    CALL_TRACE;
    // Test to initialize the manager.
    ASSERT_NO_FATAL_FAILURE(ApiTestHelper::test_init(manager, ErrorCode::OK));
  }

  // Get the current number of records.
  auto current_record_count = metadata_test->get_record_count();

  auto test_metadata = test_metadata_sptr.get();
  auto [unique_data_creator, create_data_max] =
      metadata_test->get_unique_data_creator();
  std::vector<ptree> test_metadata_list{};
  // Generate test metadata.
  {
    CALL_TRACE;
    EXPECT_NO_FATAL_FAILURE({
      test_metadata_list =
          this->metadata_add(manager, test_metadata->get_metadata_ptree(),
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
    ApiTestHelper::test_getall_next(manager, ErrorCode::OK, metadata_container);

    int32_t expect_count = current_record_count + test_metadata_list.size();
    int32_t actual_count = metadata_container.size();
    ASSERT_EQ(expect_count, actual_count);

    // Inspect the returned metadata.
    this->metadata_compare_all(test_metadata, test_metadata_list,
                               metadata_container);
  }

  // Remove metadata.
  this->metadata_remove(manager, test_metadata_list);
}

/**
 * @brief This is a test of the basic paths of metadata management (add, get,
 *   update, remove).
 *   add: metadata is ptree type.
 *   get: object ID as a key.
 *   update: object ID as a key.
 *   remove: object ID as a key.
 */
TEST_P(UpdateTest, test_update) {
  constexpr const char* const kTestTitle =
      "Add(ptree)-Get(ID/ptree)-Update(ID/ptree)-Remove(ID)";

  CALL_TRACE;
  auto& metadata_test = GetParam();

  UTUtils::print(">> Scenario test: ", kTestTitle);
  UTUtils::print("[", typeid(*metadata_test->get_metadata_manager()).name(),
                 "]");

  if (metadata_test->is_test_skip()) {
    GTEST_SKIP();
  }

  auto manager_sptr       = metadata_test->get_metadata_manager();
  auto test_metadata_sptr = metadata_test->get_test_metadata(test_table_id);

  auto manager = manager_sptr.get();
  ASSERT_TRUE(manager);

  // Test to initialize the manager.
  ASSERT_NO_FATAL_FAILURE(ApiTestHelper::test_init(manager, ErrorCode::OK));

  auto test_metadata     = test_metadata_sptr.get();
  auto inserted_metadata = test_metadata->get_metadata_ptree();
  ObjectId new_object_id = INVALID_OBJECT_ID;
  // Add metadata.
  {
    CALL_TRACE;
    EXPECT_NO_FATAL_FAILURE({
      // Test to add metadata.
      new_object_id =
          ApiTestHelper::test_add(manager, inserted_metadata, ErrorCode::OK);
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
      ApiTestHelper::test_get(manager, new_object_id, ErrorCode::OK,
                              before_ptree);

      // Verifies that the returned metadata is expected one.
      test_metadata->CHECK_METADATA_EXPECTED(inserted_metadata, before_ptree);
    });
    // If there is an error in the test, it terminates.
    if (::testing::Test::HasFailure()) {
      FAIL();
    }
  }

  auto update_data_creator = metadata_test->get_update_data_creator();
  // Generate data for updating.
  auto ut_metadata_update = std::move(update_data_creator(before_ptree));

  auto updated_metadata = ut_metadata_update->get_metadata_ptree();
  // Update metadata.
  {
    CALL_TRACE;
    // Set object ID.
    updated_metadata.put(Object::ID, new_object_id);

    // Test to update metadata.
    ApiTestHelper::test_update(manager, new_object_id, updated_metadata,
                               ErrorCode::OK);
  }

  ptree after_ptree;
  // Get metadata with ptree.
  {
    CALL_TRACE;
    // Test to get metadata with ptree.
    ApiTestHelper::test_get(manager, new_object_id, ErrorCode::OK, after_ptree);
    // Verifies that the returned metadata is expected one.
    test_metadata->CHECK_METADATA_EXPECTED(updated_metadata, after_ptree);
  }

  {
    CALL_TRACE;
    // Test to remove metadata.
    ApiTestHelper::test_remove(manager, new_object_id, ErrorCode::OK);
  }
}

}  // namespace manager::metadata::testing
