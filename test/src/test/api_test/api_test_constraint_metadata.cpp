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

#include "manager/metadata/metadata_factory.h"
#include "test/common/global_test_environment.h"
#include "test/common/ut_utils.h"
#include "test/helper/constraint_metadata_helper.h"
#include "test/helper/table_metadata_helper.h"
#include "test/metadata/ut_constraint_metadata.h"

namespace {

manager::metadata::ObjectId table_id = 0;

}  // namespace

namespace manager::metadata::testing {

using boost::property_tree::ptree;

// class ApiTestConstraintMetadata : public ::testing::Test {
class ApiTestConstraintMetadata : public ApiTest {
 public:
  ApiTestConstraintMetadata()
      : ApiTest(get_constraint_metadata(GlobalTestEnvironment::TEST_DB)),
        metadata_struct_(std::make_unique<::manager::metadata::Constraint>()) {}

  int64_t get_record_count() const override {
    return ConstraintMetadataHelper::get_record_count();
  }

  ::manager::metadata::Constraint* get_structure() const override {
    return metadata_struct_.get();
  }

  void SetUp() override {
    UTUtils::skip_if_connection_not_opened();

    UTUtils::print(">> gtest::SetUp()");

    // Change to a unique table name.
    std::string table_name =
        "ApiTestConstraintMetadata_" + UTUtils::generate_narrow_uid();

    // Add table metadata.
    TableMetadataHelper::add_table(table_name, &table_id);
  }

  void TearDown() override {
    if (global->is_open()) {
      UTUtils::print(">> gtest::TearDown()");

      // Remove table metadata.
      TableMetadataHelper::remove_table(table_id);
    }
  }

 private:
  std::unique_ptr<::manager::metadata::Constraint> metadata_struct_;
};

/**
 * @brief Test to add new metadata and get it in ptree type
 *   with object ID as key.
 */
TEST_F(ApiTestConstraintMetadata, test_add_get_remove_by_id_with_ptree) {
  SCOPED_TRACE("");

  // Generate test metadata.
  UTConstraintMetadata ut_metadata(table_id);
  ut_metadata.generate_test_metadata();

  // Execute the test.
  this->test_add_get_remove_by_id(&ut_metadata);
}

/**
 * @brief Test to add new metadata and get it in ptree type
 *   with object ID as key.
 */
TEST_F(ApiTestConstraintMetadata, test_add_get_remove_by_id_with_struct) {
  SCOPED_TRACE("");

  // Generate test metadata.
  UTConstraintMetadata ut_metadata(table_id);
  ut_metadata.generate_test_metadata();

  // Execute the test.
  this->test_add_get_remove_by_id_with_struct(&ut_metadata);
}

/**
 * @brief Test to add new metadata and get it in ptree type
 *   with object name as key.
 */
TEST_F(ApiTestConstraintMetadata, test_add_get_remove_by_name_with_ptree) {
  SCOPED_TRACE("");

  // Generate constraints metadata manager.
  auto managers = get_constraint_metadata(GlobalTestEnvironment::TEST_DB);

  auto object_name = "dummy_name";
  ptree retrieved_metadata;

  // Execute the test.
  this->test_get(managers.get(), object_name, retrieved_metadata,
                 ErrorCode::UNKNOWN);
  this->test_remove(managers.get(), object_name, ErrorCode::UNKNOWN);
}

/**
 * @brief Test to add new metadata and get it in ptree type
 *   with object name as key.
 */
TEST_F(ApiTestConstraintMetadata, test_add_get_remove_by_name_with_struct) {
  SCOPED_TRACE("");

  // Generate constraints metadata manager.
  auto managers = get_constraint_metadata(GlobalTestEnvironment::TEST_DB);

  auto object_name = "dummy_name";
  Constraint retrieved_metadata;

  // Execute the test.
  this->test_get(managers.get(), object_name, retrieved_metadata,
                 ErrorCode::UNKNOWN);
  this->test_remove(managers.get(), object_name, ErrorCode::UNKNOWN);
}

/**
 * @brief Test to add new metadata and get_all it in ptree type.
 */
TEST_F(ApiTestConstraintMetadata, test_add_getall_remove) {
  SCOPED_TRACE("");

  // Generate test metadata.
  UTConstraintMetadata ut_metadata(table_id);
  ut_metadata.generate_test_metadata();

  // Execute the test.
  this->test_add_getall_remove(&ut_metadata);
}

/**
 * @brief Test for incorrect constraint IDs.
 */
TEST_F(ApiTestConstraintMetadata, test_not_found) {
  SCOPED_TRACE("");

  // Generate constraints metadata manager.
  auto managers = get_constraint_metadata(GlobalTestEnvironment::TEST_DB);

  // Test to initialize the manager.
  this->test_init(managers.get());

  // Generate test metadata.
  UTConstraintMetadata ut_metadata(table_id);
  ut_metadata.generate_test_metadata();

  ObjectId invalid_id      = INVALID_OBJECT_ID;
  std::string invalid_name = "invalid_name";

  // get constraint metadata by constraint id/name.
  {
    ptree retrieved_metadata;
    this->test_get(managers.get(), invalid_id, retrieved_metadata,
                   ErrorCode::ID_NOT_FOUND);
    EXPECT_TRUE(retrieved_metadata.empty());

    this->test_get(managers.get(), invalid_name, retrieved_metadata,
                   ErrorCode::UNKNOWN);
    EXPECT_TRUE(retrieved_metadata.empty());
  }

  // remove constraint metadata by constraint id/name.
  {
    this->test_remove(managers.get(), invalid_id, ErrorCode::ID_NOT_FOUND);
    this->test_remove(managers.get(), invalid_name, ErrorCode::UNKNOWN);
  }
}

/**
 * @brief Test for incorrect constraint IDs.
 */
TEST_F(ApiTestConstraintMetadata, test_invalid_parameter) {
  SCOPED_TRACE("");

  // Generate constraints metadata manager.
  auto managers = get_constraint_metadata(GlobalTestEnvironment::TEST_DB);

  // Test to initialize the manager.
  this->test_init(managers.get());

  ObjectId invalid_id = INVALID_OBJECT_ID;

  // add constraint metadata by constraint id.
  {
    ptree constraint_metadata;
    this->test_add(managers.get(), constraint_metadata,
                   ErrorCode::INVALID_PARAMETER);

    constraint_metadata.put(Constraint::TABLE_ID, invalid_id);
    this->test_add(managers.get(), constraint_metadata,
                   ErrorCode::INVALID_PARAMETER);
  }
}

/**
 * @brief happy test for adding, getting and removing
 *   one new table metadata without initialization of all api.
 */
TEST_F(ApiTestConstraintMetadata, test_without_initialized) {
  SCOPED_TRACE("");

  // Generate test metadata.
  UTConstraintMetadata ut_metadata(table_id);
  ut_metadata.generate_test_metadata();

  auto inserted_metadata = ut_metadata.get_metadata_ptree();
  ObjectId object_id     = -1;
  UTUtils::print("-- add constraint metadata --");
  {
    // Generate constraints metadata manager.
    auto managers = get_constraint_metadata(GlobalTestEnvironment::TEST_DB);
    // Test to add metadata.
    object_id = this->test_add(managers_.get(), inserted_metadata);
  }

  UTUtils::print("-- get constraint metadata --");
  {
    // Generate constraints metadata manager.
    auto managers = get_constraint_metadata(GlobalTestEnvironment::TEST_DB);

    ptree retrieved_metadata;
    // get constraint metadata by constraint id.
    this->test_get(managers_.get(), object_id, retrieved_metadata);
  }

  UTUtils::print("-- get_all constraint metadata --");
  {
    // Generate constraints metadata manager.
    auto managers = get_constraint_metadata(GlobalTestEnvironment::TEST_DB);

    std::vector<ptree> container = {};
    // get constraint metadata by constraint id.
    container = this->test_getall(managers_.get());
  }

  UTUtils::print("-- remove constraint metadata --");
  {
    // Generate constraints metadata manager.
    auto managers = get_constraint_metadata(GlobalTestEnvironment::TEST_DB);

    // remove constraint metadata by constraint id.
    this->test_remove(managers_.get(), object_id);
  }
}

/**
 * @brief happy test for removing one new table metadata by table name.
 */
TEST_F(ApiTestConstraintMetadata, test_unsupported_apis) {
  SCOPED_TRACE("");

  // Generate constraints metadata manager.
  auto managers = get_constraint_metadata(GlobalTestEnvironment::TEST_DB);

  // Test to initialize the manager.
  this->test_init(managers.get());

  std::string object_name = "dummy-name";
  boost::property_tree::ptree metadata;
  ObjectId object_id = INVALID_OBJECT_ID;

  // get() with name specification.
  this->test_get(managers.get(), object_name, metadata, ErrorCode::UNKNOWN);

  // update().
  this->test_update(managers.get(), object_id, metadata, ErrorCode::UNKNOWN);

  // remove() with name specification.
  this->test_remove(managers.get(), object_name, ErrorCode::UNKNOWN);
}

}  // namespace manager::metadata::testing
