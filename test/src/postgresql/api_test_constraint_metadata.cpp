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
#include "manager/metadata/constraints.h"
#include "manager/metadata/error_code.h"
#include "manager/metadata/metadata_factory.h"
#include "manager/metadata/tables.h"
#include "test/common/ut_utils.h"
#include "test/environment/global_test_environment.h"
#include "test/helper/constraint_metadata_helper.h"
#include "test/helper/table_metadata_helper.h"
#include "test/metadata/ut_table_metadata.h"

namespace {

manager::metadata::ObjectId table_id = 0;

}  // namespace

namespace manager::metadata::testing {

using boost::property_tree::ptree;

class ApiTestConstraintMetadata : public ::testing::Test {
  void SetUp() override {
    if (!global->is_open()) {
      GTEST_SKIP_("metadata repository is not started.");
    }

    // Get table metadata for testing.
    UTTableMetadata testdata_table_metadata =
        *(global->testdata_table_metadata.get());
    // Copy table metadata.
    ptree new_table = testdata_table_metadata.tables;
    // Change to a unique table name.
    std::string new_table_name = new_table.get<std::string>(Tables::NAME) +
                                 "_ApiTestConstraintMetadata" +
                                 std::to_string(__LINE__);
    new_table.put(Tables::NAME, new_table_name);

    // Add table metadata.
    TableMetadataHelper::add_table(new_table, &table_id);
  }

  void TearDown() override {
    if (global->is_open()) {
      // Remove table metadata.
      TableMetadataHelper::remove_table(table_id);
    }
  }
};

/**
 * @brief Test that adds metadata for a new constraint and retrieves it using
 * the constraint id as the key with the ptree type.
 * - add:
 *     patterns that obtain a constraint id.
 * - get:
 *     constraint id as a key.
 * - remove:
 *     constraint id as a key.
 */
TEST_F(ApiTestConstraintMetadata, add_get_constraint_metadata) {
  std::unique_ptr<UTConstraintMetadata> constraint_metadata;

  // generate metadata.
  ConstraintMetadataHelper::generate_test_metadata(table_id,
                                                   constraint_metadata);
  ptree new_constraints = constraint_metadata->constraints_metadata;
  // set table id.
  new_constraints.put(Constraint::TABLE_ID, table_id);

  // generate constraint metadata manager.
  auto constraints =
      std::make_unique<Constraints>(GlobalTestEnvironment::TEST_DB);
  ErrorCode error = constraints->init();
  ASSERT_EQ(ErrorCode::OK, error);

  ObjectId ret_id_value = -1;
  // add constraint metadata.
  ConstraintMetadataHelper::add(constraints.get(), new_constraints,
                                &ret_id_value);
  // set constraint id.
  new_constraints.put(Constraint::ID, ret_id_value);

  UTUtils::print("-- get constraint metadata --");
  {
    ptree constraint_metadata_inserted;
    // get constraint metadata by constraint id.
    error = constraints->get(ret_id_value, constraint_metadata_inserted);
    EXPECT_EQ(ErrorCode::OK, error);

    UTUtils::print(UTUtils::get_tree_string(constraint_metadata_inserted));

    // verifies that the returned constraint metadata is expected one.
    ConstraintMetadataHelper::check_metadata_expected(
        new_constraints, constraint_metadata_inserted);
  }

  // remove constraint metadata by constraint id.
  ConstraintMetadataHelper::remove(constraints.get(), ret_id_value);
}

/**
 * @brief Test that adds metadata for a new constraint and retrieves it using
 * the constraint id as the key with the ptree type.
 * - add:
 *     patterns that do not obtain a constraint id.
 * - get_all:
 * - remove:
 *     constraint id as a key.
 */
TEST_F(ApiTestConstraintMetadata, add_get_all_constraint_metadata) {
  static constexpr const int32_t kTestConstraintCount = 5;

  std::unique_ptr<UTConstraintMetadata> constraint_metadata;
  auto base_constraint_count = ConstraintMetadataHelper::get_record_count();

  // generate constraint metadata manager.
  auto constraints =
      std::make_unique<Constraints>(GlobalTestEnvironment::TEST_DB);
  ErrorCode error = constraints->init();
  ASSERT_EQ(ErrorCode::OK, error);

  // generate metadata.
  ConstraintMetadataHelper::generate_test_metadata(table_id,
                                                   constraint_metadata);
  ptree new_constraints = constraint_metadata->constraints_metadata;
  // set table id.
  new_constraints.put(Constraint::TABLE_ID, table_id);

  // add constraint metadata.
  ObjectId constraint_ids[kTestConstraintCount];
  for (auto& constraint_id : constraint_ids) {
    constraint_id = 0;
    ConstraintMetadataHelper::add(constraints.get(), new_constraints,
                                  &constraint_id);
  }

  std::vector<boost::property_tree::ptree> container = {};
  // get constraint metadata.
  error = constraints->get_all(container);
  EXPECT_EQ(ErrorCode::OK, error);
  EXPECT_EQ(base_constraint_count + kTestConstraintCount, container.size());

  UTUtils::print("-- get all constraint metadata --");
  {
    ptree expected_constraints = new_constraints;
    for (int32_t index = 0; index < kTestConstraintCount; index++) {
      ptree actual_constraints = container[base_constraint_count + index];
      UTUtils::print(UTUtils::get_tree_string(actual_constraints));

      // set constraint id.
      expected_constraints.put(Tables::ID, constraint_ids[index]);
      // verifies that the returned table metadata is expected one.
      ConstraintMetadataHelper::check_metadata_expected(expected_constraints,
                                                        actual_constraints);
    }
  }

  // cleanup
  UTUtils::print("-- remove constraint metadata --");
  {
    for (auto& constraint_id : constraint_ids) {
      UTUtils::print(" constraint_id: ", constraint_id);
      constraints->remove(constraint_id);
      EXPECT_EQ(ErrorCode::OK, error);
    }
  }
}

/**
 * @brief Test removes constraint metadata.
 * - add:
 *     patterns that do not obtain a constraint id.
 * - remove:
 *     constraint id as a key.
 */
TEST_F(ApiTestConstraintMetadata, remove_constraint_metadata) {
  std::unique_ptr<UTConstraintMetadata> constraint_metadata;

  // generate metadata.
  ConstraintMetadataHelper::generate_test_metadata(table_id,
                                                   constraint_metadata);
  ptree new_constraints = constraint_metadata->constraints_metadata;
  // set table id.
  new_constraints.put(Constraint::TABLE_ID, table_id);

  // generate constraint metadata manager.
  auto constraints =
      std::make_unique<Constraints>(GlobalTestEnvironment::TEST_DB);
  ErrorCode error = constraints->init();
  ASSERT_EQ(ErrorCode::OK, error);

  ObjectId ret_id_value = -1;
  // add constraint metadata.
  ConstraintMetadataHelper::add(constraints.get(), new_constraints,
                                &ret_id_value);

  // remove constraint metadata by constraint id.
  ConstraintMetadataHelper::remove(constraints.get(), ret_id_value);

  UTUtils::print("-- get constraint metadata --");
  {
    ptree constraint_metadata_removed;
    // get constraint metadata by constraint id.
    error = constraints->get(ret_id_value, constraint_metadata_removed);
    EXPECT_EQ(ErrorCode::ID_NOT_FOUND, error);

    UTUtils::print(UTUtils::get_tree_string(constraint_metadata_removed));
  }

  UTUtils::print("-- re-remove constraint metadata --");
  {
    ptree constraint_metadata_removed;
    // get constraint metadata by constraint id.
    error = constraints->remove(ret_id_value);
    EXPECT_EQ(ErrorCode::ID_NOT_FOUND, error);
  }
}

/**
 * @brief Test for incorrect constraint IDs.
 */
TEST_F(ApiTestConstraintMetadata, all_invalid_parameter) {
  // generate constraint metadata manager.
  auto constraints =
      std::make_unique<Constraints>(GlobalTestEnvironment::TEST_DB);
  ErrorCode error = constraints->init();
  ASSERT_EQ(ErrorCode::OK, error);

  ObjectId table_id      = -1;
  ObjectId constraint_id = -1;

  // add constraint metadata by constraint id.
  UTUtils::print("-- add constraint metadata --");
  {
    ptree constraint_metadata;
    error = constraints->add(constraint_metadata);
    EXPECT_EQ(ErrorCode::INVALID_PARAMETER, error);

    constraint_metadata.put(Constraint::TABLE_ID, table_id);
    error = constraints->add(constraint_metadata);
    EXPECT_EQ(ErrorCode::INVALID_PARAMETER, error);
  }

  // get constraint metadata by constraint id.
  UTUtils::print("-- get constraint metadata --");
  {
    ptree constraint_metadata;
    error = constraints->get(constraint_id, constraint_metadata);
    EXPECT_EQ(ErrorCode::ID_NOT_FOUND, error);
  }

  // remove constraint metadata by constraint id.
  UTUtils::print("-- remove constraint metadata --");
  {
    error = constraints->remove(constraint_id);
    EXPECT_EQ(ErrorCode::ID_NOT_FOUND, error);
  }
}

/**
 * @brief happy test for all constraint metadata getting.
 */
TEST_F(ApiTestConstraintMetadata, get_all_constraint_metadata_empty) {
  // get base count
  std::int64_t base_table_count = ConstraintMetadataHelper::get_record_count();

  // generate constraint metadata manager.
  auto constraints =
      std::make_unique<Constraints>(GlobalTestEnvironment::TEST_DB);
  ErrorCode error = constraints->init();
  EXPECT_EQ(ErrorCode::OK, error);

  std::vector<boost::property_tree::ptree> container = {};
  // get constraint metadata.
  error = constraints->get_all(container);
  EXPECT_EQ(ErrorCode::OK, error);
  EXPECT_EQ(base_table_count, container.size());
}

/**
 * @brief happy test for adding, getting and removing
 *   one new table metadata without initialization of all api.
 */
TEST_F(ApiTestConstraintMetadata, add_get_remove_without_initialized) {
  std::unique_ptr<UTConstraintMetadata> constraint_metadata;

  // generate metadata.
  ConstraintMetadataHelper::generate_test_metadata(table_id,
                                                   constraint_metadata);
  ptree new_constraints = constraint_metadata->constraints_metadata;
  // set table id.
  new_constraints.put(Constraint::TABLE_ID, table_id);

  ObjectId object_id = -1;
  UTUtils::print("-- add constraint metadata --");
  {
    // generate constraint metadata manager.
    auto constraints =
        std::make_unique<Constraints>(GlobalTestEnvironment::TEST_DB);
    // add constraint metadata.
    ErrorCode error = constraints->add(new_constraints, &object_id);
    EXPECT_EQ(ErrorCode::OK, error);
  }

  UTUtils::print("-- get constraint metadata --");
  {
    ptree constraint_metadata_inserted;
    // generate constraint metadata manager.
    auto constraints =
        std::make_unique<Constraints>(GlobalTestEnvironment::TEST_DB);
    // get constraint metadata by constraint id.
    ErrorCode error = constraints->get(object_id, constraint_metadata_inserted);
    EXPECT_EQ(ErrorCode::OK, error);
  }

  UTUtils::print("-- get_all constraint metadata --");
  {
    std::vector<boost::property_tree::ptree> container = {};
    // generate constraint metadata manager.
    auto constraints =
        std::make_unique<Constraints>(GlobalTestEnvironment::TEST_DB);
    // get constraint metadata by constraint id.
    ErrorCode error = constraints->get_all(container);
    EXPECT_EQ(ErrorCode::OK, error);
  }

  UTUtils::print("-- remove constraint metadata --");
  {
    // generate constraint metadata manager.
    auto constraints =
        std::make_unique<Constraints>(GlobalTestEnvironment::TEST_DB);
    // remove constraint metadata by constraint id.
    ErrorCode error = constraints->remove(object_id);
    EXPECT_EQ(ErrorCode::OK, error);
  }
}

/**
 * @brief happy test for removing one new table metadata by table name.
 */
TEST_F(ApiTestConstraintMetadata, unsupported_apis) {
  // generate constraint metadata manager.
  auto constraints =
      std::make_unique<Constraints>(GlobalTestEnvironment::TEST_DB);
  ErrorCode error = constraints->init();
  EXPECT_EQ(ErrorCode::OK, error);

  std::string object_name = "dummy-name";
  boost::property_tree::ptree object;
  ObjectId object_id;

  // get() with name specification.
  error = constraints->get(object_name, object);
  EXPECT_EQ(ErrorCode::UNKNOWN, error);

  // update().
  error = constraints->update(object_id, object);
  EXPECT_EQ(ErrorCode::UNKNOWN, error);

  // remove() with name specification.
  error = constraints->remove(object_name, &object_id);
  EXPECT_EQ(ErrorCode::UNKNOWN, error);
}

/**
 * @brief Test that adds metadata for a new constraint and retrieves it using
 * the constraint id as the key with the ptree type.
 * - add:
 *     struct: patterns that obtain a constraint id.
 * - get:
 *     struct: constraint id as a key.
 *     ptree : constraint id as a key.
 * - remove:
 *     constraint id as a key.
 */
TEST_F(ApiTestConstraintMetadata, add_get_constraint_metadata_object_ptree) {
  std::unique_ptr<UTConstraintMetadata> constraint_metadata;

  // generate metadata.
  ConstraintMetadataHelper::generate_test_metadata(table_id,
                                                   constraint_metadata);
  Constraint new_constraints;
  new_constraints.convert_from_ptree(constraint_metadata->constraints_metadata);
  // set table id.
  new_constraints.table_id = table_id;

  // generate constraint metadata manager.
  auto constraints =
      manager::metadata::get_constraints_ptr(GlobalTestEnvironment::TEST_DB);
  ErrorCode error = constraints->init();
  ASSERT_EQ(ErrorCode::OK, error);

  ObjectId ret_id_value = -1;
  // add constraint metadata.
  ConstraintMetadataHelper::add(constraints.get(), new_constraints,
                                &ret_id_value);
  // set constraint id.
  new_constraints.id = ret_id_value;

  UTUtils::print("-- get constraint metadata in ptree --");
  {
    ptree get_constraint_metadata;
    // get constraint metadata by constraint id.
    error = constraints->get(ret_id_value, get_constraint_metadata);
    EXPECT_EQ(ErrorCode::OK, error);

    UTUtils::print(UTUtils::get_tree_string(get_constraint_metadata));

    // verifies that the returned constraint metadata is expected one.
    ConstraintMetadataHelper::check_metadata_expected(
        new_constraints.convert_to_ptree(), get_constraint_metadata);
  }

  UTUtils::print("-- get constraint metadata in object --");
  {
    Constraint get_constraint_metadata;
    // get constraint metadata by constraint id.
    error = constraints->get(ret_id_value, get_constraint_metadata);
    EXPECT_EQ(ErrorCode::OK, error);

    UTUtils::print(
        UTUtils::get_tree_string(get_constraint_metadata.convert_to_ptree()));

    // verifies that the returned constraint metadata is expected one.
    ConstraintMetadataHelper::check_metadata_expected(
        new_constraints.convert_to_ptree(),
        get_constraint_metadata.convert_to_ptree());
  }

  // remove constraint metadata by constraint id.
  ConstraintMetadataHelper::remove(constraints.get(), ret_id_value);
}

/**
 * @brief Test that adds metadata for a new constraint and retrieves it using
 * the constraint id as the key with the ptree type.
 * @brief Test that adds metadata for a new constraint and retrieves it using
 * the constraint id as the key with the ptree type.
 * - add:
 *     ptree: patterns that obtain a constraint id.
 * - get:
 *     struct: constraint id as a key.
 *     ptree : constraint id as a key.
 * - remove:
 *     constraint id as a key.
 */
TEST_F(ApiTestConstraintMetadata, add_get_constraint_metadata_ptree_object) {
  std::unique_ptr<UTConstraintMetadata> constraint_metadata;

  // generate metadata.
  ConstraintMetadataHelper::generate_test_metadata(table_id,
                                                   constraint_metadata);
  ptree new_constraints = constraint_metadata->constraints_metadata;
  // set table id.
  new_constraints.put(Constraint::TABLE_ID, table_id);

  // generate constraint metadata manager.
  auto constraints =
      manager::metadata::get_constraints_ptr(GlobalTestEnvironment::TEST_DB);
  ErrorCode error = constraints->init();
  ASSERT_EQ(ErrorCode::OK, error);

  ObjectId ret_id_value = -1;
  // add constraint metadata.
  ConstraintMetadataHelper::add(constraints.get(), new_constraints,
                                &ret_id_value);
  // set constraint id.
  new_constraints.put(Constraint::ID, ret_id_value);

  UTUtils::print("-- get constraint metadata in ptree --");
  {
    ptree get_constraint_metadata;
    // get constraint metadata by constraint id.
    error = constraints->get(ret_id_value, get_constraint_metadata);
    EXPECT_EQ(ErrorCode::OK, error);

    UTUtils::print(UTUtils::get_tree_string(get_constraint_metadata));

    // verifies that the returned constraint metadata is expected one.
    ConstraintMetadataHelper::check_metadata_expected(new_constraints,
                                                      get_constraint_metadata);
  }

  UTUtils::print("-- get constraint metadata in struct --");
  {
    Constraint get_constraint_metadata;
    // get constraint metadata by constraint id.
    error = constraints->get(ret_id_value, get_constraint_metadata);
    EXPECT_EQ(ErrorCode::OK, error);

    UTUtils::print(
        UTUtils::get_tree_string(get_constraint_metadata.convert_to_ptree()));

    // verifies that the returned constraint metadata is expected one.
    ConstraintMetadataHelper::check_metadata_expected(
        new_constraints, get_constraint_metadata.convert_to_ptree());
  }

  // remove constraint metadata by constraint id.
  ConstraintMetadataHelper::remove(constraints.get(), ret_id_value);
}

}  // namespace manager::metadata::testing
