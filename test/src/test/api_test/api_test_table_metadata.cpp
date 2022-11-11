/*
 * Copyright 2020-2022 tsurugi project.
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

#include "manager/metadata/metadata_factory.h"
#include "test/common/global_test_environment.h"
#include "test/common/ut_utils.h"
#include "test/helper/table_metadata_helper.h"
#include "test/test/api_test_facade.h"

namespace manager::metadata::testing {

using boost::property_tree::ptree;

class ApiTestTableMetadata : public ApiTestFacade<Table, TableMetadataHelper> {
 public:
  ApiTestTableMetadata()
      : ApiTestFacade(get_table_metadata(GlobalTestEnvironment::TEST_DB)) {}

  void SetUp() override { UTUtils::skip_if_connection_not_opened(); }

  static std::unique_ptr<UtMetadataInterface> generate_update_metadata(
      const boost::property_tree::ptree& metadata) {
    // Base metadata.
    auto metadata_base = *(UtTableMetadata(metadata).get_metadata_struct());

    // Copy
    manager::metadata::Table metadata_update;
    metadata_update.convert_from_ptree(metadata);

    // name
    metadata_update.name += "-update";
    // namespace
    metadata_update.namespace_name += "-update";
    // number_of_tuples
    metadata_update.number_of_tuples *= 2;

    // columns
    metadata_update.columns.clear();
    {
      /*
       * Updated-Column[1] <- Added-Columns[2].
       * Updated-Column[2] <- New Column.
       * Updated-Column[3] <- Added-Columns[3].
       */

      // Columns-1: Copy and update added-columns[2].
      {
        manager::metadata::Column column;
        column = metadata_base.columns[1];
        column.name += "-update";
        column.column_number = 1;
        metadata_update.columns.push_back(column);
      }

      // Columns-2: New creation.
      {
        manager::metadata::Column column;
        column.name               = "new-col";
        column.column_number      = 2;
        column.data_type_id       = 13;
        column.varying            = false;
        column.data_length        = {32};
        column.is_not_null        = false;
        column.default_expression = "default-value";
        metadata_update.columns.push_back(column);
      }

      // Columns-3: Copy added-columns[3].
      {
        manager::metadata::Column column;
        column = metadata_base.columns[2];
        metadata_update.columns.push_back(column);
      }
    }

    // constraint
    metadata_update.constraints.clear();
    {
      /*
       * Updated-Constraint[1] <- Added-Constraint[2].
       * Updated-Constraint[2] <- New Constraint.
       */

      // Columns-1: Copy and update added-columns[2].
      {
        manager::metadata::Constraint constraint;
        constraint = metadata_base.constraints[1];
        constraint.name += "-update";
        constraint.columns    = {3};
        constraint.columns_id = {9876};
        metadata_update.constraints.push_back(constraint);
      }

      // Columns-2: New creation.
      {
        manager::metadata::Constraint constraint;
        constraint.name       = "new unique constraint";
        constraint.type       = Constraint::ConstraintType::UNIQUE;
        constraint.columns    = {11};
        constraint.columns_id = {111};
        constraint.index_id   = {1111};
        metadata_update.constraints.push_back(constraint);
      }
    }

    return std::make_unique<UtTableMetadata>(metadata_update);
  }
};

/**
 * @brief Test to add metadata with ptree type and
 *   get it with object ID as key.
 */
TEST_F(ApiTestTableMetadata, test_get_by_id_with_ptree) {
  CALL_TRACE;

  // Execute the test.
  this->test_flow_get_by_id(UtTableMetadata());
}

/**
 * @brief Test to add metadata with structure type and
 *   get it with object ID as key.
 */
TEST_F(ApiTestTableMetadata, test_get_by_id_with_struct) {
  CALL_TRACE;

  // Execute the test.
  this->test_flow_get_by_id_with_struct(UtTableMetadata());
}

/**
 * @brief Test to add metadata with ptree type and
 *   get it with object name as key.
 */
TEST_F(ApiTestTableMetadata, test_get_by_name_with_ptree) {
  CALL_TRACE;

  // Execute the test.
  this->test_flow_get_by_name(UtTableMetadata());
}

/**
 * @brief Test to add metadata with structure type and
 *   get it with object name as key.
 */
TEST_F(ApiTestTableMetadata, test_get_by_name_with_struct) {
  CALL_TRACE;

  // Execute the test.
  this->test_flow_get_by_name_with_struct(UtTableMetadata());
}

/**
 * @brief Test to add new metadata and get_all it in ptree type.
 */
TEST_F(ApiTestTableMetadata, test_getall_with_ptree) {
  CALL_TRACE;

  // Execute the test.
  this->test_flow_getall(UtTableMetadata());
}

/**
 * @brief Test to add new metadata and get_all/next it in ptree type.
 */
TEST_F(ApiTestTableMetadata, test_get_all_table_next) {
  CALL_TRACE;

  // Execute the test.
  this->test_flow_getall_next(UtTableMetadata());
}

/**
 * @brief Test to add new metadata and update.
 */
TEST_F(ApiTestTableMetadata, test_update) {
  CALL_TRACE;

  // Generate test metadata.
  UtTableMetadata ut_metadata;

  // Execute the test.
  this->test_flow_update(ut_metadata, generate_update_metadata);
}

/**
 * @brief This is a test for duplicate table names.
 */
TEST_F(ApiTestTableMetadata, test_duplicate_table_name) {
  CALL_TRACE;

  // Generate tables metadata manager.
  auto managers = get_table_metadata(GlobalTestEnvironment::TEST_DB);

  // Generate test metadata.
  UtTableMetadata ut_metadata;

  auto inserted_metadata = ut_metadata.get_metadata_ptree();

  // Test initialization.
  this->test_init(managers.get(), ErrorCode::OK);

  // add first table metadata.
  ObjectId object_id_1st =
      this->test_add(managers.get(), inserted_metadata, ErrorCode::OK);
  EXPECT_GT(object_id_1st, INVALID_OBJECT_ID);

  // add second table metadata.
  ObjectId object_id_2nd = this->test_add(managers.get(), inserted_metadata,
                                          ErrorCode::ALREADY_EXISTS);
  EXPECT_EQ(object_id_2nd, INVALID_OBJECT_ID);

  // remove table metadata.
  this->test_remove(managers.get(), object_id_1st, ErrorCode::OK);
}

/**
 * @brief This test executes all APIs without initialization.
 */
TEST_F(ApiTestTableMetadata, test_without_initialized) {
  CALL_TRACE;

  // Generate test metadata.
  UtTableMetadata ut_metadata;

  auto inserted_metadata  = ut_metadata.get_metadata_ptree();
  std::string object_name = ut_metadata.get_metadata_struct()->name;
  ObjectId object_id      = -1;

  // Add table metadata.
  {
    // Generate tables metadata manager.
    auto managers = get_table_metadata(GlobalTestEnvironment::TEST_DB);

    object_id =
        this->test_add(managers.get(), inserted_metadata, ErrorCode::OK);
  }

  // Get table metadata by table id with ptree.
  {
    // Generate tables metadata manager.
    auto managers = get_table_metadata(GlobalTestEnvironment::TEST_DB);

    ptree retrieved_metadata;
    this->test_get(managers.get(), object_id, ErrorCode::OK,
                   retrieved_metadata);
  }

  // Get table metadata by table name with ptree.
  {
    // Generate tables metadata manager.
    auto managers = get_table_metadata(GlobalTestEnvironment::TEST_DB);

    ptree retrieved_metadata;
    this->test_get(managers.get(), object_name, ErrorCode::OK,
                   retrieved_metadata);
  }

  // Get table metadata by table id with structure.
  {
    // Generate tables metadata manager.
    auto managers = get_table_metadata(GlobalTestEnvironment::TEST_DB);

    Table retrieved_metadata;
    this->test_get(managers.get(), object_id, ErrorCode::OK,
                   retrieved_metadata);
  }

  // Get table metadata by table name with structure.
  {
    // Generate tables metadata manager.
    auto managers = get_table_metadata(GlobalTestEnvironment::TEST_DB);

    Table retrieved_metadata;
    this->test_get(managers.get(), object_name, ErrorCode::OK,
                   retrieved_metadata);
  }

  // Get all table metadata with ptree.
  {
    // Generate tables metadata manager.
    auto managers = get_table_metadata(GlobalTestEnvironment::TEST_DB);

    std::vector<ptree> container = {};
    // Get all table metadata.
    this->test_getall(managers.get(), ErrorCode::OK, container);
  }

  // Update table metadata.
  {
    // Generate tables metadata manager.
    auto managers = get_table_metadata(GlobalTestEnvironment::TEST_DB);

    // Execute the test.
    this->test_update(managers.get(), object_id, inserted_metadata,
                      ErrorCode::OK);
  }

  // Remove table metadata by table id.
  {
    // Generate tables metadata manager.
    auto managers = get_table_metadata(GlobalTestEnvironment::TEST_DB);

    // Remove table metadata by table id.
    this->test_remove(managers.get(), object_id, ErrorCode::OK);
  }

  // Add table metadata.
  {
    // Generate tables metadata manager.
    auto managers = get_table_metadata(GlobalTestEnvironment::TEST_DB);

    object_id =
        this->test_add(managers.get(), inserted_metadata, ErrorCode::OK);
  }

  // Remove table metadata by table name.
  {
    // Generate tables metadata manager.
    auto managers = get_table_metadata(GlobalTestEnvironment::TEST_DB);

    // Remove table metadata by table id.
    this->test_remove(managers.get(), object_name, ErrorCode::OK);
  }
}

}  // namespace manager::metadata::testing
