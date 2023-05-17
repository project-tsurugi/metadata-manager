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
#ifndef TEST_INCLUDE_TEST_TEST_TABLE_METADATA_TEST_H_
#define TEST_INCLUDE_TEST_TEST_TABLE_METADATA_TEST_H_

#include <memory>
#include <utility>

#include "manager/metadata/metadata_factory.h"
#include "test/metadata/ut_table_metadata.h"
#include "test/test/metadata_test.h"

#if defined(STORAGE_POSTGRESQL)
#include "test/helper/postgresql/metadata_helper_pg.h"
#elif defined(STORAGE_JSON)
#include "test/helper/json/metadata_helper_json.h"
#endif

namespace manager::metadata::testing {

class TableMetadataTest : public MetadataTest {
 public:
  TableMetadataTest() : metadata_struct_(std::make_unique<Table>()) {}

  /**
   * @brief Returns the metadata management object under test.
   * @return std::unique_ptr<Metadata> - metadata management object.
   */
  std::unique_ptr<Metadata> get_metadata_manager() const override {
    return get_tables_ptr(TEST_DB);
  }

  /**
   * @brief Return metadata for testing.
   * @param table_id ID of the table for testing.
   * @return std::unique_ptr<UtMetadataInterface> - test metadata
   */
  std::unique_ptr<UtMetadataInterface> get_test_metadata(
      [[maybe_unused]] ObjectId table_id) const override {
    return std::make_unique<UtTableMetadata>();
  }

  /**
   * @brief Get the current number of records.
   * @return int64_t - number of records.
   */
  int64_t get_record_count() const override {
#if defined(STORAGE_POSTGRESQL)
    MetadataHelperPg helper(kTableName);
#elif defined(STORAGE_JSON)
    MetadataHelperJson helper(kMetadataName, kRootNode);
#endif

    return helper.get_record_count();
  }

  /**
   * @brief Tests whether the test should be skipped.
   * @retval true - Skip the test.
   * @retval false - Run the test.
   */
  bool is_test_skip() const override {
    return false;
  }

  /**
   * @brief Returns a creator function that creates update data.
   * @return Object* - structure.
   */
  Object* get_structure() const override { return metadata_struct_.get(); }

  /**
   * @brief Returns a creator function that creates unique data.
   * @return std::pair<UniqueDataCreator, int32_t> -
   *   callback function to create unique data,
   *   number of data to be created.
   */
  std::pair<UniqueDataCreator, int32_t> get_unique_data_creator()
      const override {
    return {MetadataTest::make_default_unique_data_,
            MetadataTest::kDefaultCreateMax};
  }

  /**
   * @brief Returns a creator function that creates update data.
   * @return UpdateDataCreator - creator function.
   */
  UpdateDataCreator get_update_data_creator() const override {
    return make_update_data_;
  }

 private:
#if defined(STORAGE_POSTGRESQL)
  static constexpr const char* const kTableName = "tsurugi_class";
#elif defined(STORAGE_JSON)
  static constexpr const char* const kMetadataName = "tables";
  static constexpr const char* const kRootNode = "tables";
#endif

  std::unique_ptr<Table> metadata_struct_;

  /**
   * @brief Function to return updated data.
   */
  UpdateDataCreator make_update_data_ =
      []([[maybe_unused]] const boost::property_tree::ptree& metadata)
      -> std::unique_ptr<UtMetadataInterface> {
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
  };
};

}  // namespace manager::metadata::testing

#endif  // TEST_INCLUDE_TEST_TEST_TABLE_METADATA_TEST_H_
