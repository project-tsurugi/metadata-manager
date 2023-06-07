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
#ifndef TEST_INCLUDE_TEST_TEST_CONSTRAINT_METADATA_TEST_H_
#define TEST_INCLUDE_TEST_TEST_CONSTRAINT_METADATA_TEST_H_

#include <memory>
#include <utility>

#include "manager/metadata/metadata_factory.h"
#include "test/metadata/ut_constraint_metadata.h"
#include "test/test/metadata_test.h"

#if defined(STORAGE_POSTGRESQL)
#include "test/helper/postgresql/metadata_helper_pg.h"
#elif defined(STORAGE_JSON)
#include "test/helper/json/metadata_helper_json.h"
#endif

namespace manager::metadata::testing {

class ConstraintMetadataTest : public MetadataTest {
 public:
  ConstraintMetadataTest() : metadata_struct_(std::make_unique<Constraint>()) {}

  /**
   * @brief Returns the metadata management object under test.
   * @return std::unique_ptr<Metadata> - metadata management object.
   */
  std::unique_ptr<Metadata> get_metadata_manager() const override {
    return get_constraints_ptr(TEST_DB);
  }

  /**
   * @brief Return metadata for testing.
   * @param table_id ID of the table for testing.
   * @return std::unique_ptr<UtMetadataInterface> - test metadata
   */
  std::unique_ptr<UtMetadataInterface> get_test_metadata(
      ObjectId table_id) const override {
    return std::make_unique<UtConstraintMetadata>(table_id);
  }

  /**
   * @brief Get the current number of records.
   * @return int64_t - number of records.
   */
  int64_t get_record_count() const override {
#if defined(STORAGE_POSTGRESQL)
    MetadataHelperPg helper(kTableName);
#elif defined(STORAGE_JSON)
    MetadataHelperJson helper(kMetadataName, kRootNode, kSubNode);
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
    return MetadataTest::make_default_update_data_;
  }

 private:
#if defined(STORAGE_POSTGRESQL)
  static constexpr const char* const kTableName = "tsurugi_constraint";
#elif defined(STORAGE_JSON)
  static constexpr const char* const kMetadataName = "tables";
  static constexpr const char* const kRootNode = "tables";
  static constexpr const char* const kSubNode = "constraints";
#endif

  std::unique_ptr<Constraint> metadata_struct_;
};

}  // namespace manager::metadata::testing

#endif  // TEST_INCLUDE_TEST_TEST_CONSTRAINT_METADATA_TEST_H_
