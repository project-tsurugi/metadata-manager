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
#ifndef TEST_INCLUDE_TEST_TEST_COLUMN_STATISTICS_TEST_H_
#define TEST_INCLUDE_TEST_TEST_COLUMN_STATISTICS_TEST_H_

#include <memory>
#include <string>
#include <utility>

#include "manager/metadata/metadata_factory.h"
#include "test/metadata/ut_column_statistics.h"
#include "test/test/metadata_test.h"

#if defined(STORAGE_POSTGRESQL)
#include "test/helper/postgresql/metadata_helper_pg.h"
#endif

namespace manager::metadata::testing {

class StatisticsMetadataTest : public MetadataTest {
 public:
  /**
   * @brief Returns the metadata management object under test.
   * @return std::unique_ptr<Metadata> - metadata management object.
   */
  std::unique_ptr<Metadata> get_metadata_manager() const override {
    return get_statistics_ptr(TEST_DB);
  }

  /**
   * @brief Return metadata for testing.
   * @param table_id ID of the table for testing.
   * @return std::unique_ptr<UtMetadataInterface> - test metadata
   */
  std::unique_ptr<UtMetadataInterface> get_test_metadata(
      ObjectId table_id) const override {
    return std::make_unique<UtColumnStatistics>(table_id);
  }

  /**
   * @brief Get the current number of records.
   * @return int64_t - number of records.
   */
  int64_t get_record_count() const override {
#if defined(STORAGE_POSTGRESQL)
    MetadataHelperPg helper(kTableName);
    return helper.get_record_count();
#elif defined(STORAGE_JSON)
    return 0L;
#endif
  }

  /**
   * @brief Tests whether the test should be skipped.
   * @retval true - Skip the test.
   * @retval false - Run the test.
   */
  bool is_test_skip() const override {
    bool result = false;
    if (UTUtils::is_json()) {
      UTUtils::print("  Skipped: Metadata storage is Json.");
      result = true;
    }
    return result;
  }

  /**
   * @brief Returns a creator function that creates update data.
   * @return Object* - structure.
   */
  Object* get_structure() const override { return nullptr; }

  /**
   * @brief Returns a creator function that creates unique data.
   * @return std::pair<UniqueDataCreator, int32_t> -
   *   callback function to create unique data,
   *   number of data to be created.
   */
  std::pair<UniqueDataCreator, int32_t> get_unique_data_creator()
      const override {
    return {make_unique_data_, kStatisticsCreateMax};
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
  static constexpr const char* const kTableName = "statistics";
#endif
  static constexpr const int32_t kStatisticsCreateMax = 2;

  /**
   * @brief Function to make `name` and `columnNumber` a unique value.
   */
  UniqueDataCreator make_unique_data_ = [](boost::property_tree::ptree& object,
                                           const int64_t unique_num) {
    std::string metadata_name = "metadata_name_" +
                                UTUtils::generate_narrow_uid() + "_" +
                                std::to_string(unique_num);
    object.put(Statistics::NAME, metadata_name);
    object.put(Statistics::COLUMN_NUMBER, unique_num);
  };
};

}  // namespace manager::metadata::testing

#endif  // TEST_INCLUDE_TEST_TEST_COLUMN_STATISTICS_TEST_H_
