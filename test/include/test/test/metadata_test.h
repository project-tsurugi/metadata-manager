/*
 * Copyright 2022 Project Tsurugi.
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
#ifndef TEST_INCLUDE_TEST_TEST_METADATA_TEST_H_
#define TEST_INCLUDE_TEST_TEST_METADATA_TEST_H_

#include <memory>
#include <utility>

#include "manager/metadata/metadata.h"
#include "test/common/dummy_object.h"
#include "test/common/global_test_environment.h"
#include "test/common/ut_utils.h"
#include "test/metadata/ut_metadata.h"

namespace manager::metadata::testing {

using UniqueDataCreator =
    std::function<void(boost::property_tree::ptree&, const int64_t)>;
using UpdateDataCreator = std::function<std::unique_ptr<UtMetadataInterface>(
    const boost::property_tree::ptree&)>;

class MetadataTest {
 public:
  virtual std::unique_ptr<Metadata> get_metadata_manager() const = 0;
  virtual std::unique_ptr<UtMetadataInterface> get_test_metadata(
      ObjectId table_id) const             = 0;
  virtual int64_t get_record_count() const = 0;
  virtual bool is_test_skip() const        = 0;
  virtual Object* get_structure() const { return nullptr; }
  virtual std::pair<UniqueDataCreator, int32_t> get_unique_data_creator()
      const                                                 = 0;
  virtual UpdateDataCreator get_update_data_creator() const = 0;

 protected:
  static constexpr const char* const TEST_DB = "test";
  static constexpr int32_t kDefaultCreateMax = 5;

  /**
   * @brief Function to make `name` a unique value.
   */
  UniqueDataCreator make_default_unique_data_ =
      [](boost::property_tree::ptree& object, const int64_t unique_num) {
        object.put(manager::metadata::Object::NAME,
                   "metadata_name_" + UTUtils::generate_narrow_uid() + "_" +
                       std::to_string(unique_num));
      };

  /**
   * @brief Function to return dummy updated data.
   */
  UpdateDataCreator make_default_update_data_ =
      []([[maybe_unused]] const boost::property_tree::ptree& metadata)
      -> std::unique_ptr<UtMetadataInterface> {
    assert(false);
    return std::make_unique<UtDummyMetadata>();
  };

 private:
  class UtDummyMetadata : public UtMetadataInterface {
   public:
    const Object* get_metadata_struct() const override { return nullptr; }

    boost::property_tree::ptree get_metadata_ptree() const override {
      boost::property_tree::ptree dummy;
      return dummy;
    }

    void check_metadata_expected(
        [[maybe_unused]] const boost::property_tree::ptree& expected,
        [[maybe_unused]] const boost::property_tree::ptree& actual,
        [[maybe_unused]] const char* file,
        [[maybe_unused]] const int64_t line) const override {}
    void check_metadata_expected(
        [[maybe_unused]] const boost::property_tree::ptree& expected,
        [[maybe_unused]] const ::manager::metadata::Object& actual,
        [[maybe_unused]] const char* file,
        [[maybe_unused]] const int64_t line) const override {}
  };
};

}  // namespace manager::metadata::testing

#endif  // TEST_INCLUDE_TEST_TEST_METADATA_TEST_H_
