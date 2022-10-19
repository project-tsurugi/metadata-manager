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
#include "test/helper/json/index_metadata_helper_json.h"

#include <gtest/gtest.h>

#include "manager/metadata/dao/json/index_dao_json.h"
#include "test/common/json/global_test_environment_json.h"
#include "test/common/json/ut_utils_json.h"

namespace manager::metadata::testing {

#define EXPECT_EQ_T(expected, actual, text)                 \
  if (expected != actual) std::cout << "[" << text << "] "; \
  EXPECT_EQ(expected, actual)

namespace storage = manager::metadata::db::json;

using boost::property_tree::ptree;
using manager::metadata::db::IndexDaoJson;

/**
 * @brief Get the number of records in the current index metadata.
 * @return Current number of records.
 */
std::int64_t IndexMetadataHelper::get_record_count() {
  // generate index metadata manager.
  auto indexes = std::make_unique<Indexes>(GlobalTestEnvironment::TEST_DB);

  // initialize index metadata manager.
  ErrorCode error = indexes->init();

  std::vector<ptree> container = {};
  if (error == ErrorCode::OK) {
    // get all index metadata.
    indexes->get_all(container);
  }

  return container.size();
}

/**
 * @brief Generate index metadata.
 * @param table_id        [in]  table id.
 * @param index_metadata  [out] index metadata used as test data.
 */
void IndexMetadataHelper::generate_test_metadata(
    const ObjectId& table_id,
    std::unique_ptr<UTIndexMetadata>& index_metadata) {
  // generate unique index name.
  std::string index_name = "index_name" + std::to_string(time(NULL));

  index_metadata = std::make_unique<UTIndexMetadata>();

  // generate name.
  index_metadata->name = index_name;

  // generate namespace.
  index_metadata->namespace_name = "namespace_name";

  // generate owner_id
  index_metadata->owner_id = 1001;

  // generate acl.
  index_metadata->acl = "rawdDxt";

  // generate table_id.
  index_metadata->table_id = table_id;

  // generate access_method.
  index_metadata->access_method =
      static_cast<int64_t>(Index::AccessMethod::DEFAULT);

  // generate number_of_key_columns.
  index_metadata->number_of_key_columns = 1;

  // generate columns.
  index_metadata->is_unique = false;

  // generate columns.
  index_metadata->is_primary = false;

  // generate columns.
  index_metadata->columns.push_back(1);
  index_metadata->columns.push_back(2);

  // generate columns id.
  index_metadata->columns_id.push_back(2001);
  index_metadata->columns_id.push_back(2002);

  // generate options.
  index_metadata->options.push_back(
      static_cast<int64_t>(Index::Direction::ASC_NULLS_LAST));
  index_metadata->options.push_back(
      static_cast<int64_t>(Index::Direction::DESC_NULLS_FIRST));

  // generate ptree from UTTableMetadata fields.
  index_metadata->generate_ptree();
}

/**
 * @brief Add one new index metadata to index metadata table.
 * @param indexes         [in]  indexes metadata manager object.
 * @param index_metadata  [in]  new index metadata.
 * @param index_id        [out] (optional) index id returned from the api to add
 *   new index metadata.
 * @return none.
 */
void IndexMetadataHelper::add(const Indexes* indexes,
                              const boost::property_tree::ptree& index_metadata,
                              ObjectIdType* index_id) {
  UTUtils::print("-- add index metadata in ptree --");
  UTUtils::print(" " + UTUtils::get_tree_string(index_metadata));

  ObjectIdType ret_id_value = INVALID_VALUE;
  // add index metadata.
  ErrorCode error = indexes->add(index_metadata, &ret_id_value);

  ASSERT_EQ(ErrorCode::OK, error);
  ASSERT_GT(ret_id_value, 0);

  UTUtils::print(" >> new index_id: ", ret_id_value);

  if (index_id != nullptr) {
    *index_id = ret_id_value;
  }
}

void IndexMetadataHelper::add(const Metadata* indexes,
                              const boost::property_tree::ptree& index_metadata,
                              ObjectIdType* index_id) {
  UTUtils::print("-- add index metadata in ptree --");
  UTUtils::print(" " + UTUtils::get_tree_string(index_metadata));

  ObjectIdType ret_id_value = INVALID_VALUE;
  // add index metadata.
  ErrorCode error = indexes->add(index_metadata, &ret_id_value);

  ASSERT_EQ(ErrorCode::OK, error);
  ASSERT_GT(ret_id_value, 0);

  UTUtils::print(" >> new index_id: ", ret_id_value);

  if (index_id != nullptr) {
    *index_id = ret_id_value;
  }
}

/**
 * @brief Add one new index metadata to index metadata table.
 * @param indexes         [in]  indexes metadata manager object.
 * @param index_metadata  [in]  new index metadata.
 * @param index_id        [out] (optional) index id returned from the api to add
 *   new index metadata.
 * @return none.
 */
void IndexMetadataHelper::add(const Metadata* indexes,
                              const Index& index_metadata,
                              ObjectIdType* index_id) {
  UTUtils::print("-- add index metadata in struct --");
  UTUtils::print(" " +
                 UTUtils::get_tree_string(index_metadata.convert_to_ptree()));

  ObjectIdType ret_id_value = INVALID_VALUE;
  // add index metadata.
  ErrorCode error = indexes->add(index_metadata, &ret_id_value);

  ASSERT_EQ(ErrorCode::OK, error);
  ASSERT_GT(ret_id_value, 0);

  UTUtils::print(" >> new index_id: ", ret_id_value);

  if (index_id != nullptr) {
    *index_id = ret_id_value;
  }
}

/**
 * @brief Remove one index metadata to index metadata table.
 * @param indexes   [in]  indexes metadata manager object.
 * @param index_id  [in]  index id of remove index metadata.
 * @return none.
 */
void IndexMetadataHelper::remove(const Indexes* indexes,
                                 const ObjectIdType index_id) {
  UTUtils::print("-- remove index metadata --");
  UTUtils::print(" index_id: ", index_id);

  // remove index metadata.
  ErrorCode error = indexes->remove(index_id);
  ASSERT_EQ(ErrorCode::OK, error);
}

void IndexMetadataHelper::remove(const Metadata* indexes,
                                 const ObjectIdType index_id) {
  UTUtils::print("-- remove index metadata --");
  UTUtils::print(" index_id: ", index_id);

  // remove index metadata.
  ErrorCode error = indexes->remove(index_id);
  ASSERT_EQ(ErrorCode::OK, error);
}

/**
 * @brief Remove one index metadata to index metadata table.
 * @param indexes    [in]  indexes metadata manager object.
 * @param index_name [in]  index name of remove index metadata.
 * @param removed_id [in]  (optional) Index ID of the removed index metadata.
 * @return none.
 */
void IndexMetadataHelper::remove(const Metadata* indexes,
                                 std::string_view index_name,
                                 ObjectIdType* removed_id) {
  UTUtils::print("-- remove index metadata --");
  UTUtils::print(" index_name: ", index_name);

  ObjectIdType ret_id_value = INVALID_VALUE;
  // remove index metadata.
  ErrorCode error = indexes->remove(index_name, &ret_id_value);
  ASSERT_EQ(ErrorCode::OK, error);
  ASSERT_GT(ret_id_value, 0);

  UTUtils::print(" >> removed index_id: ", ret_id_value);

  if (removed_id != nullptr) {
    *removed_id = ret_id_value;
  }
}

/**
 * @brief Verifies that the actual index metadata equals expected one.
 * @param expected  [in]  expected index metadata.
 * @param actual    [in]  actual index metadata.
 * @return none.
 */
void IndexMetadataHelper::check_metadata_expected(
    const boost::property_tree::ptree& expected,
    const boost::property_tree::ptree& actual) {
  // index metadata id
  auto id_actual = actual.get_optional<ObjectIdType>(Index::ID);
  if (id_actual) {
    EXPECT_GT(id_actual.get(), static_cast<ObjectIdType>(0));
  } else {
    UTUtils::print("Object ID not found in the actual ptree.");
    EXPECT_GT(INVALID_OBJECT_ID, static_cast<ObjectIdType>(0));
  }

  // index metadata ID
  check_expected<std::string>(expected, actual, Index::ID);
  // index metadata name
  check_expected<std::string>(expected, actual, Index::NAME);
  // index metadata namespace_name
  check_expected<std::string>(expected, actual, Index::NAMESPACE);
  // index metadata owner_id
  check_expected<ObjectId>(expected, actual, Index::OWNER_ID);
  // index metadata acl
  check_expected<std::string>(expected, actual, Index::ACL);
  // index metadata table_id
  check_expected<ObjectId>(expected, actual, Index::TABLE_ID);
  // index metadata access_method
  check_expected<int64_t>(expected, actual, Index::ACCESS_METHOD);
  // index metadata is_unique
  check_expected<bool>(expected, actual, Index::IS_UNIQUE);
  // index metadata is_primary
  check_expected<bool>(expected, actual, Index::IS_PRIMARY);
  // index metadata number_of_key_columns
  check_expected<int64_t>(expected, actual, Index::NUMBER_OF_KEY_COLUMNS);

  // index metadata columns
  check_child_expected(expected, actual, Index::KEYS);
  // index metadata columns_id
  check_child_expected(expected, actual, Index::KEYS_ID);
  // index metadata options
  check_child_expected(expected, actual, Index::OPTIONS);
}

/**
 * @brief Verifies that the actual metadata equals expected one.
 * @param expected   [in]  expected metadata.
 * @param actual     [in]  actual metadata.
 * @param meta_name  [in]  name of metadata table.
 * @return none.
 */
void IndexMetadataHelper::check_child_expected(
    const boost::property_tree::ptree& expected,
    const boost::property_tree::ptree& actual, const char* meta_name) {
  auto o_expected = expected.get_child_optional(meta_name);
  auto o_actual   = actual.get_child_optional(meta_name);

  if (o_expected && o_actual) {
    auto expected_value = UTUtils::get_tree_string(o_expected.value());
    auto actual_value   = UTUtils::get_tree_string(o_actual.value());
    EXPECT_EQ_T(expected_value, actual_value, meta_name);
  } else {
    auto expected_value =
        (o_expected ? UTUtils::get_tree_string(o_expected.value()) : "<null>");
    auto actual_value =
        (o_actual ? UTUtils::get_tree_string(o_actual.value()) : "<null>");
    EXPECT_EQ_T(expected_value, actual_value, meta_name);
  }
}

/**
 * @brief Verifies that the actual metadata equals expected one.
 * @param expected   [in]  expected column metadata.
 * @param actual     [in]  actual column metadata.
 * @param meta_name  [in]  name of column metadata table.
 * @return none.
 */
template <typename T>
void IndexMetadataHelper::check_expected(
    const boost::property_tree::ptree& expected,
    const boost::property_tree::ptree& actual, const char* meta_name) {
  auto expected_value = expected.get_optional<T>(meta_name);
  auto actual_value   = actual.get_optional<T>(meta_name);

  if (expected_value && actual_value) {
    EXPECT_EQ_T(expected_value.value(), actual_value.value(), meta_name);
  } else {
    auto expected_value =
        expected.get_optional<std::string>(meta_name).value_or("<null>");
    auto actual_value =
        actual.get_optional<std::string>(meta_name).value_or("<null>");
    EXPECT_EQ_T(expected_value, actual_value, meta_name);
  }
}

}  // namespace manager::metadata::testing
