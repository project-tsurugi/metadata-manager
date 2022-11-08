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
#include "test/metadata/ut_index_metadata.h"

#include "manager/metadata/helper/ptree_helper.h"
#include "manager/metadata/indexes.h"
#include "test/common/ut_utils.h"

namespace manager::metadata::testing {

using boost::property_tree::ptree;

/**
 * @brief Generate metadata for testing.
 */
void UtIndexMetadata::generate_test_metadata() {
  // Generate unique index name.
  std::string index_name = "index_name_" + UTUtils::generate_narrow_uid();

  metadata_struct_->format_version = NOT_INITIALIZED;
  metadata_struct_->generation     = NOT_INITIALIZED;
  metadata_struct_->id             = NOT_INITIALIZED;
  metadata_struct_->name           = index_name;
  metadata_struct_->namespace_name = "namespace_name";
  metadata_struct_->owner_id       = 1001;
  metadata_struct_->acl            = "rawdDxt";
  metadata_struct_->table_id       = table_id_;
  metadata_struct_->access_method =
      static_cast<int64_t>(Index::AccessMethod::DEFAULT);
  metadata_struct_->number_of_key_columns = 1;
  metadata_struct_->is_unique             = false;
  metadata_struct_->is_primary            = false;
  metadata_struct_->keys                  = {1, 2};
  metadata_struct_->keys_id               = {1001, 1002};
  metadata_struct_->options               = {
      static_cast<int64_t>(Index::Direction::ASC_NULLS_LAST),
      static_cast<int64_t>(Index::Direction::DESC_NULLS_FIRST)};

  // Generate ptree from UTTableMetadata fields.
  metadata_ptree_ = metadata_struct_->convert_to_ptree();
}

/**
 * @brief Verifies that the actual table metadata equals expected one.
 * @param expected  [in]  expected table metadata.
 * @param actual    [in]  actual table metadata.
 * @param file      [in]  file name of the caller.
 * @param line      [in]  line number of the caller.
 * @return none.
 */
void UtIndexMetadata::check_metadata_expected(
    const boost::property_tree::ptree& expected,
    const boost::property_tree::ptree& actual, const char* file,
    const int64_t line) const {
  // index metadata id
  auto opt_id_actual = actual.get_optional<ObjectId>(Index::ID);
  ObjectId id_actual = opt_id_actual.value_or(INVALID_OBJECT_ID);
  EXPECT_GT_EX(id_actual, 0, file, line);

  // index metadata ID
  check_expected<std::string>(expected, actual, Index::ID, file, line);
  // index metadata name
  check_expected<std::string>(expected, actual, Index::NAME, file, line);
  // index metadata namespace_name
  check_expected<std::string>(expected, actual, Index::NAMESPACE, file, line);
  // index metadata owner_id
  check_expected<ObjectId>(expected, actual, Index::OWNER_ID, file, line);
  // index metadata acl
  check_expected<std::string>(expected, actual, Index::ACL, file, line);
  // index metadata table_id
  check_expected<ObjectId>(expected, actual, Index::TABLE_ID, file, line);
  // index metadata access_method
  check_expected<int64_t>(expected, actual, Index::ACCESS_METHOD, file, line);
  // index metadata is_unique
  check_expected<bool>(expected, actual, Index::IS_UNIQUE, file, line);
  // index metadata is_primary
  check_expected<bool>(expected, actual, Index::IS_PRIMARY, file, line);
  // index metadata number_of_key_columns
  check_expected<int64_t>(expected, actual, Index::NUMBER_OF_KEY_COLUMNS, file,
                          line);
  // index metadata columns
  check_child_expected(expected, actual, Index::KEYS, file, line);
  // index metadata columns_id
  check_child_expected(expected, actual, Index::KEYS_ID, file, line);
  // index metadata options
  check_child_expected(expected, actual, Index::OPTIONS, file, line);
}

}  // namespace manager::metadata::testing
