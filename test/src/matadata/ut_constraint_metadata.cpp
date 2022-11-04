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
#include "test/metadata/ut_constraint_metadata.h"

#include <gtest/gtest.h>

#include "manager/metadata/constraints.h"
#include "manager/metadata/helper/ptree_helper.h"
#include "test/common/ut_utils.h"

namespace manager::metadata::testing {

/**
 * @brief Generate metadata for testing.
 */
void UtConstraintMetadata::generate_test_metadata() {
  // Generate unique constraint name.
  std::string constraint_name =
      "constraint_name_" + UTUtils::generate_narrow_uid();

  metadata_struct_->format_version = NOT_INITIALIZED;
  metadata_struct_->generation     = NOT_INITIALIZED;
  metadata_struct_->id             = NOT_INITIALIZED;
  metadata_struct_->name           = constraint_name;
  metadata_struct_->table_id       = table_id_;
  metadata_struct_->type           = Constraint::ConstraintType::UNIQUE;
  metadata_struct_->columns        = {1, 2};
  metadata_struct_->columns_id     = {1001, 2001};
  metadata_struct_->index_id       = 3;
  metadata_struct_->expression     = "none";

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
void UtConstraintMetadata::check_metadata_expected(
    const boost::property_tree::ptree& expected,
    const boost::property_tree::ptree& actual, const char* file,
    const int64_t line) const {
  // Constraint metadata id
  auto opt_id_actual = actual.get_optional<ObjectId>(Constraint::ID);
  ObjectId id_actual = opt_id_actual.value_or(INVALID_OBJECT_ID);
  EXPECT_GT_EX(id_actual, 0, file, line);

  // Constraint metadata table id
  check_expected<ObjectId>(expected, actual, Constraint::TABLE_ID, file, line);
  // Constraint name
  check_expected<std::string>(expected, actual, Constraint::NAME, file, line);
  // Constraint type
  check_expected<int64_t>(expected, actual, Constraint::TYPE, file, line);
  // Constraint column numbers
  check_child_expected(expected, actual, Constraint::COLUMNS, file, line);
  // Constraint column IDs
  check_child_expected(expected, actual, Constraint::COLUMNS_ID, file, line);
  // Constraint index id
  check_expected<ObjectId>(expected, actual, Constraint::INDEX_ID, file, line);
  // Constraint expression
  check_expected<std::string>(expected, actual, Constraint::EXPRESSION, file,
                              line);
}

}  // namespace manager::metadata::testing
