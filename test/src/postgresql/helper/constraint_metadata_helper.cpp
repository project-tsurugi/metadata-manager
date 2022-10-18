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
#include "test/postgresql/helper/constraint_metadata_helper.h"

#include <gtest/gtest.h>
#include <libpq-fe.h>

#include <boost/format.hpp>

#include "manager/metadata/common/config.h"
#include "manager/metadata/dao/postgresql/constraints_dao_pg.h"
#include "manager/metadata/dao/postgresql/dbc_utils_pg.h"
#include "test/postgresql/global_test_environment.h"
#include "test/postgresql/utility/ut_utils.h"

namespace manager::metadata::testing {

#define EXPECT_EQ_T(expected, actual, text)                 \
  if (expected != actual) std::cout << "[" << text << "] "; \
  EXPECT_EQ(expected, actual)

namespace storage = manager::metadata::db::postgresql;

using boost::property_tree::ptree;
using manager::metadata::db::postgresql::ConstraintsDAO;

/**
 * @brief Get the number of records in the current constraint metadata.
 * @return Current number of records.
 */
std::int64_t ConstraintMetadataHelper::get_record_count() {
  PGconn* connection = PQconnectdb(Config::get_connection_string().c_str());

  boost::format statement = boost::format("SELECT COUNT(*) FROM %s.%s") % storage::SCHEMA_NAME %
                            ConstraintsDAO::kTableName;
  PGresult* res = PQexec(connection, statement.str().c_str());

  std::int64_t res_val;
  storage::DbcUtils::str_to_integral(PQgetvalue(res, 0, 0), res_val);

  PQclear(res);
  PQfinish(connection);

  return res_val;
}

/**
 * @brief Generate constraint metadata.
 * @param table_id             [in]  table id.
 * @param constraint_metadata  [out] constraint metadata used as test data.
 */
void ConstraintMetadataHelper::generate_test_metadata(
    const ObjectId& table_id, std::unique_ptr<UTConstraintMetadata>& constraint_metadata) {
  // generate unique table name.
  std::string constraint_name = "constraint_name" + std::to_string(time(NULL));

  constraint_metadata =
      std::make_unique<UTConstraintMetadata>(constraint_name, Constraint::ConstraintType::UNIQUE);

  // generate table_id.
  constraint_metadata->table_id = table_id;

  // generate columns.
  constraint_metadata->columns_list.push_back(1);
  constraint_metadata->columns_list.push_back(2);

  // generate columns id.
  constraint_metadata->columns_id_list.push_back(1001);
  constraint_metadata->columns_id_list.push_back(2001);

  // generate index id.
  constraint_metadata->index_id = 3;

  // generate expression.
  constraint_metadata->expression = "none";

  // generate ptree from UTTableMetadata fields.
  constraint_metadata->generate_ptree();
}

/**
 * @brief Add one new constraint metadata to constraint metadata table.
 * @param constraints          [in]  constraints metadata manager object.
 * @param constraint_metadata  [in]  new constraint metadata.
 * @param constraint_id        [out] (optional) constraint id returned from the api to add
 *   new constraint metadata.
 * @return none.
 */
void ConstraintMetadataHelper::add(const Metadata* constraints,
                                   const boost::property_tree::ptree& constraint_metadata,
                                   ObjectIdType* constraint_id) {
  UTUtils::print("-- add constraint metadata in ptree --");
  UTUtils::print(" " + UTUtils::get_tree_string(constraint_metadata));

  ObjectIdType ret_id_value = INVALID_VALUE;
  // add table metadata.
  ErrorCode error = constraints->add(constraint_metadata, &ret_id_value);

  ASSERT_EQ(ErrorCode::OK, error);
  ASSERT_GT(ret_id_value, 0);

  UTUtils::print(" >> new constraint_id: ", ret_id_value);

  if (constraint_id != nullptr) {
    *constraint_id = ret_id_value;
  }
}

/**
 * @brief Add one new constraint metadata to constraint metadata table.
 * @param constraints          [in]  constraints metadata manager object.
 * @param constraint_metadata  [in]  new constraint metadata.
 * @param constraint_id        [out] (optional) constraint id returned from the api to add
 *   new constraint metadata.
 * @return none.
 */
void ConstraintMetadataHelper::add(const Metadata* constraints,
                                   const Constraint& constraint_metadata,
                                   ObjectIdType* constraint_id) {
  UTUtils::print("-- add constraint metadata in struct --");
  UTUtils::print(" " + UTUtils::get_tree_string(constraint_metadata.convert_to_ptree()));

  ObjectIdType ret_id_value = INVALID_VALUE;
  // add table metadata.
  ErrorCode error = constraints->add(constraint_metadata, &ret_id_value);

  ASSERT_EQ(ErrorCode::OK, error);
  ASSERT_GT(ret_id_value, 0);

  UTUtils::print(" >> new constraint_id: ", ret_id_value);

  if (constraint_id != nullptr) {
    *constraint_id = ret_id_value;
  }
}

/**
 * @brief Remove one constraint metadata to constraint metadata table.
 * @param constraints    [in]  constraints metadata manager object.
 * @param constraint_id  [in]  constraint id of remove constraint metadata.
 * @return none.
 */
void ConstraintMetadataHelper::remove(const Metadata* constraints,
                                      const ObjectIdType constraint_id) {
  UTUtils::print("-- remove constraint metadata --");
  UTUtils::print(" constraint_id: ", constraint_id);

  // remove table metadata.
  ErrorCode error = constraints->remove(constraint_id);
  ASSERT_EQ(ErrorCode::OK, error);
}

/**
 * @brief Verifies that the actual table metadata equals expected one.
 * @param (expected)  [in]  expected table metadata.
 * @param (actual)    [in]  actual table metadata.
 * @return none.
 */
void ConstraintMetadataHelper::check_metadata_expected(const boost::property_tree::ptree& expected,
                                                       const boost::property_tree::ptree& actual) {
  // constraint metadata id
  auto id_actual = actual.get<ObjectIdType>(Constraint::ID);
  EXPECT_GT(id_actual, static_cast<ObjectIdType>(0));

  // constraint metadata table id
  check_expected<ObjectIdType>(expected, actual, Constraint::TABLE_ID);
  // constraint name
  check_expected<std::string>(expected, actual, Constraint::NAME);
  // constraint type
  check_expected<ObjectIdType>(expected, actual, Constraint::TYPE);
  // constraint column numbers
  check_child_expected(expected, actual, Constraint::COLUMNS);
  // constraint column IDs
  check_child_expected(expected, actual, Constraint::COLUMNS_ID);
  // constraint index id
  check_expected<ObjectIdType>(expected, actual, Constraint::INDEX_ID);
  // constraint expression
  check_expected<std::string>(expected, actual, Constraint::EXPRESSION);
}

/**
 * @brief Verifies that the actual metadata equals expected one.
 * @param (expected)   [in]  expected metadata.
 * @param (actual)     [in]  actual metadata.
 * @param (meta_name)  [in]  name of metadata table.
 * @return none.
 */
void ConstraintMetadataHelper::check_child_expected(const boost::property_tree::ptree& expected,
                                                    const boost::property_tree::ptree& actual,
                                                    const char* meta_name) {
  auto o_expected = expected.get_child_optional(meta_name);
  auto o_actual   = actual.get_child_optional(meta_name);

  if (o_expected && o_actual) {
    auto expected_value = UTUtils::get_tree_string(o_expected.value());
    auto actual_value   = UTUtils::get_tree_string(o_actual.value());
    EXPECT_EQ_T(expected_value, actual_value, meta_name);
  } else if (o_expected) {
    EXPECT_EQ_T(o_expected.value().empty(), !o_actual.is_initialized(), meta_name);
  } else if (o_actual) {
    EXPECT_EQ_T(!o_expected.is_initialized(), o_actual.value().empty(), meta_name);
  } else {
    EXPECT_EQ_T(o_expected.is_initialized(), o_actual.is_initialized(), meta_name);
  }
}

/**
 * @brief Verifies that the actual metadata equals expected one.
 * @param (expected)   [in]  expected column metadata.
 * @param (actual)     [in]  actual column metadata.
 * @param (meta_name)  [in]  name of column metadata table.
 * @return none.
 */
template <typename T>
void ConstraintMetadataHelper::check_expected(const boost::property_tree::ptree& expected,
                                              const boost::property_tree::ptree& actual,
                                              const char* meta_name) {
  auto value_expected = expected.get_optional<T>(meta_name);
  auto value_actual   = actual.get_optional<T>(meta_name);

  if (value_expected && value_actual) {
    EXPECT_EQ(value_expected.value(), value_actual.value());
  } else {
    if (value_expected) {
      const auto& value_expected = expected.get<std::string>(meta_name);
      EXPECT_EQ_T(value_expected.empty(), !value_actual.is_initialized(), meta_name);
    } else if (value_actual) {
      const auto& value_actual = actual.get<std::string>(meta_name);
      EXPECT_EQ_T(!value_expected.is_initialized(), value_actual.empty(), meta_name);
    } else {
      EXPECT_EQ_T(value_expected.is_initialized(), value_actual.is_initialized(), meta_name);
    }
  }
}

}  // namespace manager::metadata::testing
