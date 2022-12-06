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

#include <memory>
#include <string>

#include <boost/foreach.hpp>
#include <boost/property_tree/json_parser.hpp>
#include "jwt-cpp/jwt.h"

#include "manager/metadata/common/config.h"
#include "manager/metadata/common/utility.h"
#include "manager/metadata/error_code.h"
#include "manager/metadata/roles.h"
#include "manager/metadata/tables.h"
#include "test/global_test_environment.h"
#include "test/helper/foreign_table_helper.h"
#include "test/helper/role_metadata_helper.h"
#include "test/helper/table_metadata_helper.h"
#include "test/utility/ut_table_metadata.h"
#include "test/utility/ut_utils.h"

namespace {

namespace foreign_table_1 {

constexpr std::string_view table_name = "tsurugi_api_ut_foreign_table_1";
std::int32_t table_id                 = 0;
std::int32_t foreign_table_id         = 0;

}  // namespace foreign_table_1

namespace foreign_table_2 {

constexpr std::string_view table_name = "tsurugi_api_ut_foreign_table_2";
std::int32_t table_id                 = 0;
std::int32_t foreign_table_id         = 0;

}  // namespace foreign_table_2

namespace foreign_table_3 {

constexpr std::string_view table_name = "tsurugi_api_ut_foreign_table_3";
std::int32_t table_id                 = 0;
std::int32_t foreign_table_id         = 0;

}  // namespace foreign_table_3

namespace role_1 {

constexpr std::string_view role_name = "tsurugi_api_ut_tables_user_1";
std::int32_t role_id                 = 0;

}  // namespace role_1

namespace role_2 {

constexpr std::string_view role_name = "tsurugi_api_ut_tables_user_2";
std::int32_t role_id                 = 0;

}  // namespace role_2

namespace role_3 {

constexpr std::string_view role_name = "tsurugi_api_ut_tables_user_3";
std::int32_t role_id                 = 0;

}  // namespace role_3

}  // namespace

namespace manager::metadata::testing {

using boost::property_tree::ptree;

class ApiTestTableMetadata : public ::testing::Test {
  void SetUp() override {
    if (!global->is_open()) {
      GTEST_SKIP_("metadata repository is not started.");
    }
  }
};

class ApiTestTableAcls : public ::testing::Test {
 public:
  static void SetUpTestCase() {
    if (global->is_open()) {
      UTUtils::print(">> gtest::SetUpTestCase()");

      boost::format statement;

      // create dummy data for ROLE.
      role_1::role_id = RoleMetadataHelper::create_role(role_1::role_name, "");
      role_2::role_id = RoleMetadataHelper::create_role(role_2::role_name, "");
      role_3::role_id = RoleMetadataHelper::create_role(role_3::role_name, "");

      UTUtils::print(">> Role [", role_1::role_id, " : ", role_1::role_name,
                     "]");
      UTUtils::print(">> Role [", role_2::role_id, " : ", role_2::role_name,
                     "]");
      UTUtils::print(">> Role [", role_3::role_id, " : ", role_3::role_name,
                     "]");

      // (role-1) create dummy data for TABLE.
      foreign_table_1::table_id = ForeignTableHelper::create_table(
          foreign_table_1::table_name, role_1::role_name, "SELECT");
      foreign_table_2::table_id = ForeignTableHelper::create_table(
          foreign_table_2::table_name, role_1::role_name,
          "SELECT,INSERT,UPDATE,DELETE");
      foreign_table_3::table_id = ForeignTableHelper::create_table(
          foreign_table_3::table_name, role_1::role_name, "");

      // (role-2) grant dummy data for TABLE.
      ForeignTableHelper::grant_table(foreign_table_1::table_name,
                                      role_2::role_name,
                                      "SELECT,INSERT,UPDATE,DELETE");
      ForeignTableHelper::grant_table(foreign_table_2::table_name,
                                      role_2::role_name, "SELECT");
      ForeignTableHelper::grant_table(foreign_table_3::table_name,
                                      role_2::role_name, "SELECT,UPDATE");

      // create dummy data for pg_foreign_table.
      foreign_table_1::foreign_table_id =
          ForeignTableHelper::insert_foreign_table(foreign_table_1::table_name);
      foreign_table_2::foreign_table_id =
          ForeignTableHelper::insert_foreign_table(foreign_table_2::table_name);
      foreign_table_3::foreign_table_id =
          ForeignTableHelper::insert_foreign_table(foreign_table_3::table_name);

      // create dummy data for table metadata.
      TableMetadataHelper::add_table(foreign_table_1::table_name);
      TableMetadataHelper::add_table(foreign_table_2::table_name);
      TableMetadataHelper::add_table(foreign_table_3::table_name);

      UTUtils::print("<< gtest::SetUpTestCase()");
    }
  }

  static void TearDownTestCase() {
    if (global->is_open()) {
      UTUtils::print(">> gtest::TearDownTestCase()");

      // remove dummy data for table metadata.
      TableMetadataHelper::remove_table(foreign_table_1::table_name);
      TableMetadataHelper::remove_table(foreign_table_2::table_name);
      TableMetadataHelper::remove_table(foreign_table_3::table_name);

      // remove dummy data for pg_foreign_table.
      ForeignTableHelper::delete_foreign_table(
          foreign_table_1::foreign_table_id);
      ForeignTableHelper::delete_foreign_table(
          foreign_table_2::foreign_table_id);
      ForeignTableHelper::delete_foreign_table(
          foreign_table_3::foreign_table_id);

      // remove dummy data for TABLE.
      ForeignTableHelper::drop_table(foreign_table_1::table_name);
      ForeignTableHelper::drop_table(foreign_table_2::table_name);
      ForeignTableHelper::drop_table(foreign_table_3::table_name);

      // remove dummy data for ROLE.
      RoleMetadataHelper::drop_role(role_1::role_name);
      RoleMetadataHelper::drop_role(role_2::role_name);
      RoleMetadataHelper::drop_role(role_3::role_name);

      UTUtils::print("<< gtest::TearDownTestCase()");
    }
  }

  void SetUp() override { UTUtils::skip_if_connection_not_opened(); }
  void TearDown() override {}

  class TokenBuilder {
   public:
    TokenBuilder() : unset_user_name_(true) {}
    explicit TokenBuilder(std::string_view user_name)
        : unset_user_name_(false), user_name_(user_name) {}

    TokenBuilder& unset_issuer_at() {
      unset_issuer_at_ = true;
      return *this;
    }
    TokenBuilder& unset_expire_at() {
      unset_expire_at_ = true;
      return *this;
    }
    TokenBuilder& set_expires(int32_t expires) {
      expires_ = expires;
      return *this;
    }
    TokenBuilder& set_issuer(std::string_view issuer) {
      issuer_ = issuer;
      return *this;
    }
    TokenBuilder& set_audience(std::string_view audience) {
      audience_ = audience;
      return *this;
    }
    TokenBuilder& set_token_type(std::string_view token_type) {
      token_type_ = token_type;
      return *this;
    }

    std::string generate() {
      // Setting up data for token.
      auto jwt_builder = jwt::create()
                             .set_type("JWT")
                             .set_issuer(issuer_)
                             .set_audience(audience_)
                             .set_subject(token_type_);
      // Set the issuer date.
      if (!unset_issuer_at_) {
        jwt_builder.set_issued_at(std::chrono::system_clock::now());
      }
      // Set the expiration date.
      if (!unset_expire_at_) {
        jwt_builder.set_expires_at(std::chrono::system_clock::now() +
                                   std::chrono::seconds{expires_});
      }
      // Set the authentication user name.
      if (!unset_user_name_) {
        jwt_builder.set_payload_claim("tsurugi/auth/name",
                                      jwt::claim(user_name_));
      }

      // Cryptographic algorithms.
      auto algorithm = jwt::algorithm::hs256{Config::get_jwt_secret_key()};
      // Sign the JWT token.
      auto signed_token = jwt_builder.sign(algorithm);

      UTUtils::print(">> [", signed_token.c_str(), "]");

      return std::string(signed_token.c_str());
    }

   private:
    bool unset_issuer_at_ = false;
    bool unset_expire_at_ = false;
    bool unset_user_name_;
    std::string user_name_;
    int32_t expires_        = 300;
    std::string issuer_     = Config::get_jwt_issuer();
    std::string audience_   = Config::get_jwt_audience();
    std::string token_type_ = "access";
  };
};

/**
 * @brief Test that adds metadata for a new table and retrieves
 *   it using the table name as the key with the ptree type.
 */
TEST_F(ApiTestTableMetadata, add_get_table_metadata_by_table_name) {
  // prepare test data for adding table metadata.
  UTTableMetadata testdata_table_metadata =
      *(global->testdata_table_metadata.get());
  ptree new_table            = testdata_table_metadata.tables;
  std::string new_table_name = new_table.get<std::string>(Tables::NAME) +
                               "_ApiTestTableMetadata" +
                               std::to_string(__LINE__);
  new_table.put(Tables::NAME, new_table_name);

  // add table metadata.
  ObjectIdType ret_table_id = -1;
  TableMetadataHelper::add_table(new_table, &ret_table_id);
  new_table.put(Tables::ID, ret_table_id);

  // get table metadata by table name.
  auto tables     = std::make_unique<Tables>(GlobalTestEnvironment::TEST_DB);
  ErrorCode error = tables->init();
  EXPECT_EQ(ErrorCode::OK, error);

  ptree table_metadata_inserted;
  error = tables->get(new_table_name, table_metadata_inserted);
  EXPECT_EQ(ErrorCode::OK, error);

  UTUtils::print("-- get table metadata --");
  UTUtils::print(UTUtils::get_tree_string(table_metadata_inserted));

  // verifies that the returned table metadata is expected one.
  TableMetadataHelper::check_table_metadata_expected(new_table,
                                                     table_metadata_inserted);

  // remove table metadata.
  TableMetadataHelper::remove_table(ret_table_id);
}

/**
 * @brief happy test for adding one new table metadata without returned table
 * id and getting it by table name.
 */
TEST_F(ApiTestTableMetadata,
       add_without_returned_table_id_get_table_metadata_by_table_name) {
  // prepare test data for adding table metadata.
  UTTableMetadata testdata_table_metadata =
      *(global->testdata_table_metadata.get());
  ptree new_table            = testdata_table_metadata.tables;
  std::string new_table_name = new_table.get<std::string>(Tables::NAME) +
                               "_ApiTestTableMetadata" +
                               std::to_string(__LINE__);
  new_table.put(Tables::NAME, new_table_name);

  // add table metadata.
  auto tables = std::make_unique<Tables>(GlobalTestEnvironment::TEST_DB);

  ErrorCode error = tables->init();
  EXPECT_EQ(ErrorCode::OK, error);

  error = tables->add(new_table);
  EXPECT_EQ(ErrorCode::OK, error);

  UTUtils::print("-- add table metadata --");
  UTUtils::print(UTUtils::get_tree_string(new_table));

  // get table metadata by table name.
  ptree table_metadata_inserted;
  error = tables->get(new_table_name, table_metadata_inserted);
  EXPECT_EQ(ErrorCode::OK, error);

  UTUtils::print("-- get table metadata --");
  UTUtils::print(UTUtils::get_tree_string(table_metadata_inserted));

  // verifies that the returned table metadata is expected one.
  new_table.put(Tables::ID,
                table_metadata_inserted.get<ObjectIdType>(Tables::ID));
  TableMetadataHelper::check_table_metadata_expected(new_table,
                                                     table_metadata_inserted);

  // remove table metadata.
  TableMetadataHelper::remove_table(new_table_name);
}

/**
 * @brief happy test for adding two same table metadata
 *   and getting them by table name.
 */
TEST_F(ApiTestTableMetadata, get_two_table_metadata_by_table_name) {
  // prepare test data for adding table metadata.
  UTTableMetadata testdata_table_metadata =
      *(global->testdata_table_metadata.get());
  ptree new_table            = testdata_table_metadata.tables;
  std::string new_table_name = new_table.get<std::string>(Tables::NAME) +
                               "_ApiTestTableMetadata" +
                               std::to_string(__LINE__);
  new_table.put(Tables::NAME, new_table_name);

  auto tables = std::make_unique<Tables>(GlobalTestEnvironment::TEST_DB);

  ErrorCode error = tables->init();
  EXPECT_EQ(ErrorCode::OK, error);

  // prepare variables for returned value from add api.
  std::vector<ObjectIdType> ret_table_id;
  ret_table_id.emplace_back(-1);
  ret_table_id.emplace_back(-1);

  // add first table metadata.
  error = tables->add(new_table, &ret_table_id[0]);
  EXPECT_EQ(ErrorCode::OK, error);
  EXPECT_GT(ret_table_id[0], 0);

  // add second table metadata.
  error = tables->add(new_table, &ret_table_id[1]);
  EXPECT_EQ(ErrorCode::ALREADY_EXISTS, error);
  EXPECT_EQ(ret_table_id[1], -1);

  UTUtils::print("-- add table metadata --");
  UTUtils::print(UTUtils::get_tree_string(new_table));

  // remove table metadata by table id.
  TableMetadataHelper::remove_table(ret_table_id[0]);
}

/**
 * @brief happy test for adding one new table metadata
 *   and getting it by table id.
 */
TEST_F(ApiTestTableMetadata, add_get_table_metadata_by_table_id) {
  // prepare test data for adding table metadata.
  UTTableMetadata testdata_table_metadata =
      *(global->testdata_table_metadata.get());
  ptree new_table            = testdata_table_metadata.tables;
  std::string new_table_name = new_table.get<std::string>(Tables::NAME) +
                               "_ApiTestTableMetadata" +
                               std::to_string(__LINE__);
  new_table.put(Tables::NAME, new_table_name);

  // add table metadata.
  ObjectIdType ret_table_id = -1;
  TableMetadataHelper::add_table(new_table, &ret_table_id);
  new_table.put(Tables::ID, ret_table_id);

  // get table metadata by table id.
  auto tables     = std::make_unique<Tables>(GlobalTestEnvironment::TEST_DB);
  ErrorCode error = tables->init();
  EXPECT_EQ(ErrorCode::OK, error);

  ptree table_metadata_inserted;
  error = tables->get(ret_table_id, table_metadata_inserted);
  EXPECT_EQ(ErrorCode::OK, error);

  UTUtils::print("-- get table metadata --");
  UTUtils::print(UTUtils::get_tree_string(table_metadata_inserted));

  // verifies that the returned table metadata is expected one.
  TableMetadataHelper::check_table_metadata_expected(new_table,
                                                     table_metadata_inserted);

  // remove table metadata.
  TableMetadataHelper::remove_table(ret_table_id);
}

/**
 * @brief happy test for all table metadata getting.
 */
TEST_F(ApiTestTableMetadata, get_all_table_metadata) {
  constexpr int test_table_count = 5;
  std::string table_name_prefix =
      "ApiTestTableMetadata-GetAll-" + std::to_string(time(NULL));
  std::vector<ObjectIdType> table_ids = {};

  // get base count
  std::int64_t base_table_count = TableMetadataHelper::get_record_count();

  // gets all table metadata.
  auto tables     = std::make_unique<Tables>(GlobalTestEnvironment::TEST_DB);
  ErrorCode error = tables->init();
  EXPECT_EQ(ErrorCode::OK, error);

  // prepare test data for adding table metadata.
  UTTableMetadata testdata_table_metadata =
      *(global->testdata_table_metadata.get());
  ptree expected_table = testdata_table_metadata.tables;

  // add table metadata.
  for (int count = 1; count <= test_table_count; count++) {
    std::string table_name = table_name_prefix + std::to_string(count);
    ObjectIdType table_id;
    TableMetadataHelper::add_table(table_name, &table_id);
    table_ids.emplace_back(table_id);
  }

  std::vector<boost::property_tree::ptree> container = {};

  error = tables->get_all(container);
  EXPECT_EQ(ErrorCode::OK, error);
  EXPECT_EQ(test_table_count + base_table_count, container.size());

  UTUtils::print("-- get all table metadata --");
  for (int index = 0; index < test_table_count; index++) {
    ptree table_metadata = container[index + base_table_count];
    UTUtils::print(UTUtils::get_tree_string(table_metadata));

    std::string table_name = table_name_prefix + std::to_string(index + 1);
    expected_table.put(Tables::ID, table_ids[index]);
    expected_table.put(Tables::NAME, table_name);

    // verifies that the returned table metadata is expected one.
    TableMetadataHelper::check_table_metadata_expected(expected_table,
                                                       table_metadata);
  }

  // cleanup
  for (ObjectIdType table_id : table_ids) {
    error = tables->remove(table_id);
    EXPECT_EQ(ErrorCode::OK, error);
  }
}

/**
 * @brief happy test for all table metadata getting.
 */
TEST_F(ApiTestTableMetadata, get_all_table_metadata_empty) {
  // get base count
  std::int64_t base_table_count = TableMetadataHelper::get_record_count();

  // gets all table metadata.
  auto tables     = std::make_unique<Tables>(GlobalTestEnvironment::TEST_DB);
  ErrorCode error = tables->init();
  EXPECT_EQ(ErrorCode::OK, error);

  std::vector<boost::property_tree::ptree> container = {};
  error = tables->get_all(container);
  EXPECT_EQ(ErrorCode::OK, error);
  EXPECT_EQ(base_table_count, container.size());
}

/**
 * @brief happy test for adding one new table metadata
 *   and getting it by table id.
 */
TEST_F(ApiTestTableMetadata, update_table_metadata) {
  // prepare test data for adding table metadata.
  UTTableMetadata testdata_table_metadata =
      *(global->testdata_table_metadata.get());
  ptree new_table            = testdata_table_metadata.tables;
  std::string new_table_name = new_table.get<std::string>(Tables::NAME) +
                               "_ApiTestTableMetadata" +
                               std::to_string(__LINE__);
  new_table.put(Tables::NAME, new_table_name);

  // add table metadata.
  ObjectIdType ret_table_id = -1;
  TableMetadataHelper::add_table(new_table, &ret_table_id);
  new_table.put(Tables::ID, ret_table_id);

  // generate Tables object.
  auto tables     = std::make_unique<Tables>(GlobalTestEnvironment::TEST_DB);
  ErrorCode error = tables->init();
  EXPECT_EQ(ErrorCode::OK, error);

  ptree table_metadata_inserted;
  error = tables->get(ret_table_id, table_metadata_inserted);
  ASSERT_EQ(ErrorCode::OK, error);

  UTUtils::print("-- get table metadata of the before updating --");
  UTUtils::print(UTUtils::get_tree_string(table_metadata_inserted));

  // update table metadata.
  ptree update_table = table_metadata_inserted;
  update_table.put(Tables::NAME, "table_name-update");
  update_table.put(Tables::NAMESPACE, "namespace-update");
  update_table.put(Tables::TUPLES, 5.67f);

  update_table.erase(Tables::PRIMARY_KEY_NODE);
  ptree primary_key;
  ptree primary_keys;
  primary_key.put("", 2);
  primary_keys.push_back(std::make_pair("", primary_key));
  update_table.add_child(Tables::PRIMARY_KEY_NODE, primary_keys);

  // columns
  update_table.erase(Tables::COLUMNS_NODE);
  ptree columns;
  {
    auto columns_node = table_metadata_inserted.get_child(Tables::COLUMNS_NODE);
    auto it           = columns_node.begin();

    ptree column;
    // 1 item skip.
    // 2 item update.
    column = (++it)->second;
    column.put(Tables::Column::NAME,
               it->second.get_optional<std::string>(Tables::Column::NAME)
                       .value_or("unknown-1") +
                   "-update");
    column.put(Tables::Column::ORDINAL_POSITION, 1);
    column.put(Tables::Column::DIRECTION,
               static_cast<int>(Tables::Column::Direction::DESCENDANT));
    columns.push_back(std::make_pair("", column));

    // new column
    column.clear();
    column.put(Tables::Column::NAME, "new-col");
    column.put(Tables::Column::ORDINAL_POSITION, 2);
    column.put<ObjectIdType>(Tables::Column::DATA_TYPE_ID, 13);
    column.put<bool>(Tables::Column::VARYING, false);
    column.put(Tables::Column::DATA_LENGTH, 32);
    column.put<bool>(Tables::Column::NULLABLE, false);
    column.put(Tables::Column::DEFAULT, "default-value");
    column.put(Tables::Column::DIRECTION,
               static_cast<int>(Tables::Column::Direction::ASCENDANT));
    columns.push_back(std::make_pair("", column));

    // 3 item copy.
    column.clear();
    column = (++it)->second;
    columns.push_back(std::make_pair("", column));
  }
  update_table.add_child(Tables::COLUMNS_NODE, columns);

  // constraint
  update_table.erase(Tables::CONSTRAINTS_NODE);
  ptree constraints;
  {
    ptree constraint;
    ptree columns_num;
    ptree columns_num_value;
    ptree columns_id;
    ptree columns_id_value;

    auto constraints_node =
        table_metadata_inserted.get_child(Tables::CONSTRAINTS_NODE);
    auto it = constraints_node.begin();

    // 1 item skip.
    constraint = (++it)->second;
    // 2 item update.
    constraint.put(Constraint::NAME,
                   it->second.get_optional<std::string>(Constraint::NAME)
                           .value_or("unknown-1") +
                       "-update");
    // columns
    columns_num_value.put("", 3);
    columns_num.push_back(std::make_pair("", columns_num_value));
    constraint.add_child(Constraint::COLUMNS, columns_num);
    // columns id
    columns_id_value.put("", 9876);
    columns_id.push_back(std::make_pair("", columns_id_value));
    constraint.add_child(Constraint::COLUMNS_ID, columns_id);
    // constraints
    constraints.push_back(std::make_pair("", constraint));

    // new constraint
    constraint.clear();
    columns_num.clear();
    columns_num_value.clear();
    columns_id.clear();
    columns_id_value.clear();
    // name
    constraint.put(Constraint::NAME, "new unique constraint");
    // type
    constraint.put(Constraint::TYPE,
                   static_cast<int32_t>(Constraint::ConstraintType::UNIQUE));
    // columns
    columns_num_value.put("", 9);
    columns_num.push_back(std::make_pair("", columns_num_value));
    constraint.add_child(Constraint::COLUMNS, columns_num);
    // columns id
    columns_id_value.put("", 9999);
    columns_id.push_back(std::make_pair("", columns_id_value));
    constraint.add_child(Constraint::COLUMNS_ID, columns_id);
    // index id
    constraint.put(Constraint::INDEX_ID, 9);
    // constraints
    constraints.push_back(std::make_pair("", constraint));
  }
  update_table.add_child(Tables::CONSTRAINTS_NODE, constraints);

  // update table metadata.
  error = tables->update(ret_table_id, update_table);
  ASSERT_EQ(ErrorCode::OK, error);

  ptree table_metadata_updated;
  error = tables->get(ret_table_id, table_metadata_updated);
  ASSERT_EQ(ErrorCode::OK, error);

  UTUtils::print("-- get table metadata of the after updating --");
  UTUtils::print(UTUtils::get_tree_string(table_metadata_updated));

  // verifies that the returned table metadata is expected one.
  TableMetadataHelper::check_table_metadata_expected(update_table,
                                                     table_metadata_updated);

  // remove table metadata.
  TableMetadataHelper::remove_table(ret_table_id);
}

/**
 * @brief happy test for removing one new table metadata by table name.
 */
TEST_F(ApiTestTableMetadata, remove_table_metadata_by_table_name) {
  // prepare test data for adding table metadata.
  UTTableMetadata testdata_table_metadata =
      *(global->testdata_table_metadata.get());
  ptree new_table            = testdata_table_metadata.tables;
  std::string new_table_name = new_table.get<std::string>(Tables::NAME) +
                               "_ApiTestTableMetadata" +
                               std::to_string(__LINE__);
  new_table.put(Tables::NAME, new_table_name);

  // add table metadata.
  ObjectIdType ret_table_id = -1;
  TableMetadataHelper::add_table(new_table, &ret_table_id);

  // remove table metadata by table name.
  auto tables     = std::make_unique<Tables>(GlobalTestEnvironment::TEST_DB);
  ErrorCode error = tables->init();
  EXPECT_EQ(ErrorCode::OK, error);

  ObjectIdType table_id_to_remove = -1;
  error = tables->remove(new_table_name.c_str(), &table_id_to_remove);
  EXPECT_EQ(ErrorCode::OK, error);
  EXPECT_EQ(ret_table_id, table_id_to_remove);

  // verifies that table metadata does not exist.
  ptree table_metadata_got;
  error = tables->get(table_id_to_remove, table_metadata_got);
  EXPECT_EQ(ErrorCode::ID_NOT_FOUND, error);

  UTUtils::print("-- get table metadata --");
  UTUtils::print(UTUtils::get_tree_string(table_metadata_got));
}

/**
 * @brief happy test for removing one new table metadata by table id.
 */
TEST_F(ApiTestTableMetadata, remove_table_metadata_by_table_id) {
  // prepare test data for adding table metadata.
  UTTableMetadata testdata_table_metadata =
      *(global->testdata_table_metadata.get());
  ptree new_table            = testdata_table_metadata.tables;
  std::string new_table_name = new_table.get<std::string>(Tables::NAME) +
                               "_ApiTestTableMetadata" +
                               std::to_string(__LINE__);
  new_table.put(Tables::NAME, new_table_name);

  // add table metadata.
  ObjectIdType ret_table_id = -1;
  TableMetadataHelper::add_table(new_table, &ret_table_id);

  // remove table metadata by table id.
  auto tables     = std::make_unique<Tables>(GlobalTestEnvironment::TEST_DB);
  ErrorCode error = tables->init();
  EXPECT_EQ(ErrorCode::OK, error);

  error = tables->remove(ret_table_id);
  EXPECT_EQ(ErrorCode::OK, error);

  // verifies that table metadata does not exist.
  ptree table_metadata_got;
  error = tables->get(ret_table_id, table_metadata_got);
  EXPECT_EQ(ErrorCode::ID_NOT_FOUND, error);

  UTUtils::print("-- get table metadata --");
  UTUtils::print(UTUtils::get_tree_string(table_metadata_got));
}

/**
 * @brief happy test for adding, getting and removing
 *   one new table metadata without initialization of all api.
 */
TEST_F(ApiTestTableMetadata,
       add_get_remove_table_metadata_without_initialized) {
  // prepare test data for adding table metadata.
  UTTableMetadata testdata_table_metadata =
      *(global->testdata_table_metadata.get());
  ptree new_table            = testdata_table_metadata.tables;
  std::string new_table_name = new_table.get<std::string>(Tables::NAME) +
                               "_ApiTestTableMetadata" +
                               std::to_string(__LINE__);
  new_table.put(Tables::NAME, new_table_name);

  // add table metadata without initialized.
  auto tables_add = std::make_unique<Tables>(GlobalTestEnvironment::TEST_DB);

  UTUtils::print("-- add table metadata --");
  ObjectIdType ret_table_id = -1;
  ErrorCode error           = tables_add->add(new_table, &ret_table_id);
  new_table.put(Tables::ID, ret_table_id);

  EXPECT_EQ(ErrorCode::OK, error);
  EXPECT_GT(ret_table_id, 0);

  // get table metadata by table id without initialized.
  auto tables_get_by_id =
      std::make_unique<Tables>(GlobalTestEnvironment::TEST_DB);

  ptree table_metadata_inserted_by_id;
  error = tables_get_by_id->get(ret_table_id, table_metadata_inserted_by_id);
  EXPECT_EQ(ErrorCode::OK, error);

  UTUtils::print("-- get table metadata by table-id --");
  UTUtils::print(UTUtils::get_tree_string(table_metadata_inserted_by_id));

  // verifies that the returned table metadata is expected one.
  TableMetadataHelper::check_table_metadata_expected(
      new_table, table_metadata_inserted_by_id);

  // get table metadata by table name without initialized.
  auto tables_get_by_name =
      std::make_unique<Tables>(GlobalTestEnvironment::TEST_DB);

  ptree table_metadata_inserted_by_name;
  error =
      tables_get_by_name->get(new_table_name, table_metadata_inserted_by_name);
  EXPECT_EQ(ErrorCode::OK, error);

  UTUtils::print("-- get table metadata by table-name --");
  UTUtils::print(UTUtils::get_tree_string(table_metadata_inserted_by_name));

  // verifies that the returned table metadata is expected one.
  TableMetadataHelper::check_table_metadata_expected(
      new_table, table_metadata_inserted_by_name);

  // update table metadata without initialized.
  auto tables_update = std::make_unique<Tables>(GlobalTestEnvironment::TEST_DB);

  // update valid table metadata.
  auto update_table = new_table;
  std::string table_name =
      new_table.get_optional<std::string>(Tables::NAME).value() + "-update";
  update_table.put(Tables::NAME, table_name);

  UTUtils::print("-- update table metadata --");
  error = tables_update->update(ret_table_id, update_table);
  EXPECT_EQ(ErrorCode::OK, error);

  ptree table_metadata_updated;
  error = tables_update->get(ret_table_id, table_metadata_updated);
  EXPECT_EQ(ErrorCode::OK, error);

  UTUtils::print("-- get table metadata after updated --");
  UTUtils::print(UTUtils::get_tree_string(table_metadata_updated));

  // verifies that the returned table metadata is expected one.
  TableMetadataHelper::check_table_metadata_expected(update_table,
                                                     table_metadata_updated);

  // remove table metadata by table id without initialized.
  auto tables_remove_by_id =
      std::make_unique<Tables>(GlobalTestEnvironment::TEST_DB);

  UTUtils::print("-- remove table metadata by table-id  --");
  error = tables_remove_by_id->remove(ret_table_id);
  EXPECT_EQ(ErrorCode::OK, error);

  // add table metadata again.
  error = tables_add->add(new_table, &ret_table_id);

  // remove table metadata by table name without initialized.
  ObjectIdType table_id_to_remove = -1;
  auto tables_remove_by_name =
      std::make_unique<Tables>(GlobalTestEnvironment::TEST_DB);

  UTUtils::print("-- remove table metadata by table-name  --");
  error = tables_remove_by_name->remove(new_table_name.c_str(),
                                        &table_id_to_remove);
  EXPECT_EQ(ErrorCode::OK, error);
  EXPECT_EQ(ret_table_id, table_id_to_remove);
}

/**
 * @brief This test is to retrieve pre-defined role names and privileges.
 */
TEST_F(ApiTestTableAcls, get_acl) {
  // get table metadata by table name.
  auto tables     = std::make_unique<Tables>(GlobalTestEnvironment::TEST_DB);
  ErrorCode error = tables->init();
  EXPECT_EQ(ErrorCode::OK, error);

  {
    ptree table_metadata;
    std::string token_string = "";

    UTUtils::print("-- get acls -- [", role_1::role_name, "]");
    token_string = TokenBuilder(role_1::role_name).generate();

    error = tables->get_acls(token_string, table_metadata);
    EXPECT_EQ(ErrorCode::OK, error);

    if (error == ErrorCode::OK) {
      UTUtils::print(" ", UTUtils::get_tree_string(table_metadata));

      std::map<std::string_view, std::string_view> acls_expected;
      acls_expected[foreign_table_1::table_name] = "r";
      acls_expected[foreign_table_2::table_name] = "arwd";
      acls_expected[foreign_table_3::table_name] = "";

      TableMetadataHelper::check_table_acls_expected(acls_expected,
                                                     table_metadata);
    }
  }

  {
    ptree table_metadata;
    std::string token_string = "";

    UTUtils::print("-- get acls -- [", role_2::role_name, "]");
    token_string = TokenBuilder(role_2::role_name).generate();

    error = tables->get_acls(token_string, table_metadata);
    EXPECT_EQ(ErrorCode::OK, error);

    if (error == ErrorCode::OK) {
      UTUtils::print(" ", UTUtils::get_tree_string(table_metadata));

      std::map<std::string_view, std::string_view> acls_expected;
      acls_expected[foreign_table_1::table_name] = "arwd";
      acls_expected[foreign_table_2::table_name] = "r";
      acls_expected[foreign_table_3::table_name] = "rw";

      TableMetadataHelper::check_table_acls_expected(acls_expected,
                                                     table_metadata);
    }
  }

  {
    ptree table_metadata;
    std::string token_string = "";

    UTUtils::print("-- get acls -- [", role_3::role_name, "]");
    token_string = TokenBuilder(role_3::role_name).generate();

    error = tables->get_acls(token_string, table_metadata);
    EXPECT_EQ(ErrorCode::OK, error);

    if (error == ErrorCode::OK) {
      UTUtils::print(" ", UTUtils::get_tree_string(table_metadata));

      std::map<std::string_view, std::string_view> acls_expected;
      acls_expected[foreign_table_1::table_name] = "";
      acls_expected[foreign_table_2::table_name] = "";
      acls_expected[foreign_table_3::table_name] = "";

      TableMetadataHelper::check_table_acls_expected(acls_expected,
                                                     table_metadata);
    }
  }
}

/**
 * @brief This is a test of obtaining privileges for unregistered users.
 */
TEST_F(ApiTestTableAcls, get_acl_unknown_user) {
  // get table metadata by table name.
  auto tables     = std::make_unique<Tables>(GlobalTestEnvironment::TEST_DB);
  ErrorCode error = tables->init();
  EXPECT_EQ(ErrorCode::OK, error);

  {
    ptree table_metadata;
    std::string token_string = "";

    UTUtils::print("-- get acls -- [unknown_user]");
    token_string = TokenBuilder("unknown_user").generate();

    error = tables->get_acls(token_string, table_metadata);
    EXPECT_EQ(ErrorCode::NAME_NOT_FOUND, error);
  }
}

/**
 * @brief This is a test for obtaining privileges when the
 *   access token is invalid.
 */
TEST_F(ApiTestTableAcls, get_acl_token_invalid) {
  // get table metadata by table name.
  auto tables     = std::make_unique<Tables>(GlobalTestEnvironment::TEST_DB);
  ErrorCode error = tables->init();
  EXPECT_EQ(ErrorCode::OK, error);

  ptree table_metadata;
  {
    UTUtils::print("-- get acls -- [Invalid token]");
    std::string token_string =
        TokenBuilder(role_1::role_name).generate() + "invalid";

    error = tables->get_acls(token_string, table_metadata);
    EXPECT_EQ(ErrorCode::INVALID_PARAMETER, error);
  }

  {
    UTUtils::print("-- get acls -- [Invalid token token-type (refresh)]");
    std::string token_string =
        TokenBuilder(role_1::role_name).set_token_type("refresh").generate();

    error = tables->get_acls(token_string, table_metadata);
    EXPECT_EQ(ErrorCode::INVALID_PARAMETER, error);
  }

  {
    UTUtils::print("-- get acls -- [Invalid token token-type (unknown)]");
    std::string token_string =
        TokenBuilder(role_1::role_name).set_token_type("unknown").generate();

    error = tables->get_acls(token_string, table_metadata);
    EXPECT_EQ(ErrorCode::INVALID_PARAMETER, error);
  }

  {
    UTUtils::print("-- get acls -- [Invalid token token-type (_access_)]");
    std::string token_string =
        TokenBuilder(role_1::role_name).set_token_type("_access_").generate();

    error = tables->get_acls(token_string, table_metadata);
    EXPECT_EQ(ErrorCode::INVALID_PARAMETER, error);
  }

  {
    UTUtils::print("-- get acls -- [Invalid token issuer]");
    std::string token_string =
        TokenBuilder(role_1::role_name).set_issuer("invalid").generate();

    error = tables->get_acls(token_string, table_metadata);
    EXPECT_EQ(ErrorCode::INVALID_PARAMETER, error);
  }

  {
    UTUtils::print("-- get acls -- [Invalid token audience]");
    std::string token_string =
        TokenBuilder(role_1::role_name).set_audience("invalid").generate();

    error = tables->get_acls(token_string, table_metadata);
    EXPECT_EQ(ErrorCode::INVALID_PARAMETER, error);
  }

  {
    UTUtils::print("-- get acls -- [Empty tsurugi/auth/name]");
    std::string token_string = TokenBuilder("").generate();

    error = tables->get_acls(token_string, table_metadata);
    EXPECT_EQ(ErrorCode::INVALID_PARAMETER, error);
  }

  {
    UTUtils::print("-- get acls -- [Undefined tsurugi/auth/name]");
    std::string token_string = TokenBuilder().generate();

    error = tables->get_acls(token_string, table_metadata);
    EXPECT_EQ(ErrorCode::INVALID_PARAMETER, error);
  }

  {
    UTUtils::print("-- get acls -- [Undefined iat]");
    std::string token_string =
        TokenBuilder(role_1::role_name).unset_issuer_at().generate();

    error = tables->get_acls(token_string, table_metadata);
    EXPECT_EQ(ErrorCode::INVALID_PARAMETER, error);
  }

  {
    UTUtils::print("-- get acls -- [Undefined exp]");
    std::string token_string =
        TokenBuilder(role_1::role_name).unset_expire_at().generate();

    error = tables->get_acls(token_string, table_metadata);
    EXPECT_EQ(ErrorCode::INVALID_PARAMETER, error);
  }
}

/**
 * @brief This is a test for obtaining privileges when the
 *   access token is expired.
 */
TEST_F(ApiTestTableAcls, get_acl_expired) {
  // get table metadata by table name.
  auto tables     = std::make_unique<Tables>(GlobalTestEnvironment::TEST_DB);
  ErrorCode error = tables->init();
  EXPECT_EQ(ErrorCode::OK, error);

  {
    ptree table_metadata;
    std::string token_string = "";

    UTUtils::print("-- get acls -- [", role_1::role_name, "]");
    token_string = TokenBuilder(role_1::role_name).set_expires(-60).generate();

    error = tables->get_acls(token_string, table_metadata);
    EXPECT_EQ(ErrorCode::INVALID_PARAMETER, error);
  }
}

}  // namespace manager::metadata::testing
