/*
 * Copyright 2020-2021 tsurugi project.
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
#include <tuple>
#include <vector>

#include <boost/property_tree/ptree.hpp>

#include "manager/metadata/dao/datatypes_dao.h"
#include "manager/metadata/dao/generic_dao.h"
#include "manager/metadata/dao/postgresql/db_session_manager_pg.h"
#include "manager/metadata/error_code.h"
#include "test/common/ut_utils.h"
#include "test/environment/global_test_environment.h"
#include "test/helper/data_types_helper.h"

namespace manager::metadata::testing {

using db::postgresql::DBSessionManager;

class DaoTestDataTypesByKeyValue
    : public ::testing::TestWithParam<DataTypesHelper::BasicTestParameter> {
  void SetUp() override { UTUtils::skip_if_connection_not_opened(); }
};  // class DaoTestDataTypesByKeyValue

INSTANTIATE_TEST_CASE_P(
    ParameterizedTest, DaoTestDataTypesByKeyValue,
    ::testing::ValuesIn(DataTypesHelper::make_datatypes_tuple()));

/**
 * @brief Happy test for getting all data type metadata based on data type
 * key/value pair.
 */
TEST_P(DaoTestDataTypesByKeyValue, get_datatypes_by_key_value) {
  auto param        = GetParam();
  std::string key   = std::get<0>(param);
  std::string value = std::get<1>(param);

  std::shared_ptr<db::GenericDAO> d_gdao = nullptr;
  DBSessionManager db_session_manager;

  ErrorCode error =
      db_session_manager.get_dao(db::GenericDAO::TableName::DATATYPES, d_gdao);
  EXPECT_EQ(ErrorCode::OK, error);

  std::shared_ptr<db::DataTypesDAO> ddao =
      std::static_pointer_cast<db::DataTypesDAO>(d_gdao);

  boost::property_tree::ptree datatype;
  error = ddao->select_one_data_type_metadata(key, value, datatype);
  EXPECT_EQ(ErrorCode::OK, error);

  UTUtils::print("-- get data type metadata --");
  UTUtils::print(UTUtils::get_tree_string(datatype));

  // Verifies that returned data type metadata equals expected one.
  DataTypesHelper::check_datatype_metadata_expected(datatype);
}

/**
 * @brief Exception path test for getting non-existing data type metadata
 * based on invalid data type key/value pair.
 */
TEST_F(DaoTestDataTypesByKeyValue, get_non_existing_datatypes_by_key_value) {
  std::shared_ptr<db::GenericDAO> d_gdao = nullptr;

  DBSessionManager db_session_manager;
  ErrorCode error =
      db_session_manager.get_dao(db::GenericDAO::TableName::DATATYPES, d_gdao);
  EXPECT_EQ(ErrorCode::OK, error);

  std::shared_ptr<db::DataTypesDAO> ddao =
      std::static_pointer_cast<db::DataTypesDAO>(d_gdao);

  std::string key   = "invalid_key";
  std::string value = "INT32";

  boost::property_tree::ptree datatype;
  error = ddao->select_one_data_type_metadata(key.c_str(), value, datatype);

  EXPECT_EQ(ErrorCode::INVALID_PARAMETER, error);

  // Verifies that returned data type metadata equals expected one.
  boost::property_tree::ptree empty_ptree;
  EXPECT_EQ(UTUtils::get_tree_string(empty_ptree),
            UTUtils::get_tree_string(datatype));
}

}  // namespace manager::metadata::testing
