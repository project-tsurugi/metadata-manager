/*
 * Copyright 2020 tsurugi project.
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

#include "manager/metadata/dao/generic_dao.h"
#include "manager/metadata/dao/postgresql/db_session_manager.h"
#include "manager/metadata/error_code.h"

#include "test/utility/ut_utils.h"

namespace manager::metadata::testing {

namespace storage = manager::metadata::db::postgresql;
using namespace manager::metadata::db;
using postgresql::DBSessionManager;

class DaoTestDBAccessFailure : public ::testing::Test {
  void SetUp() override { UTUtils::skip_if_connection_opened(); }
};

TEST_F(DaoTestDBAccessFailure, all) {
  ErrorCode error = ErrorCode::INTERNAL_ERROR;

  std::shared_ptr<GenericDAO> g_gdao = nullptr;
  storage::DBSessionManager db_session_manager;

  error = db_session_manager.get_dao(GenericDAO::TableName::STATISTICS, g_gdao);
  EXPECT_EQ(ErrorCode::DATABASE_ACCESS_FAILURE, error);
  EXPECT_EQ(nullptr, g_gdao);

  error = db_session_manager.get_dao(GenericDAO::TableName::COLUMNS, g_gdao);
  EXPECT_EQ(ErrorCode::DATABASE_ACCESS_FAILURE, error);
  EXPECT_EQ(nullptr, g_gdao);

  error = db_session_manager.get_dao(GenericDAO::TableName::TABLES, g_gdao);
  EXPECT_EQ(ErrorCode::DATABASE_ACCESS_FAILURE, error);
  EXPECT_EQ(nullptr, g_gdao);

  error = db_session_manager.get_dao(GenericDAO::TableName::DATATYPES, g_gdao);
  EXPECT_EQ(ErrorCode::DATABASE_ACCESS_FAILURE, error);
  EXPECT_EQ(nullptr, g_gdao);

  ErrorCode start_transaction_error = db_session_manager.start_transaction();
  EXPECT_EQ(ErrorCode::NOT_INITIALIZED, start_transaction_error);

  ErrorCode commit_error = db_session_manager.commit();
  EXPECT_EQ(ErrorCode::NOT_INITIALIZED, commit_error);

  ErrorCode rollback_error = db_session_manager.rollback();
  EXPECT_EQ(ErrorCode::NOT_INITIALIZED, rollback_error);
}

}  // namespace manager::metadata::testing
