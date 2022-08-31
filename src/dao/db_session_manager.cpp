/*
 * Copyright 2020-2021 tsurugi project.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
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
#if !defined(STORAGE_POSTGRESQL) && !defined(STORAGE_JSON)
#define STORAGE_POSTGRESQL
#endif

#include "manager/metadata/dao/db_session_manager.h"

#if defined(STORAGE_POSTGRESQL)
#include "manager/metadata/dao/postgresql/columns_dao.h"
#include "manager/metadata/dao/postgresql/datatypes_dao.h"
#include "manager/metadata/dao/postgresql/privileges_dao.h"
#include "manager/metadata/dao/postgresql/roles_dao.h"
#include "manager/metadata/dao/postgresql/statistics_dao.h"
#include "manager/metadata/dao/postgresql/tables_dao.h"
#elif defined(STORAGE_JSON)
#include "manager/metadata/dao/json/columns_dao.h"
#include "manager/metadata/dao/json/datatypes_dao.h"
#include "manager/metadata/dao/json/privileges_dao.h"
#include "manager/metadata/dao/json/tables_dao.h"
#endif

// =============================================================================
namespace manager::metadata::db {

#if defined(STORAGE_POSTGRESQL)
namespace storage = manager::metadata::db::postgresql;
#elif defined(STORAGE_JSON)
namespace storage = manager::metadata::db::json;
#endif

/* =============================================================================
 * Private method area
 */

/**
 * @brief Create Dao instance for the requested table name.
 * @param (table_name)   [in]  unique id for the Dao.
 * @param (session_manager)   [in]  Data connector for the Dao.
 * @param (gdao)         [out] Dao instance if success.
 *     for the requested table name.
 * @return ErrorCode::OK if success, otherwise an error code.
 */
manager::metadata::ErrorCode DBSessionManager::create_dao(
    GenericDAO::TableName table_name, const DBSessionManager* session_manager,
    std::shared_ptr<GenericDAO>& gdao) const {
  ErrorCode error = ErrorCode::UNKNOWN;

  storage::DBSessionManager* storage_session_manager =
      (storage::DBSessionManager*)session_manager;

  switch (table_name) {
    case GenericDAO::TableName::TABLES: {
      auto tdao = std::make_shared<storage::TablesDAO>(storage_session_manager);
      gdao = tdao;
      break;
    }
    case GenericDAO::TableName::STATISTICS: {
#if defined(STORAGE_POSTGRESQL)
      auto sdao =
          std::make_shared<storage::StatisticsDAO>(storage_session_manager);
      gdao = sdao;
      break;
#elif defined(STORAGE_JSON)
      // Statistics are not supported in JSON.
      return ErrorCode::NOT_SUPPORTED;
#endif
    }
    case GenericDAO::TableName::DATATYPES: {
      auto ddao =
          std::make_shared<storage::DataTypesDAO>(storage_session_manager);
      gdao = ddao;
      break;
    }
    case GenericDAO::TableName::COLUMNS: {
      auto cdao =
          std::make_shared<storage::ColumnsDAO>(storage_session_manager);
      gdao = cdao;
      break;
    }
    case GenericDAO::TableName::ROLES: {
#if defined(STORAGE_POSTGRESQL)
      auto sdao = std::make_shared<storage::RolesDAO>(storage_session_manager);
      gdao = sdao;
      break;
#elif defined(STORAGE_JSON)
      // Roles are not supported in JSON.
      return ErrorCode::NOT_SUPPORTED;
#endif
    }
    case GenericDAO::TableName::PRIVILEGES: {
      auto sdao =
          std::make_shared<storage::PrivilegesDAO>(storage_session_manager);
      gdao = sdao;
      break;
    }
    default: {
      error = ErrorCode::INTERNAL_ERROR;
      return error;
    }
  }

  if (gdao == nullptr) {
    error = ErrorCode::INTERNAL_ERROR;
    return error;
  }

  // Prepare for DAO.
  error = gdao->prepare();

  return error;
}

}  // namespace manager::metadata::db