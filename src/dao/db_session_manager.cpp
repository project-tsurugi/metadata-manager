/*
 * Copyright 2020-2022 tsurugi project.
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
#include "manager/metadata/common/message.h"
#include "manager/metadata/helper/logging_helper.h"

#if defined(STORAGE_POSTGRESQL)
#include "manager/metadata/dao/postgresql/columns_dao_pg.h"
#include "manager/metadata/dao/postgresql/constraints_dao_pg.h"
#include "manager/metadata/dao/postgresql/datatypes_dao_pg.h"
#include "manager/metadata/dao/postgresql/privileges_dao_pg.h"
#include "manager/metadata/dao/postgresql/roles_dao_pg.h"
#include "manager/metadata/dao/postgresql/statistics_dao_pg.h"
#include "manager/metadata/dao/postgresql/tables_dao_pg.h"
#elif defined(STORAGE_JSON)
#include "manager/metadata/dao/json/columns_dao_json.h"
#include "manager/metadata/dao/json/constraints_dao_json.h"
#include "manager/metadata/dao/json/datatypes_dao_json.h"
#include "manager/metadata/dao/json/privileges_dao_json.h"
#include "manager/metadata/dao/json/tables_dao_json.h"
#endif

// =============================================================================
namespace manager::metadata::db {

/**
 * Switch data storage type JSON or PostgreSQL.
 */
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

  storage::DBSessionManager* storage_session_manager = (storage::DBSessionManager*)session_manager;

  gdao = nullptr;
  switch (table_name) {
    case GenericDAO::TableName::TABLES: {
      LOG_DEBUG << "Generate DAO for table metadata.";
      gdao = std::make_shared<storage::TablesDAO>(storage_session_manager);
      break;
    }
    case GenericDAO::TableName::STATISTICS: {
#if defined(STORAGE_POSTGRESQL)
      LOG_DEBUG << "Generate DAO for table statistics.";
      gdao = std::make_shared<storage::StatisticsDAO>(storage_session_manager);
      break;
#elif defined(STORAGE_JSON)
      // Statistics are not supported in JSON.
      return ErrorCode::NOT_SUPPORTED;
#endif
    }
    case GenericDAO::TableName::DATATYPES: {
      LOG_DEBUG << "Generate DAO for datatypes metadata.";
      gdao = std::make_shared<storage::DataTypesDAO>(storage_session_manager);
      break;
    }
    case GenericDAO::TableName::COLUMNS: {
      LOG_DEBUG << "Generate DAO for columns metadata.";
      gdao = std::make_shared<storage::ColumnsDAO>(storage_session_manager);
      break;
    }
    case GenericDAO::TableName::CONSTRAINTS: {
      LOG_DEBUG << "Generate DAO for constraints metadata.";
      gdao = std::make_shared<storage::ConstraintsDAO>(storage_session_manager);
      break;
    }
    case GenericDAO::TableName::ROLES: {
#if defined(STORAGE_POSTGRESQL)
      LOG_DEBUG << "Generate DAO for roles metadata.";
      gdao = std::make_shared<storage::RolesDAO>(storage_session_manager);
      break;
#elif defined(STORAGE_JSON)
      // Roles are not supported in JSON.
      return ErrorCode::NOT_SUPPORTED;
#endif
    }
    case GenericDAO::TableName::PRIVILEGES: {
      LOG_DEBUG << "Generate DAO for privileges metadata.";
      gdao = std::make_shared<storage::PrivilegesDAO>(storage_session_manager);
      break;
    }
    default: {
      error = ErrorCode::INTERNAL_ERROR;
      return error;
    }
  }

  if (gdao == nullptr) {
    LOG_ERROR << Message::GENERATE_FAILED_DAO << static_cast<std::int32_t>(table_name);
    error = ErrorCode::INTERNAL_ERROR;
    return error;
  }

  // Prepare for DAO.
  error = gdao->prepare();

  return error;
}

}  // namespace manager::metadata::db
