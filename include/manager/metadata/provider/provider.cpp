/*
 * Copyright 2021-2022 tsurugi project.
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
#if !defined(STORAGE_POSTGRESQL) && !defined(STORAGE_JSON)
#define STORAGE_POSTGRESQL
#endif

#include "manager/metadata/provider/provider.h"
#include <memory>
#include "manager/metadata/dao/db_session_manager.h"
#include "manager/metadata/dao/postgresql/db_session_manager_pg.h"
#include "manager/metadata/dao/json/db_session_manager_json.h"

namespace manager::metadata::db {

// =============================================================================
// Provider class methods.
/**
 * @brief Constructor.
 *   Create an instance of the DBSessionManager class.
 * @return none.
 */
Provider::Provider() {
#if defined(STORAGE_POSTGRESQL)
  session_ = std::make_unique<DbSessionManagerPg>();
#elif defined(STORAGE_JSON)
  session_ = std::make_unique<DbSessionManagerJson>();
#endif
}

}  // namespace manager::metadata::db
