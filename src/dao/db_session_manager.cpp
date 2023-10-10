/*
 * Copyright 2020-2022 Project Tsurugi.
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

#if defined(STORAGE_POSTGRESQL)
#include "manager/metadata/dao/postgresql/db_session_manager_pg.h"
#elif defined(STORAGE_JSON)
#include "manager/metadata/dao/json/db_session_manager_json.h"
#endif

// =============================================================================
namespace manager::metadata::db {

DbSessionManager& DbSessionManager::get_instance() {
#if defined(STORAGE_POSTGRESQL)
  static DbSessionManagerPg instance;
#elif defined(STORAGE_JSON)
  static DbSessionManagerJson instance;
#endif

  return instance;
}

}  // namespace manager::metadata::db
