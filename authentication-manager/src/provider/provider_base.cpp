/*
 * Copyright 2021 tsurugi project.
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
#if !defined(STORAGE_POSTGRESQL)
#define STORAGE_POSTGRESQL
#endif

#include "manager/authentication/provider/provider_base.h"

#include <memory>

#if defined(STORAGE_POSTGRESQL)
#include "manager/authentication/dao/postgresql/db_session_manager.h"
#endif

// =============================================================================
namespace manager::authentication::db {

#if defined(STORAGE_POSTGRESQL)
namespace storage = postgresql;
#endif

/**
 * @brief Constructor.
 *   Create an instance of the DBSessionManager class.
 * @return none.
 */
ProviderBase::ProviderBase()
    : session_manager_(std::make_unique<storage::DBSessionManager>()) {}

}  // namespace manager::authentication::db
