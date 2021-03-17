/*
 * Copyright 2020 tsurugi project.
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

#ifndef DB_SESSION_MANAGER_H_
#define DB_SESSION_MANAGER_H_

extern "C" {
#include <libpq-fe.h>
}

#include <memory>
#include <string>

#include "manager/metadata/dao/common/dbc_utils.h"
#include "manager/metadata/dao/generic_dao.h"
#include "manager/metadata/error_code.h"

namespace manager::metadata::db {

class DBSessionManager {
   public:
    manager::metadata::ErrorCode get_dao(GenericDAO::TableName table_name,
                                         std::shared_ptr<GenericDAO>& gdao);

    manager::metadata::ErrorCode start_transaction();
    manager::metadata::ErrorCode commit();
    manager::metadata::ErrorCode rollback();

   private:
    ConnectionSPtr connection;

    manager::metadata::ErrorCode connect();
    manager::metadata::ErrorCode set_always_secure_search_path();
};
}  // namespace manager::metadata::db

#endif  // DB_SESSION_MANAGER_H_
