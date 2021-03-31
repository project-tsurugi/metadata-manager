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

#include "manager/metadata/dao/dialect/dialect_strategy.h"

#include "manager/metadata/dao/dialect/postgresql_dialect.h"

namespace manager::metadata::db {

/**
 *  @brief  Dialect instance.
 */
Dialect *DialectStrategy::instance = nullptr;

/**
 *  @brief  Gets Dialect instance.
 *  If metadata repository is changed
 *  from one DBMS to another,
 *  modify this method to return
 *  xxxDialect instance for another DBMS.
 *  @param  none.
 *  @return Dialect instance.
 *  For example, PostgreSQLDialect instance
 *  if metadata repository is PostgreSQL.
 *  If metadata repository is other DBMS named xxx,
 *  xxxDialect instance for the DBMS named xxx.
 */
Dialect *DialectStrategy::get_instance() {
    if (instance == nullptr) {
        instance = new PostgreSQLDialect();
    }
    return instance;
}

}  // namespace manager::metadata::db
