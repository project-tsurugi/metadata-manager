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
#pragma once

#include <libpq-fe.h>
#include <functional>
#include <memory>

extern "C" {
typedef struct pg_conn PGconn;
typedef struct pg_result PGresult;
}

namespace manager::metadata::db {

static constexpr const int64_t FIRST_ROW = 0;    // zero origin.
static constexpr const int64_t FIRST_COLUMN = 0;   // zero origin.

// Shared smart pointer for DB connection. Just an alias for user convenience.
using PgConnectionPtr = std::shared_ptr<PGconn>;

// Unique smart pointer for DB result. Just an alias for user convenience.
using ResultPtr = std::unique_ptr<PGresult, std::function<void(PGresult*)>>;

static constexpr const char* const SCHEMA_PUBLIC = "public";
static constexpr const char* const SCHEMA_TSURUGI_CATALOG = "tsurugi_catalog";

class PgErrorCode {
 public:
  static constexpr const char* const kUniqueViolation = "23505";
  static constexpr const char* const kUndefinedObject = "42704";
};  // class PgErrorCode

class PgCatalog {
 public:
  class PgClass {
   public:
    /**
     * @brief Column name of the pg_class in the PostgreSQL system catalog.
     */
    class ColumnName {
     public:
      static constexpr const char* const kName = "relname";
      static constexpr const char* const kOwner = "relowner";
      static constexpr const char* const kAcl = "relacl";
    };  // class ColumnName

    /**
     * @brief Table name of pg_class in the PostgreSQL system catalog.
     */
    static constexpr const char* const kTableName = "pg_class";
  };  // class PgClass

  class PgAuth {
   public:
    /**
     * @brief Column name of the pg_class in the PostgreSQL system catalog.
     */
    class ColumnName {
     public:
      static constexpr const char* const kOid = "oid";
      static constexpr const char* const kName = "rolname";
      static constexpr const char* const kSuper = "rolsuper";
      static constexpr const char* const kInherit = "rolinherit";
      static constexpr const char* const kCreateRole = "rolcreaterole";
      static constexpr const char* const kCreateDb = "rolcreatedb";
      static constexpr const char* const kCanLogin = "rolcanlogin";
      static constexpr const char* const kReplication = "rolreplication";
      static constexpr const char* const kBypassRls = "rolbypassrls";
      static constexpr const char* const kConnLimit = "rolconnlimit";
      static constexpr const char* const kPassword = "rolpassword";
      static constexpr const char* const kValidUntil = "rolvaliduntil";
    };  // class ColumnName

    /**
     * @brief Table name of pg_class in the PostgreSQL system catalog.
     */
    static constexpr const char* const kTableName = "pg_authid";
  };  // class PgAuth

  class PgForeignTable {
   public:
    /**
     * @brief Column name of the pg_foreign_table
     *   in the PostgreSQL system catalog.
     */
    class ColumnName {
     public:
      static constexpr const char* const kOptions = "ftoptions";
    };  // class ColumnName

    /**
     * @brief Table name of pg_foreign_table in the PostgreSQL system catalog.
     */
    static constexpr const char* const kTableName = "pg_foreign_table";
  };  // class PgForeignTable
};  // class PgCatalog

}  // namespace manager::metadata::db
