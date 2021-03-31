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

#ifndef GENERIC_DAO_H_
#define GENERIC_DAO_H_

extern "C" {
#include <libpq-fe.h>
}

#include <string>
#include <unordered_map>
#include <vector>

#include "manager/metadata/dao/common/dbc_utils.h"
#include "manager/metadata/dao/common/statement_name.h"
#include "manager/metadata/error_code.h"

namespace manager::metadata::db {

class GenericDAO {
   public:
    enum TableName { STATISTICS = 0, TABLES, DATATYPES, COLUMNS };

    GenericDAO(ConnectionSPtr connection, TableName table_name)
        : connection(connection), table_name(table_name){};
    virtual ~GenericDAO() {}

    virtual manager::metadata::ErrorCode prepare() const = 0;

   protected:
    ConnectionSPtr connection;
    TableName table_name;

    std::vector<std::string> column_names;
    std::unordered_map<std::string, std::string>
        statement_names_select_equal_to;

    manager::metadata::ErrorCode prepare(const StatementName &statement_name,
                                         const std::string &statement) const;
    manager::metadata::ErrorCode prepare(const std::string &statement_name,
                                         const std::string &statement) const;

    manager::metadata::ErrorCode exec_prepared(
        const StatementName &statement_name,
        const std::vector<char const *> &param_values, PGresult *&res) const;
    manager::metadata::ErrorCode exec_prepared(
        const std::string &statement_name,
        const std::vector<char const *> &param_values, PGresult *&res) const;
};

}  // namespace manager::metadata::db

#endif  // GENERIC_DAO_H_
