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

#ifndef COLUMNS_DAO_H_
#define COLUMNS_DAO_H_

#include <boost/property_tree/ptree.hpp>
#include <string>

#include "manager/metadata/dao/common/dbc_utils.h"
#include "manager/metadata/dao/generic_dao.h"
#include "manager/metadata/error_code.h"
#include "manager/metadata/metadata.h"

namespace manager::metadata::db {

class ColumnsDAO : public GenericDAO {
   public:
    explicit ColumnsDAO(ConnectionSPtr connection)
        : GenericDAO(connection, TableName::COLUMNS) {}

    manager::metadata::ErrorCode prepare() const override;

    manager::metadata::ErrorCode insert_one_column_metadata(
        ObjectIdType table_id, boost::property_tree::ptree& column) const;
    manager::metadata::ErrorCode select_column_metadata(
        const std::string& object_key, const std::string& object_value,
        boost::property_tree::ptree& object) const;

   private:
    manager::metadata::ErrorCode get_ptree_from_p_gresult(
        PGresult*& res, int ordinal_position,
        boost::property_tree::ptree& column) const;
};

}  // namespace manager::metadata::db

#endif  // COLUMNS_DAO_H_
