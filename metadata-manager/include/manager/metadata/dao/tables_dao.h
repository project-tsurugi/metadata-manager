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

#ifndef TABLES_DAO_H_
#define TABLES_DAO_H_

extern "C" {
#include <libpq-fe.h>
}

#include <boost/property_tree/ptree.hpp>
#include <string>

#include "manager/metadata/dao/common/dbc_utils.h"
#include "manager/metadata/dao/generic_dao.h"
#include "manager/metadata/entity/table_statistic.h"
#include "manager/metadata/metadata.h"

namespace manager::metadata::db {

class TablesDAO : public GenericDAO {
   public:
    explicit TablesDAO(ConnectionSPtr connection);

    manager::metadata::ErrorCode prepare() const override;

    manager::metadata::ErrorCode update_reltuples_by_table_id(
        float reltuples, ObjectIdType table_id) const;
    manager::metadata::ErrorCode update_reltuples_by_table_name(
        float reltuples, const std::string& table_name,
        ObjectIdType& table_id) const;
    manager::metadata::ErrorCode select_table_statistic_by_table_id(
        ObjectIdType table_id,
        manager::metadata::TableStatistic& table_statistic) const;
    manager::metadata::ErrorCode select_table_statistic_by_table_name(
        const std::string& table_name, TableStatistic& table_statistic) const;
    manager::metadata::ErrorCode insert_table_metadata(
        boost::property_tree::ptree& table, ObjectIdType& table_id) const;
    manager::metadata::ErrorCode select_table_metadata(
        const std::string& object_key, const std::string& object_value,
        boost::property_tree::ptree& object) const;
    manager::metadata::ErrorCode delete_table_metadata_by_table_id(
        ObjectIdType table_id) const;
    manager::metadata::ErrorCode delete_table_metadata_by_table_name(
        const std::string& table_name, ObjectIdType& table_id) const;

   private:
    manager::metadata::ErrorCode get_table_statistic_from_p_gresult(
        PGresult*& res, int ordinal_position,
        TableStatistic& table_statistic) const;
    manager::metadata::ErrorCode get_ptree_from_p_gresult(
        PGresult*& res, int ordinal_position,
        boost::property_tree::ptree& table) const;
};

}  // namespace manager::metadata::db

#endif  // TABLES_DAO_H_
