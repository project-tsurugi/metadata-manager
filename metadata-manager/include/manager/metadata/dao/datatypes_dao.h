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

#ifndef DATA_TYPES_DAO_H_
#define DATA_TYPES_DAO_H_

extern "C" {
#include <libpq-fe.h>
}

#include <boost/property_tree/ptree.hpp>
#include <string>

#include "manager/metadata/dao/common/dbc_utils.h"
#include "manager/metadata/dao/generic_dao.h"
#include "manager/metadata/error_code.h"

namespace manager::metadata::db {

class DataTypesDAO : public GenericDAO {
   public:
    explicit DataTypesDAO(ConnectionSPtr connection);

    manager::metadata::ErrorCode prepare() const override;

    manager::metadata::ErrorCode select_one_data_type_metadata(
        const std::string &object_key, const std::string &object_value,
        boost::property_tree::ptree &object) const;

   private:
    manager::metadata::ErrorCode get_ptree_from_p_gresult(
        PGresult *&res, int ordinal_position,
        boost::property_tree::ptree &object) const;
};

}  // namespace manager::metadata::db

#endif  // DATA_TYPES_DAO_H_
