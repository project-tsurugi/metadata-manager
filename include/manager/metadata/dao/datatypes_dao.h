/*
 * Copyright 2020-2021 tsurugi project.
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
#ifndef MANAGER_METADATA_DAO_DATATYPES_DAO_H_
#define MANAGER_METADATA_DAO_DATATYPES_DAO_H_

#include <string>
#include <string_view>

#include <boost/property_tree/ptree.hpp>

#include "manager/metadata/dao/generic_dao.h"
#include "manager/metadata/datatypes.h"
#include "manager/metadata/error_code.h"

namespace manager::metadata::db {

class DataTypesDAO : public GenericDAO {
 public:
  virtual ~DataTypesDAO() {}

  virtual manager::metadata::ErrorCode select_one_data_type_metadata(
      std::string_view object_key, std::string_view object_value,
      boost::property_tree::ptree& object) const = 0;
};  // class DataTypesDAO

}  // namespace manager::metadata::db

#endif  // MANAGER_METADATA_DAO_DATATYPES_DAO_H_