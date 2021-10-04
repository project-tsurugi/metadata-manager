/*
 * Copyright 2021 tsurugi project.
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
#ifndef MANAGER_METADATA_PROVIDER_DATATYPES_PROVIDER_H_
#define MANAGER_METADATA_PROVIDER_DATATYPES_PROVIDER_H_

#include <boost/property_tree/ptree.hpp>
#include <string_view>

#include "manager/metadata/dao/datatypes_dao.h"
#include "manager/metadata/error_code.h"
#include "manager/metadata/provider/provider_base.h"

namespace manager::metadata::db {

class DataTypesProvider : public ProviderBase {
 public:
  manager::metadata::ErrorCode init();
  manager::metadata::ErrorCode get_datatype_metadata(
      std::string_view key, std::string_view value,
      boost::property_tree::ptree& object);

 private:
  std::shared_ptr<DataTypesDAO> datatypes_dao_ = nullptr;
};  // class DataTypesProvider

}  // namespace manager::metadata::db

#endif  // MANAGER_METADATA_PROVIDER_DATATYPES_PROVIDER_H_
