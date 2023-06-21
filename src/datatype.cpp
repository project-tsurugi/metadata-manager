/*
 * Copyright 2023 tsurugi project.
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
#include "manager/metadata/datatype.h"

// =============================================================================
namespace manager::metadata {

boost::property_tree::ptree DataType::convert_to_ptree() const {
  auto pt = Object::convert_to_ptree();

  // pg_data_type
  pt.put(PG_DATA_TYPE, this->pg_data_type);
  // pg_data_type_name
  pt.put(PG_DATA_TYPE_NAME, this->pg_data_type_name);
  // pg_data_type_qualified_name
  pt.put(PG_DATA_TYPE_QUALIFIED_NAME, this->pg_data_type_qualified_name);

  return pt;
}

void DataType::convert_from_ptree(const boost::property_tree::ptree& pt) {
  Object::convert_from_ptree(pt);

  // pg_dataType
  this->pg_data_type =
      pt.get_optional<int64_t>(PG_DATA_TYPE).value_or(INVALID_VALUE);
  // pg_data_type_name
  this->pg_data_type_name =
      pt.get_optional<std::string>(PG_DATA_TYPE_NAME).value_or("");
  // pg_data_type_qualified_name
  this->pg_data_type_qualified_name =
      pt.get_optional<std::string>(PG_DATA_TYPE_QUALIFIED_NAME).value_or("");
}

}  // namespace manager::metadata
