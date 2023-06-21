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
#include "manager/metadata/column.h"

#include "manager/metadata/helper/ptree_helper.h"

// =============================================================================
namespace manager::metadata {

boost::property_tree::ptree Column::convert_to_ptree() const {
  auto pt = Object::convert_to_ptree();

  // table_id
  pt.put(TABLE_ID, this->table_id);
  // column_number
  pt.put(COLUMN_NUMBER, this->column_number);
  // data_type_id
  pt.put(DATA_TYPE_ID, this->data_type_id);
  // varying
  pt.put(VARYING, this->varying);
  // is_not_null
  pt.put(IS_NOT_NULL, this->is_not_null);
  // default_expression
  pt.put(DEFAULT_EXPR, this->default_expression);
  // is_funcexpr
  pt.put(IS_FUNCEXPR, this->is_funcexpr);
  // data_length
  pt.push_back(std::make_pair(
      DATA_LENGTH, ptree_helper::make_array_ptree(this->data_length)));

  return pt;
}

void Column::convert_from_ptree(const boost::property_tree::ptree& pt) {
  Object::convert_from_ptree(pt);

  // table_id
  this->table_id =
      pt.get_optional<ObjectId>(TABLE_ID).get_value_or(INVALID_OBJECT_ID);
  // column_number
  this->column_number =
      pt.get_optional<int64_t>(COLUMN_NUMBER).get_value_or(INVALID_VALUE);
  // data_type_id
  this->data_type_id =
      pt.get_optional<ObjectId>(DATA_TYPE_ID).get_value_or(INVALID_OBJECT_ID);
  // data_length
  this->data_length = ptree_helper::make_vector_int(pt, DATA_LENGTH);
  // varying
  this->varying = pt.get_optional<bool>(VARYING).get_value_or(false);
  // is_not_null
  this->is_not_null = pt.get_optional<bool>(IS_NOT_NULL).get_value_or(false);
  // default_expression
  this->default_expression =
      pt.get_optional<std::string>(DEFAULT_EXPR).get_value_or("");
  // is_funcexpr
  this->is_funcexpr = pt.get_optional<bool>(IS_FUNCEXPR).get_value_or(false);
}

}  // namespace manager::metadata
