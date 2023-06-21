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
#include "manager/metadata/statistic.h"

#include "manager/metadata/helper/ptree_helper.h"

// =============================================================================
namespace manager::metadata {

using boost::property_tree::ptree;

boost::property_tree::ptree Statistic::convert_to_ptree() const {
  auto pt = Object::convert_to_ptree();

  // table_id
  pt.put(TABLE_ID, this->table_id);
  // column_number
  pt.put(COLUMN_NUMBER, this->column_number);
  // column_id
  pt.put(COLUMN_ID, this->column_id);
  // column_name
  pt.put(COLUMN_NAME, this->column_name);
  // column_statistic
  ptree statistics;
  ptree_helper::json_to_ptree(this->column_statistic, statistics);
  pt.put_child(COLUMN_STATISTIC, statistics);

  return pt;
}

void Statistic::convert_from_ptree(const boost::property_tree::ptree& pt) {
  Object::convert_from_ptree(pt);

  // table_id
  this->table_id =
      pt.get_optional<ObjectId>(TABLE_ID).get_value_or(INVALID_OBJECT_ID);
  // column_number
  this->column_number =
      pt.get_optional<int64_t>(COLUMN_NUMBER).get_value_or(INVALID_VALUE);
  // column_id
  this->column_id =
      pt.get_optional<ObjectId>(COLUMN_ID).get_value_or(INVALID_OBJECT_ID);
  // column_name
  this->column_name =
      pt.get_optional<std::string>(COLUMN_NAME).get_value_or("");
  // opt_statistic
  auto opt_statistic = pt.get_child_optional(COLUMN_STATISTIC);
  if (opt_statistic) {
    this->column_statistic = ptree_helper::ptree_to_json(opt_statistic.get());
  } else {
    this->column_statistic = kEmptyStringJson;
  }
}

}  // namespace manager::metadata
