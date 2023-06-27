/*
 * Copyright 2022-2023 tsurugi project.
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
#include "manager/metadata/index.h"

#include "manager/metadata/helper/ptree_helper.h"

namespace manager::metadata {

using boost::property_tree::ptree;

boost::property_tree::ptree Index::convert_to_ptree() const {
  auto pt = ClassObject::convert_to_ptree();

  // owner_id
  pt.put(OWNER_ID, this->owner_id);
  // table_id
  pt.put(TABLE_ID, this->table_id);
  // access_method
  pt.put(ACCESS_METHOD, this->access_method);
  // is_primary
  pt.put(IS_PRIMARY, this->is_primary);
  // is_unique
  pt.put(IS_UNIQUE, this->is_unique);
  // number_of_columns
  pt.put(NUMBER_OF_COLUMNS, this->number_of_columns);
  // number_of_key_columns
  pt.put(NUMBER_OF_KEY_COLUMNS, this->number_of_key_columns);
  // keys
  ptree child = ptree_helper::make_array_ptree(this->keys);
  pt.add_child(KEYS, child);
  // keys_id
  child = ptree_helper::make_array_ptree(this->keys_id);
  pt.add_child(KEYS_ID, child);
  // options
  child = ptree_helper::make_array_ptree(this->options);
  pt.add_child(OPTIONS, child);

  return pt;
}

void Index::convert_from_ptree(const boost::property_tree::ptree& pt) {
  ClassObject::convert_from_ptree(pt);

  // table_id
  this->table_id =
      pt.get_optional<ObjectId>(TABLE_ID).value_or(INVALID_OBJECT_ID);
  // access_method
  this->access_method =
      pt.get_optional<int64_t>(ACCESS_METHOD).value_or(INVALID_VALUE);
  // is_primary
  this->is_primary = pt.get_optional<bool>(IS_PRIMARY).value_or(false);
  // is_unique
  this->is_unique = pt.get_optional<bool>(IS_UNIQUE).value_or(false);
  // number_of_columns
  this->number_of_columns =
      pt.get_optional<int64_t>(NUMBER_OF_COLUMNS).value_or(INVALID_VALUE);
  // number_of_key_columns
  this->number_of_key_columns =
      pt.get_optional<int64_t>(NUMBER_OF_KEY_COLUMNS).value_or(INVALID_VALUE);
  // keys
  this->keys = ptree_helper::make_vector_int(pt, KEYS);
  // keys_id
  this->keys_id = ptree_helper::make_vector_int(pt, KEYS_ID);
  // options
  this->options = ptree_helper::make_vector_int(pt, OPTIONS);
}

}  // namespace manager::metadata
