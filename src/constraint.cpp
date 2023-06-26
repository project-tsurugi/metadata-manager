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
#include "manager/metadata/constraint.h"

#include "manager/metadata/helper/ptree_helper.h"

// =============================================================================
namespace manager::metadata {

boost::property_tree::ptree Constraint::convert_to_ptree() const {
  auto pt = Object::convert_to_ptree();

  // ID.
  if (this->id <= 0) {
    // If the value is invalid, the put data is erased.
    pt.erase(ID);
  }
  // table ID.
  if (this->table_id > 0) {
    // Put only if the value is valid.
    pt.put(TABLE_ID, this->table_id);
  }
  // constraint type.
  pt.put(TYPE, static_cast<int64_t>(this->type));
  // column numbers.
  auto columns_node = ptree_helper::make_array_ptree(this->columns);
  pt.add_child(COLUMNS, columns_node);
  // column IDs.
  auto columns_id_node = ptree_helper::make_array_ptree(this->columns_id);
  pt.add_child(COLUMNS_ID, columns_id_node);
  // index ID.
  pt.put(INDEX_ID, this->index_id);
  // constraint expression.
  pt.put(EXPRESSION, this->expression);
  // referenced table name.
  pt.put(PK_TABLE, this->pk_table);
  // referenced column numbers.
  auto pk_columns_node = ptree_helper::make_array_ptree(this->pk_columns);
  pt.add_child(PK_COLUMNS, pk_columns_node);
  // referenced column IDs.
  auto pk_columns_id_node = ptree_helper::make_array_ptree(this->pk_columns_id);
  pt.add_child(PK_COLUMNS_ID, pk_columns_id_node);
  // referenced rows match type.
  pt.put(FK_MATCH_TYPE, static_cast<int64_t>(this->fk_match_type));
  // referenced row delete action.
  pt.put(FK_DELETE_ACTION, static_cast<int64_t>(this->fk_delete_action));
  // referenced row update action.
  pt.put(FK_UPDATE_ACTION, static_cast<int64_t>(this->fk_update_action));

  return pt;
}

void Constraint::convert_from_ptree(const boost::property_tree::ptree& pt) {
  Object::convert_from_ptree(pt);

  // table ID.
  this->table_id =
      pt.get_optional<ObjectId>(TABLE_ID).value_or(INVALID_OBJECT_ID);
  // constraint type.
  this->type =
      static_cast<ConstraintType>(pt.get_optional<int64_t>(TYPE).value_or(-1));
  // column numbers.
  this->columns = ptree_helper::make_vector_int(pt, COLUMNS);
  // column IDs.
  this->columns_id = ptree_helper::make_vector_int(pt, COLUMNS_ID);
  // index ID.
  this->index_id = pt.get_optional<int64_t>(INDEX_ID).value_or(INVALID_VALUE);
  // constraint expression.
  this->expression = pt.get_optional<std::string>(EXPRESSION).value_or("");
  // referenced table name.
  this->pk_table = pt.get_optional<std::string>(PK_TABLE).value_or("");
  // referenced column numbers.
  this->pk_columns = ptree_helper::make_vector_int(pt, PK_COLUMNS);
  // referenced column IDs.
  this->pk_columns_id = ptree_helper::make_vector_int(pt, PK_COLUMNS_ID);
  // referenced rows match type.
  this->fk_match_type = static_cast<MatchType>(
      pt.get_optional<int64_t>(FK_MATCH_TYPE).value_or(-1));
  // referenced row delete action.
  this->fk_delete_action = static_cast<ActionType>(
      pt.get_optional<int64_t>(FK_DELETE_ACTION).value_or(-1));
  // referenced row update action.
  this->fk_update_action = static_cast<ActionType>(
      pt.get_optional<int64_t>(FK_UPDATE_ACTION).value_or(-1));
}

}  // namespace manager::metadata
