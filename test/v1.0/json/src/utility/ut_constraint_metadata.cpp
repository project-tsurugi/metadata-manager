/*
 * Copyright 2022 tsurugi project.
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
#include "test/utility/ut_constraint_metadata.h"

#include <utility>

#include "manager/metadata/constraints.h"

namespace manager::metadata::testing {

using boost::property_tree::ptree;

/**
 * @brief Generate ptree type constrain metadata from UTConstraintMetadata fields.
 * @return none.
 */
void UTConstraintMetadata::generate_ptree() {
  constraints_metadata.clear();

  // id
  if (id != NOT_INITIALIZED) {
    constraints_metadata.put(Constraint::ID, id);
  }

  // name
  if (!name.empty()) {
    constraints_metadata.put(Constraint::NAME, name);
  }

  // table id
  if (table_id != NOT_INITIALIZED) {
    constraints_metadata.put(Constraint::TABLE_ID, table_id);
  }

  // type
  constraints_metadata.put(Constraint::TYPE, type);

  // columns
  if (columns_list.size() > 0) {
    ptree element;
    ptree elements;

    for (int value : columns_list) {
      element.put("", value);
      elements.push_back(std::make_pair("", element));
    }
    constraints_metadata.add_child(Constraint::COLUMNS, elements);
  }

  // columns id
  if (columns_id_list.size() > 0) {
    ptree element;
    ptree elements;

    for (int value : columns_id_list) {
      element.put("", value);
      elements.push_back(std::make_pair("", element));
    }
    constraints_metadata.add_child(Constraint::COLUMNS_ID, elements);
  }

  // index_id
  if (index_id != NOT_INITIALIZED) {
    constraints_metadata.put(Constraint::INDEX_ID, index_id);
  }

  // expression
  if (!expression.empty()) {
    constraints_metadata.put(Constraint::EXPRESSION, expression);
  }
}

}  // namespace manager::metadata::testing
