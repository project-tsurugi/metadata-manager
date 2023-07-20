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
#include "manager/metadata/table.h"

#include <boost/foreach.hpp>

// =============================================================================
namespace manager::metadata {

using boost::property_tree::ptree;

boost::property_tree::ptree Table::convert_to_ptree() const {
  auto pt = ClassObject::convert_to_ptree();

  // number_of_tuples
  pt.put<int64_t>(NUMBER_OF_TUPLES, this->number_of_tuples);

  // columns metadata
  ptree ptree_columns;
  for (const auto& column : columns) {
    auto child = column.convert_to_ptree();
    ptree_columns.push_back(std::make_pair("", child));
  }
  pt.add_child(COLUMNS_NODE, ptree_columns);

  // constraints metadata
  ptree ptree_constraints;
  for (const auto& constraint : this->constraints) {
    auto child = constraint.convert_to_ptree();
    ptree_constraints.push_back(std::make_pair("", child));
  }
  pt.add_child(CONSTRAINTS_NODE, ptree_constraints);

  return pt;
}

void Table::convert_from_ptree(const boost::property_tree::ptree& pt) {
  ClassObject::convert_from_ptree(pt);

  // number_of_tuples
  this->number_of_tuples =
      pt.get_optional<int64_t>(NUMBER_OF_TUPLES).get_value_or(INVALID_VALUE);

  // columns metadata
  this->columns.clear();
  BOOST_FOREACH (const auto& node, pt.get_child(COLUMNS_NODE)) {
    const auto& ptree_column = node.second;
    Column column;
    column.convert_from_ptree(ptree_column);
    this->columns.emplace_back(column);
  }

  // constraints metadata
  this->constraints.clear();
  BOOST_FOREACH (const auto& node, pt.get_child(CONSTRAINTS_NODE)) {
    const auto& ptree_constraint = node.second;

    Constraint constraint;
    constraint.convert_from_ptree(ptree_constraint);
    this->constraints.emplace_back(constraint);
  }
}

}  // namespace manager::metadata
