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
#ifndef TEST_INCLUDE_TEST_METADATA_UT_CONSTRAINT_METADATA_H_
#define TEST_INCLUDE_TEST_METADATA_UT_CONSTRAINT_METADATA_H_

#include <string>
#include <vector>

#include <boost/property_tree/ptree.hpp>

#include "manager/metadata/constraints.h"

namespace manager::metadata::testing {

class UTConstraintMetadata {
 public:
  int64_t id = NOT_INITIALIZED;
  std::string name;
  int64_t table_id = NOT_INITIALIZED;
  int64_t type     = NOT_INITIALIZED;
  int64_t columns  = NOT_INITIALIZED;     //!< @brief single value of columns
  boost::property_tree::ptree p_columns;  //!< @brief array of columns
  std::vector<int64_t> columns_list;
  int64_t columns_id = NOT_INITIALIZED;  //!< @brief single value of columns_id
  boost::property_tree::ptree p_columns_id;  //!< @brief array of columns_id
  std::vector<int64_t> columns_id_list;
  int64_t index_id = NOT_INITIALIZED;
  std::string expression;

  boost::property_tree::ptree constraints_metadata;

  UTConstraintMetadata() = delete;
  explicit UTConstraintMetadata(std::string name,
                                Constraint::ConstraintType type)
      : name(name), type(static_cast<int64_t>(type)) {}

  void generate_ptree();

 private:
  static constexpr int64_t NOT_INITIALIZED = -1;
};

}  // namespace manager::metadata::testing

#endif  // TEST_INCLUDE_TEST_METADATA_UT_CONSTRAINT_METADATA_H_
