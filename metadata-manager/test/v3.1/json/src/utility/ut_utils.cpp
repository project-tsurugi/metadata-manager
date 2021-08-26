/*
 * Copyright 2020 tsurugi project.
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
#include "test/utility/ut_utils.h"

#include <gtest/gtest.h>
#include <memory>
#include <random>
#include <string>
#include <vector>

#include "manager/metadata/datatypes.h"
#include "manager/metadata/metadata.h"
#include "manager/metadata/tables.h"
#include "test/global_test_environment.h"

using namespace manager::metadata;
using namespace boost::property_tree;

namespace manager::metadata::testing {

/**
 * @brief internal function used in get_tree_string_internal
 */
std::string UTUtils::indent(int level) {
  std::string s;
  for (int i = 0; i < level; i++) s += "  ";
  return s;
}

/**
 * @brief internal function used in get_tree_string, print_tree.
 * get string converted from ptree.
 * @param  (pt)                   [in]  ptree to be converted to string.
 * @param  (level)                [in]  indent level.
 * @param  (output_string)        [out] string converted from ptree.
 * @param  (print_tree_enabled)   [in]  enable/disable to print output_string.
 */
void UTUtils::get_tree_string_internal(const ptree& pt, int level,
                                       std::string& output_string,
                                       bool print_tree_enabled) {
  if (pt.empty()) {
    output_string.append("\"");
    output_string.append(pt.data());
    output_string.append("\"");

    if (print_tree_enabled) std::cerr << "\"" << pt.data() << "\"";
  } else {
    if (level && print_tree_enabled) std::cerr << std::endl;

    if (print_tree_enabled) std::cerr << indent(level) << "{" << std::endl;
    output_string.append("{");

    for (ptree::const_iterator pos = pt.begin(); pos != pt.end();) {
      if (print_tree_enabled)
        std::cerr << indent(level + 1) << "\"" << pos->first << "\": ";
      output_string.append("\"");
      output_string.append(pos->first);
      output_string.append("\": ");

      get_tree_string_internal(pos->second, level + 1, output_string,
                               print_tree_enabled);
      ++pos;
      if (pos != pt.end()) {
        if (print_tree_enabled) std::cerr << ",";
        output_string.append(",");
      }
      if (print_tree_enabled) std::cerr << std::endl;
    }

    if (print_tree_enabled) std::cerr << indent(level) << " }";
    output_string.append(" }");
  }

  return;
}

/**
 * @brief Get string converted from ptree. (not print string)
 * @param  (pt)                   [in]  ptree to be converted to string.
 */
std::string UTUtils::get_tree_string(const ptree& pt) {
  std::string output_string;
  int level = 0;
  get_tree_string_internal(pt, level, output_string, false);
  return output_string;
}

/**
 * @brief Get and print string converted from ptree.
 * @param  (pt)                   [in]  ptree to be converted to string.
 * @param  (level)                [in]  indent level.
 */
std::string UTUtils::print_tree(const ptree& pt, int level) {
  std::string output_string;
  get_tree_string_internal(pt, level, output_string, true);
  std::cerr << std::endl;
  return output_string;
}

/**
 * @brief Print column metadata fields used as test data.
 * @param  (column_metadata)    [in] column metadata used as test data.
 */
void UTUtils::print_column_metadata(const UTColumnMetadata& column_metadata) {
  print("id:", column_metadata.id);
  print("tableId:", column_metadata.table_id);
  print("name:", column_metadata.name);
  print("ordinalPosition:", column_metadata.ordinal_position);
  print("dataTypeId:", column_metadata.data_type_id);
  print("dataLength:", column_metadata.data_length);
  print("varying:", column_metadata.varying);
  print("nullable:", column_metadata.nullable);
  print("defaultExpr:", column_metadata.default_expr);
  print("direction:", column_metadata.direction);
}

/**
 * @brief Generate table metadata.
 * @param  (testdata_table_metadata)    [out] table metadata used as test data.
 */
void UTUtils::generate_table_metadata(
    std::unique_ptr<UTTableMetadata>& testdata_table_metadata) {
  // generate unique table name.
  int s = time(NULL);
  std::string table_name = "table_name" + std::to_string(s);
  testdata_table_metadata = std::make_unique<UTTableMetadata>(table_name);

  // generate namespace.
  testdata_table_metadata->namespace_name = "namespace";

  // generate three column metadatas.
  std::vector<ObjectIdType> ordinal_positions = {1, 2, 3};
  std::vector<std::string> col_names = {"col1", "col2", "col3"};

  // generate primary keys.
  testdata_table_metadata->primary_keys.push_back(ordinal_positions[0]);
  testdata_table_metadata->primary_keys.push_back(ordinal_positions[1]);

  // first column metadata
  bool is_null = true;
  UTColumnMetadata column1{
      col_names[0], ordinal_positions[0],
      static_cast<ObjectIdType>(DataTypes::DataTypesId::FLOAT32), !is_null};
  column1.direction =
      static_cast<ObjectIdType>(Tables::Column::Direction::ASCENDANT);

  // second column metadata
  UTColumnMetadata column2{
      col_names[1], ordinal_positions[1],
      static_cast<ObjectIdType>(DataTypes::DataTypesId::VARCHAR), !is_null};
  column2.direction =
      static_cast<ObjectIdType>(Tables::Column::Direction::DEFAULT);
  ptree data_length;
  data_length.put("", 8);
  column2.p_data_lengths.push_back(std::make_pair("", data_length));
  data_length.put("", 2);
  column2.p_data_lengths.push_back(std::make_pair("", data_length));

  column2.varying = true;

  // third column metadata
  UTColumnMetadata column3{
      col_names[2], ordinal_positions[2],
      static_cast<ObjectIdType>(DataTypes::DataTypesId::CHAR), is_null};
  column3.default_expr = "default";
  column3.data_length = 1;
  column3.varying = false;

  // set table metadata to three column metadata
  testdata_table_metadata->columns.emplace_back(column1);
  testdata_table_metadata->columns.emplace_back(column2);
  testdata_table_metadata->columns.emplace_back(column3);

  // generate ptree from UTTableMetadata fields.
  testdata_table_metadata->generate_ptree();
}

/**
 * @brief Generate one random string.
 */
std::string UTUtils::generate_random_string() {
  std::string random_string;
  std::random_device rd;
  std::mt19937 random_mt(rd());

  for (int i = 0;
       i < static_cast<int>(random_mt() % NUMBER_OF_RANDOM_CHARACTER + 1);
       i++) {
    random_string.push_back(ALPHANUM[random_mt() % (sizeof(ALPHANUM) - 1)]);
  }

  return random_string;
}

/**
 * @brief Generate histogram of values used as column statistics test data.
 */
ptree UTUtils::generate_histogram() {
  ptree values;
  std::random_device rd;
  std::mt19937 random_mt(rd());

  int random_number = random_mt();

  // If random number is even, generate random number histogram.
  // If random number is odd, generate random string histogram.
  if (random_number % 2 == 0) {
    for (int i = 0;
         i < static_cast<int>(random_mt() % NUMBER_OF_ITERATIONS + 1); i++) {
      ptree p_value;
      int i_value = random_mt() % UPPER_VALUE_20000 + 1;
      p_value.put("", i_value);
      values.push_back(std::make_pair("", p_value));
    }
  } else {
    for (int i = 0;
         i < static_cast<int>(random_mt() % NUMBER_OF_ITERATIONS + 1); i++) {
      ptree p_value;
      std::string random_string = generate_random_string();
      p_value.put("", random_string);
      values.push_back(std::make_pair("", p_value));
    }
  }

  return values;
}

/**
 * @brief Generate histogram of array elements used as column statistics test
 * data.
 */
ptree UTUtils::generate_histogram_array() {
  ptree array_of_values;
  std::random_device rd;
  std::mt19937 random_mt(rd());

  for (int i = 0; i < static_cast<int>(random_mt() % NUMBER_OF_ITERATIONS + 1);
       i++) {
    ptree values;
    array_of_values.push_back(std::make_pair("", generate_histogram()));
  }

  return array_of_values;
}

/**
 * @brief Generate one column statistics used as test data.
 */
ptree UTUtils::generate_column_statistic() {
  std::random_device rd;
  std::mt19937 random_mt(rd());

  double null_frac = static_cast<double>(random_mt() / RAND_MAX);
  int avg_width = random_mt() % UPPER_VALUE_100 + 1;
  int n_distinct = random_mt() % UPPER_VALUE_100 + 1;
  double correlation = -1 * static_cast<double>(random_mt() / RAND_MAX);

  ptree column_statistic;
  column_statistic.put("null_frac", null_frac);
  column_statistic.put("avg_width", avg_width);
  column_statistic.put("most_common_vals", "mcv");
  column_statistic.put("n_distinct", n_distinct);
  column_statistic.put("most_common_freqs", "mcf");
  column_statistic.add_child("histogram_bounds", UTUtils::generate_histogram());
  column_statistic.put("correlation", correlation);
  column_statistic.put("most_common_elems", "mce");
  column_statistic.put("most_common_elem_freqs", "mcef");
  column_statistic.add_child("elem_count_histogram",
                             UTUtils::generate_histogram_array());

  return column_statistic;
}

}  // namespace manager::metadata::testing
