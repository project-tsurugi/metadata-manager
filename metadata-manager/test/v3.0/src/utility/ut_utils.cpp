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
#include "manager/metadata/tables.h"

#include "test/api_test_environment.h"

using namespace manager::metadata;
using namespace boost::property_tree;

namespace manager::metadata::testing {

void UTUtils::skip_if_connection_not_opened() {
    if (!api_test_env->is_open()) {
        GTEST_SKIP_("metadata repository is not started.");
    }
}

void UTUtils::skip_if_connection_opened() {
    if (api_test_env->is_open()) {
        GTEST_SKIP_("metadata repository is started.");
    }
}

std::string UTUtils::indent(int level) {
    std::string s;
    for (int i = 0; i < level; i++) s += "  ";
    return s;
}

void UTUtils::get_tree_string_internal(const ptree &pt, int level,
                                       std::string &output_string,
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

std::string UTUtils::get_tree_string(const ptree &pt) {
    std::string output_string;
    int level = 0;
    get_tree_string_internal(pt, level, output_string, false);
    return output_string;
}

std::string UTUtils::print_tree(const ptree &pt, int level) {
    std::string output_string;
    get_tree_string_internal(pt, level, output_string, true);
    std::cerr << std::endl;
    return output_string;
}

void UTUtils::print_column_metadata(const UTColumnMetadata &column_metadata) {
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

void UTUtils::print_table_statistics(const TableStatistic &table_statistics) {
    print("id:", table_statistics.id);
    print("name:", table_statistics.name);
    print("namespace:", table_statistics.namespace_name);
    print("reltuples:", table_statistics.reltuples);
}

void UTUtils::generate_table_metadata(
    std::unique_ptr<UTTableMetadata> &testdata_table_metadata,
    bool with_primary_keys) {
    std::vector<int64_t> ordinal_positions;

    int64_t column_count = 3;
    for (int64_t op = 1; op <= column_count; op++) {
        ordinal_positions.push_back(op);
    }

    std::vector<std::string> col_names;
    for (int op : ordinal_positions) {
        std::string col_name = "col" + std::to_string(op);
        col_names.push_back(col_name);
    }

    int s = time(NULL);

    std::string table_name = "table_name" + std::to_string(s);

    testdata_table_metadata = std::make_unique<UTTableMetadata>(table_name);

    if (with_primary_keys) {
        testdata_table_metadata->primary_keys.push_back(ordinal_positions[0]);
        testdata_table_metadata->primary_keys.push_back(ordinal_positions[1]);
    }

    testdata_table_metadata->name = table_name;

    UTColumnMetadata column1{
        col_names[0], ordinal_positions[0],
        static_cast<int64_t>(DataTypes::DataTypesId::FLOAT32), false};
    column1.direction =
        static_cast<int64_t>(Tables::Column::Direction::ASCENDANT);

    UTColumnMetadata column2{
        col_names[1], ordinal_positions[1],
        static_cast<int64_t>(DataTypes::DataTypesId::VARCHAR), false};
    column2.direction =
        static_cast<int64_t>(Tables::Column::Direction::DEFAULT);
    column2.data_length = 8;
    column2.varying = true;

    UTColumnMetadata column3{col_names[2], ordinal_positions[2],
                             static_cast<int64_t>(DataTypes::DataTypesId::CHAR),
                             true};
    column3.direction =
        static_cast<int64_t>(Tables::Column::Direction::DEFAULT);
    column3.data_length = 1;
    column3.varying = false;

    testdata_table_metadata->columns.push_back(column1);
    testdata_table_metadata->columns.push_back(column2);
    testdata_table_metadata->columns.push_back(column3);

    testdata_table_metadata->generate_ptree();
}

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

ptree UTUtils::generate_histogram() {
    ptree values;
    std::random_device rd;
    std::mt19937 random_mt(rd());

    int random_number = random_mt();

    // if random number is even, generate random number histogram
    // if random number is odd, generate random string histogram
    if (random_number % 2 == 0) {
        for (int i = 0;
             i < static_cast<int>(random_mt() % NUMBER_OF_ITERATIONS + 1);
             i++) {
            ptree p_value;
            int i_value = random_mt() % UPPER_VALUE_20000 + 1;
            p_value.put("", i_value);
            values.push_back(std::make_pair("", p_value));
        }
    } else {
        for (int i = 0;
             i < static_cast<int>(random_mt() % NUMBER_OF_ITERATIONS + 1);
             i++) {
            ptree p_value;
            std::string random_string = generate_random_string();
            p_value.put("", random_string);
            values.push_back(std::make_pair("", p_value));
        }
    }

    return values;
}

ptree UTUtils::generate_histogram_array() {
    ptree array_of_values;
    std::random_device rd;
    std::mt19937 random_mt(rd());

    for (int i = 0;
         i < static_cast<int>(random_mt() % NUMBER_OF_ITERATIONS + 1); i++) {
        ptree values;
        array_of_values.push_back(std::make_pair("", generate_histogram()));
    }

    return array_of_values;
}

ptree UTUtils::generate_column_statistic() {
    ptree column;

    std::random_device rd;
    std::mt19937 random_mt(rd());

    double null_frac = static_cast<double>(random_mt() / RAND_MAX);
    int avg_width = random_mt() % UPPER_VALUE_100 + 1;
    int n_distinct = random_mt() % UPPER_VALUE_100 + 1;
    double correlation = -1 * static_cast<double>(random_mt() / RAND_MAX);

    column.put("null_frac", null_frac);
    column.put("avg_width", avg_width);
    column.put("most_common_vals", "mcv");
    column.put("n_distinct", n_distinct);
    column.put("most_common_freqs", "mcf");
    column.put("histogram_bounds", "histogram_bounds");
    column.add_child("histogram_bounds", UTUtils::generate_histogram());
    column.put("correlation", correlation);
    column.put("most_common_elems", "mce");
    column.put("most_common_elem_freqs", "mcef");
    column.add_child("elem_count_histogram",
                     UTUtils::generate_histogram_array());

    return column;
}

}  // namespace manager::metadata::testing
