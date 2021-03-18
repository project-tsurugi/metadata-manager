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

#ifndef UT_UTILS_H_
#define UT_UTILS_H_

#include <boost/property_tree/ptree.hpp>
#include <iostream>
#include <memory>
#include <string>

#include "manager/metadata/entity/table_statistic.h"

#include "test/utility/ut_column_metadata.h"
#include "test/utility/ut_table_metadata.h"

namespace manager::metadata::testing {

class UTUtils {
   public:
    static void skip_if_connection_not_opened();
    static void skip_if_connection_opened();
    static std::string indent(int level);
    static std::string get_tree_string(const boost::property_tree::ptree &pt);
    static std::string print_tree(const boost::property_tree::ptree &pt,
                                  int level);
    static void print_column_metadata(const UTColumnMetadata &column_metadata);
    static void print_table_statistics(const TableStatistic &table_statistics);
    static void generate_table_metadata(
        std::unique_ptr<UTTableMetadata> &testdata_table_metadata,
        bool with_primary_keys);
    static std::string generate_random_string();
    static boost::property_tree::ptree generate_histogram();
    static boost::property_tree::ptree generate_histogram_array();
    static boost::property_tree::ptree generate_column_statistic();

    static void print() {
#ifndef NDEBUG
        std::cout << std::endl;
#endif
    }

    template <class T, class... A>
    static void print([[maybe_unused]] const T &first,
                      [[maybe_unused]] const A &... rest) {
#ifndef NDEBUG
        std::cout << first;
        print(rest...);
#endif
    }

    template <class... A>
    static void print([[maybe_unused]] const A &... rest) {
#ifndef NDEBUG
        print(rest...);
#endif
    }

   private:
    static void get_tree_string_internal(const boost::property_tree::ptree &pt,
                                         int level, std::string &output_string,
                                         bool print_tree_enabled);
    static constexpr char ALPHANUM[] =
        "0123456789"
        "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
        "abcdefghijklmnopqrstuvwxyz";
    static constexpr int NUMBER_OF_ITERATIONS = 10;
    static constexpr int NUMBER_OF_RANDOM_CHARACTER = 10;
    static constexpr int UPPER_VALUE_20000 = 20000;
    static constexpr int UPPER_VALUE_100 = 100;
};  // class UTUtils

}  // namespace manager::metadata::testing

#endif  // UT_UTILS_H_
