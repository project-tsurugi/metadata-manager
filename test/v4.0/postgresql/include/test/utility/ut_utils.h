/*
 * Copyright 2020-2021 tsurugi project.
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
#ifndef MANAGER_METADATA_MANAGER_TEST_V4_0_POSTGRESQL_INCLUDE_TEST_UTILITY_UT_UTILS_H_
#define MANAGER_METADATA_MANAGER_TEST_V4_0_POSTGRESQL_INCLUDE_TEST_UTILITY_UT_UTILS_H_

#include <boost/property_tree/ptree.hpp>
#include <iostream>
#include <string>

namespace manager::metadata::testing {

class UTUtils {
 public:
  static void skip_if_connection_not_opened();
  static void skip_if_connection_opened();
  static std::string get_tree_string(const boost::property_tree::ptree& pt);
  static std::string print_tree(const boost::property_tree::ptree& pt,
                                int level);

  static void print() {
#ifndef NDEBUG
    std::cout << std::endl;
#endif
  }

  template <class T, class... A>
  static void print([[maybe_unused]] const T& first,
                    [[maybe_unused]] const A&... rest) {
#ifndef NDEBUG
    std::cout << first;
    print(rest...);
#endif
  }

  template <class... A>
  static void print([[maybe_unused]] const A&... rest) {
#ifndef NDEBUG
    print(rest...);
#endif
  }

 private:
  static std::string indent(int level);
  static void get_tree_string_internal(const boost::property_tree::ptree& pt,
                                       int level, std::string& output_string,
                                       bool print_tree_enabled);
};  // class UTUtils

}  // namespace manager::metadata::testing

#endif  // MANAGER_METADATA_MANAGER_TEST_V4_0_POSTGRESQL_INCLUDE_TEST_UTILITY_UT_UTILS_H_
