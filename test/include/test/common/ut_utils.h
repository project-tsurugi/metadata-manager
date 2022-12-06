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
#ifndef TEST_INCLUDE_TEST_COMMON_UT_UTILS_H_
#define TEST_INCLUDE_TEST_COMMON_UT_UTILS_H_

#include <iostream>
#include <string>

#include <boost/property_tree/ptree.hpp>

#include "manager/metadata/metadata.h"

namespace manager::metadata::testing {

class UTUtils {
 public:
  static std::string generate_narrow_uid();

  static void skip_if_connection_not_opened();
  static void skip_if_connection_opened();
  static void skip_if_json();
  static void skip_if_postgresql();

  static bool is_json();
  static bool is_postgresql();

  static std::string get_tree_string(const boost::property_tree::ptree& pt);
  static std::string get_tree_string(const manager::metadata::Object& ob);
  static std::string print_tree(const boost::property_tree::ptree& pt,
                                int level);

  template <typename T>
  static auto to_integral(std::string_view input) ->
      typename std::enable_if_t<std::is_integral<T>::value, T> {
    T res_value;
    try {
      res_value = std::stoi(std::string(input));
    } catch (...) {
      res_value = -1;
    }
    return res_value;
  }

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

#endif  // TEST_INCLUDE_TEST_COMMON_UT_UTILS_H_
