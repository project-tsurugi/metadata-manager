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
#include "test/utility/ut_utils.h"

#include <gtest/gtest.h>

#include <iostream>

#include "test/global_test_environment.h"

namespace manager::metadata::testing {

/**
 * @brief Skip tests if a connection to metadata repository is not opened.
 */
void UTUtils::skip_if_connection_not_opened() {
  if (!global->is_open()) {
    GTEST_SKIP_("metadata repository is not started.");
  }
}

/**
 * @brief Skip tests if a connection to metadata repository is opened.
 */
void UTUtils::skip_if_connection_opened() {
  if (global->is_open()) {
    GTEST_SKIP_("metadata repository is started.");
  }
}

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
 * @param (pt)                   [in]  ptree to be converted to string.
 * @param (level)                [in]  indent level.
 * @param (output_string)        [out] string converted from ptree.
 * @param (print_tree_enabled)   [in]  enable/disable to print output_string.
 */
void UTUtils::get_tree_string_internal(const boost::property_tree::ptree& pt,
                                       int level, std::string& output_string,
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

    for (auto pos = pt.begin(); pos != pt.end();) {
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
 * @param (pt)                   [in]  ptree to be converted to string.
 */
std::string UTUtils::get_tree_string(const boost::property_tree::ptree& pt) {
  std::string output_string;
  int level = 0;
  get_tree_string_internal(pt, level, output_string, false);
  return output_string;
}

/**
 * @brief Get and print string converted from ptree.
 * @param (pt)                   [in]  ptree to be converted to string.
 * @param (level)                [in]  indent level.
 */
std::string UTUtils::print_tree(const boost::property_tree::ptree& pt,
                                int level) {
  std::string output_string;
  get_tree_string_internal(pt, level, output_string, true);
  std::cerr << std::endl;
  return output_string;
}

}  // namespace manager::metadata::testing
