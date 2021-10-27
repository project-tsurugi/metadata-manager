/*
 * Copyright 2021 tsurugi project.
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
#ifndef MANAGER_METADATA_MANAGER_TEST_V4_0_POSTGRESQL_INCLUDE_TEST_HELPER_DATA_TYPES_HELPER_H_
#define MANAGER_METADATA_MANAGER_TEST_V4_0_POSTGRESQL_INCLUDE_TEST_HELPER_DATA_TYPES_HELPER_H_

#include <boost/property_tree/ptree.hpp>
#include <string>
#include <string_view>
#include <tuple>
#include <vector>

namespace manager::metadata::testing {

typedef std::tuple<std::string, std::string> TestDatatypesType;

class DataTypesHelper {
 public:
  static std::vector<TestDatatypesType> make_datatypes_tuple();
  static std::vector<std::string> make_datatype_names();

  static void check_datatype_metadata_expected(
      const boost::property_tree::ptree& datatype);

 private:
  static void make_datatypes_tuple(std::string_view key,
                                   const std::vector<std::string> values,
                                   std::vector<TestDatatypesType>& v);
};

}  // namespace manager::metadata::testing

#endif  // MANAGER_METADATA_MANAGER_TEST_V4_0_POSTGRESQL_INCLUDE_TEST_HELPER_DATA_TYPES_HELPER_H_
