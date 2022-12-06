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
#ifndef TEST_INCLUDE_TEST_METADATA_UT_DATATYPES_METADATA_H_
#define TEST_INCLUDE_TEST_METADATA_UT_DATATYPES_METADATA_H_

#include <string>
#include <vector>

#include <boost/property_tree/ptree.hpp>

#include "manager/metadata/datatypes.h"
#include "test/metadata/ut_metadata.h"

namespace manager::metadata::testing {

class UtDataTypesMetadata : public UtMetadata<manager::metadata::DataType> {
 public:
  using UtMetadata::UtMetadata;

  UtDataTypesMetadata();

  void check_metadata_expected(const boost::property_tree::ptree& expected,
                               const boost::property_tree::ptree& actual,
                               const char* file,
                               const int64_t line) const override;

  void check_metadata_expected(const boost::property_tree::ptree& actual,
                               const char* file, const int64_t line) const;

  std::vector<std::string> get_datatype_ids() const;
  std::vector<std::string> get_datatype_names() const;
  std::vector<std::string> get_pg_datatype_ids() const;
  std::vector<std::string> get_pg_datatype_names() const;
  std::vector<std::string> get_pg_datatype_qualified_names() const;
};

}  // namespace manager::metadata::testing

#endif  // TEST_INCLUDE_TEST_METADATA_UT_DATATYPES_METADATA_H_
