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
#ifndef API_TEST_TABLE_METADATAS_EXTRA_H_
#define API_TEST_TABLE_METADATAS_EXTRA_H_

#include <gtest/gtest.h>
#include <boost/property_tree/ptree.hpp>
#include <vector>

#include "manager/metadata/metadata.h"

namespace manager::metadata::testing {

class ApiTestTableMetadataExtra
    : public ::testing::TestWithParam<boost::property_tree::ptree> {
 public:
  void SetUp() override;

  static std::vector<boost::property_tree::ptree> make_valid_table_metadata();

 protected:
  std::vector<boost::property_tree::ptree> table_metadata;
};

}  // namespace manager::metadata::testing

#endif  // API_TEST_TABLE_METADATAS_EXTRA_H_
