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
#ifndef API_TEST_ROLES_H_
#define API_TEST_ROLES_H_

#include <gtest/gtest.h>
#include <boost/property_tree/ptree.hpp>

namespace manager::metadata::testing {

class DaoTestRolesMetadata : public ::testing::Test {
 public:
  void SetUp() override;
  void TearDown() override;

  static void check_roles_expected(const boost::property_tree::ptree& actual,
                                   const boost::property_tree::ptree& expect);
};

}  // namespace manager::metadata::testing

#endif  // API_TEST_DATA_TYPES_H_
