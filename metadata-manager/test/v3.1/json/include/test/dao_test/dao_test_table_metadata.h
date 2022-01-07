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
#ifndef DAO_TEST_TABLE_METADATAS_H_
#define DAO_TEST_TABLE_METADATAS_H_

#include <gtest/gtest.h>
#include <string>

#include "manager/metadata/metadata.h"
#include "test/utility/ut_utils.h"

namespace manager::metadata::testing {

class DaoTestTableMetadata : public ::testing::Test {
 public:
  void SetUp() override {}
  static void add_table(std::string table_name, ObjectIdType* object_id);
  static void get_table_metadata(std::string_view object_name,
                                 boost::property_tree::ptree& object);
  static void get_table_metadata(ObjectIdType object_id,
                                 boost::property_tree::ptree& object);
  static void remove_table_metadata(const ObjectIdType object_id);
  static void remove_table_metadata(const char* object_name,
                                    ObjectIdType* object_id);
};

}  // namespace manager::metadata::testing

#endif  // DAO_TEST_TABLE_METADATAS_H_
