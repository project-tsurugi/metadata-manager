/*
 * Copyright 2022 Project Tsurugi.
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
#ifndef TEST_INCLUDE_TEST_HELPER_API_TEST_HELPER_H_
#define TEST_INCLUDE_TEST_HELPER_API_TEST_HELPER_H_

#include <memory>
#include <utility>
#include <vector>

#include <boost/property_tree/ptree.hpp>

#include "manager/metadata/metadata.h"

namespace manager::metadata::testing {

class ApiTestHelper final {
 public:
  ApiTestHelper() = delete;

  static void test_init(const Metadata* manager, ErrorCode expect_code);
  static ObjectId test_add(const Metadata* manager,
                    boost::property_tree::ptree& metadata_object,
                    ErrorCode expect_code);
  static ObjectId test_add(const Metadata* manager, Object& metadata_object,
                    ErrorCode expect_code);

  static void test_get(const Metadata* manager, ObjectId object_id,
                ErrorCode expect_code,
                boost::property_tree::ptree& metadata_object);
  static void test_get(const Metadata* manager, ObjectId object_id,
                ErrorCode expect_code, Object& metadata_object);

  static void test_get(const Metadata* manager, std::string_view object_name,
                ErrorCode expect_code,
                boost::property_tree::ptree& metadata_object);
  static void test_get(const Metadata* manager, std::string_view object_name,
                ErrorCode expect_code, Object& metadata_object);

  static void test_getall(const Metadata* manager, ErrorCode expected,
                   std::vector<boost::property_tree::ptree>& container);
  static void test_getall_next(
      Metadata* manager, ErrorCode expected,
      std::vector<boost::property_tree::ptree>& container);

  static void test_exists(const Metadata* manager, ObjectId object_id,
                   bool expected);
  static void test_exists(const Metadata* manager, std::string_view object_name,
                   bool expected);

  static void test_update(const Metadata* manager, ObjectId object_id,
                   boost::property_tree::ptree& metadata_object,
                   ErrorCode expected);
#if 0
  static void test_update(const Metadata* manager, ObjectId object_id,
                   const Object& metadata_object, ErrorCode expected);
#endif

  static void test_remove(const Metadata* manager, ObjectId object_id,
                   ErrorCode expect_code);
  static void test_remove(const Metadata* manager, std::string_view object_name,
                   ErrorCode expect_code);

  template <typename T>
  static ObjectId metadata_add(const Metadata* manager, T& metadata_object,
                        ErrorCode expect_code);

  template <typename KEY, typename T>
  static void metadata_get(const Metadata* manager, KEY object_key,
                    ErrorCode expect_code, T& metadata_object);

  template <typename KEY, typename T>
  static void metadata_update(const Metadata* manager, KEY object_key,
                       T& metadata_object, ErrorCode expect_code);
};  // class ApiTest

}  // namespace manager::metadata::testing

#endif  // TEST_INCLUDE_TEST_HELPER_API_TEST_HELPER_H_
