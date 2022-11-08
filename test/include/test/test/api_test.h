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
#ifndef TEST_INCLUDE_TEST_TEST_API_TEST_H_
#define TEST_INCLUDE_TEST_TEST_API_TEST_H_

#include <memory>
#include <utility>
#include <vector>

#include <boost/property_tree/ptree.hpp>

#include "manager/metadata/metadata.h"
#include "test/metadata/ut_metadata_interface.h"

namespace manager::metadata::testing {

class ApiTest {
 public:
  explicit ApiTest(std::unique_ptr<Metadata>&& manager)
      : managers_(std::move(manager)) {}
  virtual ~ApiTest() {}

  virtual int64_t get_record_count() const                  = 0;
  virtual Object* get_structure() const                     = 0;
  virtual std::vector<Object>* get_structure_vector() const = 0;

 protected:
  // Series of flow tests.
  void test_flow_get_by_id(const UtMetadataInterface* ut_metadata) const;
  void test_flow_get_by_id_with_struct(
      const UtMetadataInterface* ut_metadata) const;
  void test_flow_get_by_name(const UtMetadataInterface* ut_metadata) const;
  void test_flow_get_by_name_with_struct(
      const UtMetadataInterface* ut_metadata) const;
  void test_flow_getall(const UtMetadataInterface* ut_metadata) const;
  void test_flow_getall_with_struct(
      const UtMetadataInterface* ut_metadata) const;
  void test_flow_update(const UtMetadataInterface* ut_metadata,
                        const UtMetadataInterface* ut_metadata_update) const;

  // Standalone tests.
  void test_init(const Metadata* managers, ErrorCode expect_code) const;
  ObjectId test_add(const Metadata* managers,
                    boost::property_tree::ptree& metadata_object,
                    ErrorCode expect_code) const;
  ObjectId test_add(const Metadata* managers, Object& metadata_object,
                    ErrorCode expect_code) const;

  void test_get(const Metadata* managers, ObjectId object_id,
                ErrorCode expect_code,
                boost::property_tree::ptree& metadata_object) const;
  void test_get(const Metadata* managers, ObjectId object_id,
                ErrorCode expect_code, Object& metadata_object) const;

  void test_get(const Metadata* managers, std::string_view object_name,
                ErrorCode expect_code,
                boost::property_tree::ptree& metadata_object) const;
  void test_get(const Metadata* managers, std::string_view object_name,
                ErrorCode expect_code, Object& metadata_object) const;

  void test_getall(const Metadata* managers, ErrorCode expected,
                   std::vector<boost::property_tree::ptree>& container) const;
#if 0
  void test_getall(const Metadata* managers,
                   ErrorCode expected, std::vector<Object>& container) const;
#endif

  void test_update(const Metadata* managers, ObjectId object_id,
                   boost::property_tree::ptree& metadata_object,
                   ErrorCode expected) const;
#if 0
  void test_update(const Metadata* managers, ObjectId object_id,
                   const Object& metadata_object, ErrorCode expected) const;
#endif

  void test_remove(const Metadata* managers, ObjectId object_id,
                   ErrorCode expect_code) const;
  void test_remove(const Metadata* managers, std::string_view object_name,
                   ErrorCode expect_code) const;

 protected:
  std::unique_ptr<Metadata> managers_;

 private:
  template <typename T>
  ObjectId metadata_add(const Metadata* managers, T& metadata_object,
                        ErrorCode expect_code) const;

  template <typename KEY, typename T>
  void metadata_get(const Metadata* managers, KEY object_key,
                    ErrorCode expect_code, T& metadata_object) const;

  template <typename KEY, typename T>
  void metadata_update(const Metadata* managers, KEY object_key,
                       T& metadata_object, ErrorCode expect_code) const;
};  // class ApiTest

}  // namespace manager::metadata::testing

#endif  // TEST_INCLUDE_TEST_TEST_API_TEST_H_
