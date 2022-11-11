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

using UniqueDataCreator =
    std::function<void(boost::property_tree::ptree&, const int64_t)>;
using UpdateDataCreator = std::function<std::unique_ptr<UtMetadataInterface>(
    const boost::property_tree::ptree&)>;

class ApiTest {
 public:
  explicit ApiTest(std::unique_ptr<Metadata>&& manager)
      : managers_(std::move(manager)) {}
  virtual ~ApiTest() {}

  virtual int64_t get_record_count() const = 0;
  virtual Object* get_structure() const    = 0;

 protected:
  // Series of flow tests.
  void test_flow_get_by_id(const UtMetadataInterface* ut_metadata) const;
  void test_flow_get_by_id_with_struct(
      const UtMetadataInterface* ut_metadata) const;
  void test_flow_get_by_name(const UtMetadataInterface* ut_metadata) const;
  void test_flow_get_by_name_with_struct(
      const UtMetadataInterface* ut_metadata) const;
  void test_flow_getall(const UtMetadataInterface* ut_metadata,
                        UniqueDataCreator creator,
                        const int32_t create_data_max) const;
  void test_flow_getall_next(const UtMetadataInterface* ut_metadata,
                             UniqueDataCreator creator,
                             const int32_t create_data_max) const;
  void test_flow_update(const UtMetadataInterface* ut_metadata,
                        UpdateDataCreator update_data_creator) const;

  // Standalone tests.
  void test_init(const Metadata* metadata_manager, ErrorCode expect_code) const;
  ObjectId test_add(const Metadata* metadata_manager,
                    boost::property_tree::ptree& metadata_object,
                    ErrorCode expect_code) const;
  ObjectId test_add(const Metadata* metadata_manager, Object& metadata_object,
                    ErrorCode expect_code) const;

  void test_get(const Metadata* metadata_manager, ObjectId object_id,
                ErrorCode expect_code,
                boost::property_tree::ptree& metadata_object) const;
  void test_get(const Metadata* metadata_manager, ObjectId object_id,
                ErrorCode expect_code, Object& metadata_object) const;

  void test_get(const Metadata* metadata_manager, std::string_view object_name,
                ErrorCode expect_code,
                boost::property_tree::ptree& metadata_object) const;
  void test_get(const Metadata* metadata_manager, std::string_view object_name,
                ErrorCode expect_code, Object& metadata_object) const;

  void test_getall(const Metadata* metadata_manager, ErrorCode expected,
                   std::vector<boost::property_tree::ptree>& container) const;
  void test_getall_next(
      const Metadata* metadata_manager, ErrorCode expected,
      std::vector<boost::property_tree::ptree>& container) const;

  void test_exists(const Metadata* metadata_manager, ObjectId object_id,
                   bool expected) const;
  void test_exists(const Metadata* metadata_manager,
                   std::string_view object_name, bool expected) const;

  void test_update(const Metadata* metadata_manager, ObjectId object_id,
                   boost::property_tree::ptree& metadata_object,
                   ErrorCode expected) const;
#if 0
  void test_update(const Metadata* metadata_manager, ObjectId object_id,
                   const Object& metadata_object, ErrorCode expected) const;
#endif

  void test_remove(const Metadata* metadata_manager, ObjectId object_id,
                   ErrorCode expect_code) const;
  void test_remove(const Metadata* metadata_manager,
                   std::string_view object_name, ErrorCode expect_code) const;

 protected:
  std::unique_ptr<Metadata> managers_;

 private:
  template <typename T>
  ObjectId metadata_add(const Metadata* metadata_manager, T& metadata_object,
                        ErrorCode expect_code) const;

  std::vector<boost::property_tree::ptree> metadata_add(
      const Metadata* metadata_manager,
      const boost::property_tree::ptree& metadata,
      UniqueDataCreator unique_data_creator,
      const int32_t create_data_max) const;

  template <typename KEY, typename T>
  void metadata_get(const Metadata* metadata_manager, KEY object_key,
                    ErrorCode expect_code, T& metadata_object) const;

  template <typename KEY, typename T>
  void metadata_update(const Metadata* metadata_manager, KEY object_key,
                       T& metadata_object, ErrorCode expect_code) const;

  void metadata_remove(
      const Metadata* metadata_manager,
      const std::vector<boost::property_tree::ptree>& metadata_list) const;

  void metadata_compare_all(
      const UtMetadataInterface* ut_metadata,
      const std::vector<boost::property_tree::ptree>& expect_metadata_list,
      const std::vector<boost::property_tree::ptree>& actual_metadata_list)
      const;
};  // class ApiTest

}  // namespace manager::metadata::testing

#endif  // TEST_INCLUDE_TEST_TEST_API_TEST_H_
