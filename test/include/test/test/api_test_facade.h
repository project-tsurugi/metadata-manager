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
#ifndef TEST_INCLUDE_TEST_TEST_API_TEST_FACADE_H_
#define TEST_INCLUDE_TEST_TEST_API_TEST_FACADE_H_

#include <gtest/gtest.h>

#include <memory>
#include <utility>
#include <vector>

#include <boost/property_tree/ptree.hpp>

#include "manager/metadata/metadata.h"
#include "test/common/global_test_environment.h"
#include "test/helper/metadata_helper.h"
#include "test/metadata/ut_metadata.h"
#include "test/test/api_test.h"

namespace manager::metadata::testing {

#if 0
class ApiTestFacadeBasic : protected ApiTest, public ::testing::Test {
 public:
  explicit ApiTestFacadeBasic(std::unique_ptr<Metadata>&& manager)
      : ApiTest(std::move(manager)) {}
  virtual ~ApiTestFacadeBasic() {}

  virtual int64_t get_record_count() const { return 0L; }

  virtual Object* get_structure() const { return nullptr; }
  virtual std::vector<Object>* get_structure_vector() const { return nullptr; }

  /**
   * @brief This is a test to init metadata.
   * @param managers     [in]  metadata management class object.
   * @param expect_code  [in]  expected result code.
   */
  void test_init(const Metadata* managers, ErrorCode expect_code) const {
    SCOPED_TRACE("");
    ApiTest::test_init(managers, expect_code);
  }

  /**
   * @brief This is a test to add metadata.
   * @param managers         [in]  metadata management class object.
   * @param metadata_object  [in]  metadata object.
   * @param expect_code      [in]  expected result code.
   * @return ObjectId - added object ID.
   */
  ObjectId test_add(const Metadata* managers,
                    boost::property_tree::ptree& metadata_object,
                    ErrorCode expect_code) const {
    SCOPED_TRACE("");
    return ApiTest::test_add(managers, metadata_object, expect_code);
  }

  /**
   * @brief This is a test to add metadata.
   * @param managers         [in]  metadata management class object.
   * @param metadata_object  [in]  metadata object.
   * @param expect_code      [in]  expected result code.
   * @return ObjectId - added object ID.
   */
  ObjectId test_add(const Metadata* managers, Object& metadata_object,
                    ErrorCode expect_code) const {
    SCOPED_TRACE("");
    return ApiTest::test_add(managers, metadata_object, expect_code);
  }

  /**
   * @brief This is a test to get metadata.
   * @param managers         [in]  metadata management class object.
   * @param object_id        [in]  object ID of the metadata to be retrieved.
   * @param expect_code      [in]  expected result code.
   * @param metadata_object  [out] retrieved metadata object.
   * @return boost::property_tree::ptree - retrieved metadata.
   */
  void test_get(const Metadata* managers, ObjectId object_id,
                ErrorCode expect_code,
                boost::property_tree::ptree& metadata_object) const {
    SCOPED_TRACE("");
    ApiTest::test_get(managers, object_id, expect_code, metadata_object);
  }

  /**
   * @brief This is a test to get metadata.
   * @param managers         [in]  metadata management class object.
   * @param object_id        [in]  object ID of the metadata to be retrieved.
   * @param expect_code      [in]  expected result code.
   * @param metadata_object  [out] retrieved metadata object.
   */
  void test_get(const Metadata* managers, ObjectId object_id,
                ErrorCode expect_code, Object& metadata_object) const {
    SCOPED_TRACE("");
    ApiTest::test_get(managers, object_id, expect_code, metadata_object);
  }

  /**
   * @brief This is a test to get metadata.
   * @param managers         [in]  metadata management class object.
   * @param object_name      [in]  object name of the metadata to be retrieved.
   * @param expect_code      [in]  expected result code.
   * @param metadata_object  [out] retrieved metadata object.
   */
  void test_get(const Metadata* managers, std::string_view object_name,
                ErrorCode expect_code,
                boost::property_tree::ptree& metadata_object) const {
    SCOPED_TRACE("");
    ApiTest::test_get(managers, object_name, expect_code, metadata_object);
  }

  /**
   * @brief This is a test to get metadata.
   * @param managers         [in]  metadata management class object.
   * @param object_name      [in]  object name of the metadata to be retrieved.
   * @param expect_code      [in]  expected result code.
   * @param metadata_object  [out] retrieved metadata object.
   */
  void test_get(const Metadata* managers, std::string_view object_name,
                ErrorCode expect_code, Object& metadata_object) const {
    SCOPED_TRACE("");
    ApiTest::test_get(managers, object_name, expect_code, metadata_object);
  }

  /**
   * @brief This is a test to get metadata.
   * @param managers     [in]  metadata management class object.
   * @param expect_code  [in]  expected result code.
   * @param container    [out] retrieved metadata.
   */
  void test_getall(const Metadata* managers, ErrorCode expected,
                   std::vector<boost::property_tree::ptree>& container) const {
    SCOPED_TRACE("");
    ApiTest::test_getall(managers, expected, container);
  }

#if 0
  /**
   * @brief This is a test to get metadata.
   * @param managers     [in]  metadata management class object.
   * @param expect_code  [in]  expected result code.
   * @param container    [out] retrieved metadata.
   */
  void test_getall(const Metadata* managers, ErrorCode expected,
                   std::vector<OBJECT>& container) const {
    SCOPED_TRACE("");
    ApiTest::test_getall(managers, expected, container);
  }
#endif

  /**
   * @brief This is a test to update metadata.
   * @param managers         [in]  metadata management class object.
   * @param object_id        [in]  object ID of the metadata to be retrieved.
   * @param metadata_object  [in]  metadata object.
   * @param expect_code      [in]  expected result code.
   */
  void test_update(const Metadata* managers, ObjectId object_id,
                   boost::property_tree::ptree& metadata_object,
                   ErrorCode expected) const {
    SCOPED_TRACE("");
    ApiTest::test_update(managers, object_id, metadata_object, expected);
  }

#if 0
  /**
   * @brief This is a test to update metadata.
   * @param managers         [in]  metadata management class object.
   * @param object_id        [in]  object ID of the metadata to be retrieved.
   * @param metadata_object  [in]  metadata object.
   * @param expect_code      [in]  expected result code.
   */
  void test_update(const Metadata* managers, ObjectId object_id,
                   const Object& metadata_object, ErrorCode expected) const {
    SCOPED_TRACE("");
    ApiTest::test_update(managers, object_id, metadata_object, expected);
  }
#endif

  /**
   * @brief This is a test to remove metadata.
   * @param managers     [in]  metadata management class object.
   * @param object_id    [in]  object ID of the metadata to be removed.
   * @param expect_code  [in]  expected result code.
   */
  void test_remove(const Metadata* managers, ObjectId object_id,
                   ErrorCode expect_code) const {
    SCOPED_TRACE("");
    ApiTest::test_remove(managers, object_id, expect_code);
  }

  /**
   * @brief This is a test to remove metadata.
   * @param managers    [in]  metadata management class object.
   * @param object_name [in]  object name of the metadata to be removed.
   * @param expect_code [in]  expected result code.
   */
  void test_remove(const Metadata* managers, std::string_view object_name,
                   ErrorCode expect_code) const {
    SCOPED_TRACE("");
    ApiTest::test_remove(managers, object_name, expect_code);
  }
};  // class ApiTestFacadeBasic
#endif

template <class OBJECT = ::manager::metadata::Object,
          class HELPER = MetadataHelper>
class ApiTestFacade : protected ApiTest, public ::testing::Test {
 public:
  explicit ApiTestFacade(std::unique_ptr<Metadata>&& manager)
      : ApiTest(std::move(manager)),
        metadata_struct_(std::make_unique<OBJECT>()),
        metadata_struct_array_(std::make_unique<std::vector<OBJECT>>()),
        metadata_helper_(std::make_unique<HELPER>()) {}
  virtual ~ApiTestFacade() {}

  int64_t get_record_count() const override {
    return metadata_helper_->get_record_count();
  }

  OBJECT* get_structure() const override { return metadata_struct_.get(); }
  std::vector<Object>* get_structure_vector() const override {
    auto struct_array = metadata_struct_array_.get();
    return reinterpret_cast<std::vector<Object>*>(struct_array);
  }

  // Series of flow tests.
  /**
   * @brief This is a test of the basic paths of metadata management (add, get,
   *   remove).
   *   add: patterns that obtain a object ID.
   *   get: object ID as a key.
   *   remove: object ID as a key.
   * @param ut_metadata  [in]  test metadata.
   */
  void test_flow_get_by_id(const UtMetadata<OBJECT>& ut_metadata) const {
    SCOPED_TRACE("");
    ApiTest::test_flow_get_by_id(&ut_metadata);
  }

  /**
   * @brief This is a test of the basic paths of metadata management (add, get,
   *   remove).
   *   add: patterns that obtain a object ID.
   *   get: object ID as a key.
   *   remove: object ID as a key.
   * @param ut_metadata  [in]  test metadata.
   */
  void test_flow_get_by_id_with_struct(
      const UtMetadata<OBJECT>& ut_metadata) const {
    SCOPED_TRACE("");
    ApiTest::test_flow_get_by_id_with_struct(&ut_metadata);
  }

  /**
   * @brief This is a test of the basic paths of metadata management (add, get,
   *   remove).
   *   add: patterns that obtain a object ID.
   *   get: object name as a key.
   *   remove: object name as a key.
   * @param ut_metadata  [in]  test metadata.
   */
  void test_flow_get_by_name(const UtMetadata<OBJECT>& ut_metadata) const {
    SCOPED_TRACE("");
    ApiTest::test_flow_get_by_name(&ut_metadata);
  }

  /**
   * @brief This is a test of the basic paths of metadata management (add, get,
   *   remove).
   *   add: patterns that obtain a object ID.
   *   get: object name as a key.
   *   remove: object name as a key.
   * @param ut_metadata  [in]  test metadata.
   */
  void test_flow_get_by_name_with_struct(
      const UtMetadata<OBJECT>& ut_metadata) const {
    SCOPED_TRACE("");
    ApiTest::test_flow_get_by_name_with_struct(&ut_metadata);
  }

  /**
   * @brief This is a test of the basic paths of metadata management (add,
   *   get_all, remove).
   *   add: patterns that obtain a object ID.
   *   get_all: all object.
   *   remove: object ID as a key.
   * @param ut_metadata  [in]  test metadata.
   */
  void test_flow_getall(const UtMetadata<OBJECT>& ut_metadata) const {
    SCOPED_TRACE("");
    ApiTest::test_flow_getall(&ut_metadata);
  }

#if 0
  /**
   * @brief This is a test of the basic paths of metadata management (add,
   *   get_all, remove).
   *   add: patterns that obtain a object ID.
   *   get_all: all object.
   *   remove: object ID as a key.
   * @param ut_metadata  [in]  test metadata.
   */
  void test_flow_getall_with_struct(const UtMetadata<OBJECT>& ut_metadata) const {
    SCOPED_TRACE("");
    ApiTest::test_flow_getall_with_struct(&ut_metadata);
  }
#endif

  /**
   * @brief This is a test of the basic paths of metadata management (add, get,
   *   update, remove).
   *   add: patterns that obtain a object ID.
   *   get: object ID as a key.
   *   update: object ID as a key.
   *   remove: object ID as a key.
   * @param ut_metadata         [in]  test metadata.
   * @param ut_metadata_update  [in]  update test metadata.
   */
  void test_flow_update(const UtMetadata<OBJECT>& ut_metadata,
                        const UtMetadata<OBJECT>& ut_metadata_update) const {
    SCOPED_TRACE("");
    ApiTest::test_flow_update(&ut_metadata, &ut_metadata_update);
  }

  /**
   * @brief This is a test to init metadata.
   * @param managers     [in]  metadata management class object.
   * @param expect_code  [in]  expected result code.
   */
  void test_init(const Metadata* managers, ErrorCode expect_code) const {
    SCOPED_TRACE("");
    ApiTest::test_init(managers, expect_code);
  }

  /**
   * @brief This is a test to add metadata.
   * @param managers         [in]  metadata management class object.
   * @param metadata_object  [in]  metadata object.
   * @param expect_code      [in]  expected result code.
   * @return ObjectId - added object ID.
   */
  ObjectId test_add(const Metadata* managers,
                    boost::property_tree::ptree& metadata_object,
                    ErrorCode expect_code) const {
    SCOPED_TRACE("");
    return ApiTest::test_add(managers, metadata_object, expect_code);
  }

  /**
   * @brief This is a test to add metadata.
   * @param managers         [in]  metadata management class object.
   * @param metadata_object  [in]  metadata object.
   * @param expect_code      [in]  expected result code.
   * @return ObjectId - added object ID.
   */
  ObjectId test_add(const Metadata* managers, Object& metadata_object,
                    ErrorCode expect_code) const {
    SCOPED_TRACE("");
    return ApiTest::test_add(managers, metadata_object, expect_code);
  }

  /**
   * @brief This is a test to get metadata.
   * @param managers         [in]  metadata management class object.
   * @param object_id        [in]  object ID of the metadata to be retrieved.
   * @param expect_code      [in]  expected result code.
   * @param metadata_object  [out] retrieved metadata object.
   * @return boost::property_tree::ptree - retrieved metadata.
   */
  void test_get(const Metadata* managers, ObjectId object_id,
                ErrorCode expect_code,
                boost::property_tree::ptree& metadata_object) const {
    SCOPED_TRACE("");
    ApiTest::test_get(managers, object_id, expect_code, metadata_object);
  }

  /**
   * @brief This is a test to get metadata.
   * @param managers         [in]  metadata management class object.
   * @param object_id        [in]  object ID of the metadata to be retrieved.
   * @param expect_code      [in]  expected result code.
   * @param metadata_object  [out] retrieved metadata object.
   */
  void test_get(const Metadata* managers, ObjectId object_id,
                ErrorCode expect_code, Object& metadata_object) const {
    SCOPED_TRACE("");
    ApiTest::test_get(managers, object_id, expect_code, metadata_object);
  }

  /**
   * @brief This is a test to get metadata.
   * @param managers         [in]  metadata management class object.
   * @param object_name      [in]  object name of the metadata to be retrieved.
   * @param expect_code      [in]  expected result code.
   * @param metadata_object  [out] retrieved metadata object.
   */
  void test_get(const Metadata* managers, std::string_view object_name,
                ErrorCode expect_code,
                boost::property_tree::ptree& metadata_object) const {
    SCOPED_TRACE("");
    ApiTest::test_get(managers, object_name, expect_code, metadata_object);
  }

  /**
   * @brief This is a test to get metadata.
   * @param managers         [in]  metadata management class object.
   * @param object_name      [in]  object name of the metadata to be retrieved.
   * @param expect_code      [in]  expected result code.
   * @param metadata_object  [out] retrieved metadata object.
   */
  void test_get(const Metadata* managers, std::string_view object_name,
                ErrorCode expect_code, Object& metadata_object) const {
    SCOPED_TRACE("");
    ApiTest::test_get(managers, object_name, expect_code, metadata_object);
  }

  /**
   * @brief This is a test to get metadata.
   * @param managers     [in]  metadata management class object.
   * @param expect_code  [in]  expected result code.
   * @param container    [out] retrieved metadata.
   */
  void test_getall(const Metadata* managers, ErrorCode expected,
                   std::vector<boost::property_tree::ptree>& container) const {
    SCOPED_TRACE("");
    ApiTest::test_getall(managers, expected, container);
  }

#if 0
  /**
   * @brief This is a test to get metadata.
   * @param managers     [in]  metadata management class object.
   * @param expect_code  [in]  expected result code.
   * @param container    [out] retrieved metadata.
   */
  void test_getall(const Metadata* managers, ErrorCode expected,
                   std::vector<OBJECT>& container) const {
    SCOPED_TRACE("");
    ApiTest::test_getall(managers, expected, container);
  }
#endif

  /**
   * @brief This is a test to update metadata.
   * @param managers         [in]  metadata management class object.
   * @param object_id        [in]  object ID of the metadata to be retrieved.
   * @param metadata_object  [in]  metadata object.
   * @param expect_code      [in]  expected result code.
   */
  void test_update(const Metadata* managers, ObjectId object_id,
                   boost::property_tree::ptree& metadata_object,
                   ErrorCode expected) const {
    SCOPED_TRACE("");
    ApiTest::test_update(managers, object_id, metadata_object, expected);
  }

#if 0
  /**
   * @brief This is a test to update metadata.
   * @param managers         [in]  metadata management class object.
   * @param object_id        [in]  object ID of the metadata to be retrieved.
   * @param metadata_object  [in]  metadata object.
   * @param expect_code      [in]  expected result code.
   */
  void test_update(const Metadata* managers, ObjectId object_id,
                   const Object& metadata_object, ErrorCode expected) const {
    SCOPED_TRACE("");
    ApiTest::test_update(managers, object_id, metadata_object, expected);
  }
#endif

  /**
   * @brief This is a test to remove metadata.
   * @param managers     [in]  metadata management class object.
   * @param object_id    [in]  object ID of the metadata to be removed.
   * @param expect_code  [in]  expected result code.
   */
  void test_remove(const Metadata* managers, ObjectId object_id,
                   ErrorCode expect_code) const {
    SCOPED_TRACE("");
    ApiTest::test_remove(managers, object_id, expect_code);
  }

  /**
   * @brief This is a test to remove metadata.
   * @param managers    [in]  metadata management class object.
   * @param object_name [in]  object name of the metadata to be removed.
   * @param expect_code [in]  expected result code.
   */
  void test_remove(const Metadata* managers, std::string_view object_name,
                   ErrorCode expect_code) const {
    SCOPED_TRACE("");
    ApiTest::test_remove(managers, object_name, expect_code);
  }

 private:
  std::unique_ptr<OBJECT> metadata_struct_;
  std::unique_ptr<std::vector<OBJECT>> metadata_struct_array_;
  std::unique_ptr<HELPER> metadata_helper_;
};  // class ApiTest

}  // namespace manager::metadata::testing

#endif  // TEST_INCLUDE_TEST_TEST_API_TEST_FACADE_H_
