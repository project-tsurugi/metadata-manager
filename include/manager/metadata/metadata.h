/*
 * Copyright 2020-2023 tsurugi project.
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
#pragma once

#include <memory>
#include <string>
#include <string_view>
#include <vector>

#include <boost/iterator_adaptors.hpp>
#include <boost/property_tree/ptree.hpp>

#include "manager/metadata/common/constants.h"
#include "manager/metadata/error_code.h"
#include "manager/metadata/object.h"

namespace manager::metadata {

class Iterator {
 public:
  virtual ~Iterator() {}

  virtual bool has_next() const       = 0;
  virtual ErrorCode next(Object& obj) = 0;
};

/**
 * @brief
 */
class Metadata {
 public:
  /**
   * @brief Field name constant indicating the format version of the metadata.
   * @deprecated Deprecated in the future. Please use Object::FORMAT_VERSION.
   */
  static constexpr const char* const FORMAT_VERSION = "formatVersion";
  /**
   * @brief Field name constant indicating the generation of the metadata.
   * @deprecated Deprecated in the future. Please use Object::GENERATION.
   */
  static constexpr const char* const GENERATION = "generation";
  /**
   * @brief Field name constant indicating the object id of the metadata.
   * @deprecated Deprecated in the future. Please use Object::ID.
   */
  static constexpr const char* const ID = "id";
  /**
   * @brief Field name constant indicating the object name of the metadata.
   * @deprecated Deprecated in the future. Please use Object::NAME.
   */
  static constexpr const char* const NAME = "name";

  Metadata(std::string_view database, std::string_view component);
  Metadata(const Metadata&)            = delete;
  Metadata& operator=(const Metadata&) = delete;
  virtual ~Metadata() {}

  /**
   * @brief Get the generation of the metadata.
   * @return Metadata generation.
   */
  static GenerationType generation() {
    return Object::DEFAULT_GENERATION;
  }

  /**
   * @brief Get the format version of the metadata.
   * @return Metadata format version.
   */
  static FormatVersionType format_version() {
    return Object::DEFAULT_FORMAT_VERSION;
  }

  std::string_view database() const { return database_; }
  std::string_view component() const { return component_; }

  /**
   * @brief Initialization.
   * @param none.
   * @return ErrorCode::OK
   * if all the following steps are successfully completed.
   * 1. Establishes a connection to the metadata repository.
   * 2. Sends a query to set always-secure search path
   *    to the metadata repository.
   * 3. Defines prepared statements in the metadata repository.
   * @return otherwise an error code.
   */
  virtual ErrorCode init() const = 0;

  /**
   * @brief Load the latest metadata from metadata-table.
   * @param database  [in]  database name.
   * @param component [in]  component name.
   * @return ErrorCode::OK if success, otherwise an error code.
   */
  ErrorCode load() const;

  /**
   * @brief Load metadata from metadata-table.
   * @param database   [in]  database name
   * @param pt         [out] property_tree object to populating metadata.
   * @param generation [in]  metadata generation to load.
   *   load latest generation if NOT provided.
   * @return ErrorCode::OK if success, otherwise an error code.
   */
  static ErrorCode load(std::string_view database,
                        boost::property_tree::ptree& object,
                        const GenerationType generation = kLatestVersion);

  /**
   * @brief Add metadata-object to metadata-table.
   * @param object [in]  metadata-object to add.
   * @return ErrorCode::OK if success, otherwise an error code.
   */
  virtual ErrorCode add(const boost::property_tree::ptree& object) const = 0;

  /**
   * @brief Add metadata-object to metadata-table.
   * @param object    [in]  metadata-object to add.
   * @param object_id [out] ID of the added metadata-object.
   * @return ErrorCode::OK if success, otherwise an error code.
   */
  virtual ErrorCode add(const boost::property_tree::ptree& object,
                        ObjectId* object_id) const = 0;

  /**
   * @brief Get metadata-object.
   * @param object_id [in]  metadata-object ID.
   * @param object    [out] metadata-object with the specified ID.
   * @return ErrorCode::OK if success, otherwise an error code.
   */
  virtual ErrorCode get(const ObjectId object_id,
                        boost::property_tree::ptree& object) const = 0;

  /**
   * @brief  Get metadata-object.
   * @param  object_name [in]  metadata-object name. (Value of "name" key.)
   * @param  object      [out] metadata-object with the specified name.
   * @return ErrorCode::OK if success, otherwise an error code.
   */
  virtual ErrorCode get(std::string_view object_name,
                        boost::property_tree::ptree& object) const = 0;

  /**
   * @brief Get all table metadata-objects.
   * @param container [out] Container for metadata-objects.
   * @return ErrorCode::OK if success, otherwise an error code.
   */
  virtual ErrorCode get_all(
      std::vector<boost::property_tree::ptree>& container) const = 0;

  /**
   * @brief Update metadata-table with metadata-object.
   * @param object_id [in]  ID of the metadata-table to update.
   * @param object    [in]  metadata-object to update.
   * @return ErrorCode::OK if success, otherwise an error code.
   */
  virtual ErrorCode update(const ObjectIdType object_id,
                           const boost::property_tree::ptree& object) const = 0;

  /**
   * @brief Remove metadata-object from metadata-table.
   * @param object_id [in]  ID of the metadata-table to remove.
   * @return ErrorCode::OK if success, otherwise an error code.
   */
  virtual ErrorCode remove(const ObjectId object_id) const = 0;

  /**
   * @brief Remove metadata-object from metadata-table.
   * @param object_name [in]  name of metadata-object. (Value of "name" key.)
   * @param object_id   [out] ID of the added metadata-object.
   * @return ErrorCode::OK if success, otherwise an error code.
   */
  virtual ErrorCode remove(std::string_view object_name,
                           ObjectId* object_id) const = 0;

  /**
   *  @brief  Check if the object with the specified object ID exists.
   *  @param  object_id   [in]  object ID of metadata object.
   *  @return true if success.
   */
  bool exists(const ObjectIdType object_id) const {
    boost::property_tree::ptree object;
    ErrorCode error = this->get(object_id, object);
    return (error == ErrorCode::OK) ? true : false;
  }

  /**
   *  @brief  Check if the object with the specified name exists.
   *  @param  name   [in]  name of metadata.
   *  @return true if success.
   */
  bool exists(std::string_view object_name) const {
    boost::property_tree::ptree object;
    ErrorCode error = this->get(object_name, object);
    return (error == ErrorCode::OK) ? true : false;
  }

  /**
   * @brief Add a metadata object to the metadata table.
   * @param object    [in]  metadata object to add.
   * @param object_id [out] ID of the added metadata object.
   * @return ErrorCode::OK if success, otherwise an error code.
   */
  ErrorCode add(const manager::metadata::Object& object,
                ObjectIdType* object_id) const;

  /**
   * @brief Add a metadata object to table metadata table.
   * @param object  [in]  table metadata to add.
   * @return ErrorCode::OK if success, otherwise an error code.
   */
  ErrorCode add(const manager::metadata::Object& object) const;

  /**
   * @brief Get a metadata object.
   * @param object_id [in]  object id.
   * @param object     [out] metadata object with the specified ID.
   * @retval ErrorCode::OK if success,
   * @retval ErrorCode::ID_NOT_FOUND if the table id does not exist.
   * @retval otherwise an error code.
   */
  ErrorCode get(const ObjectId object_id,
                manager::metadata::Object& object) const;

  /**
   * @brief Get a metadata object object based on object name.
   * @param object_name [in]  object name. (Value of "name" key.)
   * @param object       [out] metadata object object with the specified name.
   * @retval ErrorCode::OK if success,
   * @retval ErrorCode::NAME_NOT_FOUND if the table name does not exist.
   * @retval otherwise an error code.
   */
  ErrorCode get(std::string_view object_name,
                manager::metadata::Object& object) const;

  ErrorCode update(const manager::metadata::ObjectIdType object_id,
                   const manager::metadata::Object& object) const;

  ErrorCode get_all();
  ErrorCode next(boost::property_tree::ptree& object);
  ErrorCode next(manager::metadata::Object& object);

  // for iterator
  std::unique_ptr<Iterator> iterator();
  size_t size() const { return objects_.size(); }
  void get(const size_t index, Object& obj) const {
    return obj.convert_from_ptree(objects_[index]);
  }

 protected:
  static constexpr const char* const kDefaultComponent = "visitor";
  static const Generation kLatestVersion               = 0;

 private:
  std::string database_;
  std::string component_;
  std::vector<boost::property_tree::ptree> objects_;
  int64_t cursor_;
};

class MetadataIterator : public Iterator {
 public:
  explicit MetadataIterator(const Metadata* metadata)
      : metadata_(metadata), cursor_(0) {}

  bool has_next() const override;
  ErrorCode next(Object& obj) override;
 private:
  const Metadata* metadata_;
  size_t cursor_;
};

}  // namespace manager::metadata
