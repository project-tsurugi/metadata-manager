/*
 * Copyright 2020-2021 tsurugi project.
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
#ifndef MANAGER_METADATA_METADATA_H_
#define MANAGER_METADATA_METADATA_H_

#include <string>
#include <string_view>
#include <vector>

#include <boost/property_tree/ptree.hpp>
#include "manager/metadata/error_code.h"

namespace manager::metadata {

using FormatVersionType = std::int32_t;
using GenerationType = std::int64_t;
using ObjectId = std::int64_t;
using ObjectIdType = ObjectId;

static constexpr ObjectId INVALID_OBJECT_ID = -1;
static constexpr int64_t INVALID_VALUE = -1;

/**
 * @brief
 */
struct Object {
  Object()
      : format_version(1), 
        generation(1), 
        id(INVALID_OBJECT_ID), 
        name("") {}
  int64_t format_version;
  int64_t generation;
  int64_t id;
  std::string name;
};

/**
 * @brief
 */
struct ClassObject : public Object {
  ClassObject()
      : Object(),
        database_name(""),
        schema_name(""),
        acl("") {}
  std::string database_name;
  std::string schema_name;
  std::string acl;

  std::string get_fullname() {
    return database_name + '.' + schema_name + '.' + this->name;
  }
};

/**
 * @brief
 */
class Metadata {
 public:
  /**
   * @brief Field name constant indicating the format version of the metadata.
   */
  static constexpr const char* const FORMAT_VERSION = "formatVersion";
  /**
   * @brief Field name constant indicating the generation of the metadata.
   */
  static constexpr const char* const GENERATION = "generation";
  /**
   * @brief Field name constant indicating the object id of the metadata.
   */
  static constexpr const char* const ID = "id";
  /**
   * @brief Field name constant indicating the object name of the metadata.
   */
  static constexpr const char* const NAME = "name";

  Metadata(std::string_view database, std::string_view component);

  virtual ~Metadata() {}

  static GenerationType generation() { return kGeneration; }
  static FormatVersionType format_version() { return kFormatVersion; }

  std::string_view database() const { return database_; }
  std::string_view component() const { return component_; }

  /**
   *  @brief  Initialization.
   *  @param  none.
   *  @return ErrorCode::OK
   *  if all the following steps are successfully completed.
   *  1. Establishes a connection to the metadata repository.
   *  2. Sends a query to set always-secure search path
   *     to the metadata repository.
   *  3. Defines prepared statements in the metadata repository.
   *  @return otherwise an error code.
   */
  virtual ErrorCode init() const = 0;

  /**
   *  @brief  Load the latest metadata from metadata-table.
   *  @param  (database)  [in]  database name.
   *  @param  (component) [in]  component name.
   *  @return ErrorCode::OK if success, otherwise an error code.
   */
  ErrorCode load() const;

  /**
   *  @brief  Load metadata from metadata-table.
   *  @param  (database)   [in]  database name
   *  @param  (pt)         [out] property_tree object to populating metadata.
   *  @param  (generation) [in]  metadata generation to load. load latest
   * generation if NOT provided.
   *  @return ErrorCode::OK if success, otherwise an error code.
   */
  static ErrorCode load(std::string_view database,
                        boost::property_tree::ptree& object,
                        const GenerationType generation = kLatestVersion);

  /**
   *  @brief  Check if the object with the specified object ID exists.
   *  @param  object_id   [in]  ID of metadata.
   *  @return true if success.
   */
  virtual bool exists(const ObjectId object_id) const;

  /**
   *  @brief  Check if the object with the specified name exists.
   *  @param  name   [in]  name of metadata.
   *  @return true if success.
   */
  virtual bool exists(std::string_view object_name) const;

  /**
   *  @brief  Add metadata-object to metadata-table.
   *  @param  (object) [in]  metadata-object to add.
   *  @return ErrorCode::OK if success, otherwise an error code.
   */
  virtual ErrorCode add(const boost::property_tree::ptree& object) const = 0;

  /**
   *  @brief  Add metadata-object to metadata-table.
   *  @param  (object)      [in]  metadata-object to add.
   *  @param  (object_id)   [out] ID of the added metadata-object.
   *  @return ErrorCode::OK if success, otherwise an error code.
   */
  virtual ErrorCode add(const boost::property_tree::ptree& object,
                        ObjectId* object_id) const = 0;

  /**
   *  @brief  Get metadata-object.
   *  @param  (object_id) [in]  metadata-object ID.
   *  @param  (object)    [out] metadata-object with the specified ID.
   *  @return ErrorCode::OK if success, otherwise an error code.
   */
  virtual ErrorCode get(const ObjectId object_id,
                        boost::property_tree::ptree& object) const = 0;

  /**
   *  @brief  Get metadata-object.
   *  @param  (object_name)   [in]  metadata-object name. (Value of "name"
   * key.)
   *  @param  (object)        [out] metadata-object with the specified name.
   *  @return ErrorCode::OK if success, otherwise an error code.
   */
  virtual ErrorCode get(std::string_view object_name,
                        boost::property_tree::ptree& object) const = 0;

  /**
   *  @brief  Get all table metadata-objects.
   *  @param  (container)  [out] Container for metadata-objects.
   *  @return ErrorCode::OK if success, otherwise an error code.
   */
  virtual ErrorCode get_all(
      std::vector<boost::property_tree::ptree>& container) const = 0;

  /**
   *  @brief  Remove metadata-object from metadata-table.
   *  @param  [in] metadata-object ID.
   *  @return ErrorCode::OK if success, otherwise an error code.
   */
  virtual ErrorCode remove(const ObjectId object_id) const = 0;

  /**
   *  @brief  Remove metadata-object from metadata-table.
   *  @param  (object_name) [in] name of metadata-object. (Value of "name"
   * key.)
   *  @param  (object_id)   [out] ID of the added metadata-object.
   *  @return ErrorCode::OK if success, otherwise an error code.
   */
  virtual ErrorCode remove(std::string_view object_name,
                           ObjectId* object_id) const = 0;

  Metadata(const Metadata&) = delete;
  Metadata& operator=(const Metadata&) = delete;

 protected:
  static constexpr const char* const kDefaultComponent = "visitor";
  static const GenerationType kLatestVersion = 0;

 private:
  static constexpr GenerationType kGeneration = 1;
  static constexpr FormatVersionType kFormatVersion = 1;

  std::string database_;
  std::string component_;
};

}  // namespace manager::metadata

#endif  // MANAGER_METADATA_METADATA_H_
