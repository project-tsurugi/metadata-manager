/*
 * Copyright 2020-2022 tsurugi project.
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

#include <string>
#include <string_view>
#include <vector>

#include <boost/property_tree/ptree.hpp>

#include "manager/metadata/error_code.h"

namespace manager::metadata {

using FormatVersion     = std::int32_t;
using FormatVersionType = FormatVersion;
using Generation        = std::int64_t;
using GenerationType    = Generation;
using ObjectId          = std::int64_t;
using ObjectIdType      = ObjectId;

static constexpr const ObjectId INVALID_OBJECT_ID = -1;
static constexpr const int64_t INVALID_VALUE      = -1;

/**
 * @brief This class manage common metadata of all metadata objects.
 */
struct Object {
  /**
   * @brief Field name constant indicating the format version of the metadata.
   */
  static constexpr const char* FORMAT_VERSION = "formatVersion";
  /**
   * @brief Field name constant indicating the generation of the metadata.
   */
  static constexpr const char* GENERATION = "generation";
  /**
   * @brief Field name constant indicating the object id of the metadata.
   */
  static constexpr const char* ID = "id";
  /**
   * @brief Field name constant indicating the column name of the metadata.
   */
  static constexpr const char* NAME = "name";

  int64_t format_version;  //!< format version of metadata table schema.
  int64_t generation;      //!< generation.
  int64_t id;              //!< object ID.
  std::string name;        //!< object name.

  Object()
      : format_version(1), generation(1), id(INVALID_OBJECT_ID), name("") {}

  /**
   * @brief  Convert metadata from structure object to ptree object.
   * @return ptree object.
   */
  virtual boost::property_tree::ptree convert_to_ptree() const;
  virtual void convert_from_ptree(const boost::property_tree::ptree& pt);
};

/**
 * @brief This class manage common metadata of class metadata objects.
 * @note  Class  metadata objects are such as table objects.
 * e.g.) table, index, view, materialized-view, etc...
 */
struct ClassObject : public Object {
  static constexpr const char* const DATABASE_NAME = "databaseName";
  static constexpr const char* const SCHEMA_NAME   = "schemaName";
  static constexpr const char* const NAMESPACE     = "namespace";
  static constexpr const char* const OWNER_ID      = "ownerId";
  static constexpr const char* const ACL           = "acl";

  std::string database_name;  //!< 1st namespace of full qualified object name.
  std::string schema_name;    //!< 2nd namespace of full qualified object name.
  std::string namespace_name;
  int64_t owner_id;
  std::string acl;  //!< access control list.

  ClassObject()
      : Object(),
        database_name(""),
        schema_name(""),
        namespace_name(""),
        owner_id(INVALID_OBJECT_ID),
        acl("") {}
  /** @brief  Convert metadata from structure object to ptree object.
   *  @return ptree object.
   */
  boost::property_tree::ptree convert_to_ptree() const override;
  void convert_from_ptree(const boost::property_tree::ptree& pt) override;
  /**
   * @brief Obtain a full qualified object name.
   * e.g. database.schema.table
   * @return a full qualified object name.
   */
  std::string full_qualified_name() {
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
  Metadata(const Metadata&)            = delete;
  Metadata& operator=(const Metadata&) = delete;
  virtual ~Metadata() {}

  static GenerationType generation() { return kGeneration; }
  static FormatVersionType format_version() { return kFormatVersion; }

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
  ErrorCode get(const ObjectIdType object_id,
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

  /**
   * @brief Get all metadata object objects from the metadata table.
   *   If no metadata object exists, return the container as empty.
   * @param objects  [out] Container of metadata objects.
   * @return ErrorCode::OK if success, otherwise an error code.
   */
  ErrorCode get_all(std::vector<manager::metadata::Object>& objects) const;


  ErrorCode update(const manager::metadata::ObjectIdType object_id,
                   const manager::metadata::Object& object) const;

 protected:
  static constexpr const char* const kDefaultComponent = "visitor";
  static const Generation kLatestVersion               = 0;

 private:
  static constexpr Generation kGeneration       = 1;
  static constexpr FormatVersion kFormatVersion = 1;

  std::string database_;
  std::string component_;
};

}  // namespace manager::metadata
