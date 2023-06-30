/*
 * Copyright 2023 tsurugi project.
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
#ifndef MANAGER_METADATA_OBJECT_H_
#define MANAGER_METADATA_OBJECT_H_

#include <string>

#include <boost/property_tree/ptree.hpp>

#include "manager/metadata/common/constants.h"

namespace manager::metadata {

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

  /**
   * @brief Constant for the default format version.
   */
  static constexpr FormatVersion DEFAULT_FORMAT_VERSION = 1;
  /**
   * @brief Constant for the default generation.
   */
  static constexpr Generation DEFAULT_GENERATION = 1;

  FormatVersion format_version;  //!< format version of metadata table schema.
  Generation generation;         //!< generation.
  ObjectId id;                   //!< object ID.
  std::string name;              //!< object name.

  Object()
      : format_version(DEFAULT_FORMAT_VERSION),
        generation(DEFAULT_GENERATION),
        id(INVALID_OBJECT_ID),
        name("") {}

  /**
   * @brief Transform metadata from structure object to ptree object.
   * @return ptree object.
   */
  virtual boost::property_tree::ptree convert_to_ptree() const {
    boost::property_tree::ptree pt;

    // format_version
    pt.put(FORMAT_VERSION, this->format_version);
    // generation
    pt.put(GENERATION, this->generation);
    // id
    pt.put(ID, this->id);
    // name
    pt.put(NAME, this->name);

    return pt;
  }

  /**
   * @brief  Transform metadata from ptree object to structure object.
   * @param  pt  [in]  ptree object of metadata.
   * @return structure object of metadata.
   */
  virtual void convert_from_ptree(const boost::property_tree::ptree& pt) {
    // format_version
    this->format_version =
        pt.get_optional<FormatVersion>(FORMAT_VERSION).value_or(INVALID_VALUE);
    // generation
    this->generation =
        pt.get_optional<Generation>(GENERATION).value_or(INVALID_VALUE);
    // id
    this->id = pt.get_optional<ObjectId>(ID).value_or(INVALID_OBJECT_ID);
    // name
    this->name = pt.get_optional<std::string>(NAME).value_or("");
  }
};  // class Object

/**
 * @brief This class manage common metadata of class metadata objects.
 * @note Class  metadata objects are such as table objects.
 * @note e.g.) table, index, view, materialized-view, etc...
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

  /**
   *  @brief  Convert metadata from structure object to ptree object.
   *  @return ptree object.
   */
  boost::property_tree::ptree convert_to_ptree() const override {
    auto pt = Object::convert_to_ptree();

    // database_name
    pt.put(DATABASE_NAME, this->database_name);
    // schema_name
    pt.put(SCHEMA_NAME, this->schema_name);
    // namespace_name
    pt.put(NAMESPACE, namespace_name);
    // owner_id
    pt.put<ObjectId>(OWNER_ID, this->owner_id);
    // acl
    pt.put(ACL, this->acl);

    return pt;
  };

  /**
   * @brief  Convert metadata from ptree object to structure object.
   * @param  ptree  [in]  ptree object of metadata.
   * @return structure object of metadata.
   */
  void convert_from_ptree(const boost::property_tree::ptree& pt) override {
    Object::convert_from_ptree(pt);

    // database_name
    this->database_name =
        pt.get_optional<std::string>(DATABASE_NAME).value_or("");
    // schema_name
    this->schema_name = pt.get_optional<std::string>(SCHEMA_NAME).value_or("");
    // namespace_name
    this->namespace_name = pt.get_optional<std::string>(NAMESPACE).value_or("");
    // owner_id
    this->owner_id =
        pt.get_optional<ObjectId>(OWNER_ID).value_or(INVALID_OBJECT_ID);
    // acl
    this->acl = pt.get_optional<std::string>(ACL).value_or("");
  };

  /**
   * @brief Obtain a full qualified object name.
   *   e.g. database.schema.table
   * @return a full qualified object name.
   */
  std::string full_qualified_name() {
    return database_name + '.' + schema_name + '.' + this->name;
  }
};  // class ClassObject

}  // namespace manager::metadata

#endif  // MANAGER_METADATA_OBJECT_H_
