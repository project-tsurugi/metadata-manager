/*
 * Copyright 2020-2021 tsurugi project.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
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
#include "manager/metadata/metadata.h"

#include "manager/metadata/log/default_logger.h"
#include "manager/metadata/log/log_controller.h"

#ifndef LOG_LEVEL
#define LOG_LEVEL 1
#endif

namespace manager::metadata {

using boost::property_tree::ptree;

// ==========================================================================
// Object struct methods.
/** 
 * @brief  Transform metadata from structure object to ptree object.
 * @return ptree object.
 */
boost::property_tree::ptree Object::convert_to_ptree() const {
  boost::property_tree::ptree pt;
  pt.put<int64_t>(FORMAT_VERSION, this->format_version);
  pt.put<int64_t>(GENERATION, this->generation);
  pt.put<ObjectId>(ID, this->id);
  pt.put(NAME, this->name);

  return pt;
};

/**
 * @brief   Transform metadata from ptree object to structure object.
 * @param   pt [in] ptree object of metdata.
 * @return  structure object of metadata.
 */
void 
Object::convert_from_ptree(const boost::property_tree::ptree& pt) {
  auto opt_int = pt.get_optional<int64_t>(FORMAT_VERSION);
  this->format_version = opt_int  ? opt_int.get() : INVALID_VALUE;

  opt_int = pt.get_optional<int64_t>(GENERATION);
  this->generation  = opt_int ? opt_int.get() : INVALID_VALUE;

  auto opt_id = pt.get_optional<ObjectId>(ID);
  this->id = opt_id ? opt_id.get() : INVALID_OBJECT_ID;

  auto opt_str = pt.get_optional<std::string>(NAME);
  this->name = opt_str ? opt_str.get() : "";
};

// ==========================================================================
// ClassObject struct methods.
/** @brief  Convert metadata from structure object to ptree object.
 *  @return ptree object.
 */
boost::property_tree::ptree ClassObject::convert_to_ptree() const {
  auto pt = Object::convert_to_ptree();
  pt.put(DATABASE_NAME, this->database_name);
  pt.put(SCHEMA_NAME, this->schema_name);
  pt.put(NAMESPACE, namespace_name);
  pt.put(ACL, this->acl);

  return pt;
};

/**
 * @brief   Convert metadata from ptree object to structure object.
 * @param   ptree [in] ptree object of metdata.
 * @return  structure object of metadata.
 */
void 
ClassObject::convert_from_ptree(const boost::property_tree::ptree& pt) {
  Object::convert_from_ptree(pt);
  auto opt_str = pt.get_optional<std::string>(DATABASE_NAME);
  this->database_name = opt_str ? opt_str.get() : "";

  opt_str = pt.get_optional<std::string>(SCHEMA_NAME);
  this->schema_name = opt_str ? opt_str.get()   : "";

  auto namespace_name = pt.get_optional<std::string>(NAMESPACE);
  this->namespace_name = namespace_name ? namespace_name.get()  : "";

  opt_str = pt.get_optional<std::string>(ACL);
  this->acl = opt_str ? opt_str.get() : "";
};

// ==========================================================================
// Metadata class methods.
/**
 *  @brief Constructor
 *  @param (database)  [in]  database name.
 *  @param (component) [in]  your component name.
 *  @return none.
 */
Metadata::Metadata(std::string_view database, std::string_view component)
    : database_(database), component_(component) {
  if (!log::LogController::get_logger()) {
    // Register a default logger.
    log::LogController::set_logger(std::make_shared<log::DefaultLogger>());
#if LOG_LEVEL == 4
    log::LogController::set_filter(log::logging::Severity::DEBUG);
#elif LOG_LEVEL == 3
    log::LogController::set_filter(log::logging::Severity::INFO);
#elif LOG_LEVEL == 2
    log::LogController::set_filter(log::logging::Severity::WARNING);
#elif LOG_LEVEL == 1
    log::LogController::set_filter(log::logging::Severity::ERROR);
#elif LOG_LEVEL == 0
    log::LogController::set_filter(log::logging::Severity::NONE);
#else
    log::LogController::set_filter(log::logging::Severity::ERROR);
#endif
  }
}

/**
 * @brief Read latest table-metadata from metadata-table.
 * @return ErrorCode::OK if success, otherwise an error code.
 */
[[deprecated("don't use load() function.")]] ErrorCode Metadata::load() const {
  return ErrorCode::OK;
}

/**
 * @brief Load metadata from metadata-table.
 * @param (database)   [in]  database name
 * @param (pt)         [out] property_tree object to populating metadata.
 * @param (generation) [in]  metadata generation to load.
 *   load latest generation if NOT provided.
 * @return ErrorCode::OK if success, otherwise an error code.
 */
[[deprecated("don't use load() function.")]] ErrorCode Metadata::load(
    std::string_view database, boost::property_tree::ptree& object,
    const GenerationType generation) {
  return ErrorCode::OK;
}

/**
 * @brief Add a metadata object to the metadata table.
 * @param object    [in]  metadata object to add.
 * @param object_id [out] ID of the added metadata object.
 * @return ErrorCode::OK if success, otherwise an error code.
 */
ErrorCode Metadata::add(const manager::metadata::Object* object,
                      ObjectIdType* object_id) const {

  ptree pt = object->convert_to_ptree();
  ErrorCode error = this->add(pt, object_id);
  if (error != ErrorCode::OK) {
    return error;
  }

  return error;
}

/**
 * @brief Add a metadata object to table metadata table.
 * @param object  [in]  table metadata to add.
 * @return ErrorCode::OK if success, otherwise an error code.
 */
ErrorCode Metadata::add(const manager::metadata::Object* object) const
{
  ObjectId object_id = INVALID_OBJECT_ID;
  ErrorCode error = this->add(object, &object_id);
  if (error != ErrorCode::OK) {
    return error;
  }

  return error;
}

/**
 * @brief Get a metadata object.
 * @param object_id [in]  object id.
 * @param object     [out] metadata object with the specified ID.
 * @retval ErrorCode::OK if success,
 * @retval ErrorCode::ID_NOT_FOUND if the table id does not exist.
 * @retval otherwise an error code.
 */
ErrorCode Metadata::get(const ObjectIdType object_id,
                      manager::metadata::Object* object) const
{
  ptree pt;

  ErrorCode error = this->get(object_id, pt);
  if (error == ErrorCode::OK) {
    object->convert_from_ptree(pt);
  }

  return error;
}

/**
 * @brief Get a metadata object object based on object name.
 * @param object_name [in]  object name. (Value of "name" key.)
 * @param object       [out] metadata object object with the specified name.
 * @retval ErrorCode::OK if success,
 * @retval ErrorCode::NAME_NOT_FOUND if the table name does not exist.
 * @retval otherwise an error code.
 */
ErrorCode Metadata::get(std::string_view object_name,
                      manager::metadata::Object* object) const
{
  ptree pt;

  ErrorCode error = this->get(object_name, pt);
  if (error == ErrorCode::OK) {
    object->convert_from_ptree(pt);
  }

  return error;
}


/**
 * @brief Get all metadata object objects from the metadata table.
 *   If no metadata object existst, return the container as empty.
 * @param objects  [out] Container of metadata objects.
 * @return ErrorCode::OK if success, otherwise an error code.
 */
ErrorCode Metadata::get_all(
    std::vector<manager::metadata::Object>& objects) const {

  std::vector<ptree> pts;
  ErrorCode error = this->get_all(pts);
  if (error != ErrorCode::OK) {
    return error;
  }

  for (const auto& pt : pts) {
    Object object;
    object.convert_from_ptree(pt);
    objects.emplace_back(object);
  }

  return error;  
}

}  // namespace manager::metadata
