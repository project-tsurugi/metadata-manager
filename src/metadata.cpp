/*
 * Copyright 2020-2021 Project Tsurugi.
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

#include <memory>

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
boost::property_tree::ptree Object::base_convert_to_ptree() const {
  boost::property_tree::ptree pt;
  pt.put<int64_t>(FORMAT_VERSION, this->format_version);
  pt.put<int64_t>(GENERATION, this->generation);
  pt.put<ObjectId>(ID, this->id);
  pt.put(NAME, this->name);

  return pt;
};

/**
 * @brief   Transform metadata from ptree object to structure object.
 * @param   pt [in] ptree object of metadata.
 * @return  structure object of metadata.
 */
void 
Object::base_convert_from_ptree(const boost::property_tree::ptree& pt) {
  this->format_version = 
      pt.get_optional<int64_t>(FORMAT_VERSION).value_or(INVALID_VALUE);
  this->generation = 
      pt.get_optional<int64_t>(GENERATION).value_or(INVALID_VALUE);
  this->id = pt.get_optional<ObjectId>(ID).value_or(INVALID_OBJECT_ID);
  this->name = pt.get_optional<std::string>(NAME).value_or("");
};

// ==========================================================================
// ClassObject struct methods.
/** 
 *  @brief  Convert metadata from structure object to ptree object.
 *  @return ptree object.
 */
boost::property_tree::ptree ClassObject::base_convert_to_ptree() const {
  auto pt = Object::base_convert_to_ptree();
  pt.put(DATABASE_NAME, this->database_name);
  pt.put(SCHEMA_NAME, this->schema_name);
  pt.put(NAMESPACE, namespace_name);
  pt.put<ObjectId>(OWNER_ID, this->owner_id);
  pt.put(ACL, this->acl);

  return pt;
};

/**
 * @brief   Convert metadata from ptree object to structure object.
 * @param   ptree [in] ptree object of metadata.
 * @return  structure object of metadata.
 */
void 
ClassObject::base_convert_from_ptree(const boost::property_tree::ptree& pt) {
  Object::base_convert_from_ptree(pt);
  this->database_name = 
      pt.get_optional<std::string>(DATABASE_NAME).value_or("");
  this->schema_name = pt.get_optional<std::string>(SCHEMA_NAME).value_or("");
  this->namespace_name = pt.get_optional<std::string>(NAMESPACE).value_or("");
  this->owner_id = 
      pt.get_optional<ObjectId>(OWNER_ID).value_or(INVALID_OBJECT_ID);
  this->acl = pt.get_optional<std::string>(ACL).value_or("");
};

/** 
 *  @brief  Convert metadata from structure object to ptree object.
 *  @return ptree object.
 */
boost::property_tree::ptree ClassObject::convert_to_ptree() const {
  return this->base_convert_to_ptree();
}

/**
 * @brief   Convert metadata from ptree object to structure object.
 * @param   ptree [in] ptree object of metadata.
 * @return  structure object of metadata.
 */
void ClassObject::convert_from_ptree(const boost::property_tree::ptree& pt) {
  this->base_convert_from_ptree(pt);
}

// ==========================================================================
// Metadata class methods.
/**
 *  @brief Constructor
 *  @param (database)  [in]  database name.
 *  @param (component) [in]  your component name.
 *  @return none.
 */
Metadata::Metadata(std::string_view database, std::string_view component)
    : database_(database), component_(component), cursor_(0) {
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
ErrorCode Metadata::add(const manager::metadata::Object& object,
                      ObjectIdType* object_id) const {

  ptree pt = object.convert_to_ptree();
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
ErrorCode Metadata::add(const manager::metadata::Object& object) const
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
ErrorCode Metadata::get(const ObjectId object_id,
                      manager::metadata::Object& object) const
{
  ptree pt;

  ErrorCode error = this->get(object_id, pt);
  if (error == ErrorCode::OK) {
    object.convert_from_ptree(pt);
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
                      manager::metadata::Object& object) const
{
  ptree pt;

  ErrorCode error = this->get(object_name, pt);
  if (error == ErrorCode::OK) {
    object.convert_from_ptree(pt);
  }

  return error;
}

/**
 * @brief
 */
ErrorCode Metadata::get_all() {
  objects_.clear();
  ErrorCode error = this->get_all(objects_);
  cursor_ = 0;

  return error;
}

/**
 * @brief
 */
ErrorCode Metadata::next(boost::property_tree::ptree& object) {   
  ErrorCode error = ErrorCode::UNKNOWN;
  if (objects_.empty()) {
    error = this->get_all(objects_);
    if (error != ErrorCode::OK) {
      return error;
    }
    cursor_ = 0;
  }

  if (objects_.size() > cursor_) {
    object = objects_[cursor_];
    cursor_++;
    error = ErrorCode::OK;
  } else {
    cursor_ = 0;
    error =  ErrorCode::END_OF_ROW;
  }

  return error;
}

/**
 * @brief
 */
ErrorCode Metadata::next(manager::metadata::Object& object) {
    ptree pt;
    ErrorCode error = this->next(pt);
    if (error == ErrorCode::OK) {
      object.convert_from_ptree(pt);
    }

    return error;
}

/**
 * @brief Update the metadata-table with metadata-object.
 * @param object_id [in]  ID of the metadata-object to update.
 * @param object    [in]  metadata-object to update.
 * @return ErrorCode::OK if success, otherwise an error code.
 */
ErrorCode Metadata::update(const manager::metadata::ObjectIdType object_id,
                          const manager::metadata::Object& object) const {

  ptree pt = object.convert_to_ptree();
  return this->update(object_id, pt);
}

/**
 * @brief
 */
std::unique_ptr<Iterator> Metadata::iterator() {
  return std::make_unique<MetadataIterator>(this);
}

// ==========================================================================
// MetadataIterator class methods.

bool MetadataIterator::has_next() const {
  return (metadata_->size() > cursor_) ? true : false;
}

/**
 * @brief
 */
ErrorCode MetadataIterator::next(Object& obj) {
  ErrorCode error = ErrorCode::UNKNOWN;
  if (metadata_->size() > cursor_) {
    metadata_->get(cursor_, obj);
    cursor_++;
    error = ErrorCode::OK;
  } else {
    error =  ErrorCode::END_OF_ROW;
  }

  return error;
}

}  // namespace manager::metadata
