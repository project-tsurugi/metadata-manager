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

// ==========================================================================
// Object class methods.
/** 
 * @brief  Transform metadata from structure object to ptree object.
 * @return ptree object.
 */
boost::property_tree::ptree BaseObject::convert_to_ptree() const {
  boost::property_tree::ptree ptree;
  ptree.put<int64_t>(FORMAT_VERSION,  format_version_);
  ptree.put<int64_t>(GENERATION,      generation_);
  ptree.put<ObjectId>(ID,             id_);
  ptree.put(NAME,                     name_);

  return ptree;
};
/**
 * @brief   Transform metadata from ptree object to structure object.
 * @param   ptree [in] ptree object of metdata.
 * @return  structure object of metadata.
 */
void BaseObject::convert_from_ptree(const boost::property_tree::ptree& ptree) {
  auto format_version = ptree.get_optional<int64_t>(FORMAT_VERSION);
  auto generation     = ptree.get_optional<int64_t>(GENERATION);
  auto id             = ptree.get_optional<ObjectId>(ID);
  auto name           = ptree.get_optional<std::string>(NAME);

  format_version_ = format_version  ? format_version.get()  : INVALID_VALUE;
  generation_     = generation      ? generation.get()  : INVALID_VALUE;
  id_             = id              ? id.get()          : INVALID_OBJECT_ID;
  name_           = name            ? name.get()        : "";
};

// ==========================================================================
// ClassObject class methods.
/** @brief  Transform metadata from structure object to ptree object.
 *  @return ptree object.
 */
boost::property_tree::ptree ClassObject::convert_to_ptree() const {
  boost::property_tree::ptree ptree = base_obj_.convert_to_ptree();
  ptree.put(DATABASE_NAME, database_name_);
  ptree.put(SCHEMA_NAME,   schema_name_);
  ptree.put(ACL,           acl_);

  return ptree;
};
/**
 * @brief   Transform metadata from ptree object to structure object.
 * @param   ptree [in] ptree object of metdata.
 * @return  structure object of metadata.
 */
void 
ClassObject::convert_from_ptree(const boost::property_tree::ptree& ptree) {
  base_obj_.convert_from_ptree(ptree);
  auto database_name  = ptree.get_optional<std::string>(DATABASE_NAME);
  auto schema_name    = ptree.get_optional<std::string>(SCHEMA_NAME);
  auto acl            = ptree.get_optional<std::string>(ACL);

  database_name_  = database_name ? database_name.get() : "";
  schema_name_    = schema_name   ? schema_name.get()   : "";
  acl_            = acl           ? acl.get()           : "";
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

}  // namespace manager::metadata
