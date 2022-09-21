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
   *  @brief  Check if the object with the specified object ID exists.
   *  @param  object_id   [in]  object ID of metadata object.
   *  @return true if success.
   */
  bool Metadata::exists(const ObjectIdType object_id) const
  {
    bool result = false;

    boost::property_tree::ptree object;
    ErrorCode error = this->get(object_id, object);
    return (error == ErrorCode::OK) ? true : false;
  }

  /**
   *  @brief  Check if the object with the specified name exists.
   *  @param  name   [in]  name of metadata.
   *  @return true if success.
   */
  bool Metadata::exists(std::string_view object_name) const
  {
    bool result = false;

    boost::property_tree::ptree object;
    ErrorCode error = this->get(object_name, object);
    if (error != ErrorCode::OK) {
      return result;
    }
    result = true;

    return result;
  }

}  // namespace manager::metadata
