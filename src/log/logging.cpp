/*
 * Copyright 2020-2022 Project Tsurugi.
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
#include "manager/metadata/log/logging.h"

#include "manager/metadata/log/log_controller.h"

namespace manager::metadata::log::logging {

/**
 *  @brief Get to the logger object.
 *  @return logger object.
 */
const std::shared_ptr<Logger>& get_logger() {
  return LogController::get_logger();
}

/**
 *  @brief Set to the specified logger.
 *  @param (logger)  [in]  logger.
 */
void set_logger(const std::shared_ptr<Logger>& logger) {
  LogController::set_logger(logger);
}

/**
 *  @brief Set to the specified logger.
 *  @param (logger)  [in]  logger.
 */
void set_filter(const Severity severity) {
  LogController::set_filter(severity);
}

}  // namespace manager::metadata::log::logging
