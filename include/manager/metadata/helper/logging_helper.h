/*
 * Copyright 2020-2023 tsurugi project.
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
#ifndef MANAGER_METADATA_HELPER_LOGGING_HELPER_H_
#define MANAGER_METADATA_HELPER_LOGGING_HELPER_H_

#include <iomanip>
#include <map>
#include <vector>

#include "manager/metadata/error_code.h"
#include "manager/metadata/log/log_controller.h"

/**
 * @brief Output log of error severity.
 */
#define LOG_ERROR \
  manager::metadata::log::LogController::logger_error(__FILE__, __LINE__)

/**
 * @brief Output log of warning severity.
 */
#define LOG_WARNING \
  manager::metadata::log::LogController::logger_warn(__FILE__, __LINE__)

/**
 * @brief Output log of info severity.
 */
#define LOG_INFO \
  manager::metadata::log::LogController::logger_info(__FILE__, __LINE__)

/**
 * @brief Output log of debug severity.
 */
#define LOG_DEBUG \
  manager::metadata::log::LogController::logger_debug(__FILE__, __LINE__)

namespace manager::metadata {

std::ostream& operator<<(std::ostream& os, const ErrorCode& ec);
std::ostream& operator<<(std::ostream& os,
                         const std::vector<std::string_view>& vc);
std::ostream& operator<<(
    std::ostream& os, const std::map<std::string_view, std::string_view>& mp);

namespace log {

void function_start(const char* function);
void function_finish(const char* function,
                     const ErrorCode& error = ErrorCode::UNKNOWN);

}  // namespace log

}  // namespace manager::metadata

#endif  // MANAGER_METADATA_HELPER_LOGGING_HELPER_H_
