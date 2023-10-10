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
#include "manager/metadata/log/log_controller.h"

namespace manager::metadata::log {

std::shared_ptr<logging::Logger> LogController::logger_;
logging::Severity LogController::filter_severity_ = logging::Severity::ERROR;

/**
 * @brief Destroy the LogController object.
 *   Execute a function based on the importance level and output the data
 *   in the string stream.
 */
LogController::~LogController() noexcept {
  // Check if the logger object is registered.
  if (!logger_) {
    return;
  }

  // Filter the importance of the logs.
  if (severity_ > filter_severity_) {
    return;
  }

  std::stringstream stream;
  try {
    // Compose the log data.
    stream << buffer_.str();
    if (file_ != nullptr) {
      stream << " [" << file_ << ':' << line_ << "]";
    }
    stream << std::flush;
  } catch (const std::exception& e) {
    // Compose the log data.
    stream << "\n<<exception on " << __PRETTY_FUNCTION__
           << "\nwhat=" << e.what() << "\n>>\n";
  } catch (...) {
    stream << "\n<<exception on " << __PRETTY_FUNCTION__ << "\nunknown\n>>\n";
  }

  // Output logs.
  switch (severity_) {
    case logging::Severity::ERROR:
      logger_->error(stream.str());
      break;
    case logging::Severity::WARNING:
      logger_->warn(stream.str());
      break;
    case logging::Severity::INFO:
      logger_->info(stream.str());
      break;
    case logging::Severity::DEBUG:
      logger_->debug(stream.str());
      break;
    default:
      break;
  }
}

/**
 * @brief Get the logger object.
 * @return logger object.
 */
const std::shared_ptr<logging::Logger>& LogController::get_logger() {
  return logger_;
}

/**
 * @brief Set the logger object.
 * @param (logger) [in]  logger object.
 */
void LogController::set_logger(const std::shared_ptr<logging::Logger>& logger) {
  logger_ = logger;
}

/**
 * @brief Set the current filter.
 *   Only logs of severity that pass this filter will be output.
 * @param (severity) [in]  severity.
 */
void LogController::set_filter(const logging::Severity severity) {
  filter_severity_ = severity;
}

/**
 * @brief Output log of error severity.
 * @param (file) [in]  file name.
 * @param (line) [in]  number of lines.
 * @return LogController object.
 */
LogController LogController::logger_error(const char* file, const size_t line) {
  return LogController(logging::Severity::ERROR, file, line);
}

/**
 * @brief Output log of warning severity.
 * @param (file) [in]  file name.
 * @param (line) [in]  number of lines.
 * @return LogController object.
 */
LogController LogController::logger_warn(const char* file, const size_t line) {
  return LogController(logging::Severity::WARNING, file, line);
}

/**
 * @brief Output log of info severity.
 * @param (file) [in]  file name.
 * @param (line) [in]  number of lines.
 * @return LogController object.
 */
LogController LogController::logger_info(const char* file, const size_t line) {
  return LogController(logging::Severity::INFO, file, line);
}

/**
 * @brief Output log of debug severity.
 * @param (file) [in]  file name.
 * @param (line) [in]  number of lines.
 * @return LogController object.
 */
LogController LogController::logger_debug(const char* file, const size_t line) {
  return LogController(logging::Severity::DEBUG, file, line);
}

}  // namespace manager::metadata::log
