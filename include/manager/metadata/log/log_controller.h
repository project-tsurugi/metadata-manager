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
#ifndef MANAGER_METADATA_LOG_LOG_CONTROLLER_H_
#define MANAGER_METADATA_LOG_LOG_CONTROLLER_H_

#include <iomanip>

#include "manager/metadata/log/logging.h"

namespace manager::metadata::log {

class LogController {
 public:
  explicit LogController(LogController&& object)
      : buffer_(std::move(object.buffer_)),
        severity_(object.severity_),
        file_(object.file_),
        line_(object.line_) {}

  template <typename T>
  decltype(auto) operator<<(const T& in) {
    return buffer_ << in;
  }

  ~LogController() noexcept;

  static const std::shared_ptr<logging::Logger>& get_logger(void);
  static void set_logger(const std::shared_ptr<logging::Logger>& logger);
  static void set_filter(const logging::Severity severity);

  static LogController logger_debug(const char* file, const std::size_t line);
  static LogController logger_info(const char* file, const std::size_t line);
  static LogController logger_warn(const char* file, const std::size_t line);
  static LogController logger_error(const char* file, const std::size_t line);

 private:
  LogController(logging::Severity severity, const char* file,
                const std::size_t line)
      : severity_(severity), file_(file), line_(line) {}

  static std::shared_ptr<logging::Logger> logger_;
  static logging::Severity filter_severity_;

  std::stringstream buffer_;
  const logging::Severity severity_;
  const char* file_ = nullptr;
  const std::size_t line_ = 0;
};  //  class LogController

}  // namespace manager::metadata::log

#endif  // MANAGER_METADATA_LOG_LOG_CONTROLLER_H_
