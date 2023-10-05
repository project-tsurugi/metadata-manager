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
#ifndef MANAGER_METADATA_LOG_LOGGING_H_
#define MANAGER_METADATA_LOG_LOGGING_H_

#include <memory>
#include <string_view>

namespace manager::metadata::log::logging {

/**
 * @brief The logger is changed by registering a class object inheriting from
 *   this class with the set_logger() function.
 */
class Logger {
 public:
  virtual ~Logger() {}

  virtual void error(std::string_view log_string) const = 0;
  virtual void warn(std::string_view log_string) const = 0;
  virtual void info(std::string_view log_string) const = 0;
  virtual void debug(std::string_view log_string) const = 0;
};

/**
 * @brief Log severity levels.
 *
 */
enum class Severity { NONE, ERROR, WARNING, INFO, DEBUG };

const std::shared_ptr<Logger>& get_logger();
void set_logger(const std::shared_ptr<Logger>& logger);
void set_filter(const Severity severity);

}  // namespace manager::metadata::log::logging

#endif  // MANAGER_METADATA_LOG_LOGGING_H_
