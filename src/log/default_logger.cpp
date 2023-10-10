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
#include "manager/metadata/log/default_logger.h"

#include <chrono>
#include <iomanip>
#include <iostream>

namespace {

constexpr auto kLogPrefixError = "[ERROR]";
constexpr auto kLogPrefixWarn = "[WARN]";
constexpr auto kLogPrefixInfo = "[INFO]";
constexpr auto kLogPrefixDebug = "[DEBUG]";

}  // namespace

namespace manager::metadata::log {

/**
 * @brief Outputs error level logs.
 * @param (log_string)  [in]  log string.
 */
void DefaultLogger::error(std::string_view log_string) const {
  output(kLogPrefixError, log_string);
}

/**
 * @brief Outputs warning level logs.
 * @param (log_string)  [in]  log string.
 */
void DefaultLogger::warn(std::string_view log_string) const {
  output(kLogPrefixWarn, log_string);
}

/**
 * @brief Outputs infomation level logs.
 * @param (log_string)  [in]  log string.
 */
void DefaultLogger::info(std::string_view log_string) const {
  output(kLogPrefixInfo, log_string);
}

/**
 * @brief Outputs debug level logs.
 * @param (log_string)  [in]  log string.
 */
void DefaultLogger::debug(std::string_view log_string) const {
  output(kLogPrefixDebug, log_string);
}

/**
 * @brief Logs to standard output.
 * @param (prefix_string) [in]  prefix string.
 * @param (log_string)    [in]  log string.
 */
void DefaultLogger::output(std::string_view prefix_string,
                           std::string_view log_string) const {
  try {
    std::stringstream stream;

    // Obtain the current time.
    auto system_time = std::chrono::system_clock::now();
    std::time_t now_time = std::chrono::system_clock::to_time_t(system_time);
    auto msec = std::chrono::duration_cast<std::chrono::milliseconds>(
        std::chrono::nanoseconds{system_time.time_since_epoch()});

    // Save the current format flags.
    auto fillch = std::cout.fill('0');

    // Compose and output the log data.
    std::cout << "["
              << std::put_time(std::localtime(&now_time), "%Y-%m-%dT%H:%M:%S")
              << "." << std::setw(3) << msec.count() % 1000
              << "] " << prefix_string << " " << log_string << std::endl;

    // Restore format flags.
    std::cout.fill(fillch);
  } catch (const std::exception& e) {
    std::cerr << "\n\n<<<<<\nexception on " << __PRETTY_FUNCTION__
              << "\nwhat=" << e.what() << "\n>>>>>" << std::endl;
  } catch (...) {
    std::cerr << "\n\n<<<<<\nexception on " << __PRETTY_FUNCTION__
              << "\nunknown\n>>>>>" << std::endl;
  }
}

}  // namespace manager::metadata::log
