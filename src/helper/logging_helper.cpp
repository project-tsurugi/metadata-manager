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
#include "manager/metadata/helper/logging_helper.h"

#include <algorithm>
#include <iterator>
#include <string>
#include <vector>

namespace manager::metadata {

/**
 * @brief Outputs a numerically represented ErrorCode enumeration type
 *   to the stream.
 */
std::ostream& operator<<(std::ostream& os, const ErrorCode& ec) {
  os << std::to_string(static_cast<std::int32_t>(ec));
  return os;
}

/**
 * @brief Outputs a vector element (std::string_view) to a stream.
 */
std::ostream& operator<<(std::ostream& os,
                         const std::vector<std::string_view>& vc) {
  // Transform key values for logging.
  std::stringstream ss;
  std::transform(vc.begin(), vc.end(),
                 std::ostream_iterator<std::string_view>(ss, ","),
                 [](std::string_view s) { return s; });

  std::string result(ss.str());
  if (!result.empty()) {
    result.pop_back();
  }

  os << result;
  return os;
}

/**
 * @brief Outputs a map element (std::string_view) to a stream.
 */
std::ostream& operator<<(
    std::ostream& os, const std::map<std::string_view, std::string_view>& mp) {
  // Transform key values for logging.
  std::stringstream ss;
  std::transform(
      mp.begin(), mp.end(), std::ostream_iterator<std::string_view>(ss, ","),
      [](std::pair<std::string_view, std::string_view> v) {
        std::stringstream ss;
        ss << "\"" << v.first << "\": \"" << v.second << "\"";
        return ss.str();
      });

  std::string result(ss.str());
  if (!result.empty()) {
    result.pop_back();
  }

  os << result;
  return os;
}

namespace log {

/**
 * @brief Outputs a log indicating the start of function processing.
 * @param (function) [in]  function name.
 */
void function_start(const char* function) {
  LogController::logger_info(nullptr, 0) << function << " - START";
}

/**
 * @brief Outputs a log indicating the end of function processing.
 * @param (function) [in]  function name.
 * @param (error)    [in]  result (ErrorCode).
 */
void function_finish(const char* function, const ErrorCode& error) {
  auto log_controller = LogController::logger_info(nullptr, 0);
  log_controller << function << " - END";
  if (error != ErrorCode::UNKNOWN) {
    log_controller << " => " << error;
  }
}

}  // namespace log

}  // namespace manager::metadata
