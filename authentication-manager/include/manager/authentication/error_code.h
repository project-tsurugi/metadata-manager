/*
 * Copyright 2021 tsurugi project.
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
#ifndef MANAGER_AUTHENTICATION_MANAGER_INCLUDE_MANAGER_AUTHENTICATION_ERROR_CODE_H_
#define MANAGER_AUTHENTICATION_MANAGER_INCLUDE_MANAGER_AUTHENTICATION_ERROR_CODE_H_

namespace manager::authentication {

enum class ErrorCode {
  /**
   *  @brief Success.
   */
  OK = 0,

  /**
   * @brief Authentication failed.
   *   The authentication information is invalid.
   */
  AUTHENTICATION_FAILURE,

  /**
   * @brief Failed to connect to the database.
   */
  CONNECTION_FAILURE,

  /**
   * @brief Unknown error.
   */
  UNKNOWN
};  // enum ErrorCode

}  // namespace manager::authentication

#endif  // MANAGER_AUTHENTICATION_MANAGER_INCLUDE_MANAGER_AUTHENTICATION_ERROR_CODE_H_
