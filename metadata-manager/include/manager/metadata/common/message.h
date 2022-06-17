/*
 * Copyright 2020 tsurugi project.
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

#ifndef MANAGER_METADATA_MANAGER_INCLUDE_MANAGER_METADATA_COMMON_MESSAGE_H_
#define MANAGER_METADATA_MANAGER_INCLUDE_MANAGER_METADATA_COMMON_MESSAGE_H_

namespace manager::metadata {
/**
 * @brief message.
 */
struct Message {
  static constexpr const char* const CONNECT_FAILURE =
      "Failed to open connection to database.";
  static constexpr const char* const CLOSE_FAILURE =
      "Failed to close connection to database.";
  static constexpr const char* const NOT_INITIALIZED = "not initialized.";
  static constexpr const char* const START_TRANSACTION_FAILURE =
      "Failed to start transaction.: ";
  static constexpr const char* const COMMIT_FAILURE = "Failed to commit.: ";
  static constexpr const char* const ROLLBACK_FAILURE = "Failed to rollback: ";
  static constexpr const char* const SET_ALWAYS_SECURE_SEARCH_PATH =
      "Failed to set always-secure search path.: ";
  static constexpr const char* const PREPARE_FAILURE =
      "Failed to prepare of prepared statement.: ";
  static constexpr const char* const PREPARED_STATEMENT_EXECUTION_FAILURE =
      "Failed to execute prepared statement.: ";
  static constexpr const char* const READ_JSON_FAILURE =
      "Failed to read json. ";
  static constexpr const char* const WRITE_JSON_FAILURE =
      "Failed to write json. ";
  static constexpr const char* const CONVERT_STRING_TO_FLOAT_FAILURE =
      "Failed to convert a string to a floating point. ";
  static constexpr const char* const CONVERT_STRING_TO_INT_FAILURE =
      "Failed to convert a string to an integer. ";
  static constexpr const char* const METADATA_KEY_NOT_FOUND =
      "Could not find such column name of metadata.: ";
};  // class Message

}  // namespace manager::metadata

#endif  // MANAGER_METADATA_MANAGER_INCLUDE_MANAGER_METADATA_COMMON_MESSAGE_H_
