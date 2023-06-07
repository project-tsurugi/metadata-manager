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

#ifndef MANAGER_METADATA_COMMON_MESSAGE_H_
#define MANAGER_METADATA_COMMON_MESSAGE_H_

namespace manager::metadata {

/**
 * @brief message.
 */
struct Message {
  static constexpr const char* const CONNECT_FAILURE =
      "Failed to open connection to database.";
  static constexpr const char* const CLOSE_FAILURE =
      "Failed to close connection to database.";
  static constexpr const char* const NOT_CONNECT =
      "Not connected to the database.";
  static constexpr const char* const NOT_INITIALIZED = "not initialized.";
  static constexpr const char* const START_TRANSACTION_FAILURE =
      "Failed to start transaction.: ";
  static constexpr const char* const COMMIT_FAILURE = "Failed to commit.: ";
  static constexpr const char* const ROLLBACK_FAILURE = "Failed to rollback.: ";
  static constexpr const char* const SET_ALWAYS_SECURE_SEARCH_PATH =
      "Failed to set always-secure search path.: ";
  static constexpr const char* const PREPARE_FAILURE =
      "Failed to prepare of prepared statement.: ";
  static constexpr const char* const PREPARED_STATEMENT_EXECUTION_FAILURE =
      "Failed to execute prepared statement.: ";
  static constexpr const char* const READ_JSON_FAILURE =
      "Failed to parse json.: JSON to Property-tree. ";
  static constexpr const char* const WRITE_JSON_FAILURE =
      "Failed to parse json.: Property-tree to JSON. ";
  static constexpr const char* const READ_JSON_FILE_FAILURE =
      "Failed to read json file.: ";
  static constexpr const char* const WRITE_JSON_FILE_FAILURE =
      "Failed to write json file.: ";
  static constexpr const char* const READ_INI_FILE_FAILURE =
      "Failed to read INI file.: ";
  static constexpr const char* const WRITE_INI_FILE_FAILURE =
      "Failed to write INI file.: ";
  static constexpr const char* const CONVERT_STRING_TO_FLOAT_FAILURE =
      "Failed to convert a string to a floating point. ";
  static constexpr const char* const CONVERT_STRING_TO_INT_FAILURE =
      "Failed to convert a string to an integer. ";
  static constexpr const char* const METADATA_KEY_NOT_FOUND =
      "Could not find such column name of metadata.: ";
  static constexpr const char* const GENERATE_FAILED_DAO =
      "Failed to generate DAO.: ";
  static constexpr const char* const PARAMETER_FAILED = "Invalid parameter.: ";
  static constexpr const char* const INVALID_STATEMENT_KEY =
      "SQL statement is undefined.: ";
  static constexpr const char* const ALREADY_EXISTS =
      "Object with same parameter exists.: ";
  static constexpr const char* const RECORD_INSERT_FAILURE =
      "Failed to insert record. ";
  static constexpr const char* const INCORRECT_DATA = "Incorrect data.: ";
  static constexpr const char* const INVALID_TOKEN = "Token is invalid.: ";
};  // struct Message

}  // namespace manager::metadata

#endif  // MANAGER_METADATA_COMMON_MESSAGE_H_
