/*
 * Copyright 2020-2021 tsurugi project.
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
#ifndef MANAGER_METADATA_MANAGER_INCLUDE_MANAGER_METADATA_ERROR_CODE_H_
#define MANAGER_METADATA_MANAGER_INCLUDE_MANAGER_METADATA_ERROR_CODE_H_

namespace manager::metadata {

enum class ErrorCode {
  /**
   *  @brief Success.
   */
  OK = 0,

  /**
   *  @brief The target object not found.
   */
  NOT_FOUND,

  /**
   *  @brief ID of the metadata-object not found from metadata-table.
   */
  ID_NOT_FOUND,

  /**
   *  @brief name of the metadata-object not found from metadata-table.
   */
  NAME_NOT_FOUND,

  /**
   *  @brief Current in the Metadata stepped over the last row (a result of
   * successful processing).
   */
  END_OF_ROW,

  /**
   *  @brief the object with same parameter exists.
   */
  ALREADY_EXISTS,

  /**
   *  @brief The target object not supported.
   */
  NOT_SUPPORTED,

  /**
   * @brief Unknown error.
   */
  UNKNOWN,

  /**
   *  @brief table name already exists.
   */
  TABLE_NAME_ALREADY_EXISTS,

  /**
   * @brief Invalid parameter input.
   */
  INVALID_PARAMETER,

  /**
   * @brief Failed to access metadata repository.
   */
  DATABASE_ACCESS_FAILURE,

  /**
   * @brief Service class instance is not initialized.
   * Connection is not established,
   * Failed to send prepared statement query to metadata store or
   * Failed to set always-secure search path.
   */
  NOT_INITIALIZED,

  /**
   * @brief internal error.
   */
  INTERNAL_ERROR
};  // enum ErrorCode

}  // namespace manager::metadata

#endif  // MANAGER_METADATA_MANAGER_INCLUDE_MANAGER_METADATA_ERROR_CODE_H_
