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
#ifndef METADATA_ERROR_CODE_H_
#define METADATA_ERROR_CODE_H_

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
     *  @brief Current in the Metadata stepped over the last row (a result of successful processing).
     */
    END_OF_ROW,

    /**
     *  @brief the object with same parameter exists.
     */
    ALREADY_EXISTS,

    /**
     * @brief Unknown error.
     */
    UNKNOWN,

    /**
     *  @brief table name already exists.
     */
    TABLE_NAME_ALREADY_EXISTS,
};

}   // namespace manager::metadadta

/* ============================================================================================= */
namespace manager::metadata_manager {

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
     *  @brief Current in the Metadata stepped over the last row (a result of successful processing).
     */
    END_OF_ROW,

    /**
     *  @brief the object with same parameter exists.
     */
    ALREADY_EXISTS,

    /**
     * @brief Unknown error.
     */
    UNKNOWN,
};

} // namespace manager::metadata_manager

#endif // METADATA_ERROR_CODE_H_
