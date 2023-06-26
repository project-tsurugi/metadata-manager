/*
 * Copyright 2023 tsurugi project.
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
#ifndef MANAGER_METADATA_COMMON_CONSTANTS_H_
#define MANAGER_METADATA_COMMON_CONSTANTS_H_

#include <cstdint>

namespace manager::metadata {

using FormatVersion     = std::int32_t;
using FormatVersionType = FormatVersion;
using Generation        = std::int64_t;
using GenerationType    = Generation;
using ObjectId          = std::int64_t;
using ObjectIdType      = ObjectId;

static constexpr const ObjectId INVALID_OBJECT_ID = -1;
static constexpr const std::int64_t INVALID_VALUE = -1;

static constexpr const char* const kEmptyStringJson = "{}";

}  // namespace manager::metadata

#endif  // MANAGER_METADATA_COMMON_CONSTANTS_H_
