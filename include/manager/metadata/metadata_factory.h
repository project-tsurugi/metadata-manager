/*
 * Copyright 2022 tsurugi project.
 *
 * Licensed under the Apache License, version 2.0 (the "License");
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
#pragma once

#include <memory>
#include <string_view>

#include "manager/metadata/datatypes.h"
#include "manager/metadata/indexes.h"
#include "manager/metadata/metadata.h"
#include "manager/metadata/roles.h"
#include "manager/metadata/statistics.h"
#include "manager/metadata/tables.h"

namespace manager::metadata {

/**
 * @brief Generate tables metadata management.
 * @param database database name.
 * @deprecated Please use get_tables_ptr function.
 */
[[deprecated("please use get_tables_ptr function")]]
std::unique_ptr<Metadata> get_table_metadata(std::string_view database);

/**
 * @brief Generate indexes metadata management.
 * @param database database name.
 * @deprecated Please use get_indexes_ptr function.
 */
[[deprecated("please use get_indexes_ptr function")]]
std::unique_ptr<Metadata> get_index_metadata(std::string_view database);

/**
 * @brief Generate constraints metadata management.
 * @param database database name.
 * @deprecated Please use get_constraints_ptr function.
 */
[[deprecated("please use get_constraints_ptr function")]]
std::unique_ptr<Metadata> get_constraint_metadata(std::string_view database);

/**
 * @brief Generate tables metadata management.
 * @param database database name.
 */
std::unique_ptr<Metadata> get_tables_ptr(std::string_view database);

/**
 * @brief Generate indexes metadata management.
 * @param database database name.
 */
std::unique_ptr<Metadata> get_indexes_ptr(std::string_view database);

/**
 * @brief Generate constraints metadata management.
 * @param database database name.
 */
std::unique_ptr<Metadata> get_constraints_ptr(std::string_view database);

/**
 * @brief Generate datatypes metadata management.
 * @param database database name.
 */
std::unique_ptr<Metadata> get_datatypes_ptr(std::string_view database);

/**
 * @brief Generate roles metadata management.
 * @param database database name.
 */
std::unique_ptr<Metadata> get_roles_ptr(std::string_view database);

/**
 * @brief Generate columns statistics metadata management.
 * @param database database name.
 */
std::unique_ptr<Metadata> get_statistics_ptr(std::string_view database);

}  // namespace manager::metadata
