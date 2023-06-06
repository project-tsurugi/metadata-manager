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

#include <string_view>
#include <memory>
#include "manager/metadata/metadata.h"
#include "manager/metadata/tables.h"
#include "manager/metadata/indexes.h"

namespace manager::metadata {
  std::unique_ptr<Metadata> get_table_metadata(std::string_view database);
  std::unique_ptr<Metadata> get_index_metadata(std::string_view database);
  std::unique_ptr<Metadata> get_constraint_metadata(std::string_view database);

  std::unique_ptr<Metadata> get_tables_ptr(std::string_view database);
  std::unique_ptr<Metadata> get_indexes_ptr(std::string_view database);
  std::unique_ptr<Metadata> get_constraints_ptr(std::string_view database);
} // namespace manager::metadata
