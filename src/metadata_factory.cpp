/*
 * Copyright 2022 Project Tsurugi.
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
#include "manager/metadata/metadata_factory.h"

namespace manager::metadata {

std::unique_ptr<Metadata> get_table_metadata(std::string_view database) {
  return std::make_unique<Tables>(database);
}

std::unique_ptr<Metadata> get_index_metadata(std::string_view database) {
  return std::make_unique<Indexes>(database);
}

std::unique_ptr<Metadata> get_constraint_metadata(std::string_view database) {
  return std::make_unique<Constraints>(database);
}

std::unique_ptr<Metadata> get_tables_ptr(std::string_view database) {
  return std::make_unique<Tables>(database);
}

std::unique_ptr<Metadata> get_indexes_ptr(std::string_view database) {
  return std::make_unique<Indexes>(database);
}

std::unique_ptr<Metadata> get_constraints_ptr(std::string_view database) {
  return std::make_unique<Constraints>(database);
}

std::unique_ptr<Metadata> get_datatypes_ptr(std::string_view database) {
  return std::make_unique<DataTypes>(database);
}

std::unique_ptr<Metadata> get_roles_ptr(std::string_view database) {
  return std::make_unique<Roles>(database);
}

std::unique_ptr<Metadata> get_statistics_ptr(std::string_view database) {
  return std::make_unique<Statistics>(database);
}

}  // namespace manager::metadata
