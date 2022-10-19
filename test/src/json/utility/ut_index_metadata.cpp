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
#include "test/metadata/json/ut_index_metadata_json.h"

#include <utility>

#include "manager/metadata/indexes.h"

namespace manager::metadata::testing {

using boost::property_tree::ptree;

/**
 * @brief Generate ptree type constrain metadata from UTIndexMetadata fields.
 * @return none.
 */
void UTIndexMetadata::generate_ptree() {
  indexes_metadata.clear();

  // id
  if (id != NOT_INITIALIZED) {
    indexes_metadata.put(Index::ID, id);
  }

  // name
  if (!name.empty()) {
    indexes_metadata.put(Index::NAME, name);
  }

  // schema
  if (!namespace_name.empty()) {
    indexes_metadata.put(Index::NAMESPACE, namespace_name);
  }

  // owner_id
  if (owner_id != NOT_INITIALIZED) {
    indexes_metadata.put(Index::OWNER_ID, owner_id);
  }

  // acl
  if (!acl.empty()) {
    indexes_metadata.put(Index::ACL, acl);
  }

  // table id
  if (table_id != NOT_INITIALIZED) {
    indexes_metadata.put(Index::TABLE_ID, table_id);
  }

  // access_method
  if (access_method != NOT_INITIALIZED) {
    indexes_metadata.put(Index::ACCESS_METHOD, access_method);
  }

  // number_of_key_columns
  if (number_of_key_columns != NOT_INITIALIZED) {
    indexes_metadata.put(Index::NUMBER_OF_KEY_COLUMNS, number_of_key_columns);
  }

  // is_unique
  indexes_metadata.put(Index::IS_UNIQUE, is_unique);

  // is_primary
  indexes_metadata.put(Index::IS_PRIMARY, is_primary);

  // columns
  if (columns.size() > 0) {
    ptree elements;
    ptree element;
    for (auto value : columns) {
      element.put("", value);
      elements.push_back(std::make_pair("", element));
    }
    indexes_metadata.add_child(Index::KEYS, elements);
  }

  // columns id
  if (columns_id.size() > 0) {
    ptree element;
    ptree elements;
    for (auto value : columns_id) {
      element.put("", value);
      elements.push_back(std::make_pair("", element));
    }
    indexes_metadata.add_child(Index::KEYS_ID, elements);
  }

  // options
  if (options.size() > 0) {
    ptree elements;
    ptree element;
    for (auto value : options) {
      element.put("", value);
      elements.push_back(std::make_pair("", element));
    }
    indexes_metadata.add_child(Index::OPTIONS, elements);
  }
}

}  // namespace manager::metadata::testing
