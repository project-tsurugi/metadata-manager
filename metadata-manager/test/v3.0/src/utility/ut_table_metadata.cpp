/*
 * Copyright 2020 tsurugi project.
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

#include "test/utility/ut_table_metadata.h"

#include <utility>

#include "manager/metadata/tables.h"

using namespace boost::property_tree;

namespace manager::metadata::testing {

void UTTableMetadata::generate_ptree() {
    tables.put(Tables::NAME, name);

    if (primary_keys.size() > 0) {
        ptree p_primary_key;
        ptree p_primary_keys;

        for (int pkey_val : primary_keys) {
            p_primary_key.put("", pkey_val);
            p_primary_keys.push_back(std::make_pair("", p_primary_key));
        }

        tables.add_child(Tables::PRIMARY_KEY_NODE, p_primary_keys);
    }

    ptree ptree_columns;
    for (UTColumnMetadata column : columns) {
        ptree ptree_column;

        ptree_column.put(Tables::Column::NAME, column.name);
        ptree_column.put(Tables::Column::ORDINAL_POSITION,
                         column.ordinal_position);

        ptree_column.put<ObjectIdType>(Tables::Column::DATA_TYPE_ID,
                                       column.data_type_id);

        ptree_column.put<bool>(Tables::Column::NULLABLE, column.nullable);

        if (column.data_length >= 0) {
            ptree_column.put(Tables::Column::DATA_LENGTH, column.data_length);
        }

        if (column.varying >= 0) {
            ptree_column.put<bool>(Tables::Column::VARYING,
                                   static_cast<bool>(column.varying));
        }

        if (!column.default_expr.empty()) {
            ptree_column.put(Tables::Column::DEFAULT, column.default_expr);
        }

        if (column.direction >=
            static_cast<int>(Tables::Column::Direction::DEFAULT)) {
            ptree_column.put(Tables::Column::DIRECTION, column.direction);
        }

        ptree_columns.push_back(std::make_pair("", ptree_column));
    }

    tables.add_child(Tables::COLUMNS_NODE, ptree_columns);
}

}  // namespace manager::metadata::testing
