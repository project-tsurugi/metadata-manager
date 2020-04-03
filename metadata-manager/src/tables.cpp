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
#include <boost/optional.hpp>
#include <boost/foreach.hpp>
#include <boost/property_tree/ptree.hpp>
#include <boost/property_tree/json_parser.hpp>

#include "manager/metadata/error_code.h"
#include "manager/metadata/object_id.h"
#include "manager/metadata/metadata.h"
#include "manager/metadata/tables.h"

using namespace boost::property_tree;

namespace manager::metadata {

ErrorCode Tables::init()
{
    ErrorCode error = ErrorCode::UNKNOWN;

    try {
        std::string filename = std::string{Tables::TABLE_NAME} + ".json";
        std::ifstream file(filename);

        if (!file.is_open()) {
            // create metadata-table
            ptree root;
            Metadata::init(root);
            root.put(Tables::TABLES_NODE, "");
            error = Tables::save("", root);
            if (error != ErrorCode::OK) {
                return error;
            }
        }    
    } catch (...) {
        return error;
    }

    error = ErrorCode::OK;

    return error;
}

/**
 *  @brief  Load metadata from metadata-table.
 *  @param  (database)   [in]  database name
 *  @param  (pt)         [out] property_tree object to populating metadata.
 *  @param  (generation) [in]  metadata generation to load. load latest generation if NOT provided.
 *  @return ErrorCode::OK if success, otherwise an error code.
 */
ErrorCode Tables::load(
    std::string_view database, boost::property_tree::ptree& pt, const uint64_t generation)
{
    return Metadata::load(database, Tables::TABLE_NAME, pt, generation);
}

/**
 *  @brief  Save the metadta to metadta-table.
 *  @param  (database)   [in]  database name.
 *  @param  (pt)         [in]  property_tree object that stores metadata to be saved.
 *  @param  (generation) [out] the generation of saved metadata.
 *  @return ErrorCode::OK if success, otherwise an error code.
 */
ErrorCode Tables::save(
    std::string_view database, boost::property_tree::ptree& pt, uint64_t* generation)
{
    return Metadata::save(database, Tables::TABLE_NAME, pt, generation);
}

/**
 *  @brief  Generate the object ID of table-metadata.
 *  @return new object ID.
 */
ObjectIdType Tables::generate_object_id() const
{
    return ObjectId::generate(TABLE_NAME);
}

/**
 *  @brief  Generate the object ID of column-metadata.
 *  @return new object ID.
 */
ObjectIdType generate_column_id()
{
    return ObjectId::generate("column");
}

/**
 *  @brief  Generate the object ID of constraint-metadata.
 *  @return new object ID.
 */
ObjectIdType generate_constraint_id()
{
    return ObjectId::generate("constraint");
}

ErrorCode Tables::fill_parameters(boost::property_tree::ptree& table)
{
    ErrorCode error = ErrorCode::UNKNOWN;

    //
    // column metdata
    // 
    BOOST_FOREACH (ptree::value_type& node, table.get_child(COLUMNS_NODE)) {
        ptree& column = node.second;
        // column ID
        column.put(Column::ID, generate_column_id());

        // table ID
        column.put(Column::TABLE_ID, table.get<ObjectIdType>(ID));

        // data-type ID.
        boost::optional<ObjectIdType> data_type_id 
            = column.get_optional<ObjectIdType>(Column::DATA_TYPE_ID);
        if (!data_type_id) {
            return ErrorCode::NOT_FOUND;
        }
    }

    error = ErrorCode::OK;

    return error;
}

} // namespace manager::metadata
