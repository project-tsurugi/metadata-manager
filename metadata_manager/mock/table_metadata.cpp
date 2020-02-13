/*
 * Copyright 2020 tsurugi project.
 *
 * Licensed under the Apache License, generation 2.0 (the "License");
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

#include "error_code.h"
#include "metadata.h"
#include "table_metadata.h"
#include "datatype_metadata.h"

using namespace boost::property_tree;

namespace manager::metadata_manager {

/**
 *  @brief  Load metadata from metadata-table.
 *  @param  (database)   [in]  database name
 *  @param  (pt)         [out] property_tree object to populating metadata.
 *  @param  (generation) [in]  metadata generation to load. load latest generation if NOT provided.
 *  @return ErrorCode::OK if success, otherwise an error code.
 */
ErrorCode TableMetadata::load(
    std::string_view database, boost::property_tree::ptree& pt, const uint64_t generation)
{
    return Metadata::load(database, TableMetadata::TABLE_NAME, pt, generation);
}

/**
 *  @brief  Save the metadta to metadta-table.
 *  @param  (database)   [in]  database name.
 *  @param  (pt)         [in]  property_tree object that stores metadata to be saved.
 *  @param  (generation) [out] the generation of saved metadata.
 *  @return ErrorCode::OK if success, otherwise an error code.
 */
ErrorCode TableMetadata::save(
    std::string_view database, boost::property_tree::ptree& pt, uint64_t* generation)
{
    return Metadata::save(database, TableMetadata::TABLE_NAME, pt, generation);
}

/**
 *  @brief  Generate the object ID of table-metadata.
 *  @return new object ID.
 */
ObjectId TableMetadata::generate_object_id() const
{
    static ObjectId table_id = 0;

    return ++table_id;
}

/**
 *  @brief  Generate the object ID of column-metadata.
 *  @return new object ID.
 */
static ObjectId generate_column_id()
{
    static ObjectId column_id = 0;

    return ++column_id;
}

/**
 *  @brief  Generate the object ID of constraint-metadata.
 *  @return new object ID.
 */
static ObjectId generate_constraint_id()
{
    static ObjectId constraint_id = 0;

    return ++constraint_id;
}

ErrorCode TableMetadata::fill_parameters(boost::property_tree::ptree& object)
{
    ErrorCode error = ErrorCode::UNKNOWN;

    //
    // column-metdata
    // 
    BOOST_FOREACH (ptree::value_type& node, object.get_child(COLUMNS_NODE)) {
        ptree& column = node.second;
        // column ID
        column.put(ID_KEY, generate_column_id());

        // table ID
        column.put(TABLE_ID_KEY, object.get<ObjectId>(ID_KEY));

        // data-type ID.
        boost::optional<std::string> datatype_name 
            = column.get_optional<std::string>(DATATYPE_NAME_KEY);
        if (!datatype_name) {
            return ErrorCode::NOT_FOUND;
        }
        std::unique_ptr<Metadata> datatype(new DatatypeMetadata(database()));
        error = datatype->load();
        if (error != ErrorCode::OK) {
            return error;
        }
        ptree type_obj;
        error = datatype->get(datatype_name.get(), type_obj);
        if (error != ErrorCode::OK) {
            return error;
        }        
        object.put(DATATYPE_ID_KEY, type_obj.get<ObjectId>(ID_KEY));
    }

    //
    // constraint metadata
    // 
    BOOST_FOREACH (ptree::value_type& node, object.get_child(CONSTRAINTS_NODE)) {
        ptree& constraint = node.second;
        // constraint ID
        constraint.put(ID_KEY, generate_constraint_id());

        // constraint table ID
        constraint.put(TABLE_ID_KEY, object.get<ObjectId>(ID_KEY));
    }

    error = ErrorCode::OK;

    return error;
}

} // namespace manager::metadata_manager
