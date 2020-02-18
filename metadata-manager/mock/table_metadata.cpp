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
#include <fstream>

#include <boost/optional.hpp>
#include <boost/foreach.hpp>
#include <boost/property_tree/ptree.hpp>
#include <boost/property_tree/json_parser.hpp>

#include "error_code.h"
#include "object_id.h"
#include "metadata.h"
#include "table_metadata.h"

using namespace boost::property_tree;

namespace manager::metadata_manager {

// root object.
const char * TableMetadata::TABLES_NODE = "tables";

// table metadata-object.
// ID is defined in base class.
// NAME is defined in base class.
const char * TableMetadata::NAMESPACE                 = "namespace";
const char * TableMetadata::COLUMNS_NODE              = "columns";
const char * TableMetadata::PRIMARY_INDEX_OBJECT      = "primaryIndex";
const char * TableMetadata::SECONDARY_INDICES_NODDE   = "secondaryIndices";
const char * TableMetadata::CONSTRAINTS_NODE          = "tableConstraints";

// column metadata-object.
const char * TableMetadata::Column::ID                = "id";
const char * TableMetadata::Column::TABLE_ID          = "tableId";
const char * TableMetadata::Column::NAME              = "name";
const char * TableMetadata::Column::ORDINAL_POSITION  = "ordinalPosition";
const char * TableMetadata::Column::DATA_TYPE_ID      = "dataTypeId";
const char * TableMetadata::Column::DATA_LENGTH       = "dataLength";
const char * TableMetadata::Column::NULLABLE          = "nullable";
const char * TableMetadata::Column::CONSTRAINTS_NODE  = "columnConstraints";

// constraint metadata-object.
const char * TableMetadata::Constraint::ID                = "id";
const char * TableMetadata::Constraint::TABLE_ID          = "tableId";
const char * TableMetadata::Constraint::COLUMN_KEY_NODE   = "columnKey";
const char * TableMetadata::Constraint::NAME              = "name";
const char * TableMetadata::Constraint::TYPE              = "type";       
const char * TableMetadata::Constraint::CONTENTS          = "contents";
// constraint-type
const char * TableMetadata::Constraint::Type::CHECK       = "C";
const char * TableMetadata::Constraint::Type::FOREIGN_KEY = "F";
const char * TableMetadata::Constraint::Type::PRIMARY_KEY = "P";
const char * TableMetadata::Constraint::Type::UNIQUE      = "U";

// Index metadata-object.
const char * TableMetadata::Index::NAME           = "name";
const char * TableMetadata::Index::COLUMN_OBJECT  = "column";

// Index-Column metadata-object.
const char * TableMetadata::Index::Column::NAME       = "name";
const char * TableMetadata::Index::Column::DIRECTION  = "direction";

const char * TableMetadata::TABLE_NAME = "tables";

ErrorCode TableMetadata::init()
{
    ErrorCode error = ErrorCode::UNKNOWN;

    try {
        std::string filename = std::string{TableMetadata::TABLE_NAME} + ".json";
        std::ifstream file(filename);

        if (!file.is_open()) {
            // create metadata-table
            ptree root;
            Metadata::init(root);
            root.put(TableMetadata::TABLES_NODE, "");
            error = TableMetadata::save("", root);
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
ObjectIdType TableMetadata::generate_object_id() const
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

void TableMetadata::fill_constraint(ptree& constraint, const ptree& table)
{
    // constraint ID
    constraint.put(Constraint::ID, generate_constraint_id());

    // constraint table ID
    constraint.put(Constraint::TABLE_ID, table.get<ObjectIdType>(Constraint::ID));

    // constraint name
    boost::optional<std::string> name 
        = constraint.get_optional<std::string>(TableMetadata::Constraint::NAME);
    if (!name) {
        constraint.put(Constraint::NAME, "default_constraint_name");
    }

}

ErrorCode TableMetadata::fill_parameters(boost::property_tree::ptree& table)
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
        column.put(Column::DATA_TYPE_ID, data_type_id);

        // column-constraint
        boost::optional<ptree&> constraints 
            = column.get_child_optional(Column::CONSTRAINTS_NODE);
        if (constraints) {
            BOOST_FOREACH (ptree::value_type& node, column.get_child(Column::CONSTRAINTS_NODE)) {
                ptree& constraint = node.second;
                fill_constraint(constraint, table);
            }
        }
    }

    //
    // table-constraint metadata
    // 
    boost::optional<ptree&> constraints 
        = table.get_child_optional(CONSTRAINTS_NODE);
    if (constraints) {
        BOOST_FOREACH (ptree::value_type& node, table.get_child(CONSTRAINTS_NODE)) {
            ptree& constraint = node.second;
            fill_constraint(constraint, table);
        }
    }

    error = ErrorCode::OK;

    return error;
}

} // namespace manager::metadata_manager
