/*
 * Copyright 2019-2020 tsurugi project.
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
#include <boost/foreach.hpp>
#include <boost/property_tree/ptree.hpp>
#include <boost/property_tree/json_parser.hpp>

#include "manager/metadata/error_code.h"
#include "manager/metadata/metadata.h"
#include "manager/metadata/datatype_metadata.h"

using namespace boost::property_tree;

namespace manager::metadata {

const char * DataTypeMetadata::DATATYPES_NODE   = "dataTypes";
const char * DataTypeMetadata::PG_DATA_TYPE     = "pg_dataType";

const char * DataTypeMetadata::TABLE_NAME = "datatypes";

/*
 *  @biref  initialization of DataTypeMetadata.
 */
ErrorCode DataTypeMetadata::init() 
{
    ErrorCode error = ErrorCode::UNKNOWN;

    ptree root;
    Metadata::init(root);
    
    ptree datatypes;
    {
        ptree datatype;
        // NULL_VALUE
        datatype.put(DataTypeMetadata::ID, 1);
        datatype.put(DataTypeMetadata::NAME, "NULL_VALUE");
        datatype.put(DataTypeMetadata::PG_DATA_TYPE, 0);
        datatypes.push_back(std::make_pair("", datatype));

        // INT16
        datatype.put(DataTypeMetadata::ID, 1);
        datatype.put(DataTypeMetadata::NAME, "INT16");
        datatype.put(DataTypeMetadata::PG_DATA_TYPE, 0);
        datatypes.push_back(std::make_pair("", datatype));

        // INT32
        datatype.put(DataTypeMetadata::ID, 2);
        datatype.put(DataTypeMetadata::NAME, "INT32");
        datatype.put(DataTypeMetadata::PG_DATA_TYPE, 0);
        datatypes.push_back(std::make_pair("", datatype));

        // INT64
        datatype.put(DataTypeMetadata::ID, 3);
        datatype.put(DataTypeMetadata::NAME, "INT64");
        datatype.put(DataTypeMetadata::PG_DATA_TYPE, 0);
        datatypes.push_back(std::make_pair("", datatype));

        // FLOAT32
        datatype.put(DataTypeMetadata::ID, 4);
        datatype.put(DataTypeMetadata::NAME, "FLOAT32");
        datatype.put(DataTypeMetadata::PG_DATA_TYPE, 0);
        datatypes.push_back(std::make_pair("", datatype));

        // FLOAT64
        datatype.put(DataTypeMetadata::ID, 5);
        datatype.put(DataTypeMetadata::NAME, "FLOAT64");
        datatype.put(DataTypeMetadata::PG_DATA_TYPE, 0);
        datatypes.push_back(std::make_pair("", datatype));

        // TEXT
        datatype.put(DataTypeMetadata::ID, 6);
        datatype.put(DataTypeMetadata::NAME, "TEXT");
        datatype.put(DataTypeMetadata::PG_DATA_TYPE, 0);
        datatypes.push_back(std::make_pair("", datatype));
    }
    root.add_child(DataTypeMetadata::DATATYPES_NODE, datatypes);

    error = DataTypeMetadata::save("", root);
    if (error != ErrorCode::OK) {
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
ErrorCode DataTypeMetadata::load(
    std::string_view database, boost::property_tree::ptree& pt, const GenerationType generation)
{
    return Metadata::load(database, DataTypeMetadata::TABLE_NAME, pt, generation);
}

/**
 *  @brief  Save the metadta to metadta-table.
 *  @param  (database)   [in]  database name.
 *  @param  (pt)         [in]  property_tree object that stores metadata to be saved.
 *  @param  (generation) [out] the generation of saved metadata.
 */
ErrorCode DataTypeMetadata::save(
    std::string_view database, boost::property_tree::ptree& pt, GenerationType* generation)
{
    return Metadata::save(database, DataTypeMetadata::TABLE_NAME, pt, generation);
}

} // namespace manager::metadata_manager

