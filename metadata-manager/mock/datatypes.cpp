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
#include <boost/foreach.hpp>
#include <boost/property_tree/ptree.hpp>
#include <boost/property_tree/json_parser.hpp>

#include "manager/metadata/error_code.h"
#include "manager/metadata/metadata.h"
#include "manager/metadata/datatypes.h"

using namespace boost::property_tree;

namespace manager::metadata {
/*
 *  @biref  initialization of DataType Metadata.
 */
ErrorCode DataTypes::init() 
{
    ErrorCode error = ErrorCode::UNKNOWN;

    ptree root;
    Metadata::init(root);
    
    ptree datatypes;
    {
        ptree datatype;
        uint64_t id = 0;

        // INT : smallint
        datatype.put(DataTypes::ID, ++id);
        datatype.put(DataTypes::NAME, "INT");
        datatype.put(DataTypes::PG_DATA_TYPE, 0);
        datatype.put(DataTypes::PG_DATA_TYPE_NAME, "smallint");
        datatypes.push_back(std::make_pair("", datatype));

        // INT16
        datatype.put(DataTypes::ID, ++id);
        datatype.put(DataTypes::NAME, "INT16");
        datatype.put(DataTypes::PG_DATA_TYPE, 0);
        datatype.put(DataTypes::PG_DATA_TYPE_NAME, "");
        datatypes.push_back(std::make_pair("", datatype));

        // INT : integer
        datatype.put(DataTypes::ID, ++id);
        datatype.put(DataTypes::NAME, "INT");
        datatype.put(DataTypes::PG_DATA_TYPE, 0);
        datatype.put(DataTypes::PG_DATA_TYPE_NAME, "integer");
        datatypes.push_back(std::make_pair("", datatype));

        // INT32 : 
        datatype.put(DataTypes::ID, ++id);
        datatype.put(DataTypes::NAME, "INT32");
        datatype.put(DataTypes::PG_DATA_TYPE, 0);
        datatype.put(DataTypes::PG_DATA_TYPE_NAME, "");
        datatypes.push_back(std::make_pair("", datatype));

        // BIGINT: bigint
        datatype.put(DataTypes::ID, ++id);
        datatype.put(DataTypes::NAME, "BIGINT");
        datatype.put(DataTypes::PG_DATA_TYPE, 0);
        datatype.put(DataTypes::PG_DATA_TYPE_NAME, "bigint");
        datatypes.push_back(std::make_pair("", datatype));

        // INT64 :
        datatype.put(DataTypes::ID, ++id);
        datatype.put(DataTypes::NAME, "INT64");
        datatype.put(DataTypes::PG_DATA_TYPE, 0);
        datatype.put(DataTypes::PG_DATA_TYPE_NAME, "");
        datatypes.push_back(std::make_pair("", datatype));

        // FLOAT : real 
        datatype.put(DataTypes::ID, ++id);
        datatype.put(DataTypes::NAME, "FLOAT");
        datatype.put(DataTypes::PG_DATA_TYPE, 0);
        datatype.put(DataTypes::PG_DATA_TYPE_NAME, "real");
        datatypes.push_back(std::make_pair("", datatype));

        // FLOAT32 : 
        datatype.put(DataTypes::ID, ++id);
        datatype.put(DataTypes::NAME, "FLOAT32");
        datatype.put(DataTypes::PG_DATA_TYPE, 0);
        datatype.put(DataTypes::PG_DATA_TYPE_NAME, "");
        datatypes.push_back(std::make_pair("", datatype));

        // FLOAT64 :  
        datatype.put(DataTypes::ID, ++id);
        datatype.put(DataTypes::NAME, "FLOAT64");
        datatype.put(DataTypes::PG_DATA_TYPE, 0);
        datatype.put(DataTypes::PG_DATA_TYPE_NAME, "");
        datatypes.push_back(std::make_pair("", datatype));

        // DOUBLE : double precision 
        datatype.put(DataTypes::ID, ++id);
        datatype.put(DataTypes::NAME, "DOUBLE");
        datatype.put(DataTypes::PG_DATA_TYPE, 0);
        datatype.put(DataTypes::PG_DATA_TYPE_NAME, "double precision");
        datatypes.push_back(std::make_pair("", datatype));

        // TEXT : 
        datatype.put(DataTypes::ID, ++id);
        datatype.put(DataTypes::NAME, "TEXT");
        datatype.put(DataTypes::PG_DATA_TYPE, 0);
        datatype.put(DataTypes::PG_DATA_TYPE_NAME, "");
        datatypes.push_back(std::make_pair("", datatype));

        // STRING : text
        datatype.put(DataTypes::ID, ++id);
        datatype.put(DataTypes::NAME, "STRING");
        datatype.put(DataTypes::PG_DATA_TYPE, 0);
        datatype.put(DataTypes::PG_DATA_TYPE_NAME, "text");
        datatypes.push_back(std::make_pair("", datatype));

        // CHAR : character, char
        datatype.put(DataTypes::ID, ++id);
        datatype.put(DataTypes::NAME, "CHAR");
        datatype.put(DataTypes::PG_DATA_TYPE, 0);
        datatype.put(DataTypes::PG_DATA_TYPE_NAME, "char");
        datatypes.push_back(std::make_pair("", datatype));

        // VARCHAR : character varying, varchar
        datatype.put(DataTypes::ID, ++id);
        datatype.put(DataTypes::NAME, "VARCHAR");
        datatype.put(DataTypes::PG_DATA_TYPE, 0);
        datatype.put(DataTypes::PG_DATA_TYPE_NAME, "varchar");
        datatypes.push_back(std::make_pair("", datatype));
    }
    root.add_child(DataTypes::DATATYPES_NODE, datatypes);

    error = DataTypes::save("", root);
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
ErrorCode DataTypes::load(
    std::string_view database, boost::property_tree::ptree& pt, const GenerationType generation)
{
    return Metadata::load(database, DataTypes::TABLE_NAME, pt, generation);
}

/**
 *  @brief  Save the metadta to metadta-table.
 *  @param  (database)   [in]  database name.
 *  @param  (pt)         [in]  property_tree object that stores metadata to be saved.
 *  @param  (generation) [out] the generation of saved metadata.
 */
ErrorCode DataTypes::save(
    std::string_view database, boost::property_tree::ptree& pt, GenerationType* generation)
{
    return Metadata::save(database, DataTypes::TABLE_NAME, pt, generation);
}

} // namespace manager::metadata

