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

        // INT32 :
        datatype.put(DataTypes::ID, 4);
        datatype.put(DataTypes::NAME, "INT32");
        datatype.put(DataTypes::PG_DATA_TYPE, 23);
        datatype.put(DataTypes::PG_DATA_TYPE_NAME, "integer");
        datatype.put(DataTypes::PG_DATA_TYPE_QUALIFIED_NAME, "int4");
        datatypes.push_back(std::make_pair("", datatype));

        // INT64 :
        datatype.put(DataTypes::ID, 6);
        datatype.put(DataTypes::NAME, "INT64");
        datatype.put(DataTypes::PG_DATA_TYPE, 20);
        datatype.put(DataTypes::PG_DATA_TYPE_NAME, "bigint");
        datatype.put(DataTypes::PG_DATA_TYPE_QUALIFIED_NAME, "int8");
        datatypes.push_back(std::make_pair("", datatype));

        // FLOAT32 :
        datatype.put(DataTypes::ID, 8);
        datatype.put(DataTypes::NAME, "FLOAT32");
        datatype.put(DataTypes::PG_DATA_TYPE, 700);
        datatype.put(DataTypes::PG_DATA_TYPE_NAME, "real");
        datatype.put(DataTypes::PG_DATA_TYPE_QUALIFIED_NAME, "float4");
        datatypes.push_back(std::make_pair("", datatype));

        // FLOAT64 :
        datatype.put(DataTypes::ID, 9);
        datatype.put(DataTypes::NAME, "FLOAT64");
        datatype.put(DataTypes::PG_DATA_TYPE, 701);
        datatype.put(DataTypes::PG_DATA_TYPE_NAME, "double precision");
        datatype.put(DataTypes::PG_DATA_TYPE_QUALIFIED_NAME, "float8");
        datatypes.push_back(std::make_pair("", datatype));

        // CHAR : character, char
        datatype.put(DataTypes::ID, 13);
        datatype.put(DataTypes::NAME, "CHAR");
        datatype.put(DataTypes::PG_DATA_TYPE, 1042);
        datatype.put(DataTypes::PG_DATA_TYPE_NAME, "char");
        datatype.put(DataTypes::PG_DATA_TYPE_QUALIFIED_NAME, "bpchar");
        datatypes.push_back(std::make_pair("", datatype));

        // VARCHAR : character varying, varchar
        datatype.put(DataTypes::ID, 14);
        datatype.put(DataTypes::NAME, "VARCHAR");
        datatype.put(DataTypes::PG_DATA_TYPE, 1043);
        datatype.put(DataTypes::PG_DATA_TYPE_NAME, "varchar");
        datatype.put(DataTypes::PG_DATA_TYPE_QUALIFIED_NAME, "varchar");
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
