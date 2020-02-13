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
#include <boost/foreach.hpp>
#include <boost/property_tree/ptree.hpp>
#include <boost/property_tree/json_parser.hpp>

#include "error_code.h"
#include "metadata.h"
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
ErrorCode DatatypeMetadata::load(
    std::string_view database, boost::property_tree::ptree& pt, const uint64_t generation)
{
    return Metadata::load(database, DatatypeMetadata::TABLE_NAME, pt, generation);
}

/**
 *  @brief  Save the metadta to metadta-table.
 *  @param  (database)   [in]  database name.
 *  @param  (pt)         [in]  property_tree object that stores metadata to be saved.
 *  @param  (generation) [out] the generation of saved metadata.
 */
ErrorCode DatatypeMetadata::save(
    std::string_view database, boost::property_tree::ptree& pt, uint64_t* generation)
{
    return Metadata::save(database, DatatypeMetadata::TABLE_NAME, pt, generation);
}

} // namespace manager::metadata_manager

