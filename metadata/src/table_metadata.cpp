/*
 * Copyright 2019-2020 tsurugi project.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
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

#include <iostream>
#include <queue>

#include <boost/foreach.hpp>
#include <boost/property_tree/ptree.hpp>
#include <boost/property_tree/json_parser.hpp>
#include <boost/property_tree/ini_parser.hpp>

#include "error_code.h"
#include "table_metadata.h"

using namespace boost::property_tree;

namespace management::metadata {

const char* const TableMetadata::ROOT_NODE = "";
const char* const TableMetadata::TABLES_NODE = "tables";
const char* const TableMetadata::COLUMNS_NODE = "tables.columns";
const char* const TableMetadata::COLUMN_CONSTRAINTS_NODE = "tables.columns.constraints";
const char* const TableMetadata::TABLE_CONSTRAINTS_NODE = "tables.constraints";

/**
 *  @brief  Read latest table-metadata from metadata-table.
 *  @return ErrorCode::OK if success, otherwise an error code.
 */
ErrorCode TableMetadata::load()
{
    return this->load(Metadata::LATEST_VERSION);
}

/**
 *  @brief  Read table-metadata which specific version from metadata-table.
 *  @param  (version)   [in]  metadata version to read. 
 *  @return ErrorCode::OK if success, otherwise an error code.
 */
ErrorCode TableMetadata::load(const uint64_t version)
{
    return Metadata::load(this->database(), MetadataClass::TABLE, this->prop_tree_, version);
}

/**
 *  @brief  Add table-object to metadata-table.
 *  @param  (pt)        [in]  table-object to add.
 *  @return ErrorCode::OK if success, otherwise an error code.
 */
ErrorCode TableMetadata::add(boost::property_tree::ptree pt)
{
    return this->add(pt, nullptr);
}

/**
 *  @brief  Add table-object to metadata-table.
 *  @param  (pt)        [in]  table-object to add.
 *  @param  (table_id)  [out] ID of the added table-object.
 *  @return ErrorCode::OK if success, otherwise an error code.
 */
ErrorCode TableMetadata::add(boost::property_tree::ptree pt, uint64_t* table_id)
{
    ErrorCode error = ErrorCode::UNKNOWN;

    ptree child;

    // re-create child tree.
    BOOST_FOREACH(auto e, this->prop_tree_.get_child(TABLES_NODE)) {
        const ptree& table = e.second;
        child.push_back(std::make_pair("", table));
    }
    // add new element.
    child.push_back(std::make_pair("", pt));
    this->prop_tree_.put_child(TABLES_NODE, child);

    write_json(FILE_NAME, this->prop_tree_);

    if (table_id != nullptr) {
        *table_id = 1;
    }

    error = ErrorCode::OK;

    return error;    
}

/**
 *  @brief  Get next table-object.
 *  @param  (pt) [out] property_tree object to populating metadata.
 *  @return ErrorCode::OK if success, otherwise an error code.
 *  @note   Return ErrorCode::END_OF_ROW if there is no more data to read.
 */
ErrorCode TableMetadata::next(boost::property_tree::ptree& pt) const
{
    ErrorCode error = ErrorCode::UNKNOWN;
    static std::queue<boost::property_tree::ptree> table_queue;

    if (!table_queue.empty()) {
        table_queue.pop();
        if (!table_queue.empty()) {
            pt = table_queue.front();
            error = ErrorCode::OK;
        } else {
            error = ErrorCode::END_OF_ROW;
        }
    } else {
        // create table-object queue
        BOOST_FOREACH (const ptree::value_type& e, this->prop_tree_.get_child(TABLES_NODE)) {
            ptree table = e.second;
            table_queue.push(table);
        }
    }

    return error;
}

} // namespace management::metadata
