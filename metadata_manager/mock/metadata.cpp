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

#include "error_code.h"
#include "metadata.h"

using namespace boost::property_tree;

namespace manager::metadata_manager {

/**
 *  @brief  Load metadata from metadata-table.
 *  @param  (database)  [in]  database name.
 *  @param  (tablename) [in]  metadata-table name.
 *  @param  (pt)        [out] property_tree object to populating metadata.
 *  @param  (version)   [in]  metadata version to load. load latest version if NOT provided.
 *  @return ErrorCode::OK if success, otherwise an error code.
 */
ErrorCode Metadata::load(
    __attribute__((unused)) std::string_view database, std::string_view tablename,
    boost::property_tree::ptree& pt, __attribute__((unused)) const uint64_t version)
{
    ErrorCode error = ErrorCode::UNKNOWN;
    std::string filename{tablename};
    
    try {
        read_json(filename, pt);
        error = ErrorCode::OK;
    } catch (boost::property_tree::json_parser_error& e) {
        std::wcout << "read_json() error. " << e.what() << std::endl;
    } catch (...) {
        std::cout << "read_json() error." << std::endl;
        return error;
    }

    return error;
}

/**
 *  @brief  Save the metadta to metadta-table.
 *  @param  (database) [in]  database name.
 *  @param  (tablename) [in]  metadata-table name.
 *  @param  (pt)       [in]  property_tree object that stores metadata to be saved.
 *  @param  (version)  [out] the version of saved metadata.
 */
ErrorCode Metadata::save(
    __attribute__((unused)) std::string_view database, std::string_view tablename, 
    boost::property_tree::ptree& pt, __attribute__((unused)) uint64_t* version)
{
    ErrorCode error = ErrorCode::UNKNOWN;
    std::string filename{tablename};

    try {
        write_json(filename, pt);
        error = ErrorCode::OK;
    } 
    catch (...) {
        std::cout << "write_json() error." << std::endl;
    }

    return error;
}

/**
 *  @brief  Read latest table-metadata from metadata-table.
 *  @return ErrorCode::OK if success, otherwise an error code.
 */
ErrorCode Metadata::load()
{
    return this->load(Metadata::LATEST_VERSION);
}

/**
 *  @brief  Read table-metadata which specific version from metadata-table.
 *  @param  (version)   [in]  metadata version to read. 
 *  @return ErrorCode::OK if success, otherwise an error code.
 */
ErrorCode Metadata::load(const uint64_t version)
{
    return Metadata::load(this->database(), this->tablename(), this->metadata_, version);
}

/**
 *  @brief  Add metadata-object to metadata-table.
 *  @param  (pt)        [in]  metadata-object to add.
 *  @return ErrorCode::OK if success, otherwise an error code.
 */
ErrorCode Metadata::add(boost::property_tree::ptree pt)
{
    return this->add(pt, nullptr);
}

/**
 *  @brief  Add metadata-object to metadata-table.
 *  @param  (pt)        [in]  metadata-object to add.
 *  @param  (table_id)  [out] ID of the added metadata-object.
 *  @return ErrorCode::OK if success, otherwise an error code.
 */
ErrorCode Metadata::add(boost::property_tree::ptree pt, uint64_t* table_id)
{
    ErrorCode error = ErrorCode::UNKNOWN;

    ptree child;

    // re-create child tree.
    BOOST_FOREACH(const ptree::value_type& e, this->metadata_.get_child(this->root_node())) {
        const ptree& e_ptree = e.second;
        child.push_back(std::make_pair("", e_ptree));
    }
    // add new element.
    child.push_back(std::make_pair("", pt));
    this->metadata_.put_child(root_node(), child);

    Metadata::save(this->database(), this->tablename(), this->metadata_);

    if (table_id != nullptr) {
        *table_id = 1;
    }

    error = ErrorCode::OK;

    return error;    
}

/**
 *  @brief  Get next metadata-object.
 *  @param  (pt) [out] property_tree object to populating metadata.
 *  @return ErrorCode::OK if success, otherwise an error code.
 *  @note   Return ErrorCode::END_OF_ROW if there is no more data to read.
 */
ErrorCode Metadata::next(boost::property_tree::ptree& pt)
{
    ErrorCode error = ErrorCode::UNKNOWN;

    if (!this->object_queue_.empty()) {
        this->object_queue_.pop();
        if (!object_queue_.empty()) {
            pt = object_queue_.front();
            error = ErrorCode::OK;
        } else {
            error = ErrorCode::END_OF_ROW;
        }
    } else {
        // create metadata-object queue
        BOOST_FOREACH (const ptree::value_type& e, this->metadata_.get_child(root_node())) {
            const ptree e_ptree = e.second;
            this->object_queue_.push(e_ptree);
        }
        if (!this->object_queue_.empty()) {
            pt = this->object_queue_.front();
            error = ErrorCode::OK;
        } else {
            error = ErrorCode::END_OF_ROW;
        }
    }

    return error;
}

} // namespace manager::metadata_manager
