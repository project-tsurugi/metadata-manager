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
#include <string>
#include <string_view>
#include <boost/property_tree/ptree.hpp>
#include <boost/property_tree/json_parser.hpp>

#include "error_code.h"
#include "metadata.h"

namespace management::metadata {

/**
 *  @brief  Load metadata from metadata-table.
 *  @param  (database)      [in]  database name
 *  @param  (MetadataClass) [in]  classification of metadata-table to load.
 *  @param  (pt)            [out] property_tree object for storing metadata.
 *  @param  (version)       [in]  metadata version to load. load latest version if NOT provided.
 *  @return ErrorCode::OK if success, otherwise an error code.
 */
ErrorCode Metadata::load(
    __attribute__((unused))  std::string_view database, MetadataClass MetadataClass, 
    boost::property_tree::ptree& pt, __attribute__((unused))  const uint64_t version)
{
    ErrorCode error = ErrorCode::UNKNOWN;
    std::string file;

    switch (MetadataClass) {
        case MetadataClass::TABLE:
            file = FILE_NAME;
            break;
        default:
            file.clear();
            break;
    }

    if (!file.empty()) {
        try {
            read_json(file, pt);
        } catch (boost::property_tree::json_parser_error& e) {
            std::wcout << "read_json() error. " << e.what() << std::endl;
        } catch (...) {
            std::cout << "read_json() error." << std::endl;
            return error;
        }
        error = ErrorCode::OK;
    } else {
        error = ErrorCode::NOT_FOUND;
    }

    return error;
}

/**
 *  @brief  Save metadta to metadta-table.
 *  @param  (database)      [in]  database name.
 *  @param  (MetadataClass) [in]  classification of metadata-table to save.
 *  @param  (pt)            [in]  property_tree object that stores metadata to be saved.
 */
ErrorCode Metadata::save(
    __attribute__((unused)) std::string_view database, MetadataClass MetadataClass, 
    boost::property_tree::ptree& pt)
{
    ErrorCode error = ErrorCode::UNKNOWN;
    std::string file;

    switch (MetadataClass) {
        case MetadataClass::TABLE:
            file = FILE_NAME;
            break;
        default:
            error = ErrorCode::NOT_FOUND;
            break;
    }

    if (!file.empty()) {
        try {
            write_json(file, pt);
        } 
        catch (...) {
            std::cout << "write_json() error." << std::endl;
            return error;
        }
        error = ErrorCode::OK;
    }

    return error;
}

} // namespace management::metadata
