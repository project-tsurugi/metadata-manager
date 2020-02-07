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
#ifndef METADATA_H_
#define METADATA_H_

#include <string>
#include <string_view>
#include <queue>
#include <boost/property_tree/ptree.hpp>

#include "error_code.h"

namespace management::metadata {

class Metadata {
    public:
        /**
         *  @brief  Constructor
         *  @param  (database)  [in]  database name.
         *  @param  (component) [in]  your component name.
         *  @return none.
         */
        Metadata(std::string_view database, std::string_view component) 
            : database_(database), component_(component) {}

        std::string_view database() const { return database_; }
        std::string_view component() const { return component_; }
        uint64_t version() const { return version_; }

        /**
         *  @brief  Load the latest metadata from metadata-table.
         *  @return ErrorCode::OK if success, otherwise an error code.
         */
        ErrorCode load();

        /**
         *  @brief  Loads the metadata which specific version from metadata-table.
         *  @param  (version) [in]  metadata version to load. 
         *  @return ErrorCode::OK if success, otherwise an error code.
         */
        ErrorCode load(uint64_t version);

        /**
         *  @brief  Add metadata-object to metadata-table.
         *  @param  (pt) [in]  metadata-object to add.
         *  @return ErrorCode::OK if success, otherwise an error code.
         */
        ErrorCode add(boost::property_tree::ptree pt);

        /**
         *  @brief  Add metadata-object to metadata-table.
         *  @param  (pt)          [in]  metadata-object to add.
         *  @param  (metadata_id) [out] ID of the added metadata-object.
         *  @return ErrorCode::OK if success, otherwise an error code.
         */
        ErrorCode add(boost::property_tree::ptree pt, uint64_t* metadata_id);
#if 0
        /**
         *  @brief  Get metadata-object.
         *  @param  (metadata_id) [in]  metadata-object ID.
         *  @param  (pt)          [out] property_tree object to populating metadata.
         *  @return ErrorCode::OK if success, otherwise an error code.
         */
        virtual ErrorCode get(const uint64_t metadata_id, boost::property_tree::ptree& pt) const = 0;

        /**
         *  @brief  Get metadata-object.
         *  @param  (name)  [in]  name of metadata-object. (Value of "name" key.)
         *  @param  (pt)    [out] Reference of property_tree object.
         *  @return ErrorCode::OK if success, otherwise an error code.
         */
        virtual ErrorCode get(std::vector<std::string> name, boost::property_tree::ptree& pt) const = 0;

        /**
         *  @brief  Set metadata-object to metadata-table.
         *  @param  (metadata_id)   [in]  metadata-object ID.
         *  @param  (pt)            [in]  property_tree object containing metadata.
         *  @return ErrorCode::OK if success, otherwise an error code.
         *  @note   Return ErrorCode::ID_NOT_FOUND, if table_id NOT found.
         */
        virtual ErrorCode set(const uint64_t metadata_id, boost::property_tree::ptree pt) = 0;

        /**
         *  @brief  Set metadata-object to metadata-table.
         *  @param  (name)  [in]  name of metadata-object. (Value of "name" key.)
         *  @param  (pt)    [in]  property_tree object containing metadata.
         *  @return ErrorCode::OK if success, otherwise an error code.
         *  @note   Return ErrorCode::ID_NOT_FOUND, if name NOT found.
         */
        virtual ErrorCode set(const std::string_view name, boost::property_tree::ptree pt) = 0;

        /**
         *  @brief  Remove metadata-object from metadata-table.
         *  @param  [in] metadata-object ID.
         *  @return ErrorCode::OK if success, otherwise an error code.
         */
        virtual ErrorCode remove(const uint64_t metadata_id) = 0;

        /**
         *  @brief  Remove metadata-object from metadata-table.
         *  @param  [in] name of metadata-object. (Value of "name" key.)
         *  @return ErrorCode::OK if success, otherwise an error code.
         */
        virtual ErrorCode remove(std::string_view name) = 0;
#endif
        /**
         *  @brief  Get next metadata-object.
         *  @param  (pt) [out] property_tree object to populating metadata.
         *  @return ErrorCode::OK if success, otherwise an error code.
         *  @note   Return ErrorCode::END_OF_ROW if there is no more data to read.
         */
        ErrorCode next(boost::property_tree::ptree& pt);
       
    protected:
        /**
         *  @brief  Load metadata from metadata-table.
         *  @param  (database)  [in]  database name.
         *  @param  (tablename) [in]  metadata-table name.
         *  @param  (pt)        [out] property_tree object to populating metadata.
         *  @param  (version)   [in]  metadata version to load. load latest version if NOT provided.
         *  @return ErrorCode::OK if success, otherwise an error code.
         */
        static ErrorCode load(
            std::string_view database, std::string_view tablename,
            boost::property_tree::ptree& pt, const uint64_t version = LATEST_VERSION);

        /**
         *  @brief  Save the metadta to metadta-table.
         *  @param  (database)  [in]  database name.
         *  @param  (tablename) [in]  metadata-table name.
         *  @param  (pt)        [in]  property_tree object that stores metadata to be saved.
         *  @param  (version)   [out] the version of saved metadata.
         */
        static ErrorCode save(
            std::string_view database, std::string_view tablename, boost::property_tree::ptree& pt, 
            uint64_t* version = nullptr);


        static const uint64_t LATEST_VERSION = 0;
        boost::property_tree::ptree prop_tree_;

        virtual std::string_view tablename() const = 0;
        virtual const std::string first_node() const = 0;

    private:
        std::string database_;
        std::string component_;
        uint64_t version_;
        std::queue<boost::property_tree::ptree> table_queue_;
};

} // namespace management::metadata


#endif // METADATA_H_
