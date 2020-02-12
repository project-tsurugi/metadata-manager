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
#ifndef MANAGER_TABLE_METADATA_H_
#define MANAGER_TABLE_METADATA_H_

#include <string>
#include <string_view>
#include <boost/property_tree/ptree.hpp>

#include "error_code.h"
#include "metadata.h"

namespace manager::metadata_manager {

class TableMetadata : public Metadata {
    public:
        static constexpr char const * TABLES_NODE = "tables";   
        static constexpr char const * COLUMNS_NODE = "columns";
        static constexpr char const * CONSTRAINTS_NODE = "constraints";

        /**
         *  @brief  Load metadata from metadata-table.
         *  @param  (database) [in]  database name
         *  @param  (pt)       [out] property_tree object to populating metadata.
         *  @param  (version)  [in]  metadata version to load. load latest version if NOT provided.
         *  @return ErrorCode::OK if success, otherwise an error code.
         */
        static ErrorCode load(
            std::string_view database, boost::property_tree::ptree& pt, 
            const uint64_t version = LATEST_VERSION);

        /**
         *  @brief  Save the metadta to metadta-table.
         *  @param  (database) [in]  database name.
         *  @param  (pt)       [in]  property_tree object that stores metadata to be saved.
         *  @param  (version)  [out] the version of saved metadata.
         */
        static ErrorCode save(
            std::string_view database, boost::property_tree::ptree& pt, 
            uint64_t* version = nullptr);

        /**
         *  @brief  Constructor
         *  @param  (database) [in]  database name.
         *  @return none.
         */
        TableMetadata(std::string_view database, std::string_view component = "visitor") 
            : Metadata(database, component) {}

    protected:
        // functions for template-method
        std::string_view tablename() const { return TABLE_NAME; }
        const std::string root_node() const { return TABLES_NODE; }

    private:
        static constexpr char const * TABLE_NAME = "tables.json";
};

}

#endif // MANAGER_TABLE_METADATA_H_
