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
#ifndef MANAGER_DATATYPE_METADATA
#define MANAGER_DATATYPE_METADATA

#include <string>
#include <string_view>
#include <boost/property_tree/ptree.hpp>

#include "error_code.h"
#include "metadata.h"

namespace manager::metadata_manager {

class DatatypeMetadata : public Metadata {
    public:
        static constexpr char const * DATATYPES_NODE = "datatypes";

        static ErrorCode init();

        /**
         *  @brief  Load metadata from metadata-table.
         *  @param  (database)   [in]  database name
         *  @param  (pt)         [out] property_tree object to populating metadata.
         *  @param  (generation) [in]  metadata generation to load. load latest generation if NOT provided.
         *  @return ErrorCode::OK if success, otherwise an error code.
         */
        static ErrorCode load(
            std::string_view database, boost::property_tree::ptree& pt, 
            const GenerationType generation = LATEST_GENERATION);

        /**
         *  @brief  Save the metadta to metadta-table.
         *  @param  (database)   [in]  database name.
         *  @param  (pt)         [in]  property_tree object that stores metadata to be saved.
         *  @param  (generation) [out] the generation of saved metadata.
         */
        static ErrorCode save(
            std::string_view database, boost::property_tree::ptree& pt, 
            GenerationType* generation = nullptr);

        /**
         *  @brief  Constructor
         *  @param  (database) [in]  database name.
         *  @return none.
         */
        DatatypeMetadata(std::string_view database, std::string_view component = "visitor") 
            : Metadata(database, component) {}

    protected:
        // functions for template-method
        std::string_view tablename() const { return TABLE_NAME; }
        const std::string root_node() const { return DATATYPES_NODE; }
        uint64_t generate_object_id() const { 
            static ObjectIdType datatype_id = 0; 
            return ++datatype_id; 
        }
        ErrorCode fill_parameters( __attribute__((unused)) boost::property_tree::ptree& object) { 
            return ErrorCode::OK; 
        }

    private:
        static constexpr char const * TABLE_NAME = "datatypes";
};

}

#endif // MANAGER_DATATYPE_METADATA
