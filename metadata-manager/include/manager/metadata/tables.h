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
#pragma once

#include <string>
#include <string_view>
#include <boost/property_tree/ptree.hpp>

#include "manager/metadata/error_code.h"
#include "manager/metadata/metadata.h"

namespace manager::metadata {
class Tables : public Metadata {
    public:
        // root node.
        static constexpr const char* const TABLES_NODE = "tables";

        // table metadata-object.
        // ID is defined in base class.
        // NAME is defined in base class.
        static constexpr const char* const NAMESPACE         = "namespace";
        static constexpr const char* const COLUMNS_NODE      = "columns";
        static constexpr const char* const PRIMARY_KEY_NODE  = "primaryKey";

        // column metadata-object.
        struct Column {
            static constexpr const char* const ID                = "id";
            static constexpr const char* const TABLE_ID          = "tableId";
            static constexpr const char* const NAME              = "name";
            static constexpr const char* const ORDINAL_POSITION  = "ordinalPosition";
            static constexpr const char* const DATA_TYPE_ID      = "dataTypeId";
            static constexpr const char* const DATA_LENGTH       = "dataLength";
            static constexpr const char* const VARYING           = "varying";
            static constexpr const char* const NULLABLE          = "nullable";
            static constexpr const char* const DEFAULT           = "default";
            static constexpr const char* const DIRECTION         = "direction";
        };

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
            const GenerationType generation = LATEST_VERSION);

        /**
         *  @brief  Save the metadta to metadta-table.
         *  @param  (database)   [in]  database name.
         *  @param  (pt)         [in]  property_tree object that stores metadata to be saved.
         *  @param  (generation) [out] the generation of saved metadata.
         *  @return ErrorCode::OK if success, otherwise an error code.
         */
        static ErrorCode save(
            std::string_view database, boost::property_tree::ptree& pt, 
            GenerationType* generation = nullptr);

        /**
         *  @brief  Constructor
         *  @param  (database) [in]  database name.
         *  @return none.
         */
        Tables(std::string_view database, std::string_view component = "visitor") 
            : Metadata(database, component) { init(); }

        Tables(const Tables&) = delete;
        Tables& operator=(const Tables&) = delete;

    protected:
        // functions for template-method
        std::string_view table_name() const { return TABLE_NAME; }
        const std::string root_node() const { return TABLES_NODE; }
        ObjectIdType generate_object_id() const;
        ErrorCode fill_parameters(boost::property_tree::ptree& object);

    private:
        static constexpr const char* const TABLE_NAME = "tables";
};  // class Tables

} // namespace manager::metadata
