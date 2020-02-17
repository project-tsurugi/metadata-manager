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
        // root object.
        static constexpr char const * TABLES_NODE = "tables";

        // table metadata-object.
        // ID is defined in base class.
        // NAME is defined in base class.
        static constexpr char const * NAMESPACE                 = "namespace";
        static constexpr char const * COLUMNS_NODE              = "columns";
        static constexpr char const * PRIMARY_INDEX_OBJECT      = "primaryIndex";
        static constexpr char const * SECONDARY_INDICES_NODDE   = "secondaryIndices";
        static constexpr char const * CONSTRAINTS_NODE          = "constraints";
      
        // column metadata-object.
        struct Column {
            static constexpr char const * ID                 = "id";
            static constexpr char const * TABLE_ID           = "tableId";
            static constexpr char const * NAME               = "name";
            static constexpr char const * ORDINAL_POSITION   = "ordinalPosition";
            static constexpr char const * DATA_TYPE_ID       = "dataTypeId";
            static constexpr char const * DATA_LENGTH        = "dataLength";
            static constexpr char const * NULLABLE           = "nullable";
            static constexpr char const * CONSTRAINS_NODE    = "constraints";
        };

        // constraint metadata-object.
        struct Constraint {
            static constexpr char const * ID                 = "id";
            static constexpr char const * TABLE_ID           = "tableId";
            static constexpr char const * COLUMN_KEY_NODE    = "columnKey";
            static constexpr char const * NAME               = "name";
            static constexpr char const * TYPE               = "type";       
            static constexpr char const * CONTENTS           = "contents";
            struct Type {
                static constexpr char const * CHECK         = "C";
                static constexpr char const * FOREIGN_KEY   = "F";
                static constexpr char const * PRIMARY_KEY   = "P";
                static constexpr char const * UNIQUE        = "U";
            };
        };

        // Index metadata-object.
        struct Index {
            static constexpr char const * NAME            = "name";
            static constexpr char const * COLUMN_OBJECT   = "column";
            // Index-Column metadata-object.
            struct Column {
                static constexpr char const * NAME         = "name";
                static constexpr char const * DIRECTION     = "direction";
            };
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
            const GenerationType Generation = LATEST_GENERATION);

        /**
         *  @brief  Constructor
         *  @param  (database) [in]  database name.
         *  @return none.
         */
        TableMetadata(std::string_view database, std::string_view component = "visitor") 
            : Metadata(database, component) {}

    protected:
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

        // functions for template-method
        std::string_view table_name() const { return TABLE_NAME; }
        const std::string root_node() const { return TABLES_NODE; }
        ObjectIdType generate_object_id() const;
        ErrorCode fill_parameters(boost::property_tree::ptree& object);

    private:
        static constexpr char const * TABLE_NAME = "tables";
};

} // namespace manager::metadata_manager

#endif // MANAGER_TABLE_METADATA_H_
