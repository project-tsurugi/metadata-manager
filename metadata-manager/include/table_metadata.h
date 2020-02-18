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
        static const char * TABLES_NODE;

        // table metadata-object.
        // ID is defined in base class.
        // NAME is defined in base class.
        static const char * NAMESPACE;
        static const char * COLUMNS_NODE;
        static const char * PRIMARY_INDEX_OBJECT;
        static const char * SECONDARY_INDICES_NODDE;
        static const char * CONSTRAINTS_NODE;
      
        // column metadata-object.
        struct Column {
            static const char * ID;
            static const char * TABLE_ID;
            static const char * NAME;
            static const char * ORDINAL_POSITION;
            static const char * DATA_TYPE_ID;
            static const char * DATA_LENGTH;
            static const char * NULLABLE;
            static const char * CONSTRAINTS_NODE;
        };

        // constraint metadata-object.
        struct Constraint {
            static const char * ID;
            static const char * TABLE_ID;
            static const char * COLUMN_KEY_NODE;
            static const char * NAME;
            static const char * TYPE;       
            static const char * CONTENTS;
            struct Type {
                static const char * CHECK;
                static const char * FOREIGN_KEY;
                static const char * PRIMARY_KEY;
                static const char * UNIQUE;
            };
        };

        // Index metadata-object.
        struct Index {
            static const char * NAME;
            static const char * COLUMN_OBJECT;
            // Index-Column metadata-object.
            struct Column {
                static const char * NAME;
                static const char * DIRECTION;
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

        TableMetadata(const TableMetadata&) = delete;
        TableMetadata& operator=(const TableMetadata&) = delete;

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
        static const char * TABLE_NAME;

        void fill_constraint(boost::property_tree::ptree& constraint, const boost::property_tree::ptree& table);
};

} // namespace manager::metadata_manager

#endif // MANAGER_TABLE_METADATA_H_
