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

#ifndef TABLE_METADATA_H_
#define TABLE_METADATA_H_

#include <string_view>
#include <boost/property_tree/ptree.hpp>

#include "error_code.h"
#include "metadata.h"

namespace management::metadata {

class TableMetadata : public Metadata {
    public:
        static const char* const ROOT_NODE;
        static const char* const TABLES_NODE;
        static const char* const COLUMNS_NODE;
        static const char* const COLUMN_CONSTRAINTS_NODE;
        static const char* const TABLE_CONSTRAINTS_NODE;

        /**
         *  @brief  Constructor for creating a new object.
         *  @param  (database) [in]  database name.
         *  @return none.
         */
        TableMetadata(std::string_view database, std::string_view component = "visitor") 
            : Metadata(database, component) {}

        /**
         *  @brief  Read latest table-metadata from metadata-table.
         *  @return ErrorCode::OK if success, otherwise an error code.
         */
        ErrorCode load();

        /**
         *  @brief  Read table-metadata which specific version from metadata-table.
         *  @param  (version)   [in]  metadata version to read. 
         *  @return ErrorCode::OK if success, otherwise an error code.
         */
        ErrorCode load(uint64_t version);

        /**
         *  @brief  Add table-object to metadata-table.
         *  @param  (pt)        [in]  table-object to add.
         *  @return ErrorCode::OK if success, otherwise an error code.
         */
        ErrorCode add(boost::property_tree::ptree pt);

        /**
         *  @brief  Add table-object to metadata-table.
         *  @param  (pt)        [in]  table-object to add.
         *  @param  (table_id)  [out] ID of the added table-object.
         *  @return ErrorCode::OK if success, otherwise an error code.
         */
        ErrorCode add(boost::property_tree::ptree pt, uint64_t* table_id);
#if 0
        /**
         *  @brief  Get table-object.
         *  @param  (table_id)  [in]  table-object ID
         *  @param  (pt)        [out] property_tree object to populating metadata.
         *  @return ErrorCode::OK if success, otherwise an error code.
         */
        ErrorCode get(const uint64_t table_id, boost::property_tree::ptree& pt) const;

        /**
         *  @brief  Get table-object.
         *  @param  (name)  [in]  name of table-object. (Value of "name" key.)
         *  @param  (pt)    [out] property_tree object to populating metadata.
         *  @return ErrorCode::OK if success, otherwise an error code.
         */
        ErrorCode get(std::vector<std::string> name, boost::property_tree::ptree& pt) const;

        /**
         *  @brief  Set table-object to metadata-table.
         *  @param  (table_id)  [in] table-object ID.
         *  @param  (pt)        [in] property_tree object containing metadata.
         *  @return ErrorCode::OK if success, otherwise an error code.
         *  @note   Return ErrorCode::NOT_FOUND, if table_id not found.
         */
        ErrorCode set(const uint64_t table_id, boost::property_tree::ptree const pt);

        /**
         *  @brief  Set table-object to metadata-table.
         *  @param  (name)  [in] name of table-object. (Value of "name" key.)
         *  @param  (pt)    [in] property_tree object containing metadata.
         *  @return ErrorCode::OK if success, otherwise an error code.
         *  @note   Return ErrorCode::NOT_FOUND, if name not found.
         */
        ErrorCode set(std::string_view name, boost::property_tree::ptree const pt);

        /**
         *  @brief  Remove table-object from metadata-table.
         *  @param  (talbe_id)  [in]  table-object ID.
         *  @return ErrorCode::OK if success, otherwise an error code.
         */
        ErrorCode remove(const uint64_t table_id);

        /**
         *  @brief  Remove table-object from metadata-table.
         *  @param  (name)  [in] name of table-object. (Value of "name" key.)
         *  @return ErrorCode::OK if success, otherwise an error code.
         */
        ErrorCode remove(std::string_view name);
#endif
        /**
         *  @brief  Get next table-object.
         *  @param  (pt) [out] property_tree object to populating metadata.
         *  @return ErrorCode::OK if success, otherwise an error code.
         *  @note   Return ErrorCode::END_OF_ROW if there is no more data to read.
         */
        ErrorCode next(boost::property_tree::ptree& pt) const;

};

}

#endif // TABLE_METADATA_H_
