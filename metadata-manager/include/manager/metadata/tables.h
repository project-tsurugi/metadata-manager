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

#include <boost/property_tree/ptree.hpp>
#include <memory>
#include <string_view>

#include "manager/metadata/dao/columns_dao.h"
#include "manager/metadata/dao/tables_dao.h"
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
    static constexpr const char* const NAMESPACE = "namespace";
    static constexpr const char* const COLUMNS_NODE = "columns";
    static constexpr const char* const PRIMARY_KEY_NODE = "primaryKey";
    static constexpr const char* const RELTUPLES = "reltuples";

    // column metadata-object.
    struct Column {
        static constexpr const char* const ID = "id";
        static constexpr const char* const TABLE_ID = "tableId";
        static constexpr const char* const NAME = "name";
        static constexpr const char* const ORDINAL_POSITION = "ordinalPosition";
        static constexpr const char* const DATA_TYPE_ID = "dataTypeId";
        static constexpr const char* const DATA_LENGTH = "dataLength";
        static constexpr const char* const VARYING = "varying";
        static constexpr const char* const NULLABLE = "nullable";
        static constexpr const char* const DEFAULT = "defaultExpr";
        static constexpr const char* const DIRECTION = "direction";

        /**
         * @brief represents sort direction of elements.
         */
        enum class Direction {

            /**
             * @brief default order.
             */
            DEFAULT = 0,

            /**
             * @brief ascendant order.
             */
            ASCENDANT,

            /**
             * @brief descendant order.
             */
            DESCENDANT,
        };
    };

    ErrorCode init() override;

    /**
     *  @brief  Add metadata-object to metadata-table.
     *  @param  (object) [in]  metadata-object to add.
     *  @return ErrorCode::OK if success, otherwise an error code.
     */
    ErrorCode add(boost::property_tree::ptree& object) override;

    /**
     *  @brief  Add metadata-object to metadata-table.
     *  @param  (object)      [in]  metadata-object to add.
     *  @param  (object_id)   [out] ID of the added metadata-object.
     *  @return ErrorCode::OK if success, otherwise an error code.
     */
    ErrorCode add(boost::property_tree::ptree& object,
                  ObjectIdType* object_id) override;

    /**
     *  @brief  Get metadata-object.
     *  @param  (object_id) [in]  metadata-object ID.
     *  @param  (object)    [out] metadata-object with the specified ID.
     *  @return ErrorCode::OK if success, otherwise an error code.
     */
    ErrorCode get(const ObjectIdType object_id,
                  boost::property_tree::ptree& object) override;

    /**
     *  @brief  Get metadata-object.
     *  @param  (object_name)   [in]  metadata-object name. (Value of "name"
     * key.)
     *  @param  (object)        [out] metadata-object with the specified name.
     *  @return ErrorCode::OK if success, otherwise an error code.
     */
    ErrorCode get(std::string_view object_name,
                  boost::property_tree::ptree& object) override;

    /**
     *  @brief  Remove all metadata-object based on the given table id
     *  (table metadata, column metadata and column statistics)
     *  from metadata-table (the table metadata table,
     *  the column metadata table and the column statistics table).
     *  @param (table_id) [in] table id.
     *  @return ErrorCode::OK if success, otherwise an error code.
     */
    ErrorCode remove(const ObjectIdType object_id) override;

    /**
     *  @brief  Remove all metadata-object based on the given table name
     *  (table metadata, column metadata and column statistics)
     *  from metadata-table (the table metadata table,
     *  the column metadata table and the column statistics table).
     *  @param (table_id) [in] table id.
     *  @return ErrorCode::OK if success, otherwise an error code.
     */
    ErrorCode remove(const char* object_name, ObjectIdType* object_id) override;

    /**
     *  @brief  Constructor
     *  @param  (database) [in]  database name.
     *  @return none.
     */
    Tables(std::string_view database, std::string_view component = "visitor")
        : Metadata(database, component) {}

    Tables(const Tables&) = delete;
    Tables& operator=(const Tables&) = delete;

   private:
    std::shared_ptr<manager::metadata::db::TablesDAO> tdao;
    std::shared_ptr<manager::metadata::db::ColumnsDAO> cdao;

    /**
     *  @brief  Get column metadata-object based on the given table id.
     *  @param  (table_id) [in]  table id.
     *  @param  (tables)   [out] table metadata-object with the specified table
     * id.
     *  @return ErrorCode::OK if success, otherwise an error code.
     */
    ErrorCode get_all_column_metadatas(const std::string& table_id,
                                       boost::property_tree::ptree& tables);

};  // class Tables

}  // namespace manager::metadata
