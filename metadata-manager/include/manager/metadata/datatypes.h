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

#include "manager/metadata/dao/datatypes_dao.h"
#include "manager/metadata/error_code.h"
#include "manager/metadata/metadata.h"

namespace manager::metadata {
    class DataTypes : public Metadata
    {
    public:
        // root object.
        static constexpr const char* const DATATYPES_NODE = "dataTypes";

        // data type metadata-object.
        // ID is defined in base class.
        // NAME is defined in base class.
        static constexpr const char* const PG_DATA_TYPE                = "pg_dataType";
        static constexpr const char* const PG_DATA_TYPE_NAME           = "pg_dataTypeName";
        static constexpr const char* const PG_DATA_TYPE_QUALIFIED_NAME = "pg_dataTypeQualifiedName";

        /**
         * @brief represents data types id.
         */
        enum class DataTypesId : ObjectIdType
        {
            INT32 = 4,   //!< @brief INT32.
            INT64 = 6,   //!< @brief INT64.
            FLOAT32 = 8, //!< @brief FLOAT32.
            FLOAT64 = 9, //!< @brief FLOAT64.
            CHAR = 13,   //!< @brief CHAR.
            VARCHAR = 14 //!< @brief VARCHAR.
        };

        ErrorCode init() override;

        /**
         *  @brief  Get metadata-object.
         *  @param  (object_name)   [in]  metadata-object name. (Value of "name"
         * key.)
         *  @param  (object)        [out] metadata-object with the specified
         * name.
         *  @return ErrorCode::OK if success, otherwise an error code.
         */
        ErrorCode get(std::string_view object_name,
                              boost::property_tree::ptree& object) override;

        /**
         *  @brief  Get metadata-object.
         *  @param  (key)           [in]  metadata-object key.
         *  @param  (value)         [in]  metadata-object value.
         *  @param  (object)        [out] metadata-object with the specified
         * name.
         *  @return ErrorCode::OK if success, otherwise an error code.
         */
        ErrorCode get(const char* object_key, std::string_view object_value,
                      boost::property_tree::ptree& object) override;

        /**
         *  @brief  Constructor
         *  @param  (database) [in]  database name.
         *  @return none.
         */
        DataTypes(std::string_view database, std::string_view component = "visitor")
            : Metadata(database, component) {}

        DataTypes(const DataTypes&) = delete;
        DataTypes& operator=(const DataTypes&) = delete;

    private:
        std::shared_ptr<manager::metadata::db::DataTypesDAO> ddao;
};

} // namespace manager::metadata
