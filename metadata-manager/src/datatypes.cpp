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

#include "manager/metadata/datatypes.h"

#include "manager/metadata/dao/generic_dao.h"

using namespace boost::property_tree;
using namespace manager::metadata::db;

namespace manager::metadata {

/**
 *  @brief  Initialization.
 *  @param  none.
 *  @return ErrorCode::OK
 *  if all the following steps are successfully completed.
 *  1. Establishes a connection to the metadata repository.
 *  2. Sends a query to set always-secure search path
 *     to the metadata repository.
 *  3. Defines prepared statements
 *     in the metadata repository.
 *  @return otherwise an error code.
 */
ErrorCode DataTypes::init() {
    if (ddao != nullptr) {
        return ErrorCode::OK;
    }

    std::shared_ptr<GenericDAO> d_gdao = nullptr;

    ErrorCode error =
        db_session_manager.get_dao(GenericDAO::TableName::DATATYPES, d_gdao);

    if (error == ErrorCode::OK) {
        ddao = std::static_pointer_cast<DataTypesDAO>(d_gdao);
    }

    return error;
}

/**
 *  @brief  Gets one data type metadata object
 *  from the data types metadata table
 *  based on the given object_name.
 *  @param  (object_name)   [in]  data type metadata name. (Value of "name"
 * key.)
 *  @param  (object)        [out] one data type metadata object to get
 *  based on the given object_name.
 *  @return ErrorCode::OK if success, otherwise an error code.
 */
ErrorCode DataTypes::get(std::string_view object_name,
                         boost::property_tree::ptree& object) {
    if (object_name.empty()) {
        return ErrorCode::INVALID_PARAMETER;
    }

    return get(DataTypes::NAME, object_name, object);
}

/**
 *  @brief  Gets one data type metadata object
 *  from the data types metadata table,
 *  where key = value.
 *  @param  (key)           [in]  key of data type metadata object.
 *  @param  (value)         [in]  value of data type metadata object.
 *  @param  (object)        [out] one data type metadata object to get,
 *  where key = value.
 *  @return ErrorCode::OK if success, otherwise an error code.
 */
ErrorCode DataTypes::get(const char* object_key, std::string_view object_value,
                         boost::property_tree::ptree& object) {
    ErrorCode error = ErrorCode::INTERNAL_ERROR;

    error = init();
    if (error != ErrorCode::OK) {
        return error;
    }

    std::string s_object_key = std::string(object_key);
    std::string s_object_value = object_value.data();

    if (s_object_key.empty() || s_object_value.empty()) {
        return ErrorCode::INVALID_PARAMETER;
    }

    error = ddao->select_one_data_type_metadata(s_object_key, s_object_value,
                                                object);

    return error;
}

}  // namespace manager::metadata
