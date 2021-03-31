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

#include "manager/metadata/tables.h"

#include <boost/foreach.hpp>
#include <boost/optional.hpp>

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
ErrorCode Tables::init() {
    if (tdao != nullptr && cdao != nullptr) {
        return ErrorCode::OK;
    }

    std::shared_ptr<GenericDAO> t_gdao = nullptr;
    ErrorCode error =
        db_session_manager.get_dao(GenericDAO::TableName::TABLES, t_gdao);

    if (error == ErrorCode::OK) {
        tdao = std::static_pointer_cast<TablesDAO>(t_gdao);
    } else {
        return error;
    }

    std::shared_ptr<GenericDAO> c_gdao = nullptr;
    error = db_session_manager.get_dao(GenericDAO::TableName::COLUMNS, c_gdao);

    if (error == ErrorCode::OK) {
        cdao = std::static_pointer_cast<ColumnsDAO>(c_gdao);
    }

    return error;
}

/**
 *  @brief  Add table metadata to table metadata table.
 *  @param  (object) [in]  table metadata to add.
 *  @return ErrorCode::OK if success, otherwise an error code.
 */
ErrorCode Tables::add(boost::property_tree::ptree& object) {
    return add(object, nullptr);
}

/**
 *  @brief  Add table metadata to table metadata table.
 *  @param  (object)      [in]  table metadata to add.
 *  @param  (object_id)   [out] ID of the added table metadata.
 *  @return ErrorCode::OK if success, otherwise an error code.
 */
ErrorCode Tables::add(boost::property_tree::ptree& object,
                      ObjectIdType* object_id) {
    ErrorCode error = ErrorCode::INTERNAL_ERROR;

    error = init();
    if (error != ErrorCode::OK) {
        return error;
    }

    error = db_session_manager.start_transaction();

    if (error != ErrorCode::OK) {
        return error;
    }

    // Add table metadata object to table metadata table.
    ObjectIdType retval_object_id;
    error = tdao->insert_table_metadata(object, retval_object_id);
    if (error != ErrorCode::OK) {
        ErrorCode rollback_error = db_session_manager.rollback();
        if (rollback_error != ErrorCode::OK) {
            return rollback_error;
        }
        return error;
    }

    // Add column metadata object to column metadata table.
    BOOST_FOREACH (const ptree::value_type& node,
                   object.get_child(Tables::COLUMNS_NODE)) {
        ptree column = node.second;
        error = cdao->insert_one_column_metadata(retval_object_id, column);
        if (error != ErrorCode::OK) {
            ErrorCode rollback_error = db_session_manager.rollback();
            if (rollback_error != ErrorCode::OK) {
                return rollback_error;
            }
            return error;
        }
    }

    if (error == ErrorCode::OK) {
        error = db_session_manager.commit();
        if (error == ErrorCode::OK && object_id != nullptr) {
            *object_id = retval_object_id;
        }
    } else {
        ErrorCode rollback_error = db_session_manager.rollback();
        if (rollback_error != ErrorCode::OK) {
            return rollback_error;
        }
        return error;
    }

    return error;
}

/**
 *  @brief  Get table metadata.
 *  @param  (object_id) [in]  table id.
 *  @param  (object)    [out] table metadata with the specified ID.
 *  @return ErrorCode::OK if success, otherwise an error code.
 */
ErrorCode Tables::get(const ObjectIdType object_id,
                      boost::property_tree::ptree& object) {
    ErrorCode error = ErrorCode::INTERNAL_ERROR;

    error = init();
    if (error != ErrorCode::OK) {
        return error;
    }

    if (object_id <= 0) {
        return ErrorCode::INVALID_PARAMETER;
    }

    std::string s_object_id = std::to_string(object_id);

    error = tdao->select_table_metadata(Tables::ID, s_object_id, object);

    if (error != ErrorCode::OK) {
        return error;
    }

    error = get_all_column_metadatas(s_object_id, object);

    return error;
}

/**
 *  @brief  Get table metadata object based on table name.
 *  @param  (object_name)   [in]  table name. (Value of "name"
 * key.)
 *  @param  (object)        [out] table metadata object with the specified name.
 *  @return ErrorCode::OK if success, otherwise an error code.
 */
ErrorCode Tables::get(std::string_view object_name,
                      boost::property_tree::ptree& object) {
    ErrorCode error = ErrorCode::INTERNAL_ERROR;

    error = init();
    if (error != ErrorCode::OK) {
        return error;
    }

    if (object_name.empty()) {
        return ErrorCode::INVALID_PARAMETER;
    }

    error =
        tdao->select_table_metadata(Tables::NAME, object_name.data(), object);

    if (error != ErrorCode::OK) {
        return error;
    }

    BOOST_FOREACH (ptree::value_type& node, object) {
        ptree& table = node.second;

        if (table.empty()) {
            boost::optional<std::string> o_table_id =
                object.get_optional<std::string>(Tables::ID);
            if (!o_table_id) {
                return ErrorCode::INTERNAL_ERROR;
            }

            error = get_all_column_metadatas(o_table_id.get(), object);
            break;
        } else {
            boost::optional<std::string> o_table_id =
                table.get_optional<std::string>(Tables::ID);
            if (!o_table_id) {
                return ErrorCode::INTERNAL_ERROR;
            }

            error = get_all_column_metadatas(o_table_id.get(), table);

            if (error != ErrorCode::OK) {
                return error;
            }
        }
    }

    return error;
}

/**
 *  @brief  Get column metadata-object based on the given table id.
 *  @param  (table_id) [in]  table id.
 *  @param  (tables)   [out] table metadata-object with the specified table
 * id.
 *  @return ErrorCode::OK if success, otherwise an error code.
 */
ErrorCode Tables::get_all_column_metadatas(
    const std::string& table_id, boost::property_tree::ptree& tables) {
    assert(!table_id.empty());

    ErrorCode error = ErrorCode::INTERNAL_ERROR;

    ptree columns;
    error = cdao->select_column_metadata(Tables::Column::TABLE_ID, table_id,
                                         columns);

    if (error == ErrorCode::OK || error == ErrorCode::INVALID_PARAMETER) {
        tables.add_child(Tables::COLUMNS_NODE, columns);
        error = ErrorCode::OK;
    }

    return error;
}

/**
 *  @brief  Remove all metadata-object based on the given table id
 *  (table metadata, column metadata and column statistics)
 *  from metadata-table (the table metadata table,
 *  the column metadata table and the column statistics table).
 *  @param (object_id) [in] table id.
 *  @return ErrorCode::OK if success, otherwise an error code.
 */
ErrorCode Tables::remove(const ObjectIdType object_id) {
    ErrorCode error = ErrorCode::INTERNAL_ERROR;

    error = init();
    if (error != ErrorCode::OK) {
        return error;
    }

    if (object_id <= 0) {
        return ErrorCode::INVALID_PARAMETER;
    }

    error = db_session_manager.start_transaction();

    if (error != ErrorCode::OK) {
        return error;
    }

    error = tdao->delete_table_metadata_by_table_id(object_id);
    if (error == ErrorCode::OK) {
        error = db_session_manager.commit();
    } else {
        ErrorCode rollback_error = db_session_manager.rollback();
        if (rollback_error != ErrorCode::OK) {
            return rollback_error;
        }
        return error;
    }

    return error;
}

/**
 *  @brief  Remove all metadata-object based on the given table name
 *  (table metadata, column metadata and column statistics)
 *  from metadata-table (the table metadata table,
 *  the column metadata table and the column statistics table).
 *  @param (object_name) [in]  table name.
 *  @param (object_id)   [out] object id of table removed.
 *  @return ErrorCode::OK if success, otherwise an error code.
 */
ErrorCode Tables::remove(const char* object_name, ObjectIdType* object_id) {
    ErrorCode error = ErrorCode::INTERNAL_ERROR;

    error = init();
    if (error != ErrorCode::OK) {
        return error;
    }

    std::string s_object_name = std::string(object_name);
    if (s_object_name.empty()) {
        return ErrorCode::INVALID_PARAMETER;
    }

    error = db_session_manager.start_transaction();

    if (error != ErrorCode::OK) {
        return error;
    }

    ObjectIdType retval_object_id;
    error = tdao->delete_table_metadata_by_table_name(s_object_name,
                                                      retval_object_id);
    if (error == ErrorCode::OK) {
        error = db_session_manager.commit();

        if (error == ErrorCode::OK && object_id != nullptr) {
            *object_id = retval_object_id;
        }
    } else {
        ErrorCode rollback_error = db_session_manager.rollback();
        if (rollback_error != ErrorCode::OK) {
            return rollback_error;
        }
        return error;
    }

    return error;
}

}  // namespace manager::metadata
