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

#include "manager/metadata/statistics.h"

#include <boost/property_tree/json_parser.hpp>
#include <iostream>
#include <memory>
#include <sstream>
#include <string>
#include <unordered_map>

#include "manager/metadata/dao/common/message.h"
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
ErrorCode Statistics::init() {
    if (tdao != nullptr && sdao != nullptr) {
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

    std::shared_ptr<GenericDAO> s_gdao = nullptr;
    error =
        db_session_manager.get_dao(GenericDAO::TableName::STATISTICS, s_gdao);

    if (error == ErrorCode::OK) {
        sdao = std::static_pointer_cast<StatisticsDAO>(s_gdao);
    }

    return error;
}

/**
 *  @brief  Adds or updates one column statistic
 *  to the column statistics table
 *  based on the given table id and the given column ordinal position.
 *  Adds one column statistic if it not exists in the metadata repository.
 *  Updates one column statistic if it already exists.
 *  @param  (table_id)          [in]  table id.
 *  @param  (ordinal_position)  [in]  column ordinal position.
 *  @param  (column_statistic)  [in]  one column statistic to add or update.
 *  @return ErrorCode::OK if success, otherwise an error code.
 */
ErrorCode Statistics::add_one_column_statistic(
    ObjectIdType table_id, ObjectIdType ordinal_position,
    boost::property_tree::ptree &column_statistic) {
    ErrorCode error = ErrorCode::INTERNAL_ERROR;

    error = init();
    if (error != ErrorCode::OK) {
        return error;
    }

    if (table_id <= 0 || ordinal_position <= 0) {
        return ErrorCode::INVALID_PARAMETER;
    }

    std::string s_column_statistic;
    if (!column_statistic.empty()) {
        std::stringstream ss;
        try {
            json_parser::write_json(ss, column_statistic, false);
        } catch (boost::property_tree::json_parser_error &e) {
            std::cerr << Message::WRITE_JSON_FAILURE << e.what() << std::endl;
            return ErrorCode::INTERNAL_ERROR;
        } catch (...) {
            std::cerr << Message::WRITE_JSON_FAILURE << std::endl;
            return ErrorCode::INTERNAL_ERROR;
        }

        s_column_statistic = ss.str();
    }

    error = db_session_manager.start_transaction();
    if (error != ErrorCode::OK) {
        return error;
    }

    error =
        sdao->upsert_one_column_statistic_by_table_id_column_ordinal_position(
            table_id, ordinal_position, s_column_statistic);

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
 *  @brief  Adds or updates table statistic
 *  to the table metadata table based on the given table id.
 *  Adds table statistic if it not exists in the metadata repository.
 *  Updates table statistic if it already exists.
 *  @param  (table_id)   [in]  table id.
 *  @param  (reltuples)  [in]  the number of rows to add or update.
 *  @return ErrorCode::OK if success, otherwise an error code.
 */
ErrorCode Statistics::add_table_statistic(ObjectIdType table_id,
                                          float reltuples) {
    ErrorCode error = ErrorCode::INTERNAL_ERROR;

    error = init();
    if (error != ErrorCode::OK) {
        return error;
    }

    if (table_id <= 0) {
        return ErrorCode::INVALID_PARAMETER;
    }

    error = db_session_manager.start_transaction();
    if (error != ErrorCode::OK) {
        return error;
    }

    error = tdao->update_reltuples_by_table_id(reltuples, table_id);

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
 *  @brief  Adds or updates table statistic
 *  to the table metadata table based on the given table name.
 *  Adds table statistic if it not exists in the metadata repository.
 *  Updates table statistic if it already exists.
 *  @param  (table_name) [in]  table name.
 *  @param  (reltuples)  [in]  the number of rows to add or update.
 *  @param  (table_id)   [out] table id of the row updated.
 *  @return ErrorCode::OK if success, otherwise an error code.
 */
ErrorCode Statistics::add_table_statistic(std::string_view table_name,
                                          float reltuples,
                                          ObjectIdType *table_id) {
    ErrorCode error = ErrorCode::INTERNAL_ERROR;

    error = init();
    if (error != ErrorCode::OK) {
        return error;
    }

    if (table_name.empty()) {
        return ErrorCode::INVALID_PARAMETER;
    }

    error = db_session_manager.start_transaction();
    if (error != ErrorCode::OK) {
        return error;
    }

    ObjectIdType retval_object_id;
    error = tdao->update_reltuples_by_table_name(reltuples, table_name.data(),
                                                 retval_object_id);

    if (error == ErrorCode::OK) {
        error = db_session_manager.commit();
        if (error == ErrorCode::OK && table_id != nullptr) {
            *table_id = retval_object_id;
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
 *  @brief  Gets one column statistic from the column statistics table
 *  based on the given table id and the given column ordinal position.
 *  @param  (table_id)          [in]  table id.
 *  @param  (ordinal_position)  [in]  column ordinal position.
 *  @param  (column_statistic)  [out] one column statistic
 *  with the specified table id and column ordinal position.
 *  @return ErrorCode::OK if success, otherwise an error code.
 */
ErrorCode Statistics::get_one_column_statistic(
    ObjectIdType table_id, ObjectIdType ordinal_position,
    ColumnStatistic &column_statistic) {
    ErrorCode error = ErrorCode::INTERNAL_ERROR;

    error = init();
    if (error != ErrorCode::OK) {
        return error;
    }

    if (table_id <= 0 || ordinal_position <= 0) {
        return ErrorCode::INVALID_PARAMETER;
    }

    error =
        sdao->select_one_column_statistic_by_table_id_column_ordinal_position(
            table_id, ordinal_position, column_statistic);

    return error;
}

/**
 *  @brief  Gets all column statistics from the column statistics table
 *  based on the given table id.
 *  @param  (table_id)           [in]  table id.
 *  @param  (column_statistics)  [out] all column statistics
 *  with the specified table id.
 *  key : column ordinal position
 *  value : one column statistic
 *  @return ErrorCode::OK if success, otherwise an error code.
 */
ErrorCode Statistics::get_all_column_statistics(
    ObjectIdType table_id,
    std::unordered_map<ObjectIdType, ColumnStatistic> &column_statistics) {
    ErrorCode error = ErrorCode::INTERNAL_ERROR;

    error = init();
    if (error != ErrorCode::OK) {
        return error;
    }

    if (table_id <= 0) {
        return ErrorCode::INVALID_PARAMETER;
    }

    error = sdao->select_all_column_statistic_by_table_id(table_id,
                                                          column_statistics);

    return error;
}

/**
 *  @brief  Gets one table statistic from the table metadata table
 *  based on the given table id.
 *  @param  (table_id)         [in]  table id.
 *  @param  (table_statistic)  [out] one table statistic
 *  with the specified table id.
 *  @return ErrorCode::OK if success, otherwise an error code.
 */
ErrorCode Statistics::get_table_statistic(
    ObjectIdType table_id, manager::metadata::TableStatistic &table_statistic) {
    ErrorCode error = ErrorCode::INTERNAL_ERROR;

    error = init();
    if (error != ErrorCode::OK) {
        return error;
    }

    if (table_id <= 0) {
        return ErrorCode::INVALID_PARAMETER;
    }

    error = tdao->select_table_statistic_by_table_id(table_id, table_statistic);

    return error;
}

/**
 *  @brief  Gets one table statistic from the table metadata table
 *  based on the given table name.
 *  @param  (table_name)       [in]  table name.
 *  @param  (table_statistic)  [out] one table statistic
 *  with the specified table name.
 *  @return ErrorCode::OK if success, otherwise an error code.
 */
ErrorCode Statistics::get_table_statistic(
    std::string_view table_name,
    manager::metadata::TableStatistic &table_statistic) {
    ErrorCode error = ErrorCode::INTERNAL_ERROR;

    error = init();
    if (error != ErrorCode::OK) {
        return error;
    }

    if (table_name.empty()) {
        return ErrorCode::INVALID_PARAMETER;
    }

    error = tdao->select_table_statistic_by_table_name(table_name.data(),
                                                       table_statistic);

    return error;
}

/**
 *  @brief  Removes one column statistic from the column statistics table
 *  based on the given table id and the given column ordinal position.
 *  @param  (table_id)          [in]  table id.
 *  @param  (ordinal_position)  [in]  column ordinal position.
 *  @return ErrorCode::OK if success, otherwise an error code.
 */
ErrorCode Statistics::remove_one_column_statistic(
    ObjectIdType table_id, ObjectIdType ordinal_position) {
    ErrorCode error = ErrorCode::INTERNAL_ERROR;

    error = init();
    if (error != ErrorCode::OK) {
        return error;
    }

    if (table_id <= 0 || ordinal_position <= 0) {
        return ErrorCode::INVALID_PARAMETER;
    }

    error = db_session_manager.start_transaction();
    if (error != ErrorCode::OK) {
        return error;
    }

    error =
        sdao->delete_one_column_statistic_by_table_id_column_ordinal_position(
            table_id, ordinal_position);

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
 *  @brief  Removes all column statistics
 *  from the column statistics table
 *  based on the given table id.
 *  @param  (table_id)          [in]  table id.
 *  @return ErrorCode::OK if success, otherwise an error code.
 */
ErrorCode Statistics::remove_all_column_statistics(ObjectIdType table_id) {
    ErrorCode error = ErrorCode::INTERNAL_ERROR;

    error = init();
    if (error != ErrorCode::OK) {
        return error;
    }

    if (table_id <= 0) {
        return ErrorCode::INVALID_PARAMETER;
    }

    error = db_session_manager.start_transaction();
    if (error != ErrorCode::OK) {
        return error;
    }

    error = sdao->delete_all_column_statistic_by_table_id(table_id);

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

}  // namespace manager::metadata
