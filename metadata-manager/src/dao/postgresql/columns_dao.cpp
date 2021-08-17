/*
 * Copyright 2020 tsurugi project.
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
#include "manager/metadata/dao/postgresql/columns_dao.h"

#include <boost/format.hpp>
#include <boost/property_tree/json_parser.hpp>
#include <iostream>
#include <sstream>
#include <string>
#include <utility>
#include <vector>

#include <libpq-fe.h>
#include "manager/metadata/dao/common/message.h"
#include "manager/metadata/dao/common/statement_name.h"
#include "manager/metadata/dao/postgresql/common.h"
#include "manager/metadata/dao/postgresql/dbc_utils.h"
#include "manager/metadata/tables.h"

// =============================================================================
namespace {

std::unordered_map<std::string, std::string> statement_names_select;
std::unordered_map<std::string, std::string> statement_names_delete;

namespace statement {

using manager::metadata::Tables;
using manager::metadata::db::postgresql::SCHEMA_NAME;
using manager::metadata::db::postgresql::TableName;

/**
 *  @brief  Returnes an INSERT stetement for one column metadata.
 *  @param  none.
 *  @return an INSERT stetement to insert one column metadata.
 */
std::string insert_one_column_metadata() {
  // SQL statement
  boost::format query =
      boost::format(
          "INSERT INTO %s.%s (%s, %s, %s, %s, %s, %s, %s, %s, %s) "
          "VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)") %
      SCHEMA_NAME % TableName::COLUMN_METADATA_TABLE %
      Tables::Column::TABLE_ID % Tables::Column::NAME %
      Tables::Column::ORDINAL_POSITION % Tables::Column::DATA_TYPE_ID %
      Tables::Column::DATA_LENGTH % Tables::Column::VARYING %
      Tables::Column::NULLABLE % Tables::Column::DEFAULT %
      Tables::Column::DIRECTION;

  return query.str();
}

/**
 *  @brief  Returnes a SELECT stetement to get all column metadata
 *  from column metadata table,
 *  based on table id.
 * @param  (column_name)  [in]  column name of metadata-columns.
 * @return a SELECT stetement to get all column metadata:
 *    select * from table_name where column_name = $1.
 */
std::string select_all_column_metadata(std::string_view column_name) {
  // SQL statement
  boost::format query =
      boost::format("SELECT * FROM %s.%s WHERE %s = $1 ORDER BY %s") %
      SCHEMA_NAME % TableName::COLUMN_METADATA_TABLE % column_name.data() %
      Tables::Column::ORDINAL_POSITION;

  return query.str();
}

/**
 *  @brief  Returnes a DELETE stetement for one column metadata
 *  from column metadata table,
 *  based on table id.
 * @param  (column_name)  [in]  column name of metadata-columns.
 * @return a DELETE stetement for one column metadata:
 *    delete from table_name where column_name = $1.
 */
std::string delete_all_column_metadata(std::string_view column_name) {
  // SQL statement
  boost::format query = boost::format("DELETE FROM %s.%s WHERE %s = $1") %
                        SCHEMA_NAME % TableName::COLUMN_METADATA_TABLE %
                        column_name.data();

  return query.str();
}

}  // namespace statement
}  // namespace

// =============================================================================
namespace manager::metadata::db::postgresql {

namespace json_parser = boost::property_tree::json_parser;
using boost::property_tree::json_parser_error;
using boost::property_tree::ptree;
using manager::metadata::ErrorCode;
using manager::metadata::db::StatementName;

/**
 * @enum ColumnMetadataTable
 * @brief Column ordinal position of the column metadata table
 * in the metadata repository.
 */
struct ColumnMetadataTable {
  enum ColumnOrdinalPosition {
    ID = 0,
    TABLE_ID,
    NAME,
    ORDINAL_POSITION,
    DATA_TYPE_ID,
    DATA_LENGTH,
    VARYING,
    NULLABLE,
    DEFAULT_EXPR,
    DIRECTION
  };
};

/**
 *  @brief  Constructor
 *  @param  (connection)  [in]  a connection to the metadata repository.
 *  @return none.
 */
ColumnsDAO::ColumnsDAO(DBSessionManager *session_manager)
    : connection_(session_manager->get_connection()) {
  // Creates a list of column names
  // in order to get values based on
  // one column included in this list
  // from metadata repository.
  //
  // For example,
  // If column name "tableId" is added to this list,
  // later defines a prepared statement
  // "select * from where tableId = ?".
  column_names_.emplace_back(Tables::Column::TABLE_ID);

  // Creates a list of unique name
  // for the new prepared statement for each column names.
  for (auto column : column_names_) {
    // Creates unique name for the new prepared statement.
    boost::format statement_name_select =
        boost::format("%1%-%2%-%3%") %
        StatementName::COLUMNS_DAO_SELECT_ALL_COLUMN_METADATA %
        TableName::COLUMNS % column.c_str();

    // Addes this list to unique name for the new prepared statement.
    // key : column name
    // value : unique name for the new prepared statement.
    statement_names_select.emplace(column, statement_name_select.str());

    // Creates unique name for the new prepared statement.
    boost::format statement_name_delete =
        boost::format("%1%-%2%-%3%") %
        StatementName::COLUMNS_DAO_DELETE_ALL_COLUMN_METADATA %
        TableName::COLUMNS % column.c_str();

    // Addes this list to unique name for the new prepared statement.
    // key : column name
    // value : unique name for the new prepared statement.
    statement_names_delete.emplace(column, statement_name_delete.str());
  }
}

/**
 *  @brief  Defines all prepared statements.
 *  @param  none.
 *  @return ErrorCode::OK if success, otherwise an error code.
 */
ErrorCode ColumnsDAO::prepare() const {
  ErrorCode error = DbcUtils::prepare(
      connection_, StatementName::COLUMNS_DAO_INSERT_ONE_COLUMN_METADATA,
      statement::insert_one_column_metadata());
  if (error != ErrorCode::OK) {
    return error;
  }

  for (const std::string &column : column_names_) {
    // select statement.
    error = DbcUtils::prepare(connection_, statement_names_select.at(column),
                              statement::select_all_column_metadata(column));
    if (error != ErrorCode::OK) {
      return error;
    }

    // delete statement.
    error = DbcUtils::prepare(connection_, statement_names_delete.at(column),
                              statement::delete_all_column_metadata(column));
    if (error != ErrorCode::OK) {
      return error;
    }
  }

  return error;
}

/**
 *  @brief  Executes INSERT statement to insert
 *  the given one column statistic
 *  into the column metadata table based on the given table id.
 *  @param  (table_id)  [in]  table id.
 *  @param  (column)    [in]  one column metadata to add.
 *  @return ErrorCode::OK if success, otherwise an error code.
 */
ErrorCode ColumnsDAO::insert_one_column_metadata(
    ObjectIdType table_id, boost::property_tree::ptree &column) const {
  std::vector<char const *> param_values;

  std::string s_table_id = std::to_string(table_id);
  param_values.emplace_back(s_table_id.c_str());

  boost::optional<std::string> name =
      column.get_optional<std::string>(Tables::Column::NAME);
  if (!name) {
    return ErrorCode::INVALID_PARAMETER;
  }
  param_values.emplace_back((name ? name.value().c_str() : nullptr));

  boost::optional<std::string> ordinal_position =
      column.get_optional<std::string>(Tables::Column::ORDINAL_POSITION);
  param_values.emplace_back(
      (ordinal_position ? ordinal_position.value().c_str() : nullptr));

  boost::optional<std::string> data_type_id =
      column.get_optional<std::string>(Tables::Column::DATA_TYPE_ID);
  param_values.emplace_back(
      (data_type_id ? data_type_id.value().c_str() : nullptr));

  boost::optional<ptree &> o_data_length =
      column.get_child_optional(Tables::Column::DATA_LENGTH);

  std::string s_data_length;
  if (!o_data_length) {
    param_values.emplace_back(nullptr);
  } else {
    ptree &p_data_length = o_data_length.value();

    if (p_data_length.empty()) {
      param_values.emplace_back(p_data_length.data().c_str());
    } else {
      std::stringstream ss;
      try {
        json_parser::write_json(ss, p_data_length, false);
      } catch (json_parser_error &e) {
        std::cerr << Message::WRITE_JSON_FAILURE << e.what() << std::endl;
        return ErrorCode::INTERNAL_ERROR;
      } catch (...) {
        std::cerr << Message::WRITE_JSON_FAILURE << std::endl;
        return ErrorCode::INTERNAL_ERROR;
      }
      s_data_length = ss.str();

      param_values.emplace_back(s_data_length.c_str());
    }
  }

  boost::optional<std::string> varying =
      column.get_optional<std::string>(Tables::Column::VARYING);
  param_values.emplace_back((varying ? varying.value().c_str() : nullptr));

  boost::optional<std::string> nullable =
      column.get_optional<std::string>(Tables::Column::NULLABLE);
  if (!nullable) {
    return ErrorCode::INVALID_PARAMETER;
  }
  param_values.emplace_back((nullable ? nullable.value().c_str() : nullptr));

  boost::optional<std::string> default_expr =
      column.get_optional<std::string>(Tables::Column::DEFAULT);
  param_values.emplace_back(
      (default_expr ? default_expr.value().c_str() : nullptr));

  boost::optional<std::string> direction =
      column.get_optional<std::string>(Tables::Column::DIRECTION);
  param_values.emplace_back((direction ? direction.value().c_str() : nullptr));

  PGresult *res;
  ErrorCode error = DbcUtils::exec_prepared(
      connection_, StatementName::COLUMNS_DAO_INSERT_ONE_COLUMN_METADATA,
      param_values, res);

  if (error == ErrorCode::OK) {
    uint64_t number_of_rows_affected = 0;
    ErrorCode error_get =
        DbcUtils::get_number_of_rows_affected(res, number_of_rows_affected);

    if (error_get != ErrorCode::OK) {
      PQclear(res);
      return error_get;
    }

    if (number_of_rows_affected != 1) {
      PQclear(res);
      return ErrorCode::INVALID_PARAMETER;
    }
  }

  PQclear(res);
  return error;
}

/**
 *  @brief  Executes a SELECT statement to get column metadata rows
 *  from the column metadata table,
 *  where the given key equals the given value.
 *  @param  (object_key)    [in]  key. column name of a column metadata table.
 *  @param  (object_value)  [in]  value to be filtered.
 *  @param  (object)        [out] column metadatas to get,
 *  where the given key equals the given value.
 *  @return ErrorCode::OK if success, otherwise an error code.
 */
ErrorCode ColumnsDAO::select_column_metadata(
    std::string_view object_key, std::string_view object_value,
    boost::property_tree::ptree &object) const {
  std::vector<const char *> param_values;

  param_values.emplace_back(object_value.data());

  std::string statement_name_found;
  try {
    statement_name_found = statement_names_select.at(std::string(object_key));
  } catch (std::out_of_range &e) {
    std::cerr << Message::METADATA_KEY_NOT_FOUND << e.what() << std::endl;
    return ErrorCode::INVALID_PARAMETER;
  } catch (...) {
    std::cerr << Message::METADATA_KEY_NOT_FOUND << std::endl;
    return ErrorCode::INVALID_PARAMETER;
  }

  PGresult *res;
  ErrorCode error = DbcUtils::exec_prepared(connection_, statement_name_found,
                                            param_values, res);

  if (error == ErrorCode::OK) {
    int nrows = PQntuples(res);

    if (nrows <= 0) {
      PQclear(res);
      return ErrorCode::INVALID_PARAMETER;
    }
    for (int ordinal_position = 0; ordinal_position < nrows;
         ordinal_position++) {
      ptree column;

      error = get_ptree_from_p_gresult(res, ordinal_position, column);

      if (error != ErrorCode::OK) {
        PQclear(res);
        return error;
      }

      object.push_back(std::make_pair("", column));
    }
  }

  PQclear(res);
  return error;
}

/**
 *  @brief  Execute DELETE statement to delete column metadata
 *  from the column metadata table
 *  where the given key equals the given value.
 *  @param  (object_key)    [in]  key. column name of a column metadata table.
 *  @param  (object_value)  [in]  value to be filtered.
 *  @return ErrorCode::OK if success, otherwise an error code.
 */
ErrorCode ColumnsDAO::delete_column_metadata(
    std::string_view object_key, std::string_view object_value) const {
  std::vector<const char *> param_values;

  param_values.emplace_back(object_value.data());

  std::string statement_name_found;
  try {
    statement_name_found = statement_names_delete.at(std::string(object_key));
  } catch (std::out_of_range &e) {
    std::cerr << Message::METADATA_KEY_NOT_FOUND << e.what() << std::endl;
    return ErrorCode::INVALID_PARAMETER;
  } catch (...) {
    std::cerr << Message::METADATA_KEY_NOT_FOUND << std::endl;
    return ErrorCode::INVALID_PARAMETER;
  }

  PGresult *res;
  ErrorCode error = DbcUtils::exec_prepared(connection_, statement_name_found,
                                            param_values, res);

  if (error == ErrorCode::OK) {
    uint64_t number_of_rows_affected = 0;
    ErrorCode error_get =
        DbcUtils::get_number_of_rows_affected(res, number_of_rows_affected);

    if (error_get != ErrorCode::OK) {
      PQclear(res);
      return error_get;
    }

    if (number_of_rows_affected < 0) {
      PQclear(res);
      return ErrorCode::INVALID_PARAMETER;
    }
  }

  PQclear(res);
  return error;
}

// -----------------------------------------------------------------------------
// Private method area

/**
 *  @brief  Gets the ptree type column metadata
 *  converted from the given PGresult type value.
 *  @param  (res)               [in]  the result of a query.
 *  @param  (ordinal_position)  [in]  column ordinal position of PGresult.
 *  @param  (column)            [out] one column metadata.
 *  @return ErrorCode::OK if success, otherwise an error code.
 */
ErrorCode ColumnsDAO::get_ptree_from_p_gresult(PGresult *&res,
                                               int ordinal_position,
                                               ptree &column) const {
  column.put(Tables::Column::ID,
             PQgetvalue(res, ordinal_position,
                        ColumnMetadataTable::ColumnOrdinalPosition::ID));
  column.put(Tables::Column::TABLE_ID,
             PQgetvalue(res, ordinal_position,
                        ColumnMetadataTable::ColumnOrdinalPosition::TABLE_ID));
  column.put(Tables::Column::NAME,
             PQgetvalue(res, ordinal_position,
                        ColumnMetadataTable::ColumnOrdinalPosition::NAME));
  column.put(
      Tables::Column::ORDINAL_POSITION,
      PQgetvalue(res, ordinal_position,
                 ColumnMetadataTable::ColumnOrdinalPosition::ORDINAL_POSITION));
  column.put(
      Tables::Column::DATA_TYPE_ID,
      PQgetvalue(res, ordinal_position,
                 ColumnMetadataTable::ColumnOrdinalPosition::DATA_TYPE_ID));

  std::string data_length = std::string(
      PQgetvalue(res, ordinal_position,
                 ColumnMetadataTable::ColumnOrdinalPosition::DATA_LENGTH));

  if (!data_length.empty()) {
    std::stringstream ss;
    ss << data_length;

    ptree p_data_length;
    try {
      json_parser::read_json(ss, p_data_length);
    } catch (json_parser_error &e) {
      std::cerr << Message::READ_JSON_FAILURE << e.what() << std::endl;

      return ErrorCode::INTERNAL_ERROR;
    } catch (...) {
      std::cerr << Message::READ_JSON_FAILURE << std::endl;

      return ErrorCode::INTERNAL_ERROR;
    }

    column.add_child(Tables::Column::DATA_LENGTH, p_data_length);
  }

  std::string varying = DbcUtils::convert_boolean_expression(
      PQgetvalue(res, ordinal_position,
                 ColumnMetadataTable::ColumnOrdinalPosition::VARYING));

  if (!varying.empty()) {
    column.put(Tables::Column::VARYING, varying);
  }

  std::string nullable = DbcUtils::convert_boolean_expression(
      PQgetvalue(res, ordinal_position,
                 ColumnMetadataTable::ColumnOrdinalPosition::NULLABLE));

  if (!nullable.empty()) {
    column.put(Tables::Column::NULLABLE, nullable);
  }

  std::string default_expr =
      PQgetvalue(res, ordinal_position,
                 ColumnMetadataTable::ColumnOrdinalPosition::DEFAULT_EXPR);

  if (!default_expr.empty()) {
    column.put(Tables::Column::DEFAULT, default_expr);
  }

  std::string direction =
      PQgetvalue(res, ordinal_position,
                 ColumnMetadataTable::ColumnOrdinalPosition::DIRECTION);

  if (!direction.empty()) {
    column.put(Tables::Column::DIRECTION, direction);
  }

  return ErrorCode::OK;
}

}  // namespace manager::metadata::db::postgresql
