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
#include "manager/metadata/dao/postgresql/tables_dao.h"

#include <boost/format.hpp>
#include <boost/optional.hpp>
#include <boost/property_tree/json_parser.hpp>
#include <iostream>
#include <string>
#include <string_view>
#include <unordered_map>

#include <libpq-fe.h>
#include "manager/metadata/dao/common/message.h"
#include "manager/metadata/dao/common/statement_name.h"
#include "manager/metadata/dao/postgresql/common.h"
#include "manager/metadata/dao/postgresql/dbc_utils.h"
#include "manager/metadata/tables.h"

// =============================================================================
namespace {

std::unordered_map<std::string, std::string> statement_names_update_reltuples;
std::unordered_map<std::string, std::string>
    statement_names_select_statistic_equal_to;
std::unordered_map<std::string, std::string> statement_names_select_equal_to;
std::unordered_map<std::string, std::string> statement_names_delete_equal_to;

namespace statement {

using manager::metadata::Tables;
using manager::metadata::db::postgresql::SCHEMA_NAME;
using manager::metadata::db::postgresql::TableName;

/**
 *  @brief  Returnes an UPDATE stetement for the number of rows
 *  based on table name.
 *  @param  (column_name)  [in]  column name of metadata-table.
 *  @return an UPDATE stetement to update the number of rows
 *  based on table name.
 */
std::string update_reltuples(std::string_view column_name) {
  // SQL statement
  boost::format query =
      boost::format("UPDATE %s.%s SET %s = $1 WHERE %s = $2 RETURNING %s") %
      SCHEMA_NAME % TableName::TABLE_METADATA_TABLE % Tables::RELTUPLES %
      column_name.data() % Tables::ID;

  return query.str();
}

/**
 *  @brief  Returnes an INSERT stetement for table metadata.
 *  @param  none.
 *  @return an INSERT stetement to insert table metadata.
 */
std::string insert_table_metadata() {
  // SQL statement
  boost::format query = boost::format(
                            "INSERT INTO %s.%s (%s, %s, %s, %s)"
                            "  VALUES ($1, $2, $3, $4)"
                            "  RETURNING %s") %
                        SCHEMA_NAME % TableName::TABLE_METADATA_TABLE %
                        Tables::NAME % Tables::NAMESPACE %
                        Tables::PRIMARY_KEY_NODE % Tables::RELTUPLES %
                        Tables::ID;

  return query.str();
}

/**
 * @brief  Returnes a SELECT stetement to get metadata:
 *   select * from table_name where column_name = $1.
 * @param  (column_name)  [in]  column name of metadata-table.
 * @return a SELECT stetement:
 *    select * from table_name where column_name = $1.
 */
std::string select_equal_to(std::string_view column_name) {
  // SQL statement
  boost::format query =
      boost::format("SELECT * FROM %s.%s WHERE %s = $1 ORDER BY %s") %
      SCHEMA_NAME % TableName::TABLE_METADATA_TABLE % column_name.data() %
      Tables::ID;

  return query.str();
}

/**
 * @brief  Returnes a SELECT stetement to get metadata:
 *   select * from table_name where column_name = $1.
 * @param  (column_name)  [in]  column name of metadata-table.
 * @return a DELETE stetement:
 *    delete from table_name where column_name = $1.
 */
std::string delete_equal_to(std::string_view column_name) {
  // SQL statement
  boost::format query =
      boost::format("DELETE FROM %s.%s WHERE %s = $1 RETURNING %s") %
      SCHEMA_NAME % TableName::TABLE_METADATA_TABLE % column_name.data() %
      Tables::ID;

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
 * @enum TableMetadataTable
 * @brief Column ordinal position of the table metadata table
 * in the metadata repository.
 */
struct TableMetadataTable {
  enum ColumnOrdinalPosition {
    ID = 0,
    NAME,
    NAMESPACE_NAME,
    PRIMARY_KEY,
    RELTUPLES
  };
};

/**
 *  @brief  Constructor
 *  @param  (connection)  [in]  a connection to the metadata repository.
 *  @return none.
 */
TablesDAO::TablesDAO(DBSessionManager* session_manager)
    : connection_(session_manager->get_connection()) {
  // Creates a list of column names
  // in order to get values based on
  // one column included in this list
  // from metadata repository.
  //
  // For example,
  // If column name "id" is added to this list,
  // later defines a prepared statement
  // "select * from where id = ?".
  column_names.emplace_back(Tables::ID);
  column_names.emplace_back(Tables::NAME);

  // Creates a list of unique name
  // for the new prepared statement for each column names.
  for (auto column : column_names) {
    // Creates unique name for the new prepared statement.
    boost::format statement_name_update_reltuples =
        boost::format("%1%-%2%-%3%") %
        StatementName::TABLES_DAO_UPDATE_RELTUPLES % TableName::TABLES %
        column.c_str();

    // Addes this list to unique name for the new prepared statement.
    // key : column name
    // value : unique name for the new prepared statement.
    statement_names_update_reltuples.emplace(
        column, statement_name_update_reltuples.str());

    // Creates unique name for the new prepared statement.
    boost::format statement_name_select_statistic =
        boost::format("%1%-%2%-%3%") %
        StatementName::TABLES_DAO_SELECT_TABLE_STATISTIC % TableName::TABLES %
        column.c_str();

    // Addes this list to unique name for the new prepared statement.
    // key : column name
    // value : unique name for the new prepared statement.
    statement_names_select_statistic_equal_to.emplace(
        column, statement_name_select_statistic.str());

    // Creates unique name for the new prepared statement.
    boost::format statement_name_select = boost::format("%1%-%2%-%3%") %
                                          StatementName::DAO_SELECT_EQUAL_TO %
                                          TableName::TABLES % column.c_str();

    // Addes this list to unique name for the new prepared statement.
    // key : column name
    // value : unique name for the new prepared statement.
    statement_names_select_equal_to.emplace(column,
                                            statement_name_select.str());

    // Creates unique name for the new prepared statement.
    boost::format statement_name_delete = boost::format("%1%-%2%-%3%") %
                                          StatementName::DAO_DELETE_EQUAL_TO %
                                          TableName::TABLES % column.c_str();

    // Addes this list to unique name for the new prepared statement.
    // key : column name
    // value : unique name for the new prepared statement.
    statement_names_delete_equal_to.emplace(column,
                                            statement_name_delete.str());
  }
}

/**
 *  @brief  Defines all prepared statements.
 *  @param  none.
 *  @return ErrorCode::OK if success, otherwise an error code.
 */
ErrorCode TablesDAO::prepare() const {
  ErrorCode error = ErrorCode::INTERNAL_ERROR;

  error = DbcUtils::prepare(connection_,
                            StatementName::TABLES_DAO_INSERT_TABLE_METADATA,
                            statement::insert_table_metadata());
  if (error != ErrorCode::OK) {
    return error;
  }

  for (const std::string& column : column_names) {
    // reltuples update statement.
    error = DbcUtils::prepare(connection_,
                              statement_names_update_reltuples.at(column),
                              statement::update_reltuples(column));
    if (error != ErrorCode::OK) {
      return error;
    }

    // statistics select statement.
    error = DbcUtils::prepare(
        connection_, statement_names_select_statistic_equal_to.at(column),
        statement::select_equal_to(column));
    if (error != ErrorCode::OK) {
      return error;
    }

    // select statement.
    error = DbcUtils::prepare(connection_,
                              statement_names_select_equal_to.at(column),
                              statement::select_equal_to(column));
    if (error != ErrorCode::OK) {
      return error;
    }

    // delete statement.
    error = DbcUtils::prepare(connection_,
                              statement_names_delete_equal_to.at(column),
                              statement::delete_equal_to(column));
    if (error != ErrorCode::OK) {
      return error;
    }
  }

  return error;
}

/**
 *  @brief  Executes UPDATE statement to update the given number of rows
 *  into the table metadata table based on the given table id.
 *  @param  (reltuples)     [in]  the number of rows to update.
 *  @param  (object_key)    [in]  key. column name of a statistic table.
 *  @param  (object_value)  [in]  value to be filtered.
 *  @return  ErrorCode::OK if success, otherwise an error code.
 */
ErrorCode TablesDAO::update_reltuples(float reltuples,
                                      std::string_view object_key,
                                      std::string_view object_value,
                                      ObjectIdType& table_id) const {
  std::vector<char const*> param_values;

  std::string s_reltuples = std::to_string(reltuples);

  param_values.emplace_back(s_reltuples.c_str());
  param_values.emplace_back(object_value.data());

  std::string statement_name_found;
  try {
    statement_name_found =
        statement_names_update_reltuples.at(std::string(object_key));
  } catch (std::out_of_range& e) {
    std::cerr << Message::METADATA_KEY_NOT_FOUND << e.what() << std::endl;
    return ErrorCode::INVALID_PARAMETER;
  } catch (...) {
    std::cerr << Message::METADATA_KEY_NOT_FOUND << std::endl;
    return ErrorCode::INVALID_PARAMETER;
  }

  PGresult* res;
  ErrorCode error = DbcUtils::exec_prepared(connection_, statement_name_found,
                                            param_values, res);
  if (error == ErrorCode::OK) {
    int nrows = PQntuples(res);
    if (nrows == 1) {
      int ordinal_position = 0;
      error = DbcUtils::str_to_integral<ObjectIdType>(
          PQgetvalue(res, ordinal_position,
                     TableMetadataTable::ColumnOrdinalPosition::ID),
          table_id);
    } else {
      error = ErrorCode::INVALID_PARAMETER;
    }
  }

  PQclear(res);
  return error;
}

/**
 *  @brief  Executes SELECT statement to get one table statistic
 *  from the table metadata table based on the given table name.
 *  @param  (object_key)        [in]  key. column name of a statistic table.
 *  @param  (object_value)      [in]  value to be filtered.
 *  @param  (table_statistic)   [out] table statistic to get.
 *  @return ErrorCode::OK if success, otherwise an error code.
 */
ErrorCode TablesDAO::select_table_statistic(
    std::string_view object_key, std::string_view object_value,
    TableStatistic& table_statistic) const {
  std::vector<const char*> param_values;

  param_values.emplace_back(object_value.data());

  std::string statement_name_found;
  try {
    statement_name_found =
        statement_names_select_statistic_equal_to.at(std::string(object_key));
  } catch (std::out_of_range& e) {
    std::cerr << Message::METADATA_KEY_NOT_FOUND << e.what() << std::endl;
    return ErrorCode::INVALID_PARAMETER;
  } catch (...) {
    std::cerr << Message::METADATA_KEY_NOT_FOUND << std::endl;
    return ErrorCode::INVALID_PARAMETER;
  }

  PGresult* res;
  ErrorCode error = DbcUtils::exec_prepared(connection_, statement_name_found,
                                            param_values, res);

  if (error == ErrorCode::OK) {
    int nrows = PQntuples(res);

    if (nrows == 1) {
      int ordinal_position = 0;
      error = get_table_statistic_from_p_gresult(res, ordinal_position,
                                                 table_statistic);
    } else {
      PQclear(res);
      return ErrorCode::INVALID_PARAMETER;
    }
  }

  PQclear(res);
  return error;
}

/**
 *  @brief  Executes INSERT statement to insert the given one table metadata
 *  into the table metadata table.
 *  @param  (table)             [in]   one table metadata to add.
 *  @param  (table_id)          [out]  table id.
 *  @return ErrorCode::OK if success, otherwise an error code.
 */
ErrorCode TablesDAO::insert_table_metadata(boost::property_tree::ptree& table,
                                           ObjectIdType& table_id) const {
  std::vector<char const*> param_values;

  boost::optional<std::string> name =
      table.get_optional<std::string>(Tables::NAME);
  param_values.emplace_back((name ? name.value().c_str() : nullptr));

  boost::optional<std::string> namespace_name =
      table.get_optional<std::string>(Tables::NAMESPACE);
  param_values.emplace_back(
      (namespace_name ? namespace_name.value().c_str() : nullptr));

  boost::optional<ptree&> o_primary_keys =
      table.get_child_optional(Tables::PRIMARY_KEY_NODE);

  std::string s_primary_keys;
  if (o_primary_keys) {
    ptree& p_primary_keys = o_primary_keys.value();

    if (!p_primary_keys.empty()) {
      std::stringstream ss;
      try {
        json_parser::write_json(ss, p_primary_keys, false);
      } catch (json_parser_error& e) {
        std::cerr << Message::WRITE_JSON_FAILURE << e.what() << std::endl;
        return ErrorCode::INTERNAL_ERROR;
      } catch (...) {
        std::cerr << Message::WRITE_JSON_FAILURE << std::endl;
        return ErrorCode::INTERNAL_ERROR;
      }
      s_primary_keys = ss.str();
    }
  }
  param_values.emplace_back(
      (!s_primary_keys.empty() ? s_primary_keys.c_str() : nullptr));

  boost::optional<std::string> reltuples =
      table.get_optional<std::string>(Tables::RELTUPLES);
  param_values.emplace_back((reltuples ? reltuples.value().c_str() : nullptr));

  PGresult* res;
  ErrorCode error = DbcUtils::exec_prepared(
      connection_, StatementName::TABLES_DAO_INSERT_TABLE_METADATA,
      param_values, res);
  if (error == ErrorCode::OK) {
    int nrows = PQntuples(res);
    if (nrows == 1) {
      int ordinal_position = 0;
      error = DbcUtils::str_to_integral<ObjectIdType>(
          PQgetvalue(res, ordinal_position,
                     TableMetadataTable::ColumnOrdinalPosition::ID),
          table_id);
    } else {
      PQclear(res);
      return ErrorCode::INVALID_PARAMETER;
    }
  }

  PQclear(res);
  return error;
}

/**
 *  @brief  Executes a SELECT statement to get table metadata rows
 *  from the table metadata table,
 *  where the given key equals the given value.
 *  @param  (object_key)          [in]  key. column name of a table metadata
 *  table.
 *  @param  (object_value)        [in]  value to be filtered.
 *  @param  (object)              [out] table metadatas to get,
 *  where the given key equals the given value.
 *  @return ErrorCode::OK if success, otherwise an error code.
 */
ErrorCode TablesDAO::select_table_metadata(std::string_view object_key,
                                           std::string_view object_value,
                                           ptree& object) const {
  std::vector<const char*> param_values;

  param_values.emplace_back(object_value.data());

  std::string statement_name_found;
  try {
    statement_name_found =
        statement_names_select_equal_to.at(std::string(object_key));
  } catch (std::out_of_range& e) {
    std::cerr << Message::METADATA_KEY_NOT_FOUND << e.what() << std::endl;
    return ErrorCode::INVALID_PARAMETER;
  } catch (...) {
    std::cerr << Message::METADATA_KEY_NOT_FOUND << std::endl;
    return ErrorCode::INVALID_PARAMETER;
  }

  PGresult* res;
  ErrorCode error = DbcUtils::exec_prepared(connection_, statement_name_found,
                                            param_values, res);

  if (error == ErrorCode::OK) {
    int nrows = PQntuples(res);

    if (nrows <= 0) {
      error = ErrorCode::NOT_FOUND;
    } else if (nrows == 1) {
      int ordinal_position = 0;

      error = get_ptree_from_p_gresult(res, ordinal_position, object);
    } else {
      for (int ordinal_position = 0; ordinal_position < nrows;
           ordinal_position++) {
        ptree table;

        error = get_ptree_from_p_gresult(res, ordinal_position, table);
        if (error == ErrorCode::OK) {
          object.push_back(std::make_pair("", table));
        }
      }
    }
  }

  PQclear(res);
  return error;
}

/**
 *  @brief  Executes DELETE statement to delete table metadata
 *  from the table metadata table based on the given table name.
 *  @param  (object_key)    [in]  key. column name of a table metadata table.
 *  @param  (object_value)  [in]  value to be filtered.
 *  @param  (table_id)      [out]  table id of the row deleted.
 *  @return ErrorCode::OK if success, otherwise an error code.
 */
ErrorCode TablesDAO::delete_table_metadata(std::string_view object_key,
                                           std::string_view object_value,
                                           ObjectIdType& table_id) const {
  std::vector<const char*> param_values;

  param_values.emplace_back(object_value.data());

  std::string statement_name_found;
  try {
    statement_name_found =
        statement_names_delete_equal_to.at(std::string(object_key));
  } catch (std::out_of_range& e) {
    std::cerr << Message::METADATA_KEY_NOT_FOUND << e.what() << std::endl;
    return ErrorCode::INVALID_PARAMETER;
  } catch (...) {
    std::cerr << Message::METADATA_KEY_NOT_FOUND << std::endl;
    return ErrorCode::INVALID_PARAMETER;
  }

  PGresult* res;
  ErrorCode error = DbcUtils::exec_prepared(connection_, statement_name_found,
                                            param_values, res);

  if (error == ErrorCode::OK) {
    uint64_t number_of_rows_affected = 0;
    ErrorCode error_get =
        DbcUtils::get_number_of_rows_affected(res, number_of_rows_affected);

    if (error_get != ErrorCode::OK) {
      error = error_get;
    } else if (number_of_rows_affected == 1) {
      int ordinal_position = 0;
      ObjectIdType retval_table_id = 0;
      error = DbcUtils::str_to_integral<ObjectIdType>(
          PQgetvalue(res, ordinal_position,
                     TableMetadataTable::ColumnOrdinalPosition::ID),
          table_id);
    } else {
      error = ErrorCode::NOT_FOUND;
    }
  }

  PQclear(res);
  return error;
}

// -----------------------------------------------------------------------------
// Private method area

/**
 *  @brief  Gets the TableStatistic type value
 *  converted from the given PGresult type value.
 *  @param  (res)               [in]  the result of a query.
 *  @param  (ordinal_position)  [in]  column ordinal position of PGresult.
 *  @param  (table_statistic)   [out] the TableStatistic type value.
 *  @return ErrorCode::OK if success, otherwise an error code.
 */
ErrorCode TablesDAO::get_table_statistic_from_p_gresult(
    PGresult*& res, int ordinal_position,
    TableStatistic& table_statistic) const {
  // id
  ErrorCode error_str_to_int = DbcUtils::str_to_integral<ObjectIdType>(
      PQgetvalue(res, ordinal_position,
                 TableMetadataTable::ColumnOrdinalPosition::ID),
      table_statistic.id);

  if (error_str_to_int != ErrorCode::OK) {
    return error_str_to_int;
  }

  // name
  table_statistic.name = std::string(PQgetvalue(
      res, ordinal_position, TableMetadataTable::ColumnOrdinalPosition::NAME));

  // namespace
  table_statistic.namespace_name = std::string(
      PQgetvalue(res, ordinal_position,
                 TableMetadataTable::ColumnOrdinalPosition::NAMESPACE_NAME));

  // reltuples
  ErrorCode error_str_to_float = DbcUtils::str_to_floating_point<float>(
      PQgetvalue(res, ordinal_position,
                 TableMetadataTable::ColumnOrdinalPosition::RELTUPLES),
      table_statistic.reltuples);

  if (error_str_to_float != ErrorCode::OK) {
    return error_str_to_float;
  }

  return ErrorCode::OK;
}

/**
 *  @brief  Gets the ptree type table metadata
 *  converted from the given PGresult type value.
 *  @param  (res)               [in]  the result of a query.
 *  @param  (ordinal_position)  [in]  column ordinal position of PGresult.
 *  @param  (table)             [out] one table metadata.
 *  @return ErrorCode::OK if success, otherwise an error code.
 */
ErrorCode TablesDAO::get_ptree_from_p_gresult(
    PGresult*& res, int ordinal_position,
    boost::property_tree::ptree& table) const {
  table.put(Tables::ID,
            PQgetvalue(res, ordinal_position,
                       TableMetadataTable::ColumnOrdinalPosition::ID));

  table.put(Tables::NAME,
            PQgetvalue(res, ordinal_position,
                       TableMetadataTable::ColumnOrdinalPosition::NAME));

  std::string namespace_name =
      PQgetvalue(res, ordinal_position,
                 TableMetadataTable::ColumnOrdinalPosition::NAMESPACE_NAME);

  if (!namespace_name.empty()) {
    table.put(Tables::NAMESPACE, namespace_name);
  }

  std::string reltuples =
      PQgetvalue(res, ordinal_position,
                 TableMetadataTable::ColumnOrdinalPosition::RELTUPLES);

  if (!reltuples.empty()) {
    table.put(Tables::RELTUPLES, reltuples);
  }

  std::string s_primary_keys =
      PQgetvalue(res, ordinal_position,
                 TableMetadataTable::ColumnOrdinalPosition::PRIMARY_KEY);

  ptree primary_keys;
  if (s_primary_keys.empty()) {
    // NOTICE:
    // MUST add empty ptree.
    // ogawayama-server read key Tables::PRIMARY_KEY_NODE.
    table.add_child(Tables::PRIMARY_KEY_NODE, primary_keys);
  } else {
    std::stringstream ss;
    ss << s_primary_keys;
    try {
      json_parser::read_json(ss, primary_keys);
    } catch (json_parser_error& e) {
      std::cerr << Message::READ_JSON_FAILURE << e.what() << std::endl;
      return ErrorCode::INTERNAL_ERROR;
    } catch (...) {
      std::cerr << Message::READ_JSON_FAILURE << std::endl;
      return ErrorCode::INTERNAL_ERROR;
    }
    table.add_child(Tables::PRIMARY_KEY_NODE, primary_keys);
  }

  return ErrorCode::OK;
}

}  // namespace manager::metadata::db::postgresql
