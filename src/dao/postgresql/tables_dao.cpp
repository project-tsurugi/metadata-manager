/*
 * Copyright 2020-2021 tsurugi project.
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

#include <libpq-fe.h>

#include <iostream>
#include <regex>
#include <string>
#include <string_view>

#include <boost/format.hpp>

#include "manager/metadata/common/message.h"
#include "manager/metadata/common/utility.h"
#include "manager/metadata/dao/common/statement_name.h"
#include "manager/metadata/dao/postgresql/common.h"
#include "manager/metadata/dao/postgresql/dbc_utils.h"
#include "manager/metadata/tables.h"

// =============================================================================
namespace {

std::unordered_map<std::string, std::string> column_names;
std::unordered_map<std::string, std::string> statement_names_update_reltuples;
std::unordered_map<std::string, std::string> statement_names_select_equal_to;
std::unordered_map<std::string, std::string> statement_names_delete_equal_to;

namespace statement {

using manager::metadata::db::postgresql::SCHEMA_NAME;

using manager::metadata::db::postgresql::PgCatalog;
using manager::metadata::db::postgresql::TablesDAO;

/**
 * @brief Returns an INSERT statement for table metadata.
 * @param none.
 * @return an INSERT statement to insert table metadata.
 */
std::string insert_table_metadata() {
  // SQL statement
  boost::format query =
      boost::format(
          "INSERT INTO %1%.%2% (%3%, %4%, %5%, %6%, %7%, %8%)"
          " VALUES ($1, $2, $3, $4, $5, $6)"
          " RETURNING %9%") %
      SCHEMA_NAME % TablesDAO::kTableName %
      TablesDAO::ColumnName::kFormatVersion %
      TablesDAO::ColumnName::kGeneration % TablesDAO::ColumnName::kName %
      TablesDAO::ColumnName::kNamespace % TablesDAO::ColumnName::kPrimaryKey %
      TablesDAO::ColumnName::kTuples % TablesDAO::ColumnName::kId;

  return query.str();
}

/**
 * @brief Returns a SELECT statement to get metadata:
 *   select * from table_name.
 * @return a SELECT statement:
 *   select * from table_name.
 */
std::string select_all() {
  // SQL statement
  boost::format query =
      boost::format(
          "SELECT"
          " tbl.%3%, tbl.%4%, tbl.%5%, tbl.%6%, tbl.%7%, tbl.%8%, tbl.%9%"
          " , cls.%11%, cls.%12%"
          " FROM %1%.%2% tbl LEFT JOIN %10% cls ON (tbl.%6% = cls.%13%)"
          " ORDER BY %5%") %
      SCHEMA_NAME % TablesDAO::kTableName %
      TablesDAO::ColumnName::kFormatVersion %
      TablesDAO::ColumnName::kGeneration % TablesDAO::ColumnName::kId %
      TablesDAO::ColumnName::kName % TablesDAO::ColumnName::kNamespace %
      TablesDAO::ColumnName::kPrimaryKey % TablesDAO::ColumnName::kTuples %
      PgCatalog::PgClass::kTableName % PgCatalog::PgClass::ColumnName::kOwner %
      PgCatalog::PgClass::ColumnName::kAcl %
      PgCatalog::PgClass::ColumnName::kName;

  return query.str();
}

/**
 * @brief Returns a SELECT statement to get metadata:
 *   select * from table_name where column_name = $1.
 * @param (column_name)  [in]  column name of metadata-table.
 * @return a SELECT statement:
 *   select * from table_name where column_name = $1.
 */
std::string select_equal_to(std::string_view column_name) {
  // SQL statement
  boost::format query =
      boost::format(
          "SELECT"
          " tbl.%3%, tbl.%4%, tbl.%5%, tbl.%6%, tbl.%7%, tbl.%8%, tbl.%9%"
          " , cls.%11%, cls.%12%"
          " FROM %1%.%2% tbl LEFT JOIN %10% cls ON (tbl.%6% = cls.%13%)"
          " WHERE tbl.%14% = $1") %
      SCHEMA_NAME % TablesDAO::kTableName %
      TablesDAO::ColumnName::kFormatVersion %
      TablesDAO::ColumnName::kGeneration % TablesDAO::ColumnName::kId %
      TablesDAO::ColumnName::kName % TablesDAO::ColumnName::kNamespace %
      TablesDAO::ColumnName::kPrimaryKey % TablesDAO::ColumnName::kTuples %
      PgCatalog::PgClass::kTableName % PgCatalog::PgClass::ColumnName::kOwner %
      PgCatalog::PgClass::ColumnName::kAcl %
      PgCatalog::PgClass::ColumnName::kName % column_name.data();

  return query.str();
}

/**
 * @brief Returns an UPDATE statement for the number of rows
 *   based on table name.
 * @param (column_name)  [in]  column name of metadata-table.
 * @return an UPDATE statement to update the number of rows
 *   based on table name.
 */
std::string update_reltuples(std::string_view column_name) {
  // SQL statement
  boost::format query =
      boost::format(
          "UPDATE %1%.%2% SET %3% = $1 WHERE %4% = $2 RETURNING %5%") %
      SCHEMA_NAME % TablesDAO::kTableName % TablesDAO::ColumnName::kTuples %
      column_name.data() % TablesDAO::ColumnName::kId;

  return query.str();
}

/**
 * @brief Returns a SELECT statement to get metadata:
 *   delete from table_name where column_name = $1.
 * @param (column_name)  [in]  column name of metadata-table.
 * @return a DELETE statement:
 *   delete from table_name where column_name = $1.
 */
std::string delete_equal_to(std::string_view column_name) {
  // SQL statement
  boost::format query =
      boost::format("DELETE FROM %1%.%2% WHERE %3% = $1 RETURNING %4%") %
      SCHEMA_NAME % TablesDAO::kTableName % column_name.data() %
      TablesDAO::ColumnName::kId;

  return query.str();
}

}  // namespace statement
}  // namespace

// =============================================================================
namespace manager::metadata::db::postgresql {

using boost::property_tree::ptree;
using manager::metadata::ErrorCode;
using manager::metadata::db::StatementName;

/**
 * @brief Constructor
 * @param (connection)  [in]  a connection to the metadata repository.
 * @return none.
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
  column_names.emplace(Tables::ID, ColumnName::kId);
  column_names.emplace(Tables::NAME, ColumnName::kName);

  // Creates a list of unique name
  // for the new prepared statement for each column names.
  for (auto column : column_names) {
    // Creates unique name for the new prepared statement.
    boost::format statement_name_update =
        boost::format("%1%-%2%-%3%") %
        static_cast<int>(StatementName::TABLES_DAO_UPDATE_TUPLES) % kTableName %
        column.first;
    boost::format statement_name_select =
        boost::format("%1%-%2%-%3%") %
        static_cast<int>(StatementName::DAO_SELECT_EQUAL_TO) % kTableName %
        column.first;
    boost::format statement_name_delete =
        boost::format("%1%-%2%-%3%") %
        static_cast<int>(StatementName::DAO_DELETE_EQUAL_TO) % kTableName %
        column.first;

    // Added this list to unique name for the new prepared statement.
    // key : column name
    // value : unique name for the new prepared statement.
    statement_names_update_reltuples.emplace(column.first,
                                             statement_name_update.str());
    statement_names_select_equal_to.emplace(column.first,
                                            statement_name_select.str());
    statement_names_delete_equal_to.emplace(column.first,
                                            statement_name_delete.str());
  }
}

/**
 * @brief Defines all prepared statements.
 * @param none.
 * @return ErrorCode::OK if success, otherwise an error code.
 */
ErrorCode TablesDAO::prepare() const {
  ErrorCode error = ErrorCode::UNKNOWN;

  // Set the INSERT statement.
  error = DbcUtils::prepare(connection_,
                            StatementName::TABLES_DAO_INSERT_TABLE_METADATA,
                            statement::insert_table_metadata());
  if (error != ErrorCode::OK) {
    return error;
  }

  // Set all table metadata SELECT statement.
  error = DbcUtils::prepare(connection_,
                            StatementName::TABLES_DAO_SELECT_TABLE_METADATA_ALL,
                            statement::select_all());
  if (error != ErrorCode::OK) {
    return error;
  }

  // Set conditional SQL statement.
  for (auto column : column_names) {
    // Set SELECT statement.
    error = DbcUtils::prepare(connection_,
                              statement_names_select_equal_to.at(column.first),
                              statement::select_equal_to(column.second));
    if (error != ErrorCode::OK) {
      return error;
    }

    // Set reltuples UPDATE statement.
    error = DbcUtils::prepare(connection_,
                              statement_names_update_reltuples.at(column.first),
                              statement::update_reltuples(column.second));
    if (error != ErrorCode::OK) {
      return error;
    }

    // Set DELETE statement.
    error = DbcUtils::prepare(connection_,
                              statement_names_delete_equal_to.at(column.first),
                              statement::delete_equal_to(column.second));
    if (error != ErrorCode::OK) {
      return error;
    }
  }

  return error;
}

/**
 * @brief Executes INSERT statement to insert the given one table metadata
 *   into the table metadata table.
 * @param (table)     [in]  one table metadata to add.
 * @param (table_id)  [out] table id.
 * @return ErrorCode::OK if success, otherwise an error code.
 */
ErrorCode TablesDAO::insert_table_metadata(
    const boost::property_tree::ptree& table, ObjectIdType& table_id) const {
  ErrorCode error = ErrorCode::UNKNOWN;
  std::vector<char const*> param_values;

  // format_version
  std::string s_format_version = std::to_string(Tables::format_version());
  param_values.emplace_back(s_format_version.c_str());

  // generation
  std::string s_generation = std::to_string(Tables::generation());
  param_values.emplace_back(s_generation.c_str());

  // name
  boost::optional<std::string> name =
      table.get_optional<std::string>(Tables::NAME);
  param_values.emplace_back((name ? name.value().c_str() : nullptr));

  // namespace
  boost::optional<std::string> namespace_name =
      table.get_optional<std::string>(Tables::NAMESPACE);
  param_values.emplace_back(
      (namespace_name ? namespace_name.value().c_str() : ""));

  // primary_keys
  boost::optional<const ptree&> o_primary_keys =
      table.get_child_optional(Tables::PRIMARY_KEY_NODE);

  std::string s_primary_keys;
  if (o_primary_keys) {
    // Converts a property_tree to a JSON string.
    error = Utility::ptree_to_json(o_primary_keys.value(), s_primary_keys);
    if (error != ErrorCode::OK) {
      return error;
    }
  }
  param_values.emplace_back(
      (!s_primary_keys.empty() ? s_primary_keys.c_str() : nullptr));

  // tuples
  boost::optional<std::string> reltuples =
      table.get_optional<std::string>(Tables::TUPLES);
  param_values.emplace_back((reltuples ? reltuples.value().c_str() : nullptr));

  PGresult* res = nullptr;
  error = DbcUtils::exec_prepared(
      connection_, StatementName::TABLES_DAO_INSERT_TABLE_METADATA,
      param_values, res);
  if (error == ErrorCode::OK) {
    int nrows = PQntuples(res);
    if (nrows == 1) {
      int ordinal_position = 0;
      error = DbcUtils::str_to_integral<ObjectIdType>(
          PQgetvalue(res, ordinal_position, 0), table_id);
    } else {
      error = ErrorCode::INVALID_PARAMETER;
    }
  }

  PQclear(res);
  return error;
}

/**
 * @brief Executes a SELECT statement to get table metadata rows
 *   from the table metadata table,
 *   where the given key equals the given value.
 * @param (object_key)    [in]  key. column name of a table metadata table.
 * @param (object_value)  [in]  value to be filtered.
 * @param (object)        [out] table metadata to get,
 *   where the given key equals the given value.
 * @retval ErrorCode::OK if success.
 * @retval ErrorCode::ID_NOT_FOUND if the table id does not exist.
 * @retval ErrorCode::NAME_NOT_FOUND if the table name does not exist.
 * @retval otherwise an error code.
 */
ErrorCode TablesDAO::select_table_metadata(
    std::string_view object_key, std::string_view object_value,
    boost::property_tree::ptree& object) const {
  ErrorCode error = ErrorCode::UNKNOWN;
  std::vector<const char*> param_values;

  param_values.emplace_back(object_value.data());

  // Get the name of the SQL statement to be executed.
  std::string statement_name;
  error = DbcUtils::find_statement_name(statement_names_select_equal_to,
                                        object_key, statement_name);
  if (error != ErrorCode::OK) {
    return error;
  }

  PGresult* res = nullptr;
  error =
      DbcUtils::exec_prepared(connection_, statement_name, param_values, res);

  if (error == ErrorCode::OK) {
    int nrows = PQntuples(res);
    if (nrows == 1) {
      int ordinal_position = 0;
      error = convert_pgresult_to_ptree(res, ordinal_position, object);
    } else if (nrows == 0) {
      // Convert the error code.
      if (object_key == Tables::ID) {
        error = ErrorCode::ID_NOT_FOUND;
      } else if (object_key == Tables::NAME) {
        error = ErrorCode::NAME_NOT_FOUND;
      } else {
        error = ErrorCode::NOT_FOUND;
      }
    } else {
      error = ErrorCode::INVALID_PARAMETER;
    }
  }

  PQclear(res);
  return error;
}

/**
 * @brief Executes a SELECT statement to get table metadata rows from the
 *   table metadata table.
 *   If the table metadata does not exist, return the container as empty.
 * @param (container)  [out] all table metadata.
 * @return ErrorCode::OK if success, otherwise an error code.
 */
ErrorCode TablesDAO::select_table_metadata(
    std::vector<boost::property_tree::ptree>& container) const {
  ErrorCode error = ErrorCode::UNKNOWN;
  std::vector<const char*> param_values;

  PGresult* res = nullptr;
  error = DbcUtils::exec_prepared(
      connection_, StatementName::TABLES_DAO_SELECT_TABLE_METADATA_ALL,
      param_values, res);

  if (error == ErrorCode::OK) {
    int nrows = PQntuples(res);
    if (nrows >= 0) {
      for (int ordinal_position = 0; ordinal_position < nrows;
           ordinal_position++) {
        ptree table;
        error = convert_pgresult_to_ptree(res, ordinal_position, table);
        if (error != ErrorCode::OK) {
          break;
        }
        container.emplace_back(table);
      }
    } else {
      error = ErrorCode::INVALID_PARAMETER;
    }
  }

  PQclear(res);
  return error;
}

/**
 * @brief Executes UPDATE statement to update the given number of rows
 *   into the table metadata table based on the given table id.
 * @param (reltuples)     [in]  the number of rows to update.
 * @param (object_key)    [in]  key. column name of a statistic table.
 * @param (object_value)  [in]  value to be filtered.
 * @param (table_id)      [out] table id of the row deleted.
 * @retval ErrorCode::OK if success,
 * @retval ErrorCode::ID_NOT_FOUND if the table id does not exist.
 * @retval ErrorCode::NAME_NOT_FOUND if the table name does not exist.
 * @retval otherwise an error code.
 */
ErrorCode TablesDAO::update_reltuples(float reltuples,
                                      std::string_view object_key,
                                      std::string_view object_value,
                                      ObjectIdType& table_id) const {
  ErrorCode error = ErrorCode::UNKNOWN;
  std::vector<char const*> param_values;

  std::string s_reltuples = std::to_string(reltuples);

  param_values.emplace_back(s_reltuples.c_str());
  param_values.emplace_back(object_value.data());

  // Get the name of the SQL statement to be executed.
  std::string statement_name;
  error = DbcUtils::find_statement_name(statement_names_update_reltuples,
                                        object_key, statement_name);
  if (error != ErrorCode::OK) {
    return error;
  }

  PGresult* res = nullptr;
  error =
      DbcUtils::exec_prepared(connection_, statement_name, param_values, res);
  if (error == ErrorCode::OK) {
    int nrows = PQntuples(res);
    if (nrows == 1) {
      int ordinal_position = 0;
      error = DbcUtils::str_to_integral<ObjectIdType>(
          PQgetvalue(res, ordinal_position, 0), table_id);
    } else if (nrows == 0) {
      // Convert the error code.
      if (object_key == Tables::ID) {
        error = ErrorCode::ID_NOT_FOUND;
      } else if (object_key == Tables::NAME) {
        error = ErrorCode::NAME_NOT_FOUND;
      } else {
        error = ErrorCode::NOT_FOUND;
      }
    } else {
      error = ErrorCode::INVALID_PARAMETER;
    }
  }

  PQclear(res);
  return error;
}

/**
 * @brief Executes DELETE statement to delete table metadata
 *   from the table metadata table based on the given table name.
 * @param (object_key)    [in]  key. column name of a table metadata table.
 * @param (object_value)  [in]  value to be filtered.
 * @param (table_id)      [out] table id of the row deleted.
 * @retval ErrorCode::OK if success.
 * @retval ErrorCode::ID_NOT_FOUND if the table id does not exist.
 * @retval ErrorCode::NAME_NOT_FOUND if the table name does not exist.
 * @retval otherwise an error code.
 */
ErrorCode TablesDAO::delete_table_metadata(std::string_view object_key,
                                           std::string_view object_value,
                                           ObjectIdType& table_id) const {
  ErrorCode error = ErrorCode::UNKNOWN;
  std::vector<const char*> param_values;

  param_values.emplace_back(object_value.data());

  // Get the name of the SQL statement to be executed.
  std::string statement_name;
  error = DbcUtils::find_statement_name(statement_names_delete_equal_to,
                                        object_key, statement_name);
  if (error != ErrorCode::OK) {
    return error;
  }

  PGresult* res = nullptr;
  error =
      DbcUtils::exec_prepared(connection_, statement_name, param_values, res);

  if (error == ErrorCode::OK) {
    uint64_t number_of_rows_affected = 0;
    ErrorCode error_get =
        DbcUtils::get_number_of_rows_affected(res, number_of_rows_affected);

    if (error_get != ErrorCode::OK) {
      error = error_get;
    } else if (number_of_rows_affected == 1) {
      int ordinal_position = 0;
      error = DbcUtils::str_to_integral<ObjectIdType>(
          PQgetvalue(res, ordinal_position, 0), table_id);
    } else if (number_of_rows_affected == 0) {
      // Convert the error code.
      if (object_key == Tables::ID) {
        error = ErrorCode::ID_NOT_FOUND;
      } else if (object_key == Tables::NAME) {
        error = ErrorCode::NAME_NOT_FOUND;
      } else {
        error = ErrorCode::NOT_FOUND;
      }
    } else {
      error = ErrorCode::INVALID_PARAMETER;
    }
  }

  PQclear(res);
  return error;
}

/* =============================================================================
 * Private method area
 */

/**
 * @brief Gets the ptree type table metadata
 *   converted from the given PGresult type value.
 * @param (res)               [in]  the result of a query.
 * @param (ordinal_position)  [in]  column ordinal position of PGresult.
 * @param (table)             [out] one table metadata.
 * @return ErrorCode::OK if success, otherwise an error code.
 */
ErrorCode TablesDAO::convert_pgresult_to_ptree(
    const PGresult* res, const int ordinal_position,
    boost::property_tree::ptree& table) const {
  ErrorCode error = ErrorCode::UNKNOWN;

  // Initialization.
  table.clear();

  // Set the value of the format_version column to ptree.
  table.put(Tables::FORMAT_VERSION,
            PQgetvalue(res, ordinal_position,
                       static_cast<int>(OrdinalPosition::kFormatVersion)));

  // Set the value of the generation column to ptree.
  table.put(Tables::GENERATION,
            PQgetvalue(res, ordinal_position,
                       static_cast<int>(OrdinalPosition::kGeneration)));

  // Set the value of the id column to ptree.
  table.put(Tables::ID, PQgetvalue(res, ordinal_position,
                                   static_cast<int>(OrdinalPosition::kId)));

  // Set the value of the name column to ptree.
  table.put(Tables::NAME, PQgetvalue(res, ordinal_position,
                                     static_cast<int>(OrdinalPosition::kName)));

  // Set the value of the namespace column to ptree.
  table.put(Tables::NAMESPACE,
            PQgetvalue(res, ordinal_position,
                       static_cast<int>(OrdinalPosition::kNamespace)));

  // Set the value of the primary_keys column to ptree.
  ptree primary_keys;
  // Converts a JSON string to a property_tree.
  error = Utility::json_to_ptree(
      PQgetvalue(res, ordinal_position,
                 static_cast<int>(OrdinalPosition::kPrimaryKey)),
      primary_keys);
  if (error != ErrorCode::OK) {
    return error;
  }
  // NOTICE:
  //   If it is not set, MUST add an empty ptree.
  //   ogawayama-server read key Tables::PRIMARY_KEY_NODE.
  table.add_child(Tables::PRIMARY_KEY_NODE, primary_keys);

  // Set the value of the tuples column to ptree.
  std::string tuples = PQgetvalue(res, ordinal_position,
                                  static_cast<int>(OrdinalPosition::kTuples));
  table.put(Tables::TUPLES, (tuples.empty() ? "0" : tuples.c_str()));

  // Set the value of the owner_role_id column to ptree.
  table.put(Tables::OWNER_ROLE_ID,
            PQgetvalue(res, ordinal_position,
                       static_cast<int>(OrdinalPosition::kOwnerRoleId)));

  // Set the value of the acl column to ptree.
  std::string acl_db_array = PQgetvalue(
      res, ordinal_position, static_cast<int>(OrdinalPosition::kAcl));
  std::regex regex("[{}]");
  acl_db_array = std::regex_replace(acl_db_array, regex, "");

  // Convert aclitem[] to ptree.
  ptree ptree_acls;
  for (const std::string acl : split(acl_db_array, ',')) {
    ptree p_value;
    p_value.put("", acl);
    ptree_acls.push_back(std::make_pair("", p_value));
  }
  // NOTICE:
  //   If it is not set, MUST add an empty ptree.
  //   ogawayama-server read key Tables::ACL.
  table.add_child(Tables::ACL, ptree_acls);

  error = ErrorCode::OK;
  return error;
}

/**
 * Split a string with a specified delimiter.
 * @param (source)     [in]  Source string to be split.
 * @param (delimiter)  [in]  Delimiter to split.
 * @return Vector of the result of the split.
 */
std::vector<std::string> TablesDAO::split(const std::string& source,
                                          const char& delimiter) const {
  std::vector<std::string> result;
  std::stringstream stream(source);
  std::string buffer;

  while (std::getline(stream, buffer, delimiter)) {
    result.push_back(buffer);
  }

  return result;
}

}  // namespace manager::metadata::db::postgresql
