/*
 * Copyright 2020-2021 tsurugi project.
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

#include <memory>
#include <boost/foreach.hpp>
#include <boost/format.hpp>
#include <jwt-cpp/jwt.h>

#include "manager/metadata/common/config.h"
#include "manager/metadata/common/jwt_claims.h"
#include "manager/metadata/common/message.h"
#include "manager/metadata/helper/logging_helper.h"
#include "manager/metadata/helper/table_metadata_helper.h"
#include "manager/metadata/provider/datatypes_provider.h"
#include "manager/metadata/provider/roles_provider.h"
#include "manager/metadata/provider/tables_provider.h"
#include "manager/metadata/helper/ptree_helper.h"

// =============================================================================
namespace {

std::unique_ptr<manager::metadata::db::TablesProvider> provider = nullptr;

}  // namespace

// =============================================================================
namespace manager::metadata {

using boost::property_tree::ptree;
using helper::TableMetadataHelper;

// ==========================================================================
// Column struct methods.
/** 
 * @brief  Transform column metadata from structure object to ptree object.
 * @return ptree object.
 */
boost::property_tree::ptree Column::convert_to_ptree() const
{
  auto pt = Object::convert_to_ptree();
  pt.put<ObjectId>(TABLE_ID,         this->table_id);
  pt.put<int64_t>(ORDINAL_POSITION,  this->ordinal_position);
  pt.put<ObjectId>(DATA_TYPE_ID,     this->data_type_id);
  pt.put<int64_t>(DATA_LENGTH,       this->data_length);
  pt.put<bool>(VARYING,              this->varying);
  pt.put<bool>(NULLABLE,             this->nullable);
  pt.put(DEFAULT_EXPR,               this->default_expr);
  pt.put<int64_t>(DIRECTION,         this->direction);
//  ptree params = ptree_helper::make_array_ptree(this->data_lengths);
//  pt.push_back(std::make_pair(DATA_LENGTHS, params));

  return pt;
}

/**
 * @brief   Transform column metadata from ptree object to structure object.
 * @param   pt [in] ptree object of metdata.
 * @return  structure object of metadata.
 */
void Column::convert_from_ptree(const boost::property_tree::ptree& pt)
{
  Object::convert_from_ptree(pt);
  auto opt_id = pt.get_optional<ObjectId>(TABLE_ID);
  this->table_id = opt_id ? opt_id.get() : INVALID_OBJECT_ID;

  auto opt_int = pt.get_optional<int64_t>(ORDINAL_POSITION);
  this->ordinal_position = opt_int ? opt_int.get() : INVALID_VALUE;

  opt_id = pt.get_optional<ObjectId>(DATA_TYPE_ID);
  this->data_type_id = opt_id ? opt_id.get() : INVALID_OBJECT_ID;

  opt_int = pt.get_optional<int64_t>(DATA_LENGTH);
  this->data_length = opt_int ? opt_int.get() : INVALID_VALUE;

//  this->data_lengths = ptree_helper::make_vector(pt, DATA_LENGTHS);

  auto opt_bool = pt.get_optional<bool>(VARYING);
  this->varying = opt_bool ? opt_bool.get() : INVALID_VALUE;

  opt_bool = pt.get_optional<bool>(NULLABLE);
  this->nullable = opt_bool ? opt_bool.get() : INVALID_VALUE;

  auto opt_str = pt.get_optional<std::string>(DEFAULT_EXPR);
  this->default_expr = opt_str ? opt_str.get() : "";

  opt_int = pt.get_optional<int64_t>(DIRECTION);
  this->direction = opt_int ? opt_int.get() : INVALID_VALUE;
}

// ==========================================================================
// Table struct methods.
/** 
 * @brief  Transform table metadata from structure object to ptree object.
 * @return ptree object.
 */
boost::property_tree::ptree Table::convert_to_ptree() const
{
  boost::property_tree::ptree ptree = ClassObject::convert_to_ptree();
  ptree.put(Table::NAMESPACE, namespace_name);
//  ptree.put<int64_t>(Table::OWNER_ROLE_ID, table.owner_id);
//  ptree.put(Table::ACL, table.acl);
  ptree.put<int64_t>(Table::TUPLES, tuples);

  boost::property_tree::ptree child;
  
  // primary keys
  boost::property_tree::ptree keys;
  for (const int64_t& ordinal_position : primary_keys) {
    keys.put("", ordinal_position);
    child.push_back(std::make_pair("", keys));
  }
  ptree.add_child(Tables::PRIMARY_KEY_NODE, child);
  
  // columns metadata
  boost::property_tree::ptree ptree_columns;
  for (const auto& column : columns) {
    boost::property_tree::ptree ptree = column.convert_to_ptree();
    ptree_columns.push_back(std::make_pair("", ptree));
  }
  ptree.add_child(Tables::COLUMNS_NODE, ptree_columns);

  return ptree;
}

/**
 * @brief   Transform table metadata from ptree object to structure object.
 * @param   ptree [in] ptree object of metdata.
 * @return  structure object of metadata.
 */
void Table::convert_from_ptree(const boost::property_tree::ptree& ptree)
{
  ClassObject::convert_from_ptree(ptree);
  auto namespace_name = ptree.get_optional<std::string>(Table::NAMESPACE);
  this->namespace_name = namespace_name ? namespace_name.get()  : "";

  auto tuples = ptree.get_optional<int64_t>(Table::TUPLES);
  this->tuples = tuples ? tuples.get() : INVALID_VALUE;

//auto owner_id = ptree.get_optional<int64_t>(Table::OWNER_ROLE_ID);
//table.owner_id = owner_id  ? owner_role_id.get() : INVALID_VALUE;

//auto acl = ptree.get_optional<std::string>(Table::ACL);
//table.acl= acl ? acl.get() : INVALID_VALUE;

  // primary keys
  BOOST_FOREACH (const auto& node, ptree.get_child(Tables::PRIMARY_KEY_NODE)) {
    const boost::property_tree::ptree& key = node.second;
    auto ordinal_position = key.get_optional<int64_t>("");
    primary_keys.emplace_back(ordinal_position.get());
  }

  // columns metadata
  BOOST_FOREACH (const auto& node, ptree.get_child(Tables::COLUMNS_NODE)) {
    const boost::property_tree::ptree& ptree_column = node.second;
    Column column;
    column.convert_from_ptree(ptree_column);
    columns.emplace_back(column);
  }
}

// ==========================================================================
// Tables class methods.
/**
 * @brief Constructor
 * @param database   [in]  database name.
 * @param component  [in]  component name.
 */
Tables::Tables(std::string_view database, std::string_view component)
    : Metadata(database, component) {
  // Create the provider.
  provider = std::make_unique<db::TablesProvider>();
}

/**
 * @brief Initialization.
 * @param none.
 * @return ErrorCode::OK if success, otherwise an error code.
 */
ErrorCode Tables::init() const {
  ErrorCode error = ErrorCode::UNKNOWN;

  // Log of API function start.
  log::function_start("Tables::init()");

  // Initialize the provider.
  error = provider->init();

  // Log of API function finish.
  log::function_finish("Tables::init()", error);

  return error;
}

/**
 * @brief Add table metadata to table metadata table.
 * @param object  [in]  table metadata to add.
 * @return ErrorCode::OK if success, otherwise an error code.
 */
ErrorCode Tables::add(const boost::property_tree::ptree& object) const {
  ErrorCode error = ErrorCode::UNKNOWN;

  // Adds the table metadata through the class method.
  error = add(object, nullptr);

  return error;
}

/**
 * @brief Add table metadata to table metadata table.
 * @param (object)      [in]  table metadata to add.
 * @param (object_id)   [out] ID of the added table metadata.
 * @return ErrorCode::OK if success, otherwise an error code.
 */
ErrorCode Tables::add(const boost::property_tree::ptree& object,
                      ObjectIdType* object_id) const 
{
  ErrorCode error = ErrorCode::UNKNOWN;

  // Log of API function start.
  log::function_start("Tables::add()");

  // Parameter value check.
  error = param_check_metadata_add(object);

  // Adds the table metadata through the provider.
  ObjectIdType retval_object_id = 0;
  if (error == ErrorCode::OK) {
    error = provider->add_table_metadata(object, retval_object_id);
  }

  // Set a value if object_id is not null.
  if ((error == ErrorCode::OK) && (object_id != nullptr)) {
    *object_id = retval_object_id;
  }

  // Log of API function finish.
  log::function_finish("Tables::add()", error);

  return error;
}

/**
 * @brief Get table metadata.
 * @param object_id  [in]  table id.
 * @param object     [out] table metadata with the specified ID.
 * @retval ErrorCode::OK if success,
 * @retval ErrorCode::ID_NOT_FOUND if the table id does not exist.
 * @retval otherwise an error code.
 */
ErrorCode Tables::get(const ObjectIdType object_id,
                      boost::property_tree::ptree& object) const {
  ErrorCode error = ErrorCode::UNKNOWN;

  // Log of API function start.
  log::function_start("Tables::get(TableId)");

  // Parameter value check.
  if (object_id > 0) {
    error = ErrorCode::OK;
  } else {
    LOG_WARNING
        << "An out-of-range value (0 or less) was specified for TableId.: "
        << object_id;
    error = ErrorCode::ID_NOT_FOUND;
  }

  // Get the table metadata through the provider.
  if (error == ErrorCode::OK) {
    std::string s_object_id = std::to_string(object_id);
    error = provider->get_table_metadata(Tables::ID, s_object_id, object);
  }

  // Log of API function finish.
  log::function_finish("Tables::get(TableId)", error);

  return error;
}

/**
 * @brief Get table metadata object based on table name.
 * @param (object_name)  [in]  table name. (Value of "name" key.)
 * @param (object)       [out] table metadata object with the specified name.
 * @retval ErrorCode::OK if success,
 * @retval ErrorCode::NAME_NOT_FOUND if the table name does not exist.
 * @retval otherwise an error code.
 */
ErrorCode Tables::get(std::string_view object_name,
                      boost::property_tree::ptree& object) const {
  ErrorCode error = ErrorCode::UNKNOWN;

  // Log of API function start.
  log::function_start("Tables::get(TableName)");

  // Parameter value check.
  if (!object_name.empty()) {
    error = ErrorCode::OK;
  } else {
    LOG_WARNING << "An empty value was specified for TableName.";
    error = ErrorCode::NAME_NOT_FOUND;
  }

  // Get the table metadata through the provider.
  if (error == ErrorCode::OK) {
    error = provider->get_table_metadata(Tables::NAME, object_name, object);
  }

  // Log of API function finish.
  log::function_finish("Tables::get(TableName)", error);

  return error;
}

/**
 * @brief Gets all table metadata object from the table metadata table.
 *   If the table metadata does not exist, return the container as empty.
 * @param (container)  [out] Container for metadata-objects.
 * @return ErrorCode::OK if success, otherwise an error code.
 */
ErrorCode Tables::get_all(
    std::vector<boost::property_tree::ptree>& container) const {
  ErrorCode error = ErrorCode::UNKNOWN;

  // Log of API function start.
  log::function_start("Tables::get_all()");

  // Get the table metadata through the provider.
  error = provider->get_table_metadata(container);

  // Log of API function finish.
  log::function_finish("Tables::get_all()", error);

  return error;
}

/**
 * @brief Gets one table statistic from the table metadata table
 *   based on the given table id.
 * @param (table_id)         [in]  table id.
 * @param (table_statistic)  [out] one table statistic
 *   with the specified table id.
 * @retval ErrorCode::OK if success,
 * @retval ErrorCode::ID_NOT_FOUND if the table id does not exist.
 * @retval otherwise an error code.
 */
ErrorCode Tables::get_statistic(const ObjectIdType table_id,
                                boost::property_tree::ptree& object) const {
  ErrorCode error = ErrorCode::UNKNOWN;

  // Log of API function start.
  log::function_start("Tables::get_statistic(TableId)");

  // Parameter value check.
  if (table_id > 0) {
    error = ErrorCode::OK;
  } else {
    LOG_WARNING
        << "An out-of-range value (0 or less) was specified for TableId.: "
        << table_id;
    error = ErrorCode::ID_NOT_FOUND;
  }

  // Get the table statistic through the provider.
  if (error == ErrorCode::OK) {
    error = provider->get_table_statistic(Tables::ID, std::to_string(table_id),
                                          object);
  }

  // Log of API function finish.
  log::function_finish("Tables::get_statistic(TableId)", error);

  return error;
}

/**
 * @brief Gets one table statistic from the table metadata table
 *   based on the given table name.
 * @param (table_name)       [in]  table name.
 * @param (table_statistic)  [out] one table statistic
 *   with the specified table name.
 * @retval ErrorCode::OK if success,
 * @retval ErrorCode::NAME_NOT_FOUND if the table name does not exist.
 * @retval otherwise an error code.
 */
ErrorCode Tables::get_statistic(std::string_view table_name,
                                boost::property_tree::ptree& object) const {
  ErrorCode error = ErrorCode::UNKNOWN;

  // Log of API function start.
  log::function_start("Tables::get_statistic(TableName)");

  // Parameter value check.
  if (!table_name.empty()) {
    error = ErrorCode::OK;
  } else {
    LOG_WARNING << "An empty value was specified for TableName.";
    error = ErrorCode::NAME_NOT_FOUND;
  }

  // Get the table statistic through the provider.
  if (error == ErrorCode::OK) {
    error = provider->get_table_statistic(Tables::NAME, table_name, object);
  }

  // Log of API function finish.
  log::function_finish("Tables::get_statistic(TableName)", error);

  return error;
}

/**
 * @brief Set table metadata table with the specified table statistics.
 * @param (object)  [in] Table statistic object.
 * @retval ErrorCode::OK if success,
 * @retval ErrorCode::ID_NOT_FOUND if the table id does not exist.
 * @retval ErrorCode::NAME_NOT_FOUND if the table name does not exist.
 * @retval otherwise an error code.
 */
ErrorCode Tables::set_statistic(boost::property_tree::ptree& object) const {
  ErrorCode error = ErrorCode::UNKNOWN;

  // Log of API function start.
  log::function_start("Tables::set_statistic()");

  // Parameter value check.
  error = param_check_statistic_update(object);

  // Adds or updates the table statistic through the provider.
  if (error == ErrorCode::OK) {
    ObjectIdType retval_object_id = 0;
    error = provider->set_table_statistic(object, retval_object_id);
  }

  // Log of API function finish.
  log::function_finish("Tables::set_statistic()", error);

  return error;
}

/**
 * @brief Update the metadata-table (table metadata table, column metadata
 *   table) based on the table ID with metadata objects.
 * @param object_id [in]  ID of the metadata-table to update.
 * @param object    [in]  metadata-object to update.
 * @return ErrorCode::OK if success, otherwise an error code.
 */
ErrorCode Tables::update(const ObjectIdType object_id,
                   const boost::property_tree::ptree& object) const {
  ErrorCode error = ErrorCode::UNKNOWN;

  // Parameter value check.
  if (object_id > 0) {
    error = param_check_metadata_add(object);
  } else {
    error = ErrorCode::ID_NOT_FOUND;
  }

  // Update the table metadata through the provider.
  if (error == ErrorCode::OK) {
    error = provider->update_table_metadata(object_id, object);
  }

  return error;
}

/**
 * @brief Remove all metadata-object based on the given table id
 *   (table metadata, column metadata and column statistics)
 *   from metadata-table (the table metadata table,
 *   the column metadata table and the column statistics table).
 * @param object_id  [in]  table id.
 * @retval ErrorCode::OK if success,
 * @retval ErrorCode::ID_NOT_FOUND if the table id does not exist.
 * @retval otherwise an error code.
 */
ErrorCode Tables::remove(const ObjectIdType object_id) const {
  ErrorCode error = ErrorCode::UNKNOWN;

  // Log of API function start.
  log::function_start("Tables::remove(TableId)");

  // Parameter value check.
  if (object_id > 0) {
    error = ErrorCode::OK;
  } else {
    LOG_WARNING
        << "An out-of-range value (0 or less) was specified for TableId.: "
        << object_id;
    error = ErrorCode::ID_NOT_FOUND;
  }

  // Remove the table metadata through the provider.
  if (error == ErrorCode::OK) {
    ObjectIdType retval_object_id = 0;
    error = provider->remove_table_metadata(
        Tables::ID, std::to_string(object_id), retval_object_id);
  }

  // Log of API function finish.
  log::function_finish("Tables::remove(TableId)", error);

  return error;
}

/**
 * @brief Remove all metadata-object based on the given table name
 *   (table metadata, column metadata and column statistics)
 *   from metadata-table (the table metadata table,
 *   the column metadata table and the column statistics table).
 * @param (object_name)  [in]  table name.
 * @param (object_id)    [out] object id of table removed.
 * @retval ErrorCode::OK if success,
 * @retval ErrorCode::NAME_NOT_FOUND if the table name does not exist.
 * @retval otherwise an error code.
 */
ErrorCode Tables::remove(std::string_view object_name,
                         ObjectIdType* object_id) const {
  ErrorCode error = ErrorCode::UNKNOWN;

  // Log of API function start.
  log::function_start("Tables::remove(TableName)");

  // Parameter value check.
  if (!object_name.empty()) {
    error = ErrorCode::OK;
  } else {
    LOG_WARNING << "An empty value was specified for TableName.";
    error = ErrorCode::NAME_NOT_FOUND;
  }

  ObjectIdType retval_object_id = 0;
  // Remove the table metadata through the provider.
  error = provider->remove_table_metadata(Tables::NAME, object_name,
                                          retval_object_id);

  // Set a value if object_id is not null.
  if ((error == ErrorCode::OK) && (object_id != nullptr)) {
    *object_id = retval_object_id;
  }

  // Log of API function finish.
  log::function_finish("Tables::remove(TableName)", error);

  return error;
}

/**
 * @brief Gets a list of table access information for authenticated users.
 * @param token [in]  authentication token. See also AutheticationManager.
 * @param acls  [out] table access information.
 * @retval ErrorCode::OK if success.
 * @retval ErrorCode::INVALID_PARAMETER if an invalid token is specified.
 * @retval ErrorCode::NAME_NOT_FOUND if the role name does not exist.
 * @retval ErrorCode::DATABASE_ACCESS_FAILURE if there is an access error to the
 * database.
 * @retval otherwise an error code.
 * @see AutheticationManager
 */
ErrorCode Tables::get_acls(std::string_view token,
                           boost::property_tree::ptree& acls) const {
  ErrorCode error = ErrorCode::UNKNOWN;

  // Log of API function start.
  log::function_start("Tables::get_acls()");

  // Parameter value check.
  if (!token.empty()) {
    error = ErrorCode::OK;
  } else {
    LOG_ERROR << Message::PARAMETER_FAILED << "Access token is empty.";
    error = ErrorCode::INVALID_PARAMETER;

    // Log of API function finish.
    log::function_finish("Tables::get_acls()", error);

    return error;
  }

  // Decode tokens.
  auto decoded_token = jwt::decode(std::string(token));
  // Cryptographic algorithms.
  auto algorithm = jwt::algorithm::hs256{Config::get_jwt_secret_key()};
  // Setting up data for verification.
  auto verifier = jwt::verify()
                      .allow_algorithm(algorithm)
                      .issued_at_leeway(Token::Leeway::kIssued)
                      .expires_at_leeway(Token::Leeway::kExpiration);

  try {
    // Verify the token.
    verifier.verify(decoded_token);
  } catch (jwt::error::token_verification_exception ex) {
    LOG_ERROR << Message::INVALID_TOKEN << ex.what();
    error = ErrorCode::INVALID_PARAMETER;
  } catch (...) {
    LOG_ERROR << Message::INVALID_TOKEN;
    error = ErrorCode::INVALID_PARAMETER;
  }

  // Get the user name from the token.
  std::string user_name;
  if (error == ErrorCode::OK) {
    auto claim_user_name =
        decoded_token.get_payload_claim(Token::Payload::kAuthUserName);
    user_name = claim_user_name.as_string();
  }

  // Check for the presence of role name.
  if (error == ErrorCode::OK) {
    auto provider_roles = std::make_unique<db::RolesProvider>();
    // Initialize the provider.
    error = provider_roles->init();

    if (error == ErrorCode::OK) {
      ptree role_metadata;
      // Get the role metadata through the provider.
      error = provider_roles->get_role_metadata(Roles::ROLE_ROLNAME, user_name,
                                                role_metadata);
    }
  }

  // Get table metadata by table name.
  std::vector<ptree> container;
  if (error == ErrorCode::OK) {
    // Get the table metadata through the provider.
    error = provider->get_table_metadata(container);
  }

  // Generate processing results.
  if (error == ErrorCode::OK) {
    ptree table_acls;
    for (const auto& table_metadata : container) {
      auto acl_list = table_metadata.get_child_optional(Tables::ACL);
      if (acl_list) {
        auto table_acl =
            TableMetadataHelper::get_table_acl(user_name, acl_list.get());
        if (!table_acl.empty()) {
          table_acls.put(table_metadata.get<std::string>(Tables::NAME, ""),
                        table_acl);
        }
      }
    }
    // Setting authorization information.
    acls.clear();
    acls.add_child(Tables::TABLE_ACL_NODE, table_acls);
  }

  // Log of API function finish.
  log::function_finish("Tables::get_acls()", error);

  return error;
}

/**
 * @brief Gets whether the specified access permissions are included.
 * @param object_id     [in]  role id.
 * @param permission    [in]  permission.
 * @param check_result  [out] presence or absence of the specified
 *   permissions.
 * @retval ErrorCode::OK if success.
 * @retval ErrorCode::NOT_FOUND if the foreign table does not exist.
 * @retval ErrorCode::ID_NOT_FOUND if the role id does not exist.
 * @retval otherwise an error code.
 */
ErrorCode Tables::confirm_permission_in_acls(const ObjectIdType object_id,
                                             const char* permission,
                                             bool& check_result) const {
  ErrorCode error = ErrorCode::UNKNOWN;

  // Log of API function start.
  log::function_start("Tables::confirm_permission_in_acls(RoleId)");

  // Parameter value check.
  if (object_id > 0) {
    error = ErrorCode::OK;
  } else {
    LOG_WARNING
        << "An out-of-range value (0 or less) was specified for RoleId.: "
        << object_id;
    error = ErrorCode::ID_NOT_FOUND;
  }

  // Get the table metadata through the provider.
  if (error == ErrorCode::OK) {
    std::string s_object_id = std::to_string(object_id);
    error = provider->confirm_permission(Metadata::ID, s_object_id, permission,
                                        check_result);
  }

  // Log of API function finish.
  log::function_finish("Tables::confirm_permission_in_acls(RoleId)", error);

  return error;
}

/**
 * @brief Gets whether or not the specified permissions have been granted.
 * @param object_name   [in]  role name.
 * @param permission    [in]  permissions.
 * @param check_result  [out] presence or absence of the specified
 *   permissions.
 * @retval ErrorCode::OK if success.
 * @retval ErrorCode::NOT_FOUND if the foreign table does not exist.
 * @retval ErrorCode::NAME_NOT_FOUND if the role name does not exist.
 * @retval otherwise an error code.
 */
ErrorCode Tables::confirm_permission_in_acls(std::string_view object_name,
                                             const char* permission,
                                             bool& check_result) const {
  ErrorCode error = ErrorCode::UNKNOWN;

  // Log of API function start.
  log::function_start("Tables::confirm_permission_in_acls(RoleName)");

  // Parameter value check.
  if (!object_name.empty()) {
    error = ErrorCode::OK;
  } else {
    LOG_WARNING << "An empty value was specified for RoleName.";
    error = ErrorCode::NAME_NOT_FOUND;
  }

  // Get the table metadata through the provider.
  if (error == ErrorCode::OK) {
    error = provider->confirm_permission(Metadata::NAME, object_name,
                                         permission, check_result);
  }

  // Log of API function finish.
  log::function_finish("Tables::confirm_permission_in_acls(RoleName)", error);

  return error;
}

/**
 *  structure interfaces.
 */

/**
 * @brief Add table metadata to table metadata table.
 * @param object    [in]  table metadata to add.
 * @param object_id [out] ID of the added table metadata.
 * @return ErrorCode::OK if success, otherwise an error code.
 */
ErrorCode Tables::add(const manager::metadata::Table& table,
                      ObjectIdType* object_id) const
{
  boost::property_tree::ptree ptree = table.convert_to_ptree();
  ErrorCode error = add(ptree, object_id);
  if (error != ErrorCode::OK) {
    return error;
  }

  return error;
}

/**
 * @brief Add table metadata to table metadata table.
 * @param object    [in]  table metadata to add.
 * @return ErrorCode::OK if success, otherwise an error code.
 */
ErrorCode Tables::add(const manager::metadata::Table& table) const
{
  ObjectId object_id = INVALID_OBJECT_ID;
  ErrorCode error = add(table, &object_id);
  if (error != ErrorCode::OK) {
    return error;
  }

  return error;
}

/**
 * @brief Get table metadata.
 * @param object_id [in]  table id.
 * @param table     [out] table metadata with the specified ID.
 * @retval ErrorCode::OK if success,
 * @retval ErrorCode::ID_NOT_FOUND if the table id does not exist.
 * @retval otherwise an error code.
 */
ErrorCode Tables::get(const ObjectIdType object_id,
                      manager::metadata::Table& table) const
{
  ptree ptree;

  ErrorCode error = get(object_id, ptree);
  if (error != ErrorCode::OK) {
    return error;
  }
  table.convert_from_ptree(ptree);

  return error;
}

/**
 * @brief Get table metadata object based on table name.
 * @param table_name  [in]  table name. (Value of "name" key.)
 * @param table       [out] table metadata object with the specified name.
 * @retval ErrorCode::OK if success,
 * @retval ErrorCode::NAME_NOT_FOUND if the table name does not exist.
 * @retval otherwise an error code.
 */
ErrorCode Tables::get(std::string_view table_name,
                      manager::metadata::Table& table) const
{

  ptree ptree;

  ErrorCode error = get(table_name, ptree);
  if (error != ErrorCode::OK) {
    return error;
  }
  table.convert_from_ptree(ptree);

  return error;
}

/* =============================================================================
 * Private method area
 */

/**
 * @brief Checks if the parameters for additional are correct.
 * @param (object)  [in]  metadata-object
 * @return ErrorCode::OK if success, otherwise an error code.
 */
ErrorCode Tables::param_check_metadata_add(
    const boost::property_tree::ptree& object) const {
  ErrorCode error = ErrorCode::UNKNOWN;
  constexpr const char* const kLogFormat = R"("%s" => undefined or empty)";

  auto table_name = object.get_optional<std::string>(Tables::NAME);
  if (!table_name || table_name.get().empty()) {
    LOG_ERROR << Message::PARAMETER_FAILED
              << (boost::format(kLogFormat) % Tables::NAME).str();

    error = ErrorCode::INVALID_PARAMETER;
    return error;
  }

  //
  // column metadata
  //
  error = ErrorCode::OK;
  BOOST_FOREACH (ptree::value_type node,
                 object.get_child(Tables::COLUMNS_NODE)) {
    auto& column = node.second;

    // name
    auto column_name = column.get_optional<std::string>(Tables::Column::NAME);
    if (!column_name || (column_name.get().empty())) {
      std::string column_name = "Column." + std::string(Tables::Column::NAME);
      LOG_ERROR << Message::PARAMETER_FAILED
                << (boost::format(kLogFormat) % column_name).str();

      error = ErrorCode::INVALID_PARAMETER;
      break;
    }

    // ordinal position
    boost::optional<std::int64_t> ordinal_position =
        column.get_optional<std::int64_t>(Tables::Column::ORDINAL_POSITION);
    if (!ordinal_position || (ordinal_position.get() <= 0)) {
      std::string column_name =
          "Column." + std::string(Tables::Column::ORDINAL_POSITION);
      LOG_ERROR << Message::PARAMETER_FAILED
                << (boost::format(kLogFormat) % column_name).str();

      error = ErrorCode::INVALID_PARAMETER;
      break;
    }

    // datatype id
    boost::optional<ObjectIdType> datatype_id =
        column.get_optional<ObjectIdType>(Tables::Column::DATA_TYPE_ID);
    if (!datatype_id || (datatype_id.get() < 0)) {
      std::string column_name =
          "Column." + std::string(Tables::Column::DATA_TYPE_ID);
      LOG_ERROR << Message::PARAMETER_FAILED
                << (boost::format(kLogFormat) % column_name).str();

      error = ErrorCode::INVALID_PARAMETER;
      break;
    }
    // DataTypes check provider.
    db::DataTypesProvider provider_data_types;
    error = provider_data_types.init();
    if (error != ErrorCode::OK) {
      break;
    }

    // Check the data types.
    boost::property_tree::ptree datatype_metadata;
    error = provider_data_types.get_datatype_metadata(
        DataTypes::ID, std::to_string(datatype_id.get()), datatype_metadata);
    if (error != ErrorCode::OK) {
      if (error == ErrorCode::ID_NOT_FOUND) {
        error = ErrorCode::INVALID_PARAMETER;
      }
      LOG_ERROR << Message::PARAMETER_FAILED
                << "DataTypes: " << std::to_string(datatype_id.get())
                << " => not found.";

      break;
    }

    // nullable
    auto nullable = column.get_optional<std::string>(Tables::Column::NULLABLE);
    if (!nullable || (nullable.get().empty())) {
      std::string column_name =
          "Column." + std::string(Tables::Column::NULLABLE);
      LOG_ERROR << Message::PARAMETER_FAILED
                << (boost::format(kLogFormat) % column_name).str();

      error = ErrorCode::INVALID_PARAMETER;
      break;
    }
  }

  return error;
}

/**
 * @brief Checks if the parameters for updating table statistics are correct.
 * @param (object)  [in]  metadata-object
 * @return ErrorCode::OK if success, otherwise an error code.
 */
ErrorCode Tables::param_check_statistic_update(
    const boost::property_tree::ptree& object) const {
  ErrorCode error = ErrorCode::UNKNOWN;
  constexpr const char* const kLogFormat = "%s => undefined or empty";

  // id
  auto optional_id = object.get_optional<std::string>(Tables::ID);
  // name
  auto optional_name = object.get_optional<std::string>(Tables::NAME);
  // tuples
  boost::optional<float> optional_tuples =
      object.get_optional<float>(Tables::TUPLES);

  // Parameter value check.
  if ((optional_id || optional_name) && (optional_tuples)) {
    error = ErrorCode::OK;
  } else {
    if (optional_id || optional_name) {
      LOG_ERROR << Message::PARAMETER_FAILED
                << (boost::format(R"("%s" or "%s" => undefined or empty)") %
                    Tables::ID % Tables::NAME)
                       .str();
    } else {
      LOG_ERROR << Message::PARAMETER_FAILED
                << (boost::format(R"("%s" => undefined or empty)") %
                    Tables::TUPLES)
                       .str();
    }
    error = ErrorCode::INVALID_PARAMETER;
  }

  return error;
}

}  // namespace manager::metadata
