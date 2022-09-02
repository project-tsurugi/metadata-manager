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

#include "jwt-cpp/jwt.h"
#include "manager/metadata/common/config.h"
#include "manager/metadata/common/jwt_claims.h"
#include "manager/metadata/helper/table_metadata_helper.h"
#include "manager/metadata/provider/datatypes_provider.h"
#include "manager/metadata/provider/roles_provider.h"
#include "manager/metadata/provider/tables_provider.h"

// =============================================================================
namespace {

std::unique_ptr<manager::metadata::db::TablesProvider> provider = nullptr;

}  // namespace

// =============================================================================
namespace manager::metadata {

using boost::property_tree::ptree;
using helper::TableMetadataHelper;

/**
 * @brief Constructor
 * @param (database)   [in]  database name.
 * @param (component)  [in]  component name.
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

  // Initialize the provider.
  error = provider->init();

  return error;
}

/**
 * @brief Add table metadata to table metadata table.
 * @param (object)  [in]  table metadata to add.
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
                      ObjectIdType* object_id) const {
  ErrorCode error = ErrorCode::UNKNOWN;

  // Parameter value check.
  error = param_check_metadata_add(object);
  if (error != ErrorCode::OK) {
    return error;
  }

  ObjectIdType retval_object_id = 0;
  // Adds the table metadata through the provider.
  error = provider->add_table_metadata(object, retval_object_id);

  // Set a value if object_id is not null.
  if ((error == ErrorCode::OK) && (object_id != nullptr)) {
    *object_id = retval_object_id;
  }

  return error;
}

/**
 * @brief Get table metadata.
 * @param (object_id)  [in]  table id.
 * @param (object)     [out] table metadata with the specified ID.
 * @retval ErrorCode::OK if success,
 * @retval ErrorCode::ID_NOT_FOUND if the table id does not exist.
 * @retval otherwise an error code.
 */
ErrorCode Tables::get(const ObjectIdType object_id,
                      boost::property_tree::ptree& object) const {
  ErrorCode error = ErrorCode::UNKNOWN;

  // Parameter value check.
  if (object_id <= 0) {
    error = ErrorCode::ID_NOT_FOUND;
    return error;
  }

  // Get the table metadata through the provider.
  std::string s_object_id = std::to_string(object_id);
  error = provider->get_table_metadata(Tables::ID, s_object_id, object);

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

  // Parameter value check.
  if (object_name.empty()) {
    error = ErrorCode::NAME_NOT_FOUND;
    return error;
  }

  // Get the table metadata through the provider.
  error = provider->get_table_metadata(Tables::NAME, object_name, object);

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

  // Get the table metadata through the provider.
  error = provider->get_table_metadata(container);

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

  // Parameter value check.
  if (table_id <= 0) {
    error = ErrorCode::ID_NOT_FOUND;
    return error;
  }

  // Get the table statistic through the provider.
  error = provider->get_table_statistic(Tables::ID, std::to_string(table_id),
                                        object);

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

  // Parameter value check.
  if (table_name.empty()) {
    error = ErrorCode::NAME_NOT_FOUND;
    return error;
  }

  // Get the table statistic through the provider.
  error = provider->get_table_statistic(Tables::NAME, table_name, object);

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

  // Parameter value check.
  error = param_check_statistic_update(object);
  if (error != ErrorCode::OK) {
    return error;
  }

  ObjectIdType retval_object_id = 0;
  // Adds or updates the table statistic through the provider.
  error = provider->set_table_statistic(object, retval_object_id);

  return error;
}

/**
 * @brief Remove all metadata-object based on the given table id
 *   (table metadata, column metadata and column statistics)
 *   from metadata-table (the table metadata table,
 *   the column metadata table and the column statistics table).
 * @param (object_id)  [in]  table id.
 * @retval ErrorCode::OK if success,
 * @retval ErrorCode::ID_NOT_FOUND if the table id does not exist.
 * @retval otherwise an error code.
 */
ErrorCode Tables::remove(const ObjectIdType object_id) const {
  ErrorCode error = ErrorCode::UNKNOWN;

  // Parameter value check.
  if (object_id <= 0) {
    error = ErrorCode::ID_NOT_FOUND;
    return error;
  }

  ObjectIdType retval_object_id = 0;
  // Remove the table metadata through the provider.
  error = provider->remove_table_metadata(Tables::ID, std::to_string(object_id),
                                          retval_object_id);

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

  // Parameter value check.
  if (object_name.empty()) {
    error = ErrorCode::NAME_NOT_FOUND;
    return error;
  }

  ObjectIdType retval_object_id = 0;
  // Remove the table metadata through the provider.
  error = provider->remove_table_metadata(Tables::NAME, object_name,
                                          retval_object_id);

  // Set a value if object_id is not null.
  if ((error == ErrorCode::OK) && (object_id != nullptr)) {
    *object_id = retval_object_id;
  }

  return error;
}

/**
 * @brief Gets a list of table access information for authenticated users.
 * @param (token) [in]  authentication token. See also AutheticationManager.
 * @param (acls)  [out] table access information.
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

  // Parameter value check.
  if (token.empty()) {
    error = ErrorCode::INVALID_PARAMETER;
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
    error = ErrorCode::INVALID_PARAMETER;
    return error;
  } catch (...) {
    error = ErrorCode::INVALID_PARAMETER;
    return error;
  }

  // Get the user name from the token.
  auto claim_user_name =
      decoded_token.get_payload_claim(Token::Payload::kAuthUserName);
  std::string user_name = claim_user_name.as_string();

  // Check for the presence of role name.
  {
    auto provider_roles = std::make_unique<db::RolesProvider>();

    // Initialize the provider.
    error = provider_roles->init();
    if (error != ErrorCode::OK) {
      return error;
    }

    ptree role_metadata;
    // Get the role metadata through the provider.
    error = provider_roles->get_role_metadata(Roles::ROLE_ROLNAME, user_name,
                                              role_metadata);
    if (error != ErrorCode::OK) {
      return error;
    }
  }

  // Get table metadata by table name.
  std::vector<ptree> container;
  // Get the table metadata through the provider.
  error = provider->get_table_metadata(container);

  if (error == ErrorCode::OK) {
    // Generate processing results.
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

  return error;
}

/**
 * @brief Gets whether the specified access permissions are included.
 * @param (object_id)     [in]  role id.
 * @param (permission)    [in]  permission.
 * @param (check_result)  [out] presence or absence of the specified
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

  // Parameter value check.
  if (object_id <= 0) {
    error = ErrorCode::ID_NOT_FOUND;
    return error;
  }

  // Get the table metadata through the provider.
  std::string s_object_id = std::to_string(object_id);
  error = provider->confirm_permission(Metadata::ID, s_object_id, permission,
                                       check_result);

  return error;
}

/**
 * @brief Gets whether or not the specified permissions have been granted.
 * @param (object_name)   [in]  role name.
 * @param (permission)    [in]  permissions.
 * @param (check_result)  [out] presence or absence of the specified
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

  // Parameter value check.
  if (object_name.empty()) {
    error = ErrorCode::NAME_NOT_FOUND;
    return error;
  }

  // Get the table metadata through the provider.
  error = provider->confirm_permission(Metadata::NAME, object_name, permission,
                                       check_result);

  return error;
}

// add
ptree transform_to_ptree(const Table& table)
{
  ptree ptree_table;

  // table metadata
  ptree_table.put<int64_t>(Tables::FORMAT_VERSION, table.format_version);
  ptree_table.put<int64_t>(Tables::GENERATION, table.generation);
  ptree_table.put<int64_t>(Tables::ID, table.id);
  ptree_table.put(Tables::NAMESPACE, table.namespace_name);
  ptree_table.put(Tables::NAME, table.name);
//  ptree_table.put<int64_t>(Tables::OWNER_ROLE_ID, table.owner_role_id);
//  ptree_table.put(Tables::ACL, table.acl);
  ptree_table.put<int64_t>(Tables::TUPLES, table.tuples);

  ptree child;
  
  // primary key
  ptree keys;
  for (const int64_t& ordinal_position : table.primary_keys) {
    keys.put("", ordinal_position);
    child.push_back(std::make_pair("", keys));
  }
  ptree_table.add_child(Tables::PRIMARY_KEY_NODE, child);
  
  // columns metadata
  ptree ptree_columns;
  for (const Column& column : table.columns) {
    ptree ptree_column;
    ptree_column.put<int64_t>(Tables::Column::ID, column.id);
    ptree_column.put<int64_t>(Tables::Column::TABLE_ID, column.table_id);
    ptree_column.put(Tables::Column::NAME, column.name);
    ptree_column.put<int64_t>(Tables::Column::ORDINAL_POSITION, column.ordinal_position);
    ptree_column.put<int64_t>(Tables::Column::DATA_TYPE_ID, column.data_type_id);  
    ptree_column.put<bool>(Tables::Column::VARYING, column.varying);  
    ptree_column.put<bool>(Tables::Column::NULLABLE, column.nullable);  
    ptree_column.put(Tables::Column::DEFAULT, column.default_expr);  
    ptree_column.put<int64_t>(Tables::Column::DIRECTION, column.direction);  

#if 0
    ptree data_length;
    for (const int64_t& param : column.data_length) {
      data_length.put("", param);
    }
    ptree_column.push_back(std::make_pair(Tables::Column::DATA_LENGTH, data_length));
#else
    ptree_column.put<int64_t>(Tables::Column::DATA_LENGTH, column.data_length);  
#endif
    ptree_columns.push_back(std::make_pair("", ptree_column));
  }

  ptree_table.add_child(Tables::COLUMNS_NODE, ptree_columns);

  return ptree_table;
}

ErrorCode Tables::add(const manager::metadata::Table& table,
                      ObjectIdType* object_id) const
{
  ptree table_tree = transform_to_ptree(table);
  ErrorCode error = this->add(table_tree, object_id);
  if (error != ErrorCode::OK) {
    return error;
  }

  return ErrorCode::OK;
}                

Table transform_from_ptree(const ptree& ptree_table)
{
  Table table;

  // table metadata
  auto format_version   = ptree_table.get_optional<int64_t>(Tables::FORMAT_VERSION);
  auto generation       = ptree_table.get_optional<int64_t>(Tables::GENERATION);
  auto id               = ptree_table.get_optional<int64_t>(Tables::ID);
  auto namespace_name   = ptree_table.get_optional<std::string>(Tables::NAMESPACE);
  auto name             = ptree_table.get_optional<std::string>(Tables::NAME);
//  auto owner_role_id    = ptree_table.get_optional<int64_t>(Tables::OWNER_ROLE_ID);
//  auto acl              = ptree_table.get_optional<std::string>(Tables::ACL);
  auto tuples           = ptree_table.get_optional<int64_t>(Tables::TUPLES);

  table.format_version = format_version.get();
  table.generation = generation.get();
  table.id = id.get();
  table.namespace_name = namespace_name.get();
  table.name = name.get();
//  table.owner_role_id = owner_role_id.get();
//  table.acl = acl.get();
  table.tuples = tuples.get();

  // primary keys
  BOOST_FOREACH (const ptree::value_type& node, ptree_table.get_child(Tables::PRIMARY_KEY_NODE)) {
    const ptree& value = node.second;
    auto ordinal_position = value.get_optional<int64_t>("");
    table.primary_keys.emplace_back(ordinal_position.get());
  }

  // columns metadata
  BOOST_FOREACH (const ptree::value_type& node, ptree_table.get_child(Tables::COLUMNS_NODE)) {
    const ptree& ptree_column = node.second;
    auto format_version = ptree_column.get_optional<int64_t>(Tables::Column::FORMAT_VERSION);
    auto generation     = ptree_column.get_optional<int64_t>(Tables::Column::GENERATION);
    auto id             = ptree_column.get_optional<int64_t>(Tables::Column::ID);
    auto table_id       = ptree_column.get_optional<int64_t>(Tables::Column::TABLE_ID);
    auto name           = ptree_column.get_optional<std::string>(Tables::Column::NAME);
    auto ordinal_position = ptree_column.get_optional<int64_t>(Tables::Column::ORDINAL_POSITION);
    auto data_type_id   = ptree_column.get_optional<int64_t>(Tables::Column::DATA_TYPE_ID);
    auto data_length    = ptree_column.get_optional<int64_t>(Tables::Column::DATA_LENGTH);
    auto varying        = ptree_column.get_optional<bool>(Tables::Column::VARYING);
    auto nullable       = ptree_column.get_optional<bool>(Tables::Column::NULLABLE);
    auto default_expr   = ptree_column.get_optional<std::string>(Tables::Column::DEFAULT);
    auto direction      = ptree_column.get_optional<int64_t>(Tables::Column::DIRECTION);

    Column column;

    id                ? column.id = id.get()                : column.id = 0;
    table_id          ? column.table_id = table_id.get()    : table_id = 0;
    name              ? column.name = name.get()            : column.name = "";
    varying           ? column.varying = varying.get()      : column.varying = 0;
    nullable          ? column.nullable = nullable.get()    : column.nullable = 0;
    direction         ? column.direction = direction.get()   : column.direction = 0;
    ordinal_position  ? column.ordinal_position = ordinal_position.get()  : column.ordinal_position = 0;
    data_type_id      ? column.data_type_id = data_type_id.get()          : column.data_type_id = 0;
    default_expr      ? column.default_expr = default_expr.get()          : column.default_expr = "";
#if 0
    BOOST_FOREACH (auto& node, ptree_column.get_child(Tables::Column::DATA_LENGTH)) {
      const ptree& value = node.second;
      auto ordinal_position = value.get_optional<int64_t>("");
      column.data_length.emplace_back(ordinal_position.get());
    }
#else
    data_length       ? column.data_length = data_length.get()            : column.data_length = 0;
#endif
    table.columns.emplace_back(column);
  }

  return table;
}

ErrorCode Tables::get(const ObjectIdType object_id,
                      manager::metadata::Table& table) const
{
  ptree table_tree;

  ErrorCode error = this->get(object_id, table_tree);
  if (error != ErrorCode::OK) {
    return error;
  }
  table = transform_from_ptree(table_tree);

  return ErrorCode::OK;
}

ErrorCode Tables::get(std::string_view object_name,
                      manager::metadata::Table& table) const
{

  ptree table_tree;

  ErrorCode error = this->get(object_name, table_tree);
  if (error != ErrorCode::OK) {
    return error;
  }
  table = transform_from_ptree(table_tree);

  return ErrorCode::OK;
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

  auto  table_name =
      object.get_optional<std::string>(Tables::NAME);
  if (!table_name || table_name.get().empty()) {
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
    auto  column_name =
        column.get_optional<std::string>(Tables::Column::NAME);
    if (!column_name || (column_name.get().empty())) {
      error = ErrorCode::INVALID_PARAMETER;
      break;
    }

    // ordinal position
    boost::optional<std::int64_t> ordinal_position =
        column.get_optional<std::int64_t>(Tables::Column::ORDINAL_POSITION);
    if (!ordinal_position || (ordinal_position.get() <= 0)) {
      error = ErrorCode::INVALID_PARAMETER;
      break;
    }

    // datatype id
    boost::optional<ObjectIdType> datatype_id =
        column.get_optional<ObjectIdType>(Tables::Column::DATA_TYPE_ID);
    if (!datatype_id || (datatype_id.get() < 0)) {
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
      break;
    }

    // nullable
    auto  nullable =
        column.get_optional<std::string>(Tables::Column::NULLABLE);
    if (!nullable || (nullable.get().empty())) {
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

  // id
  auto  optional_id =
      object.get_optional<std::string>(Tables::ID);
  // name
  auto  optional_name =
      object.get_optional<std::string>(Tables::NAME);
  // tuples
  boost::optional<float> optional_tuples =
      object.get_optional<float>(Tables::TUPLES);

  // Parameter value check.
  if ((optional_id || optional_name) && (optional_tuples)) {
    error = ErrorCode::OK;
  } else {
    error = ErrorCode::INVALID_PARAMETER;
  }

  return error;
}

}  // namespace manager::metadata
