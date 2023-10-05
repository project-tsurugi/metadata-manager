/*
 * Copyright 2020-2023 Project Tsurugi.
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

#include <regex>

#include <boost/foreach.hpp>
#include <boost/format.hpp>
#include "jwt-cpp/jwt.h"

#include "manager/metadata/common/config.h"
#include "manager/metadata/common/jwt_claims.h"
#include "manager/metadata/common/message.h"
#include "manager/metadata/common/utility.h"
#include "manager/metadata/helper/logging_helper.h"
#include "manager/metadata/helper/ptree_helper.h"
#include "manager/metadata/helper/table_metadata_helper.h"
#include "manager/metadata/provider/metadata_provider.h"
#include "manager/metadata/roles.h"
#include "manager/metadata/statistics.h"

// =============================================================================
namespace {

auto& provider = manager::metadata::db::MetadataProvider::get_instance();

}  // namespace

// =============================================================================
namespace manager::metadata {

using boost::property_tree::ptree;

// ==========================================================================
// Column struct methods.
/**
 * @brief  Transform column metadata from structure object to ptree object.
 * @return ptree object.
 */
boost::property_tree::ptree Column::convert_to_ptree() const {
  auto pt = this->base_convert_to_ptree();
  pt.put(TABLE_ID, this->table_id);
  pt.put(COLUMN_NUMBER, this->column_number);
  pt.put(DATA_TYPE_ID, this->data_type_id);
  pt.put(VARYING, this->varying);
  pt.put(IS_NOT_NULL, this->is_not_null);
  pt.put(DEFAULT_EXPR, this->default_expression);
  pt.put(IS_FUNCEXPR, this->is_funcexpr);
  pt.push_back(std::make_pair(
      DATA_LENGTH, ptree_helper::make_array_ptree(this->data_length)));

  return pt;
}

/**
 * @brief   Transform column metadata from ptree object to structure object.
 * @param   pt [in] ptree object of metadata.
 * @return  structure object of metadata.
 */
void Column::convert_from_ptree(const boost::property_tree::ptree& pt) {
  this->base_convert_from_ptree(pt);
  auto opt_table_id = pt.get_optional<ObjectId>(TABLE_ID);
  this->table_id    = opt_table_id.get_value_or(INVALID_OBJECT_ID);

  auto opt_column_number = pt.get_optional<int64_t>(COLUMN_NUMBER);
  this->column_number    = opt_column_number.get_value_or(INVALID_VALUE);

  auto opt_data_type_id = pt.get_optional<ObjectId>(DATA_TYPE_ID);
  this->data_type_id    = opt_data_type_id.get_value_or(INVALID_OBJECT_ID);

  this->data_length = ptree_helper::make_vector_int(pt, DATA_LENGTH);

  auto opt_varying = pt.get_optional<bool>(VARYING);
  this->varying    = opt_varying.get_value_or(false);

  auto opt_is_not_null = pt.get_optional<bool>(IS_NOT_NULL);
  this->is_not_null    = opt_is_not_null.get_value_or(false);

  auto opt_default_expression = pt.get_optional<std::string>(DEFAULT_EXPR);
  this->default_expression    = opt_default_expression.get_value_or("");

  auto opt_is_funcexpr = pt.get_optional<bool>(IS_FUNCEXPR);
  this->is_funcexpr    = opt_is_funcexpr.get_value_or(false);
}

// ==========================================================================
// Table struct methods.
/**
 * @brief  Transform table metadata from structure object to ptree object.
 * @return ptree object.
 */
boost::property_tree::ptree Table::convert_to_ptree() const {
  ptree pt = this->base_convert_to_ptree();
  pt.put<int64_t>(Table::NUMBER_OF_TUPLES, this->number_of_tuples);

  // columns metadata
  ptree ptree_columns;
  for (const auto& column : columns) {
    ptree child = column.convert_to_ptree();
    ptree_columns.push_back(std::make_pair("", child));
  }
  pt.add_child(Table::COLUMNS_NODE, ptree_columns);

  // constraints metadata
  ptree ptree_constraints;
  for (const auto& constraint : this->constraints) {
    ptree child = constraint.convert_to_ptree();
    ptree_constraints.push_back(std::make_pair("", child));
  }
  pt.add_child(Table::CONSTRAINTS_NODE, ptree_constraints);

  return pt;
}

/**
 * @brief   Transform table metadata from ptree object to structure object.
 * @param   ptree [in] ptree object of metadata.
 * @return  structure object of metadata.
 */
void Table::convert_from_ptree(const boost::property_tree::ptree& pt) {
  this->base_convert_from_ptree(pt);

  auto number_of_tuples  = pt.get_optional<int64_t>(Table::NUMBER_OF_TUPLES);
  this->number_of_tuples = number_of_tuples.get_value_or(INVALID_VALUE);

  // columns metadata
  this->columns.clear();
  BOOST_FOREACH (const auto& node, pt.get_child(Table::COLUMNS_NODE)) {
    const ptree& ptree_column = node.second;
    Column column;
    column.convert_from_ptree(ptree_column);
    this->columns.emplace_back(column);
  }

  // constraints metadata
  this->constraints.clear();
  BOOST_FOREACH (const auto& node, pt.get_child(Table::CONSTRAINTS_NODE)) {
    const ptree& ptree_constraint = node.second;

    Constraint constraint;
    constraint.convert_from_ptree(ptree_constraint);
    this->constraints.emplace_back(constraint);
  }
}

// ==========================================================================
// Tables class methods.
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
  error = provider.init();

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
 *   If object_id is not nullptr, store the id of the added metadata.
 * @param object     [in]  table metadata to add.
 * @param object_id  [out] ID of the added table metadata.
 * @return ErrorCode::OK if success, otherwise an error code.
 */
ErrorCode Tables::add(const boost::property_tree::ptree& object,
                      ObjectId* object_id) const {
  ErrorCode error = ErrorCode::UNKNOWN;

  // Log of API function start.
  log::function_start("Tables::add()");

  // Parameter value check.
  error = this->param_check_metadata_add(object);

  ObjectId added_oid = INVALID_OBJECT_ID;
  if (error == ErrorCode::OK) {
    // Add table metadata and related metadata simultaneously within a
    // transaction.
    error = provider.transaction([&object, &added_oid]() -> ErrorCode {
      ErrorCode result = ErrorCode::UNKNOWN;

      // Adds the table metadata through the provider.
      result = provider.add_table_metadata(object, &added_oid);
      if (result != ErrorCode::OK) {
        return result;
      }

      // Copy to the temporary area.
      ptree temp_object = object;
      auto opt_columns_node =
          temp_object.get_child_optional(Table::COLUMNS_NODE);
      auto opt_constraints_node =
          temp_object.get_child_optional(Table::CONSTRAINTS_NODE);

      // Set the table-id.
      for (auto& node : {opt_columns_node, opt_constraints_node}) {
        if (node) {
          BOOST_FOREACH (auto& item, node.get()) {
            // Set the table-id.
            item.second.put(Column::TABLE_ID, added_oid);
          }
        }
      }

      // Add column metadata object.
      if (opt_columns_node) {
        // Adds the column metadata through the provider.
        result = provider.add_column_metadata(opt_columns_node.get());
      }
      if (result != ErrorCode::OK) {
        return result;
      }

      // Adds the constraint metadata.
      if (opt_constraints_node) {
        // Adds the constraint metadata through the provider.
        result = provider.add_constraint_metadata(opt_constraints_node.get());
      }

      return result;
    });
  }

  // Set a value if object_id is not null.
  if ((error == ErrorCode::OK) && (object_id != nullptr)) {
    *object_id = added_oid;
  }

  // Log of API function finish.
  log::function_finish("Tables::add()", error);

  return error;
}

/**
 * @brief Get table metadata.
 * @param object_id  [in]  object id of the table metadata to retrieve.
 * @param object     [out] table metadata with the specified ID.
 * @retval ErrorCode::OK if success,
 * @retval ErrorCode::ID_NOT_FOUND if the table id does not exist.
 * @retval otherwise an error code.
 */
ErrorCode Tables::get(const ObjectId object_id,
                      boost::property_tree::ptree& object) const {
  ErrorCode error = ErrorCode::UNKNOWN;

  // Log of API function start.
  log::function_start("Tables::get(object_id)");

  // Parameter value check.
  if (object_id > 0) {
    error = ErrorCode::OK;
  } else {
    LOG_WARNING
        << "An out-of-range value (0 or less) was specified for object ID.: "
        << object_id;
    error = ErrorCode::ID_NOT_FOUND;
  }

  // Specify the key for the table metadata you want to retrieve.
  std::string table_id(std::to_string(object_id));
  std::map<std::string_view, std::string_view> keys = {
      {Table::ID, table_id}
  };

  // Get the table metadata through the provider.
  if (error == ErrorCode::OK) {
    LOG_INFO << "Retrieve table metadata: [" << keys << "]";

    ptree tmp_object;
    // Get the table metadata through the provider.
    error = provider.get_table_metadata(keys, tmp_object);
    if (error == ErrorCode::OK) {
      if (tmp_object.size() == 1) {
        // Copy the retrieved metadata.
        object = tmp_object.front().second;

        // Get metadata related to table metadata.
        error = this->get_related_metadata(object);
      } else {
        error = ErrorCode::RESULT_MULTIPLE_ROWS;
        LOG_WARNING << "Multiple rows retrieved.: " << keys << " exists "
                    << tmp_object.size() << " rows";
      }
    }
  }

  // Log of API function finish.
  log::function_finish("Tables::get(object_id)", error);

  return error;
}

/**
 * @brief Get table metadata object based on table name.
 * @param object_name  [in]  object name of the table metadata to retrieve.
 * @param object       [out] table metadata object with the specified name.
 * @retval ErrorCode::OK if success,
 * @retval ErrorCode::NAME_NOT_FOUND if the table name does not exist.
 * @retval otherwise an error code.
 */
ErrorCode Tables::get(std::string_view object_name,
                      boost::property_tree::ptree& object) const {
  ErrorCode error = ErrorCode::UNKNOWN;

  // Log of API function start.
  log::function_start("Tables::get(object_name)");

  // Parameter value check.
  if (!object_name.empty()) {
    error = ErrorCode::OK;
  } else {
    LOG_WARNING << "An empty value was specified for object name.";
    error = ErrorCode::NAME_NOT_FOUND;
  }

  // Specify the key for the table metadata you want to retrieve.
  std::map<std::string_view, std::string_view> keys = {
      {Table::NAME, object_name}
  };

  // Get the table metadata.
  if (error == ErrorCode::OK) {
    LOG_INFO << "Retrieve table metadata: [" << keys << "]";

    ptree tmp_object;
    // Get the table metadata through the provider.
    error = provider.get_table_metadata(keys, tmp_object);

    if (error == ErrorCode::OK) {
      if (tmp_object.size() == 1) {
        // Copy the retrieved metadata.
        object = tmp_object.front().second;

        // Get metadata related to table metadata.
        error = this->get_related_metadata(object);
      } else {
        error = ErrorCode::RESULT_MULTIPLE_ROWS;
        LOG_WARNING << "Multiple rows retrieved.: " << keys << " exists "
                    << tmp_object.size() << " rows";
      }
    }
  }

  // Log of API function finish.
  log::function_finish("Tables::get(object_name)", error);

  return error;
}

/**
 * @brief Gets all table metadata object from the table metadata table.
 *   If the table metadata does not exist, return the container as empty.
 * @param objects  [out] table metadata objects.
 * @return ErrorCode::OK if success, otherwise an error code.
 */
ErrorCode Tables::get_all(
    std::vector<boost::property_tree::ptree>& objects) const {
  ErrorCode error = ErrorCode::UNKNOWN;

  // Log of API function start.
  log::function_start("Tables::get_all()");

  ptree tables;
  std::map<std::string_view, std::string_view> keys = {};

  LOG_INFO << "Retrieve table metadata: [*]";

  // Get the table metadata through the provider.
  error = provider.get_table_metadata(keys, tables);

  if (error == ErrorCode::OK) {
    // Get columns and constraints metadata.
    BOOST_FOREACH (auto& object, tables) {
      auto& table = object.second;

      // Get metadata related to table metadata.
      error = this->get_related_metadata(table);
      if (error != ErrorCode::OK) {
        break;
      }
    }
  }

  // Converts object types.
  if (error == ErrorCode::OK) {
    objects = ptree_helper::array_to_vector(tables);
  } else if (error == ErrorCode::NOT_FOUND) {
    // Converts error code.
    error = ErrorCode::OK;
  }

  // Log of API function finish.
  log::function_finish("Tables::get_all()", error);

  return error;
}

/**
 * @brief Gets one table statistic from the table metadata table based on the
 *   given table id.
 * @param object_id  [in]  object id of the table metadata to retrieve.
 * @param object     [out] one table statistic with the specified id.
 * @retval ErrorCode::OK if success,
 * @retval ErrorCode::ID_NOT_FOUND if the table id does not exist.
 * @retval otherwise an error code.
 */
ErrorCode Tables::get_statistic(const ObjectId object_id,
                                boost::property_tree::ptree& object) const {
  ErrorCode error = ErrorCode::UNKNOWN;

  // Log of API function start.
  log::function_start("Tables::get_statistic(object_id)");

  // Parameter value check.
  if (object_id > 0) {
    error = ErrorCode::OK;
  } else {
    LOG_WARNING
        << "An out-of-range value (0 or less) was specified for object ID.: "
        << object_id;
    error = ErrorCode::ID_NOT_FOUND;
  }

  // Specify the key for the table metadata you want to retrieve.
  std::string table_id(std::to_string(object_id));
  std::map<std::string_view, std::string_view> keys = {
      {Table::ID, table_id}
  };

  ptree tables;
  if (error == ErrorCode::OK) {
    LOG_INFO << "Retrieve table metadata: [" << keys << "]";

    // Get the table metadata through the provider.
    error = provider.get_table_metadata(keys, tables);
  }

  if (error == ErrorCode::OK) {
    // Copy the retrieved metadata.
    object = tables.front().second;
  }

  // Log of API function finish.
  log::function_finish("Tables::get_statistic(object_id)", error);

  return error;
}

/**
 * @brief Gets one table statistic from the table metadata table based on the
 *   given table name.
 * @param table_name  [in]  object name of the table metadata to retrieve.
 * @param object      [out] one table statistic with the specified table name.
 * @retval ErrorCode::OK if success,
 * @retval ErrorCode::NAME_NOT_FOUND if the table name does not exist.
 * @retval otherwise an error code.
 */
ErrorCode Tables::get_statistic(std::string_view table_name,
                                boost::property_tree::ptree& object) const {
  ErrorCode error = ErrorCode::UNKNOWN;

  // Log of API function start.
  log::function_start("Tables::get_statistic(object_name)");

  // Parameter value check.
  if (!table_name.empty()) {
    error = ErrorCode::OK;
  } else {
    LOG_WARNING << "An empty value was specified for object name.";
    error = ErrorCode::NAME_NOT_FOUND;
  }

  // Specify the key for the table metadata you want to retrieve.
  std::map<std::string_view, std::string_view> keys = {
      {Table::NAME, table_name}
  };

  ptree tables;
  if (error == ErrorCode::OK) {
    LOG_INFO << "Retrieve table metadata: [" << keys << "]";

    // Get the table metadata through the provider.
    error = provider.get_table_metadata(keys, object);
  }

  if (error == ErrorCode::OK) {
    // Copy the retrieved metadata.
    object = tables.front().second;
  }

  // Log of API function finish.
  log::function_finish("Tables::get_statistic(object_name)", error);

  return error;
}

/**
 * @brief Set table metadata table with the specified table statistics.
 * @param object  [in] Table statistic object.
 * @retval ErrorCode::OK if success,
 * @retval ErrorCode::ID_NOT_FOUND if the table id does not exist.
 * @retval ErrorCode::NAME_NOT_FOUND if the table name does not exist.
 * @retval otherwise an error code.
 */
ErrorCode Tables::set_statistic(
    const boost::property_tree::ptree& object) const {
  ErrorCode error = ErrorCode::UNKNOWN;

  // Log of API function start.
  log::function_start("Tables::set_statistic()");

  // Parameter value check.
  error = this->param_check_statistic_update(object);

  uint64_t updated_rows = 0;
  std::map<std::string_view, std::string_view> keys;

  if (error == ErrorCode::OK) {
    // Specify the key for the table metadata you want to update.
    auto opt_table_id = object.get_optional<ObjectId>(Table::ID);
    if (opt_table_id) {
      // std::string table_id(std::to_string(opt_table_id.get()));
      keys[Table::ID] = std::to_string(opt_table_id.get());
    } else {
      auto opt_table_name = object.get_optional<std::string>(Table::NAME);
      keys[Table::NAME]   = opt_table_name.get_value_or("");
    }

    // Retrieve and update table metadata within a transaction.
    error = provider.transaction([this, &keys, &object,
                                  &updated_rows]() -> ErrorCode {
      ErrorCode result = ErrorCode::UNKNOWN;

      LOG_INFO << "Update table metadata: [" << keys << "]";

      ptree tables;
      // Get the table metadata through the provider.
      result = provider.get_table_metadata(keys, tables);
      if (result != ErrorCode::OK) {
        return result;
      }
      ptree& table = tables.front().second;

      // Set the updated value of number_of_tuples.
      auto opt_tuples = object.get_optional<int64_t>(Table::NUMBER_OF_TUPLES);
      table.put(Table::NUMBER_OF_TUPLES, opt_tuples.get());

      // Update table statistics to table metadata table.
      result = provider.update_table_metadata(keys, table, &updated_rows);

      return result;
    });
  }

  // Log of API function finish.
  log::function_finish("Tables::set_statistic()", error);

  return error;
}

/**
 * @brief Updates table metadata objects and related metadata objects
 *   (columns, constraints).
 * @param object_id [in]  object ID of the table metadata to update.
 * @param object    [in]  metadata-object to update.
 * @return ErrorCode::OK if success, otherwise an error code.
 */
ErrorCode Tables::update(const ObjectId object_id,
                         const boost::property_tree::ptree& object) const {
  ErrorCode error = ErrorCode::UNKNOWN;

  // Log of API function start.
  log::function_start("Tables::update()");

  // Parameter value check.
  if (object_id > 0) {
    error = this->param_check_metadata_add(object);
  } else {
    error = ErrorCode::ID_NOT_FOUND;
  }

  uint64_t updated_rows = 0;
  if (error == ErrorCode::OK) {
    // Update table metadata and related metadata simultaneously within a
    // transaction.
    error = provider.transaction(
        [this, &object_id, &object, &updated_rows]() -> ErrorCode {
          ErrorCode result = ErrorCode::UNKNOWN;
          std::string table_id(std::to_string(object_id));

          // Specify the key for the table metadata you want to update.
          std::map<std::string_view, std::string_view> keys = {
              {Table::ID, table_id}
          };
          LOG_INFO << "Update table metadata: [" << keys << "]";

          // Update the table metadata through the provider.
          result = provider.update_table_metadata(keys, object, &updated_rows);

          // Update metadata related to table metadata.
          if (result == ErrorCode::OK) {
            result = this->update_related_metadata(object_id, object);
          }

          return result;
        });
  }

  // Log of API function finish.
  log::function_finish("Tables::update()", error);

  return error;
}

/**
 * @brief Removes table metadata objects from the repository.
 *   Also removes all associated metadata objects (columns, indexes,
 *   constraints, column statistics) from the repository.
 * @param object_id  [in]  object ID of the table metadata to remove.
 * @retval ErrorCode::OK if success,
 * @retval ErrorCode::ID_NOT_FOUND if the table id does not exist.
 * @retval otherwise an error code.
 */
ErrorCode Tables::remove(const ObjectId object_id) const {
  ErrorCode error = ErrorCode::UNKNOWN;

  // Log of API function start.
  log::function_start("Tables::remove(object_id)");

  // Parameter value check.
  if (object_id > 0) {
    error = ErrorCode::OK;
  } else {
    LOG_WARNING
        << "An out-of-range value (0 or less) was specified for object ID.: "
        << object_id;
    error = ErrorCode::ID_NOT_FOUND;
  }

  // Remove the table metadata.
  if (error == ErrorCode::OK) {
    // Remove table metadata and related metadata simultaneously within a
    // transaction.
    error = provider.transaction([this, &object_id]() -> ErrorCode {
      ErrorCode result = ErrorCode::UNKNOWN;

      // Specify the key for the table metadata you want to remove.
      std::string table_id(std::to_string(object_id));
      std::map<std::string_view, std::string_view> keys = {
          {Table::ID, table_id}
      };
      LOG_INFO << "Remove table metadata: [" << keys << "]";

      // Remove the table metadata through the provider.
      result = provider.remove_table_metadata(keys);

      // Remove metadata related to table metadata.
      if (result == ErrorCode::OK) {
        result = this->remove_related_metadata(object_id);
      }

      return result;
    });
  }

  // Log of API function finish.
  log::function_finish("Tables::remove(object_id)", error);

  return error;
}

/**
 * @brief Removes table metadata objects from the repository.
 *   Also removes all associated metadata objects (columns, indexes,
 *   constraints, column statistics) from the repository.
 * @param object_name  [in]  object name of the table metadata to remove.
 * @param object_id    [out] object id of table metadata removed.
 * @retval ErrorCode::OK if success,
 * @retval ErrorCode::NAME_NOT_FOUND if the table name does not exist.
 * @retval otherwise an error code.
 */
ErrorCode Tables::remove(std::string_view object_name,
                         ObjectId* object_id) const {
  ErrorCode error = ErrorCode::UNKNOWN;

  // Log of API function start.
  log::function_start("Tables::remove(object_name)");

  // Parameter value check.
  if (!object_name.empty()) {
    error = ErrorCode::OK;
  } else {
    LOG_WARNING << "An empty value was specified for object name.";
    error = ErrorCode::NAME_NOT_FOUND;
  }

  std::vector<ObjectId> removed_ids = {};
  // Remove the table metadata.
  if (error == ErrorCode::OK) {
    // Remove table metadata and related metadata simultaneously within a
    // transaction.
    error =
        provider.transaction([this, &object_name, &removed_ids]() -> ErrorCode {
          ErrorCode result = ErrorCode::UNKNOWN;

          // Specify the key for the table metadata you want to remove.
          std::map<std::string_view, std::string_view> keys = {
              {Table::NAME, object_name}
          };
          LOG_INFO << "Update table metadata: [" << keys << "]";

          // Remove the table metadata through the provider.
          result = provider.remove_table_metadata(keys, &removed_ids);

          // Remove metadata related to table metadata.
          if (result == ErrorCode::OK) {
            result = this->remove_related_metadata(removed_ids.front());
          }

          return result;
        });
  }

  // Set a value if object_id is not null.
  if ((error == ErrorCode::OK) && (object_id != nullptr)) {
    *object_id = removed_ids.front();
  }

  // Log of API function finish.
  log::function_finish("Tables::remove(object_name)", error);

  return error;
}

/**
 * @brief Gets a list of table access information for authenticated users.
 * @param token  [in]  authentication token. See also AutheticationManager.
 * @param acls   [out] table access information.
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

  // Get the user name from the token.
  std::string user_name;
  error = get_token_user(token, user_name);

  // Get the table metadata.
  ptree tables;
  if (error == ErrorCode::OK) {
    error = provider.get_table_metadata({}, tables);
  }

  if (error == ErrorCode::OK) {
    ptree table_acls;
    // Generate processing results.
    BOOST_FOREACH (auto& node, tables) {
      auto& table = node.second;

      auto acl_list = table.get_child_optional(Table::ACL);
      if (acl_list) {
        auto table_acl =
            table_metadata_helper::get_table_acl(user_name, acl_list.get());
        if (!table_acl.empty()) {
          table_acls.put(table.get<std::string>(Table::NAME, ""), table_acl);
        }
      }
    }

    // Setting authorization information.
    acls.clear();
    acls.add_child(Table::TABLE_ACL_NODE, table_acls);
  }

  // Log of API function finish.
  log::function_finish("Tables::get_acls()", error);

  return error;
}

/**
 * @brief Gets whether the specified access permissions are included.
 * @param object_id     [in]  role id.
 * @param permission    [in]  permission.
 * @param check_result  [out] presence or absence of the specified permissions.
 * @retval ErrorCode::OK if success.
 * @retval ErrorCode::NOT_FOUND if the foreign table does not exist.
 * @retval ErrorCode::ID_NOT_FOUND if the role id does not exist.
 * @retval otherwise an error code.
 */
ErrorCode Tables::confirm_permission_in_acls(const ObjectId object_id,
                                             const char* permission,
                                             bool& check_result) const {
  ErrorCode error = ErrorCode::UNKNOWN;

  // Log of API function start.
  log::function_start("Tables::confirm_permission_in_acls(object_id)");

  // Parameter value check.
  error = this->param_check_permission(permission);

  // Parameter value check.
  if ((error == ErrorCode::OK) && (object_id <= 0)) {
    LOG_WARNING
        << "An out-of-range value (0 or less) was specified for RoleId.: "
        << object_id;
    error = ErrorCode::ID_NOT_FOUND;
  }

  ptree privileges;
  // Get the role privileges.
  if (error == ErrorCode::OK) {
    std::string role_id(std::to_string(object_id));

    // Specify the key for the role privileges you want to retrieve.
    std::map<std::string_view, std::string_view> keys = {
        {Roles::ROLE_OID, role_id}
    };
    LOG_INFO << "Retrieve role privileges: [" << keys << "]";

    // Get the role privileges through the provider.
    error = provider.get_privileges(keys, privileges);
  }

  // Checks for the presence of specified privileges.
  if (error == ErrorCode::OK) {
    check_result = this->check_of_privilege(privileges, permission);
  }

  // Log of API function finish.
  log::function_finish("Tables::confirm_permission_in_acls(object_id)", error);

  return error;
}

/**
 * @brief Gets whether or not the specified permissions have been granted.
 * @param object_name   [in]  role name.
 * @param permission    [in]  permissions.
 * @param check_result  [out] presence or absence of the specified permissions.
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
  log::function_start("Tables::confirm_permission_in_acls(object_name)");

  // Parameter value check.
  error = this->param_check_permission(permission);

  if ((error == ErrorCode::OK) && (object_name.empty())) {
    LOG_WARNING << "An empty value was specified for RoleName.";
    error = ErrorCode::NAME_NOT_FOUND;
  }

  ptree privileges;
  // Get the role privileges.
  if (error == ErrorCode::OK) {
    // Specify the key for the role privileges you want to retrieve.
    std::map<std::string_view, std::string_view> keys = {
        {Roles::ROLE_ROLNAME, object_name}
    };
    LOG_INFO << "Retrieve role privileges: [" << keys << "]";

    // Get the role privileges through the provider.
    error = provider.get_privileges(keys, privileges);
  }

  // Checks for the presence of specified privileges.
  if (error == ErrorCode::OK) {
    check_result = this->check_of_privilege(privileges, permission);
  }

  // Log of API function finish.
  log::function_finish("Tables::confirm_permission_in_acls(object_name)",
                       error);

  return error;
}

/* =============================================================================
 * Private method area
 */

/**
 * @brief Checks if the parameters for additional are correct.
 * @param object  [in]  metadata-object
 * @return ErrorCode::OK if success, otherwise an error code.
 */
ErrorCode Tables::param_check_metadata_add(
    const boost::property_tree::ptree& object) const {
  ErrorCode error = ErrorCode::UNKNOWN;

  constexpr const char* const kLogFormat = R"("%s" => undefined or empty)";

  auto table_name = object.get_optional<std::string>(Table::NAME);
  if (!table_name || table_name.get().empty()) {
    LOG_ERROR << Message::PARAMETER_FAILED
              << (boost::format(kLogFormat) % Table::NAME).str();

    error = ErrorCode::INSUFFICIENT_PARAMETERS;
    return error;
  }

  //
  // column metadata
  //
  error = ErrorCode::OK;
  BOOST_FOREACH (ptree::value_type node,
                 object.get_child(Table::COLUMNS_NODE)) {
    auto& column = node.second;

    // name
    auto column_name = column.get_optional<std::string>(Column::NAME);
    if (!column_name || (column_name.get().empty())) {
      std::string column_name = "Column." + std::string(Column::NAME);
      LOG_ERROR << Message::PARAMETER_FAILED
                << (boost::format(kLogFormat) % column_name).str();

      error = ErrorCode::INSUFFICIENT_PARAMETERS;
      break;
    }

    // column number
    boost::optional<std::int64_t> column_number =
        column.get_optional<std::int64_t>(metadata::Column::COLUMN_NUMBER);
    if (!column_number || (column_number.get() <= 0)) {
      std::string column_name =
          "Column." + std::string(metadata::Column::COLUMN_NUMBER);
      LOG_ERROR << Message::PARAMETER_FAILED
                << (boost::format(kLogFormat) % column_name).str();

      error = ErrorCode::INSUFFICIENT_PARAMETERS;
      break;
    }

    // datatype id
    boost::optional<ObjectId> opt_datatype_id =
        column.get_optional<ObjectId>(Column::DATA_TYPE_ID);
    if (!opt_datatype_id || (opt_datatype_id.get() < 0)) {
      std::string column_name = "Column." + std::string(Column::DATA_TYPE_ID);
      LOG_ERROR << Message::PARAMETER_FAILED
                << (boost::format(kLogFormat) % column_name).str();

      error = ErrorCode::INSUFFICIENT_PARAMETERS;
      break;
    }

    std::string datatype_id(std::to_string(opt_datatype_id.get()));
    // Specify the key for the table metadata you want to retrieve.
    std::map<std::string_view, std::string_view> keys = {
        {DataTypes::ID, datatype_id}
    };

    ptree datatype_metadata;
    // Check the data types.
    error = provider.get_datatype_metadata(keys, datatype_metadata);
    if (error != ErrorCode::OK) {
      break;
    } else if (datatype_metadata.empty()) {
      error = ErrorCode::INVALID_PARAMETER;
      LOG_ERROR << Message::PARAMETER_FAILED << "DataTypes: " << datatype_id
                << " => not found.";
      break;
    }

    // nullable
    auto nullable =
        column.get_optional<std::string>(metadata::Column::IS_NOT_NULL);
    if (!nullable || (nullable.get().empty())) {
      std::string column_name =
          "Column." + std::string(metadata::Column::IS_NOT_NULL);
      LOG_ERROR << Message::PARAMETER_FAILED
                << (boost::format(kLogFormat) % column_name).str();

      error = ErrorCode::INSUFFICIENT_PARAMETERS;
      break;
    }
  }

  return error;
}

/**
 * @brief Checks if the parameters for updating table statistics are correct.
 * @param object  [in]  metadata-object
 * @return ErrorCode::OK if success, otherwise an error code.
 */
ErrorCode Tables::param_check_statistic_update(
    const boost::property_tree::ptree& object) const {
  ErrorCode error = ErrorCode::UNKNOWN;

  constexpr const char* const kLogFormat = "%s => undefined or empty";

  // id
  auto optional_id = object.get_optional<std::string>(Table::ID);
  // name
  auto optional_name = object.get_optional<std::string>(Table::NAME);
  // number_of_tuples
  auto optional_tuples = object.get_optional<int64_t>(Table::NUMBER_OF_TUPLES);

  // Parameter value check.
  if ((optional_id || optional_name) && (optional_tuples)) {
    error = ErrorCode::OK;
  } else {
    if (optional_id || optional_name) {
      LOG_ERROR << Message::PARAMETER_FAILED
                << (boost::format(R"("%s" or "%s" => undefined or empty)") %
                    Table::ID % Table::NAME)
                       .str();
    } else {
      LOG_ERROR << Message::PARAMETER_FAILED
                << (boost::format(R"("%s" => undefined or empty)") %
                    Table::NUMBER_OF_TUPLES)
                       .str();
    }
    error = ErrorCode::INSUFFICIENT_PARAMETERS;
  }

  return error;
}

/**
 * @brief Checks if the parameters for confirm permission.
 * @param permission permission.
 * @return ErrorCode::OK if success, otherwise an error code.
 */
ErrorCode Tables::param_check_permission(std::string_view permission) const {
  ErrorCode error = ErrorCode::UNKNOWN;

  // Extracts the valid part of the given authentication string.
  std::string match_string(permission);
  std::smatch matcher;
  std::regex_match(match_string, matcher, std::regex(R"((.*=|)(.+)(/.*|))"));
  std::string check_permission(
      std::regex_replace(matcher[2].str(), std::regex(R"(\s)"), ""));

  if (check_permission.empty()) {
    LOG_ERROR << Message::INCORRECT_DATA << "The permission to check is empty.";
    error = ErrorCode::INVALID_PARAMETER;
    return error;
  }

  // Set regular expression for validation.
  std::regex regex_permission = std::regex(R"([rawdDxt])");

  error = ErrorCode::OK;
  // Checks the normality of the specified authorization string.
  for (char char_value : check_permission) {
    std::string perm_char(1, char_value);
    if (!std::regex_match(perm_char, regex_permission)) {
      LOG_ERROR << Message::INCORRECT_DATA
                << "The privilege format is incorrect.: "
                << "\"" << permission << "\"";
      error = ErrorCode::INVALID_PARAMETER;
      break;
    }
  }

  return error;
}

/**
 * @brief The token is verified, and if it is a valid token, the username is
 *   extracted and returned.
 * @param token      [in]  authentication token. See also AutheticationManager.
 * @param user_name  [out] user name.
 * @return ErrorCode::OK if success, otherwise an error code.
 */
ErrorCode Tables::get_token_user(std::string_view token,
                                 std::string& user_name) const {
  ErrorCode error = ErrorCode::UNKNOWN;

  // Parameter value check.
  if (token.empty()) {
    LOG_ERROR << Message::PARAMETER_FAILED << "Access token is empty.";
    error = ErrorCode::INVALID_PARAMETER;
    return error;
  }

  // Claim verification lambda.
  auto claim_verifier = [](const jwt::verify_context& ctx,
                           std::string_view regex) -> std::error_code {
    using verify_error = jwt::error::token_verification_error;
    std::error_code ec = verify_error::ok;

    // Get claims from jwt.
    auto claim_data = ctx.get_claim(false, ec);
    if (ec != verify_error::ok) {
      return ec;
    }

    // Check the type of the claim.
    if (claim_data.get_type() == jwt::json::type::string) {
      // Check the value of the claim.
      auto regex_results =
          std::regex_match(claim_data.as_string(), std::regex(regex.data()));
      ec = (regex_results ? verify_error::ok
                          : verify_error::claim_value_missmatch);
    } else {
      ec = verify_error::claim_type_missmatch;
    }
    return ec;
  };

  // Decode tokens.
  auto decoded_token = jwt::decode(std::string(token));
  // Cryptographic algorithms.
  auto algorithm = jwt::algorithm::hs256{Config::get_jwt_secret_key()};
  // Setting up data for verification.
  auto verifier = jwt::verify()
                      .allow_algorithm(algorithm)
                      .issued_at_leeway(token::Leeway::kIssued)
                      .expires_at_leeway(token::Leeway::kExpiration);
  // User name verification.
  verifier.with_claim(
      token::Payload::kAuthUserName,
      [&claim_verifier](const jwt::verify_context& ctx, std::error_code& ec) {
        // Claim verification.
        ec = claim_verifier(ctx, ".+");
      });

  // Token type verification.
  verifier.with_claim(
      token::Payload::kTokenType,
      [&claim_verifier](const jwt::verify_context& ctx, std::error_code& ec) {
        // Claim verification.
        ec = claim_verifier(ctx, token::TokenType::kAccess);
      });

  try {
    // Verify the token.
    verifier.verify(decoded_token);
    error = ErrorCode::OK;
  } catch (jwt::error::token_verification_exception ex) {
    LOG_ERROR << Message::INVALID_TOKEN << ex.what();
    error = ErrorCode::INVALID_PARAMETER;
  } catch (jwt::error::claim_not_present_exception ex) {
    LOG_ERROR << Message::INVALID_TOKEN << ex.what();
    error = ErrorCode::INVALID_PARAMETER;
  } catch (...) {
    LOG_ERROR << Message::INVALID_TOKEN;
    error = ErrorCode::INVALID_PARAMETER;
  }
  if (error != ErrorCode::OK) {
    return error;
  }

  // Get the user name from the token.
  auto claim_user_name =
      decoded_token.get_payload_claim(token::Payload::kAuthUserName);
  std::string temp_name(claim_user_name.as_string());

  // Specify the key for the role object you want to retrieve.
  std::map<std::string_view, std::string_view> keys = {
      {Roles::ROLE_ROLNAME, temp_name}
  };

  ptree roles;
  // Check for the presence of role name.
  error = provider.get_role_metadata(keys, roles);

  if (error == ErrorCode::OK) {
    if (roles.size() > 0) {
      LOG_INFO << "Role existed. [" << keys << "]";
      user_name = temp_name;
    } else {
      LOG_INFO << "Role do not exist. [" << keys << "]";
      error =
          db::MetadataProvider::get_not_found_error_code(Roles::ROLE_ROLNAME);
    }
  }

  return error;
}

/**
 * @brief Get metadata (column, constraint) that is related to the table
 *   metadata and add nodes to table metadata.
 * @param object  [in/out] table metadata object.
 * @return ErrorCode::OK if success, otherwise an error code.
 */
ErrorCode Tables::get_related_metadata(
    boost::property_tree::ptree& object) const {
  ErrorCode error = ErrorCode::UNKNOWN;

  // Extract the table-id.
  std::string table_id;
  auto opt_table_id = object.get_optional<ObjectId>(Table::ID);
  if (opt_table_id) {
    table_id = std::to_string(opt_table_id.value());
    error    = ErrorCode::OK;
  } else {
    error = ErrorCode::INTERNAL_ERROR;
  }

  // Get the column metadata.
  if (error == ErrorCode::OK) {
    // Specify the key for the column metadata you want to retrieve.
    std::map<std::string_view, std::string_view> keys = {
        {Column::TABLE_ID, table_id}
    };
    LOG_INFO << "Retrieve column metadata: [" << keys << "]";

    // Get column metadata.
    ptree columns;
    ErrorCode temp_error = provider.get_column_metadata(keys, columns);
    // In the case of a not-found error, it is considered normal.
    error = (db::MetadataProvider::is_not_found(temp_error) ? ErrorCode::OK
                                                            : temp_error);
    LOG_INFO << "Retrieve column metadata: [" << keys
             << "]=> ErrorCode:" << temp_error << "->" << error;

    // Add the child node data to the table metadata.
    object.add_child(Table::COLUMNS_NODE, columns);
  }

  // Get the constraint metadata.
  if (error == ErrorCode::OK) {
    // Specify the key for the constraint metadata you want to retrieve.
    std::map<std::string_view, std::string_view> keys = {
        {Constraint::TABLE_ID, table_id}
    };
    LOG_INFO << "Retrieve constraint metadata: [" << keys << "]";

    // Get constraint metadata.
    ptree constraint;
    ErrorCode temp_error = provider.get_constraint_metadata(keys, constraint);
    // In the case of a not-found error, it is considered normal.
    error = (db::MetadataProvider::is_not_found(temp_error) ? ErrorCode::OK
                                                            : temp_error);
    LOG_INFO << "Retrieve constraint metadata: [" << keys
             << "]=> ErrorCode:" << temp_error << "->" << error;

    // Add the child node data to the table metadata.
    object.add_child(Table::CONSTRAINTS_NODE, constraint);
  }

  return error;
}
/**
 * @brief Update metadata (column, constraint) that is related to the
 *   table metadata.
 * @param object_id  [in]  table ID to be updated.
 * @param object     [in]  table metadata object.
 * @return ErrorCode::OK if success, otherwise an error code.
 */
ErrorCode Tables::update_related_metadata(
    const ObjectId object_id, const boost::property_tree::ptree& object) const {
  ErrorCode error = ErrorCode::UNKNOWN;
  std::string table_id(std::to_string(object_id));

  // Update metadata object to column metadata table.
  auto opt_columns_node = object.get_child_optional(Table::COLUMNS_NODE);
  if (opt_columns_node) {
    // Specify the key for the column metadata you want to remove.
    std::map<std::string_view, std::string_view> keys = {
        {Column::TABLE_ID, table_id}
    };

    // Update a metadata object from the column metadata table.
    error = provider.update_column_metadata(keys, opt_columns_node.get());
    if (error != ErrorCode::OK) {
      return error;
    }
  }

  // Update metadata object to constraint metadata table.
  auto opt_constraints_node =
      object.get_child_optional(Table::CONSTRAINTS_NODE);
  if (opt_constraints_node) {
    // Specify the key for the column metadata you want to remove.
    std::map<std::string_view, std::string_view> keys = {
        {Constraint::TABLE_ID, table_id}
    };

    // Update a metadata object from the constraint metadata table.
    error =
        provider.update_constraint_metadata(keys, opt_constraints_node.get());
    if (error != ErrorCode::OK) {
      return error;
    }
  }

  error = ErrorCode::OK;
  return error;
}

/**
 * @brief Remove all metadata (column, index, constraint, statistic) that is
 *   related to the table metadata.
 * @param object_id  [in] table ID to be removed.
 * @return ErrorCode::OK if success, otherwise an error code.
 */
ErrorCode Tables::remove_related_metadata(const ObjectId object_id) const {
  ErrorCode error = ErrorCode::UNKNOWN;
  std::string table_id(std::to_string(object_id));

  // Remove the column metadata.
  {
    // Specify the key for the column metadata you want to remove.
    std::map<std::string_view, std::string_view> keys = {
        {Column::TABLE_ID, table_id}
    };
    LOG_INFO << "Remove column metadata: [" << keys << "]";

    // Remove column metadata.
    error = provider.remove_column_metadata(keys);
    // In the case of a not-found error, it is considered normal.
    error = (db::MetadataProvider::is_not_found(error) ? ErrorCode::OK : error);
  }

  // Remove the index metadata.
  if (error == ErrorCode::OK) {
    // Specify the key for the index metadata you want to remove.
    std::map<std::string_view, std::string_view> keys = {
        {Index::TABLE_ID, table_id}
    };
    LOG_INFO << "Remove index metadata: [" << keys << "]";

    // Remove constraint metadata.
    error = provider.remove_index_metadata(keys);
    // In the case of a not-found error, it is considered normal.
    error = (db::MetadataProvider::is_not_found(error) ? ErrorCode::OK : error);
  }

  // Remove the constraint metadata.
  if (error == ErrorCode::OK) {
    // Specify the key for the constraint metadata you want to remove.
    std::map<std::string_view, std::string_view> keys = {
        {Constraint::TABLE_ID, table_id}
    };
    LOG_INFO << "Remove constraint metadata: [" << keys << "]";

    // Remove constraint metadata.
    error = provider.remove_constraint_metadata(keys);
    // In the case of a not-found error, it is considered normal.
    error = (db::MetadataProvider::is_not_found(error) ? ErrorCode::OK : error);
  }

  // Remove the column statistics.
  if (error == ErrorCode::OK) {
    // Specify the key for the column statistics you want to remove.
    std::map<std::string_view, std::string_view> keys = {
        {Statistics::TABLE_ID, table_id}
    };
    LOG_INFO << "Remove column statistic: [" << keys << "]";

    // Remove column statistic.
    error = provider.remove_column_statistics(keys);
    // In the case of a not-found error, it is considered normal.
    error = (db::MetadataProvider::is_not_found(error) ? ErrorCode::OK : error);
  }

  return error;
}

/**
 * @brief Checks for the presence of specified privileges.
 * @param object      [in]  ptree of table privileges.
 * @param permission  [in]  permissions to check.
 * @retval ErrorCode::OK if success.
 * @retval otherwise an error code.
 */
bool Tables::check_of_privilege(const boost::property_tree::ptree& object,
                                const std::string& permission) const {
  auto check_result = true;

  for (auto iterator = object.begin(); iterator != object.end(); iterator++) {
    if (iterator->second.empty()) {
      break;
    }

    auto iterator_child = iterator->second.begin();
    if (iterator_child->second.empty()) {
      auto table_name       = iterator->first;
      auto table_privileges = iterator->second;

      LOG_DEBUG << "Check table privileges: [" << table_name << "]["
                << permission << "]";
      for (char char_value : permission) {
        std::string perm_char(1, char_value);
        auto opt_value = table_privileges.get_optional<std::string>(perm_char);

        check_result = Utility::str_to_boolean(opt_value.get_value_or(""));
        LOG_DEBUG << "=>[" << perm_char << "] was "
                  << opt_value.get_value_or("");

        if (!check_result) {
          break;
        }
      }
      LOG_INFO << "Check table privileges: [" << table_name << "]"
               << " [" << permission << "]"
               << " => " << Utility::boolean_to_str(check_result);
    } else {
      check_result = check_of_privilege(iterator->second, permission);
    }
    if (!check_result) {
      break;
    }
  }

  return check_result;
}

}  // namespace manager::metadata
