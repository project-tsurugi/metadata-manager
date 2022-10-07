/*
 * Copyright 2021-2022 tsurugi project.
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
#include "manager/metadata/dao/json/constraints_dao_json.h"

#include <boost/foreach.hpp>
#include <boost/format.hpp>
#include <boost/property_tree/ptree.hpp>

#include "manager/metadata/common/config.h"
#include "manager/metadata/common/message.h"
#include "manager/metadata/dao/json/object_id_json.h"
#include "manager/metadata/helper/logging_helper.h"
#include "manager/metadata/tables.h"

// =============================================================================
namespace {

std::unique_ptr<manager::metadata::db::json::ObjectId> object_id = nullptr;

}  // namespace

// =============================================================================
namespace manager::metadata::db::json {

using boost::property_tree::ptree;

/**
 * @brief Prepare to access the JSON file of constraint metadata.
 * @param none.
 * @return ErrorCode::OK if success, otherwise an error code.
 */
ErrorCode ConstraintsDAO::prepare() const {
  ErrorCode error = ErrorCode::UNKNOWN;

  // Filename of the constraint metadata.
  boost::format filename = boost::format("%s/%s.json") % Config::get_storage_dir_path() %
                           std::string(ConstraintsDAO::CONSTRAINTS_METADATA_NAME);

  // Connect to the constraint metadata file.
  error = session_manager_->connect(filename.str(), ConstraintsDAO::ROOT_NODE);

  // Create the ObjectId.
  object_id = std::make_unique<ObjectId>();

  return error;
}

/**
 * @brief Add metadata object to metadata constraint file.
 * @param constraint_metadata  [in]  one constraint metadata to add.
 * @param constraint_id        [out] constraint id.
 * @return ErrorCode::OK if success, otherwise an error code.
 */
ErrorCode ConstraintsDAO::insert_constraint_metadata(
    const boost::property_tree::ptree& constraint_metadata, ObjectIdType& constraint_id) const {
  ErrorCode error = ErrorCode::UNKNOWN;

  // Load the metadata from the JSON file.
  error = session_manager_->load_object();
  if (error != ErrorCode::OK) {
    return error;
  }

  // Getting a metadata container.
  ptree* container = session_manager_->get_container();

  // Getting a table id.
  auto optional_table_id = constraint_metadata.get_optional<ObjectIdType>(Constraint::TABLE_ID);
  std::string table_id   = std::to_string(optional_table_id.value());

  ptree::iterator table_it;
  ptree& root_node = container->get_child(ConstraintsDAO::ROOT_NODE);
  for (table_it = root_node.begin(); table_it != root_node.end(); table_it++) {
    auto value = table_it->second.get_optional<std::string>(Tables::ID);
    if (value && (value.get() == table_id)) {
      break;
    }
  }

  if (table_it == root_node.end()) {
    LOG_INFO << "Table metadata for the specified table ID does not exist.: " << table_id;
    error == ErrorCode::NOT_FOUND;
    return error;
  }

  ptree empty_ptree;
  ptree& table_metadata = table_it->second;
  if (table_metadata.find(Tables::CONSTRAINTS_NODE) == table_metadata.not_found()) {
    table_metadata.add_child(Tables::CONSTRAINTS_NODE, empty_ptree);
  }
  auto& constraints_node = table_metadata.get_child(Tables::CONSTRAINTS_NODE);

  // Copy to the temporary area.
  ptree tmp_metadata = constraint_metadata;

  // Checks for INSERT execution with object-id specified.
  auto optional_object_id = constraint_metadata.get_optional<ObjectIdType>(Constraint::ID);
  if (optional_object_id.value_or(-1) > 0) {
    // Constraint ID is specified, the metadata for that constraint ID is updated.
    constraint_id = object_id->update(OID_KEY_NAME_CONSTRAINT, optional_object_id.value());
    LOG_INFO << "Add constraint metadata with specified constraint ID. ConstraintID: "
             << optional_object_id.value();
  } else {
    // Constraint ID is not specified, the constraint ID is generated and metadata is added.
    constraint_id = object_id->generate(OID_KEY_NAME_CONSTRAINT);
    tmp_metadata.put(Constraint::ID, constraint_id);
  }

  // columns
  if (tmp_metadata.find(Constraint::COLUMNS) == tmp_metadata.not_found()) {
    tmp_metadata.add_child(Constraint::COLUMNS, empty_ptree);
  }
  // columnsId
  if (tmp_metadata.find(Constraint::COLUMNS_ID) == tmp_metadata.not_found()) {
    tmp_metadata.add_child(Constraint::COLUMNS_ID, empty_ptree);
  }

  // Add new element.
  constraints_node.push_back(std::make_pair("", tmp_metadata));

  error = ErrorCode::OK;

  return error;
}

/**
 * @brief Get metadata object from a metadata constraint file.
 * @param object_key           [in]  key. column name of a constraint metadata table.
 * @param object_value         [in]  value to be filtered.
 * @param constraint_metadata  [out] constraint metadata to get, where the given key equals the
 *   given value.
 * @retval ErrorCode::OK if success,
 * @retval ErrorCode::ID_NOT_FOUND if the constraint id does not exist.
 * @retval otherwise an error code.
 */
ErrorCode ConstraintsDAO::select_constraint_metadata(
    std::string_view object_key, std::string_view object_value,
    boost::property_tree::ptree& constraint_metadata) const {
  ErrorCode error = ErrorCode::UNKNOWN;

  // Load the meta data from the JSON file.
  error = session_manager_->load_object();
  if (error != ErrorCode::OK) {
    return error;
  }

  // Getting a metadata container.
  ptree* container = session_manager_->get_container();

  // Getting a metadata object.
  error = get_constraint_metadata_object(*container, object_key, object_value, constraint_metadata);

  // Convert the error code.
  if (error == ErrorCode::NOT_FOUND) {
    if (object_key == Tables::ID) {
      error = ErrorCode::ID_NOT_FOUND;
    } else {
      error = ErrorCode::NOT_FOUND;
    }
  }

  return error;
}

/**
 * @brief Get all metadata objects from a metadata constraint file.
 *   If the constraint metadata does not exist, return the container as empty.
 * @param constraint_container  [out] all constraint metadata.
 * @return ErrorCode::OK if success, otherwise an error code.
 */
ErrorCode ConstraintsDAO::select_constraint_metadata(
    std::vector<boost::property_tree::ptree>& constraint_container) const {
  ErrorCode error = ErrorCode::UNKNOWN;

  // Load the meta data from the JSON file.
  error = session_manager_->load_object();
  if (error != ErrorCode::OK) {
    return error;
  }

  // Initialize constraint metadata list
  constraint_container = {};

  // Getting a metadata container.
  ptree* container = session_manager_->get_container();

  // constraint metadata
  BOOST_FOREACH (ptree::value_type& root_node, container->get_child(ConstraintsDAO::ROOT_NODE)) {
    ptree& table = root_node.second;

    // Convert from ptree structure type to vector<ptree>.
    auto constraint_node = table.get_child(Tables::CONSTRAINTS_NODE);
    std::transform(constraint_node.begin(), constraint_node.end(),
                   std::back_inserter(constraint_container),
                   [](ptree::value_type v) { return v.second; });
  }

  return error;
}

/**
 * @brief Delete a metadata object from a metadata constraint file.
 * @param object_key    [in]  key. column name of a constraint metadata table.
 * @param object_value  [in]  value to be filtered.
 * @retval ErrorCode::OK if success.
 * @retval ErrorCode::ID_NOT_FOUND if the constraint id does not exist.
 * @retval otherwise an error code.
 */
ErrorCode ConstraintsDAO::delete_constraint_metadata(std::string_view object_key,
                                                     std::string_view object_value) const {
  ErrorCode error = ErrorCode::UNKNOWN;

  // Load the meta data from the JSON file.
  error = session_manager_->load_object();
  if (error != ErrorCode::OK) {
    return error;
  }

  // Getting a metadata container.
  ptree* container = session_manager_->get_container();

  // Delete a metadata object.
  error = this->delete_metadata_object(*container, object_key, object_value);

  return error;
}

/* =============================================================================
 * Private method area
 */

/**
 * @brief Get metadata-object.
 * @param container            [in]  metadata container.
 * @param object_key           [in]  key. column name of a constraint metadata table.
 * @param object_value         [in]  value to be filtered.
 * @param constraint_metadata  [out] metadata-object with the specified name.
 * @return ErrorCode::OK if success, otherwise an error code.
 */
ErrorCode ConstraintsDAO::get_constraint_metadata_object(
    const boost::property_tree::ptree& container, std::string_view object_key,
    std::string_view object_value, boost::property_tree::ptree& constraint_metadata) const {
  ErrorCode error = ErrorCode::UNKNOWN;

  LOG_DEBUG << "get_metadata metadata:" << (container.empty() ? "empty" : "exists") << " \""
            << object_key << "\"=\"" << object_value << "\"";

  error = ErrorCode::NOT_FOUND;
  BOOST_FOREACH (const ptree::value_type& root_node,
                 container.get_child(ConstraintsDAO::ROOT_NODE)) {
    const ptree& table = root_node.second;
    if (table.find(Tables::CONSTRAINTS_NODE) != table.not_found()) {
      BOOST_FOREACH (const ptree::value_type& constraint_node,
                     table.get_child(Tables::CONSTRAINTS_NODE)) {
        const ptree& constraint = constraint_node.second;

        auto value = constraint.get_optional<std::string>(object_key.data());
        if (value && (value.get() == object_value)) {
          error = ErrorCode::OK;
          // copy.
          constraint_metadata = constraint;
          break;
        }
      }
    }
    if (error == ErrorCode::OK) {
      break;
    }
  }
  return error;
}

/**
 * @brief Delete a metadata object from a metadata constraint file.
 * @param container     [in/out] metadata container.
 * @param object_key    [in]     key. column name of a constraint metadata table.
 * @param object_value  [in]     value to be filtered.
 * @retval ErrorCode::OK if success.
 * @retval ErrorCode::ID_NOT_FOUND if the constraint id does not exist.
 * @retval otherwise an error code.
 */
ErrorCode ConstraintsDAO::delete_metadata_object(boost::property_tree::ptree& container,
                                                 std::string_view object_key,
                                                 std::string_view object_value) const {
  ErrorCode error = ErrorCode::UNKNOWN;

  LOG_DEBUG << "delete_metadata_object metadata:" << (container.empty() ? "empty" : "exists")
            << " \"" << object_key << "\"=\"" << object_value << "\"";

  // Initialize the error code.
  if (object_key == Constraint::ID) {
    error = ErrorCode::ID_NOT_FOUND;
  } else {
    error = ErrorCode::NOT_FOUND;
  }

  // Getting a metadata container.
  ptree& root_node = container.get_child(ConstraintsDAO::ROOT_NODE);
  for (ptree::iterator table_it = root_node.begin(); table_it != root_node.end(); table_it++) {
    auto optional_constraint = table_it->second.get_child_optional(Tables::CONSTRAINTS_NODE);
    if (!optional_constraint) {
      continue;
    }

    ptree& constraint = optional_constraint.get();
    for (ptree::iterator constraint_it = constraint.begin(); constraint_it != constraint.end();) {
      const ptree& constraint_metadata = constraint_it->second;

      auto optional_value = constraint_metadata.get_optional<std::string>(object_key.data());

      // Delete metadata with constraint as a key.
      if (optional_value && (optional_value.value() == object_value)) {
        constraint_it = constraint.erase(constraint_it);
        error         = ErrorCode::OK;
        break;
      } else {
        constraint_it++;
      }
    }
  }

  return error;
}

}  // namespace manager::metadata::db::json
