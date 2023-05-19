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

#include "manager/metadata/common/config.h"
#include "manager/metadata/common/message.h"
#include "manager/metadata/dao/json/object_id_json.h"
#include "manager/metadata/helper/logging_helper.h"
#include "manager/metadata/helper/ptree_helper.h"

// =============================================================================
namespace {

std::unique_ptr<manager::metadata::db::ObjectIdGenerator> oid_generator =
    nullptr;

}  // namespace

// =============================================================================
namespace manager::metadata::db {

using boost::property_tree::ptree;

ErrorCode ConstraintsDaoJson::prepare() {
  ErrorCode error = ErrorCode::UNKNOWN;

  // Filename of the constraint metadata.
  boost::format filename = boost::format("%s/%s.json") %
                           Config::get_storage_dir_path() %
                           kConstraintMetadataName;

  // Connect to the constraint metadata file.
  error = session_->connect(filename.str(), kRootNode);

  // Create the ObjectId.
  oid_generator = std::make_unique<ObjectIdGenerator>();

  return error;
}

ErrorCode ConstraintsDaoJson::insert(const boost::property_tree::ptree& object,
                                     ObjectId& object_id) const {
  ErrorCode error = ErrorCode::UNKNOWN;

  // Load the metadata from the JSON file.
  error = session_->load_contents();
  if (error != ErrorCode::OK) {
    return error;
  }

  // Getting a metadata container.
  ptree* container = session_->get_contents();

  // Getting a table id.
  std::string table_id(ptree_helper::ptree_value_to_string<ObjectId>(
      object, Constraint::TABLE_ID));

  ptree::iterator it_tables;
  ptree& root_node = container->get_child(kRootNode);
  for (it_tables = root_node.begin(); it_tables != root_node.end();
       it_tables++) {
    std::string oid_value(ptree_helper::ptree_value_to_string<ObjectId>(
        it_tables->second, Table::ID));
    if (oid_value == table_id) {
      break;
    }
  }

  if (it_tables == root_node.end()) {
    LOG_INFO << "Table metadata for the specified table ID does not exist.: "
             << table_id;
    error == ErrorCode::NOT_FOUND;
    return error;
  }

  ptree empty_ptree;
  ptree& table_metadata = it_tables->second;
  if (table_metadata.find(Table::CONSTRAINTS_NODE) ==
      table_metadata.not_found()) {
    table_metadata.add_child(Table::CONSTRAINTS_NODE, empty_ptree);
  }
  auto& constraints_node = table_metadata.get_child(Table::CONSTRAINTS_NODE);

  // Copy to the temporary area.
  ptree tmp_metadata = object;

  // Checks for INSERT execution with object-id specified.
  auto opt_oid_value = object.get_optional<ObjectId>(Constraint::ID);
  if (opt_oid_value.value_or(-1) > 0) {
    // Constraint ID is specified, the metadata for that constraint ID is
    // updated.
    object_id =
        oid_generator->update(kOidKeyNameConstraint, opt_oid_value.value());
    LOG_INFO << "Add constraint metadata with specified constraint ID. "
                "ConstraintID: "
             << opt_oid_value.value();
  } else {
    // Constraint ID is not specified, the constraint ID is generated and
    // metadata is added.
    object_id = oid_generator->generate(kOidKeyNameConstraint);
    tmp_metadata.put(Constraint::ID, object_id);
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

ErrorCode ConstraintsDaoJson::select_all(
    std::vector<boost::property_tree::ptree>& objects) const {
  ErrorCode error = ErrorCode::UNKNOWN;

  // Load the metadata from the JSON file.
  error = session_->load_contents();
  if (error != ErrorCode::OK) {
    return error;
  }

  // Initialize constraint metadata list
  objects = {};

  // Getting a metadata contents.
  ptree* contents = session_->get_contents();

  // constraint metadata
  BOOST_FOREACH (ptree::value_type& root_node, contents->get_child(kRootNode)) {
    ptree& table = root_node.second;

    // Convert from ptree structure type to vector<ptree>.
    auto constraints_node = table.get_child(Table::CONSTRAINTS_NODE);
    std::transform(constraints_node.begin(), constraints_node.end(),
                   std::back_inserter(objects),
                   [](ptree::value_type v) { return v.second; });
  }

  return error;
}

ErrorCode ConstraintsDaoJson::select(
    std::string_view key, const std::vector<std::string_view>& values,
    boost::property_tree::ptree& object) const {
  ErrorCode error = ErrorCode::UNKNOWN;

  if (values.size() == 0) {
    LOG_ERROR << Message::PARAMETER_FAILED << "Key value is unspecified.";
    error = ErrorCode::INVALID_PARAMETER;
    return error;
  }

  // Load the metadata from the JSON file.
  error = session_->load_contents();
  if (error != ErrorCode::OK) {
    return error;
  }

  // Getting a metadata contents.
  ptree* contents = session_->get_contents();

  // Initialize the error code.
  error = get_not_found_error_code(key);

  // Getting a metadata object.
  BOOST_FOREACH (const ptree::value_type& root_node,
                 contents->get_child(kRootNode)) {
    const ptree& table = root_node.second;
    if (table.find(Table::CONSTRAINTS_NODE) != table.not_found()) {
      BOOST_FOREACH (const ptree::value_type& constraints_node,
                     table.get_child(Table::CONSTRAINTS_NODE)) {
        const ptree& constraint = constraints_node.second;

        std::string key_value(ptree_helper::ptree_value_to_string<std::string>(
            constraint, key.data()));
        if (key_value == values[0]) {
          error = ErrorCode::OK;
          // copy.
          object = constraint;
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

ErrorCode ConstraintsDaoJson::remove(
    std::string_view key, const std::vector<std::string_view>& values,
    ObjectId& object_id) const {
  ErrorCode error = ErrorCode::UNKNOWN;

  if (values.size() == 0) {
    LOG_ERROR << Message::PARAMETER_FAILED << "Key value is unspecified.";
    error = ErrorCode::INVALID_PARAMETER;
    return error;
  }

  // Load the metadata from the JSON file.
  error = session_->load_contents();
  if (error != ErrorCode::OK) {
    return error;
  }

  // Getting a metadata contents.
  ptree* contents = session_->get_contents();

  // Initialize the error code.
  error = get_not_found_error_code(key);

  // Getting a metadata container.
  ptree& root_node = contents->get_child(kRootNode);

  for (ptree::iterator it_tables = root_node.begin();
       it_tables != root_node.end(); it_tables++) {
    auto opt_constraints =
        it_tables->second.get_child_optional(Table::CONSTRAINTS_NODE);
    if (!opt_constraints) {
      continue;
    }
    ptree& constraints_node = opt_constraints.value();

    for (ptree::iterator it_constraints = constraints_node.begin();
         it_constraints != constraints_node.end();) {
      const ptree& metadata = it_constraints->second;

      // Get the value of the key.
      std::string key_value(ptree_helper::ptree_value_to_string<std::string>(
          metadata, key.data()));
      // If the key value matches, the metadata is removed.
      if (key_value == values[0]) {
        auto opt_oid_value = metadata.get_optional<ObjectId>(Constraint::ID);
        object_id          = opt_oid_value.get_value_or(-1);

        error = ErrorCode::OK;
        // Remove constraint metadata.
        it_constraints = constraints_node.erase(it_constraints);

        LOG_DEBUG << "Remove constraint metadata. " << key << "=\"" << values[0]
                  << "\"";
        break;
      } else {
        it_constraints++;
      }
    }
  }

  return error;
}

}  // namespace manager::metadata::db
