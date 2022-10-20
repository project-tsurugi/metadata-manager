/*
 * Copyright 2022 tsurugi project.
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
#include "manager/metadata/constraints.h"

#include <memory>

#include <boost/format.hpp>

#include "manager/metadata/common/message.h"
#include "manager/metadata/helper/logging_helper.h"
#include "manager/metadata/helper/ptree_helper.h"
#include "manager/metadata/provider/constraints_provider.h"

// =============================================================================
namespace {

std::unique_ptr<manager::metadata::db::ConstraintsProvider> provider = nullptr;

}  // namespace

// =============================================================================
namespace manager::metadata {

using boost::property_tree::ptree;

// ==========================================================================
// Constraint class methods.
/**
 * @brief Transform constraint metadata from structure object to ptree object.
 * @return ptree object.
 */
boost::property_tree::ptree Constraint::convert_to_ptree() const {
  ptree metadata = this->base_convert_to_ptree();

  // ID.
  if (this->id <= 0) {
    // If the value is invalid, the put data is erased.
    metadata.erase(ID);
  }

  // table ID.
  if (this->table_id > 0) {
    // Put only if the value is valid.
    metadata.put(TABLE_ID, this->table_id);
  }

  // constraint type.
  metadata.put(TYPE, static_cast<int32_t>(this->type));

  // column numbers.
  ptree columns_node = ptree_helper::make_array_ptree(this->columns);
  metadata.add_child(COLUMNS, columns_node);

  // column IDs.
  ptree columns_id_node = ptree_helper::make_array_ptree(this->columns_id);
  metadata.add_child(COLUMNS_ID, columns_id_node);

  // index ID.
  metadata.put(INDEX_ID, this->index_id);

  // constraint expression.
  metadata.put(EXPRESSION, this->expression);

  return metadata;
}

/**
 * @brief Transform constraint metadata from ptree object to structure object.
 * @param ptree  [in] ptree object of metadata.
 * @return structure object of metadata.
 */
void Constraint::convert_from_ptree(const boost::property_tree::ptree& ptree) {
  this->base_convert_from_ptree(ptree);

  // table ID.
  this->table_id =
      ptree.get_optional<ObjectId>(TABLE_ID).value_or(INVALID_OBJECT_ID);

  // constraint type.
  this->type = static_cast<ConstraintType>(
      ptree.get_optional<int32_t>(TYPE).value_or(-1));

  // column numbers.
  this->columns = ptree_helper::make_vector_int(ptree, COLUMNS);

  // column IDs.
  this->columns_id = ptree_helper::make_vector_int(ptree, COLUMNS_ID);

  // index ID.
  this->index_id =
      ptree.get_optional<int64_t>(INDEX_ID).value_or(INVALID_VALUE);

  // constraint expression.
  this->expression = ptree.get_optional<std::string>(EXPRESSION).value_or("");
}

// ==========================================================================
// Constraints class methods.
/**
 * @brief Constructor
 * @param database   [in]  database name.
 * @param component  [in]  component name.
 */
Constraints::Constraints(std::string_view database, std::string_view component)
    : Metadata(database, component) {
  // Create the provider.
  provider = std::make_unique<db::ConstraintsProvider>();
}

/**
 * @brief Initialization.
 * @param none.
 * @return ErrorCode::OK if success, otherwise an error code.
 */
ErrorCode Constraints::init() const {
  ErrorCode error = ErrorCode::UNKNOWN;

  // Log of API function start.
  log::function_start("Constraints::init()");

  // Initialize the provider.
  error = provider->init();

  // Log of API function finish.
  log::function_finish("Constraints::init()", error);

  return error;
}

/**
 * @brief Add constraint metadata to constraint metadata table.
 * @param object  [in]  constraint metadata to add.
 * @return ErrorCode::OK if success, otherwise an error code.
 */
ErrorCode Constraints::add(const boost::property_tree::ptree& object) const {
  ErrorCode error = ErrorCode::UNKNOWN;

  // Adds the constraint metadata through the class method.
  error = this->add(object, nullptr);

  return error;
}

/**
 * @brief Add constraint metadata to constraint metadata table.
 * @param object     [in]  constraint metadata to add.
 * @param object_id  [out] ID of the added constraint metadata.
 * @return ErrorCode::OK if success, otherwise an error code.
 */
ErrorCode Constraints::add(const boost::property_tree::ptree& object,
                           ObjectId* object_id) const {
  ErrorCode error = ErrorCode::UNKNOWN;

  // Log of API function start.
  log::function_start("Constraints::add()");

  // Parameter value check.
  error = param_check_metadata_add(object);

  // Adds the constraint metadata through the provider.
  ObjectId retval_object_id = 0;
  if (error == ErrorCode::OK) {
    error = provider->add_constraint_metadata(object, retval_object_id);
  }

  // Set a value if object_id is not null.
  if ((error == ErrorCode::OK) && (object_id != nullptr)) {
    *object_id = retval_object_id;
  }

  // Log of API function finish.
  log::function_finish("Constraints::add()", error);

  return error;
}

/**
 * @brief Get constraint metadata.
 * @param object_id  [in]  constraint id.
 * @param object     [out] constraint metadata with the specified ID.
 * @retval ErrorCode::OK if success,
 * @retval ErrorCode::ID_NOT_FOUND if the constraint id does not exist.
 * @retval otherwise an error code.
 */
ErrorCode Constraints::get(const ObjectId object_id,
                           boost::property_tree::ptree& object) const {
  ErrorCode error = ErrorCode::UNKNOWN;

  // Log of API function start.
  log::function_start("Constraints::get(ConstraintId)");

  // Parameter value check.
  if (object_id > 0) {
    error = ErrorCode::OK;
  } else {
    LOG_WARNING
        << "An out-of-range value (0 or less) was specified for ConstraintId.: "
        << object_id;
    error = ErrorCode::ID_NOT_FOUND;
  }

  // Get the constraint metadata through the provider.
  if (error == ErrorCode::OK) {
    error = provider->get_constraint_metadata(object_id, object);
  }

  // Log of API function finish.
  log::function_finish("Constraints::get(ConstraintId)", error);

  return error;
}

/**
 * @brief Gets all constraint metadata object from the constraint metadata
 * table. If the constraint metadata does not exist, return the container as
 * empty.
 * @param container  [out] Container for metadata-objects.
 * @return ErrorCode::OK if success, otherwise an error code.
 */
ErrorCode Constraints::get_all(
    std::vector<boost::property_tree::ptree>& container) const {
  ErrorCode error = ErrorCode::UNKNOWN;

  // Log of API function start.
  log::function_start("Constraints::get_all()");

  // Get the constraint metadata through the provider.
  error = provider->get_constraint_metadata(container);

  // Log of API function finish.
  log::function_finish("Constraints::get_all()", error);

  return error;
}

/**
 * @brief Remove all metadata-object based on the given constraint id from
 * metadata-constraint.
 * @param object_id  [in]  constraint id.
 * @retval ErrorCode::OK if success,
 * @retval ErrorCode::ID_NOT_FOUND if the constraint id does not exist.
 * @retval otherwise an error code.
 */
ErrorCode Constraints::remove(const ObjectId object_id) const {
  ErrorCode error = ErrorCode::UNKNOWN;

  // Log of API function start.
  log::function_start("Constraints::remove(ConstraintId)");

  // Parameter value check.
  if (object_id > 0) {
    error = ErrorCode::OK;
  } else {
    LOG_WARNING
        << "An out-of-range value (0 or less) was specified for ConstraintId.: "
        << object_id;
    error = ErrorCode::ID_NOT_FOUND;
  }

  // Remove the constraint metadata through the provider.
  if (error == ErrorCode::OK) {
    error = provider->remove_constraint_metadata(object_id);
  }

  // Log of API function finish.
  log::function_finish("Constraints::remove(ConstraintId)", error);

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
ErrorCode Constraints::param_check_metadata_add(
    const boost::property_tree::ptree& object) const {
  ErrorCode error                        = ErrorCode::UNKNOWN;
  constexpr const char* const kLogFormat = R"("%s" => undefined or empty)";

  auto table_id = object.get_optional<ObjectId>(Constraint::TABLE_ID);
  if (table_id.value_or(0) > 0) {
    error = ErrorCode::OK;
  } else {
    LOG_ERROR << Message::PARAMETER_FAILED
              << (boost::format(kLogFormat) % Constraint::TABLE_ID).str();

    error = ErrorCode::INVALID_PARAMETER;
  }

  return error;
}

}  // namespace manager::metadata
