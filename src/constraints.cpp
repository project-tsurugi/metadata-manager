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
#include "manager/metadata/provider/constraints_provider.h"

// =============================================================================
namespace {

std::unique_ptr<manager::metadata::db::ConstraintsProvider> provider = nullptr;

}  // namespace

// =============================================================================
namespace manager::metadata {

using boost::property_tree::ptree;

// ==========================================================================
// File local functions.
/**
 * @brief Transform from a std::vector object to a ptree object.
 * @tparam T Type of variable to convert.
 * @param vector_object  [in]  Object to be converted.
 * @param ptree_object   [out] Converted object.
 */
template <typename T>
void transform_object(const std::vector<T>& vector_object,
                      boost::property_tree::ptree& ptree_object) {
  ptree_object.clear();
  for (const T& value : vector_object) {
    boost::property_tree::ptree child_ptree;

    child_ptree.put("", value);
    ptree_object.push_back(std::make_pair("", child_ptree));
  }
}

/**
 * @brief Transform from a ptree object to a std::vector object.
 * @tparam T Type of variable to convert.
 * @param ptree_object   [in]  Object to be converted.
 * @param vector_object  [out] Converted object.
 */
template <typename T>
void transform_object(const boost::property_tree::ptree& ptree_object,
                      std::vector<T>& vector_object) {
  vector_object.clear();
  std::transform(
      ptree_object.begin(), ptree_object.end(), std::back_inserter(vector_object),
      [](boost::property_tree::ptree::value_type v) { return v.second.get_optional<T>("").get(); });
}

// ==========================================================================
// Constraint class methods.
/**
 * @brief Transform constraint metadata from structure object to ptree object.
 * @return ptree object.
 */
boost::property_tree::ptree Constraint::convert_to_ptree() const {
  ptree metadata = Object::convert_to_ptree();

  // table ID.
  metadata.put(TABLE_ID, this->table_id);

  // constraint type.
  metadata.put(TYPE, static_cast<int32_t>(this->type));

  // column numbers.
  ptree columns_node;
  transform_object(this->columns, columns_node);
  metadata.add_child(COLUMNS, columns_node);

  // column IDs.
  ptree columns_id_node;
  transform_object(this->columns_id, columns_id_node);
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
  Object::convert_from_ptree(ptree);

  // table ID.
  this->table_id = ptree.get_optional<ObjectId>(TABLE_ID).value_or(INVALID_OBJECT_ID);

  // constraint type.
  this->type = static_cast<ConstraintType>(ptree.get_optional<int32_t>(TYPE).value_or(-1));

  // column numbers.
  transform_object(ptree.get_child(COLUMNS), this->columns);

  // column IDs.
  transform_object(ptree.get_child(COLUMNS_ID), this->columns_id);

  // index ID.
  this->index_id = ptree.get_optional<int64_t>(INDEX_ID).value_or(INVALID_VALUE);

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
ErrorCode Constraints::add(const boost::property_tree::ptree& object, ObjectId* object_id) const {
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
ErrorCode Constraints::get(const ObjectId object_id, boost::property_tree::ptree& object) const {
  ErrorCode error = ErrorCode::UNKNOWN;

  // Log of API function start.
  log::function_start("Constraints::get(ConstraintId)");

  // Parameter value check.
  if (object_id > 0) {
    error = ErrorCode::OK;
  } else {
    LOG_WARNING << "An out-of-range value (0 or less) was specified for ConstraintId.: "
                << object_id;
    error = ErrorCode::ID_NOT_FOUND;
  }

  // Get the constraint metadata through the provider.
  if (error == ErrorCode::OK) {
    auto s_object_id = std::to_string(object_id);

    error = provider->get_constraint_metadata(Constraint::ID, s_object_id, object);
  }

  // Log of API function finish.
  log::function_finish("Constraints::get(ConstraintId)", error);

  return error;
}

/**
 * @brief Get constraint metadata object based on constraint name.
 * @param object_name  [in]  constraint name. (Value of "name" key.)
 * @param object       [out] constraint metadata object with the specified name.
 * @retval ErrorCode::OK if success,
 * @retval ErrorCode::NAME_NOT_FOUND if the constraint name does not exist.
 * @retval otherwise an error code.
 */
ErrorCode Constraints::get(std::string_view object_name,
                           boost::property_tree::ptree& object) const {
  ErrorCode error = ErrorCode::UNKNOWN;

  // Log of API function start.
  log::function_start("Constraints::get(ConstraintName)");

  // Parameter value check.
  if (!object_name.empty()) {
    error = ErrorCode::OK;
  } else {
    LOG_WARNING << "An empty value was specified for ConstraintName.";
    error = ErrorCode::NAME_NOT_FOUND;
  }

  // Get the constraint metadata through the provider.
  if (error == ErrorCode::OK) {
    error = provider->get_constraint_metadata(Constraint::NAME, object_name, object);
  }

  // Log of API function finish.
  log::function_finish("Constraints::get(ConstraintName)", error);

  return error;
}

/**
 * @brief Gets all constraint metadata object from the constraint metadata table.
 *   If the constraint metadata does not exist, return the container as empty.
 * @param container  [out] Container for metadata-objects.
 * @return ErrorCode::OK if success, otherwise an error code.
 */
ErrorCode Constraints::get_all(std::vector<boost::property_tree::ptree>& container) const {
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
 * @brief Remove all metadata-object based on the given constraint id from metadata-constraint.
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
    LOG_WARNING << "An out-of-range value (0 or less) was specified for ConstraintId.: "
                << object_id;
    error = ErrorCode::ID_NOT_FOUND;
  }

  // Remove the constraint metadata through the provider.
  if (error == ErrorCode::OK) {
    ObjectId retval_object_id = 0;

    error = provider->remove_constraint_metadata(Constraint::ID, std::to_string(object_id),
                                                 retval_object_id);
  }

  // Log of API function finish.
  log::function_finish("Constraints::remove(ConstraintId)", error);

  return error;
}

/**
 * @brief Remove all metadata-object based on the given constraint name from metadata-table.
 * @param object_name  [in]  constraint name.
 * @param object_id    [out] object id of constraint removed.
 * @retval ErrorCode::OK if success,
 * @retval ErrorCode::NAME_NOT_FOUND if the constraint name does not exist.
 * @retval otherwise an error code.
 */
ErrorCode Constraints::remove(std::string_view object_name, ObjectId* object_id) const {
  ErrorCode error = ErrorCode::UNKNOWN;

  // Log of API function start.
  log::function_start("Constraints::remove(ConstraintName)");

  // Parameter value check.
  if (!object_name.empty()) {
    error = ErrorCode::OK;
  } else {
    LOG_WARNING << "An empty value was specified for ConstraintName.";
    error = ErrorCode::NAME_NOT_FOUND;
  }

  ObjectId retval_object_id = 0;
  // Remove the constraint metadata through the provider.
  error = provider->remove_constraint_metadata(Constraint::NAME, object_name, retval_object_id);

  // Set a value if object_id is not null.
  if ((error == ErrorCode::OK) && (object_id != nullptr)) {
    *object_id = retval_object_id;
  }

  // Log of API function finish.
  log::function_finish("Constraints::remove(ConstraintName)", error);

  return error;
}

/**
 *  structure interfaces.
 */

/**
 * @brief Add constraint metadata to constraint metadata table.
 * @param object     [in]  constraint metadata to add.
 * @param object_id  [out] ID of the added constraint metadata.
 * @return ErrorCode::OK if success, otherwise an error code.
 */
ErrorCode Constraints::add(const manager::metadata::Constraint& constraint,
                           ObjectId* object_id) const {
  ErrorCode error = ErrorCode::UNKNOWN;
  ptree ptree     = constraint.convert_to_ptree();

  error = this->add(ptree, object_id);
  if (error != ErrorCode::OK) {
    return error;
  }

  return error;
}

/**
 * @brief Add constraint metadata to constraint metadata table.
 * @param object  [in]  constraint metadata to add.
 * @return ErrorCode::OK if success, otherwise an error code.
 */
ErrorCode Constraints::add(const manager::metadata::Constraint& constraint) const {
  ErrorCode error    = ErrorCode::UNKNOWN;
  ObjectId object_id = INVALID_OBJECT_ID;

  error = this->add(constraint, &object_id);
  if (error != ErrorCode::OK) {
    return error;
  }

  return error;
}

/**
 * @brief Get constraint metadata.
 * @param object_id   [in]  constraint id.
 * @param constraint  [out] constraint metadata with the specified ID.
 * @retval ErrorCode::OK if success,
 * @retval ErrorCode::ID_NOT_FOUND if the constraint id does not exist.
 * @retval otherwise an error code.
 */
ErrorCode Constraints::get(const ObjectId object_id,
                           manager::metadata::Constraint& constraint) const {
  ErrorCode error = ErrorCode::UNKNOWN;
  ptree ptree;

  error = this->get(object_id, ptree);
  if (error != ErrorCode::OK) {
    return error;
  }
  constraint.convert_from_ptree(ptree);

  return error;
}

/**
 * @brief Get constraint metadata object based on constraint name.
 * @param constraint_name  [in]  constraint name. (Value of "name" key.)
 * @param constraint       [out] constraint metadata object with the specified name.
 * @retval ErrorCode::OK if success,
 * @retval ErrorCode::NAME_NOT_FOUND if the table name does not exist.
 * @retval otherwise an error code.
 */
ErrorCode Constraints::get(std::string_view constraint_name,
                           manager::metadata::Constraint& constraint) const {
  ErrorCode error = ErrorCode::UNKNOWN;
  ptree ptree;

  error = this->get(constraint_name, ptree);
  if (error != ErrorCode::OK) {
    return error;
  }
  constraint.convert_from_ptree(ptree);

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
ErrorCode Constraints::param_check_metadata_add(const boost::property_tree::ptree& object) const {
  ErrorCode error                        = ErrorCode::UNKNOWN;
  constexpr const char* const kLogFormat = R"("%s" => undefined or empty)";

  auto table_id = object.get_optional<ObjectId>(Constraint::TABLE_ID);
  if (!table_id.value_or(0) <= 0) {
    LOG_ERROR << Message::PARAMETER_FAILED
              << (boost::format(kLogFormat) % Constraint::TABLE_ID).str();

    error = ErrorCode::INVALID_PARAMETER;
  }

  return error;
}

}  // namespace manager::metadata
