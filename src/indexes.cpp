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
#include "manager/metadata/indexes.h"

#include <memory>
#include <jwt-cpp/jwt.h>

#include "manager/metadata/common/config.h"
#include "manager/metadata/common/jwt_claims.h"
#include "manager/metadata/common/message.h"
#include "manager/metadata/helper/logging_helper.h"
#include "manager/metadata/helper/table_metadata_helper.h"
#include "manager/metadata/provider/datatypes_provider.h"
#include "manager/metadata/provider/indexes_provider.h"
#include "manager/metadata/helper/ptree_helper.h"


// =============================================================================
namespace {
std::unique_ptr<manager::metadata::db::IndexesProvider> provider = nullptr;
}

// =============================================================================
namespace manager::metadata {

using boost::property_tree::ptree;

// ==========================================================================
// Index struct methods.
/**
 * @brief 
 */
boost::property_tree::ptree Index::convert_to_ptree() const
{
  auto pt = Object::convert_to_ptree();
  pt.put(OWNER_ID, this->owner_id);
  pt.put(ACCESS_METHOD, this->access_method);
  pt.put(NUMBER_OF_COLUMNS, this->number_of_columns);
  pt.put(NUMBER_OF_KEY_COLUMNS, this->number_of_key_columns);
  
  ptree keys = ptree_helper::make_array_ptree(this->keys);
  pt.push_back(std::make_pair(KEYS, keys));
  
  ptree keys_id = ptree_helper::make_array_ptree(this->keys_id);
  pt.push_back(std::make_pair(KEYS_ID, keys_id));
  
  ptree options = ptree_helper::make_array_ptree(this->options);
  pt.push_back(std::make_pair(OPTIONS, options));

  return pt;
}

/**
 * @brief 
 */
void Index::convert_from_ptree(const boost::property_tree::ptree& pt)
{
  Object::convert_from_ptree(pt);
  auto opt_int = pt.get_optional<int64_t>(OWNER_ID);
  this->owner_id = opt_int ? opt_int.get() : INVALID_OBJECT_ID;

  opt_int = pt.get_optional<int64_t>(ACCESS_METHOD);
  this->access_method = opt_int ? opt_int.get() : INVALID_VALUE;

  opt_int = pt.get_optional<int64_t>(NUMBER_OF_COLUMNS);
  this->number_of_columns = opt_int ? opt_int.get() : INVALID_VALUE;

  opt_int = pt.get_optional<int64_t>(NUMBER_OF_KEY_COLUMNS);
  this->number_of_key_columns = opt_int ? opt_int.get() : INVALID_VALUE;

  this->keys = ptree_helper::make_vector(pt, KEYS);
  this->keys_id = ptree_helper::make_vector(pt, KEYS_ID);
  this->options = ptree_helper::make_vector(pt, OPTIONS);
}

// ==========================================================================
// Indexes class methods.
/**
 * @brief Constructor
 * @param database   [in]  database name.
 * @param component  [in]  component name.
 */
Indexes::Indexes(std::string_view database, std::string_view component)
    : Metadata(database, component) {

  provider = std::make_unique<db::IndexesProvider>();
}

/**
 * @brief Initialization.
 * @return ErrorCode::OK if success, otherwise an error code.
 */
ErrorCode Indexes::init() const {
  ErrorCode error = ErrorCode::UNKNOWN;

  log::function_start("Index::init()");

  error = provider->init();

  log::function_finish("Index::init()", error);

  return error;
}

ErrorCode Indexes::add(const boost::property_tree::ptree& object) const {

  return this->add(object, nullptr);
}

/**
 * @brief Add index metadata to the metadata table.
 * @param object      [in]  index metadata to add.
 * @param object_id   [out] ID of the added index metadata.
 * @return ErrorCode::OK if success, otherwise an error code.
 */
ErrorCode Indexes::add(const boost::property_tree::ptree& object,
                      ObjectId* object_id) const {
  
  log::function_start("Indexes::add()");

  ErrorCode error = ErrorCode::UNKNOWN;
  ObjectId id = INVALID_OBJECT_ID;

  ObjectIdType retval_object_id = 0;
  if (error == ErrorCode::OK) {
    error = provider->add_index_metadata(object, id);
  }

  if ((error == ErrorCode::OK) && (object_id != nullptr)) {
    *object_id = id;
  }

  log::function_finish("Index::add()", error);
  
  return ErrorCode::OK;
}

/**
 * @brief Get index metadata.
 * @param object_id  [in]  object ID to get.
 * @param object     [out] index metadata with the specified ID.
 * @retval ErrorCode::OK if success,
 * @retval ErrorCode::ID_NOT_FOUND if the index id does not exist.
 * @retval otherwise an error code.
 */
ErrorCode Indexes::get(const ObjectId object_id,
                      boost::property_tree::ptree& object) const {

  log::function_start("Index::get(object_id)");

  ErrorCode error = ErrorCode::UNKNOWN;

  if (object_id > 0) {
    error = provider->get_index_metadata(Object::ID, 
                                        std::to_string(object_id), 
                                        object);
  } else {
    LOG_WARNING
        << "An out-of-range value (0 or less) was specified for object ID.: "
        << object_id;
    error = ErrorCode::INVALID_PARAMETER;
  }

  log::function_finish("Index::get(object_id)", error);

  return error;
}

/**
 * @brief Get index metadata object based on name.
 * @param object_name [in]  object name.
 * @param object      [out] index metadata object with the specified name.
 * @retval ErrorCode::OK if success,
 * @retval ErrorCode::NAME_NOT_FOUND if the table name does not exist.
 * @retval otherwise an error code.
 */
ErrorCode Indexes::get(std::string_view object_name,
                      boost::property_tree::ptree& object) const {

  log::function_start("Indexes::get(object_name)");

  ErrorCode error = ErrorCode::UNKNOWN;

  if (!object_name.empty()) {
    error = provider->get_index_metadata(Object::NAME, object_name, object);
  } else {
    LOG_WARNING << "An empty value was specified for TableName.";
    error = ErrorCode::NAME_NOT_FOUND;
  }

  log::function_finish("Indexes::get(object_name)", error);

  return ErrorCode::OK;
}

/**
 * @brief Get all index metadata objects from the metadata table.
 *   If no index metadata existst, return the container as empty.
 * @param objects  [out] Container of metadata objects.
 * @return ErrorCode::OK if success, otherwise an error code.
 */
ErrorCode Indexes::get_all(
    std::vector<boost::property_tree::ptree>& objects) const {

  log::function_start("Tables::get_all()");

  ErrorCode error = provider->get_index_metadata(objects);

  log::function_finish("Tables::get_all()", error);

  return error;
}

/**
 * @brief Update metadata-index with metadata-object.
 * @param object_id [in]  ID of the metadata-index to update.
 * @param object    [in]  metadata-object to update.
 * @return ErrorCode::OK if success, otherwise an error code.
 */
ErrorCode Indexes::update(const ObjectIdType object_id,
                          const boost::property_tree::ptree& object) const {

  ErrorCode error = ErrorCode::UNKNOWN;

  if (error == ErrorCode::OK) {
    error = provider->update_index_metadata(object_id, object);
  }

  return ErrorCode::OK;
}

/**
 * @brief Remove a index metadata object which has the specified ID.
 * @param object_id  [in]  object id.
 * @retval ErrorCode::OK if success,
 * @retval ErrorCode::ID_NOT_FOUND if the object id does not exist.
 * @retval otherwise an error code.
 */
ErrorCode Indexes::remove(const ObjectId object_id) const {

  log::function_start("Indexes::remove(object_id)");

  ErrorCode error = ErrorCode::UNKNOWN;

  if (object_id > 0) {
    ObjectIdType ret_object_id = INVALID_OBJECT_ID;
    error = provider->remove_index_metadata(
        Object::ID, std::to_string(object_id), ret_object_id);
  } else {
    LOG_WARNING
        << "An out-of-range value (0 or less) was specified for TableId.: "
        << object_id;
    error = ErrorCode::ID_NOT_FOUND;
  }

  log::function_finish("Indexes::remove(object_id)", error);
}

/**
 * @brief Remove a indexmetadata object which has the specified name.
 * @param object_name  [in]  object name.
 * @param object_id    [out] ID of removed object.
 * @retval ErrorCode::OK if success,
 * @retval ErrorCode::NAME_NOT_FOUND if the table name does not exist.
 * @retval otherwise an error code.
 */
ErrorCode Indexes::remove(std::string_view object_name,
                          ObjectId* object_id) const {

  log::function_start("Indexes::remove(object_name)");

  ErrorCode error = ErrorCode::UNKNOWN;

  if (!object_name.empty()) {
    ObjectIdType ret_object_id = INVALID_OBJECT_ID;
    error = provider->remove_index_metadata(Object::NAME, object_name,
                                            ret_object_id);

    if ((error == ErrorCode::OK) && (object_id != nullptr)) {
      *object_id = ret_object_id;
    }
  } else {
    LOG_WARNING << "An empty value was specified for TableName.";
    error = ErrorCode::INVALID_PARAMETER;
  }

  log::function_finish("Indexes::remove(object_name)", error);

  return error;
              
}

/**
 * @brief Add a index metadata to the metadata table.
 * @param object    [in]  index metadata to add.
 * @param object_id [out] ID of the added index metadata.
 * @return ErrorCode::OK if success, otherwise an error code.
 */
ErrorCode Indexes::add(const manager::metadata::Index& index,
                      ObjectIdType* object_id) const {

  ptree pt = index.convert_to_ptree();
  ErrorCode error = this->add(pt, object_id);
  if (error != ErrorCode::OK) {
    return error;
  }

  return error;
}

/**
 * @brief Add a index metadata to table metadata table.
 * @param object  [in]  table metadata to add.
 * @return ErrorCode::OK if success, otherwise an error code.
 */
ErrorCode Indexes::add(const manager::metadata::Index& index) const
{
  ObjectId object_id = INVALID_OBJECT_ID;
  ErrorCode error = this->add(index, &object_id);
  if (error != ErrorCode::OK) {
    return error;
  }

  return error;
}


/**
 * @brief Get a index metadata.
 * @param object_id [in]  object id.
 * @param index     [out] index metadata with the specified ID.
 * @retval ErrorCode::OK if success,
 * @retval ErrorCode::ID_NOT_FOUND if the table id does not exist.
 * @retval otherwise an error code.
 */
ErrorCode Indexes::get(const ObjectIdType object_id,
                      manager::metadata::Index& index) const
{
  ptree pt;

  ErrorCode error = this->get(object_id, pt);
  if (error != ErrorCode::OK) {
    return error;
  }
  index.convert_from_ptree(pt);

  return error;
}

/**
 * @brief Get a index metadata object based on index name.
 * @param object_name [in]  object name. (Value of "name" key.)
 * @param index       [out] index metadata object with the specified name.
 * @retval ErrorCode::OK if success,
 * @retval ErrorCode::NAME_NOT_FOUND if the table name does not exist.
 * @retval otherwise an error code.
 */
ErrorCode Indexes::get(std::string_view object_name,
                      manager::metadata::Index& index) const
{

  ptree pt;

  ErrorCode error = this->get(object_name, pt);
  if (error != ErrorCode::OK) {
    return error;
  }
  index.convert_from_ptree(pt);

  return error;
}

/**
 * @brief Get all index metadata objects from the metadata table.
 *   If no index metadata existst, return the container as empty.
 * @param objects  [out] Container of metadata objects.
 * @return ErrorCode::OK if success, otherwise an error code.
 */
ErrorCode Indexes::get_all(
    std::vector<manager::metadata::Index>& indexes) const {

  std::vector<ptree> pts;
  ErrorCode error = this->get_all(pts);
  if (error != ErrorCode::OK) {
    return error;
  }

  for (const auto& pt : pts) {
    Index index;
    index.convert_from_ptree(pt);
    indexes.emplace_back(index);
  }

  return error;  
}

} // namespace manager::metadata
