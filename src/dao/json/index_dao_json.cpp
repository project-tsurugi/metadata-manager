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
#include "manager/metadata/dao/json/index_dao_json.h"

#include <boost/foreach.hpp>
#include <boost/format.hpp>

#include "manager/metadata/common/config.h"
#include "manager/metadata/common/message.h"
#include "manager/metadata/dao/json/object_id_json.h"
#include "manager/metadata/helper/logging_helper.h"

// =============================================================================

namespace {

using manager::metadata::db::ObjectIdGenerator;
std::unique_ptr<ObjectIdGenerator> oid_generator = nullptr;

}  // namespace

namespace manager::metadata::db {

using boost::property_tree::ptree;

/**
 * @brief Find metadata object from metadata table.
 * @param objects [in]  container of JSON object.
 * @param key     [in]  key. column name of a table metadata table.
 * e.g. object ID, object name.
 * @param value   [in]  value to be filtered.
 * @param object  [out] metadata-object with the specified name.
 * @return ErrorCode::OK if success, otherwise an error code.
 */
manager::metadata::ErrorCode find_metadata_object(
    const boost::property_tree::ptree& objects, 
    std::string_view key, std::string_view value,
    boost::property_tree::ptree& object) {

  ErrorCode error = ErrorCode::NOT_FOUND;
  BOOST_FOREACH(const auto& node,
                objects.get_child(IndexDaoJson::kRootNode)) {
    const auto& temp_obj = node.second;
    auto temp_value = temp_obj.get_optional<std::string>(key.data());
    if (temp_value && (temp_value.get() == value)) {
      // find the object.
      object = temp_obj;
      error = ErrorCode::OK;
      break;
    }
  }
  
  return error;
}

/**
 * @brief Delete a metadata object from a metadata table file.
 * @param container    [in/out] metadata container.
 * @param object_key   [in]     key. column name of a table metadata table.
 * @param object_value [in]     value to be filtered.
 * @param table_id     [out]    table id of the row deleted.
 * @retval ErrorCode::OK if success.
 * @retval ErrorCode::ID_NOT_FOUND if the table id does not exist.
 * @retval ErrorCode::NAME_NOT_FOUND if the table name does not exist.
 * @retval otherwise an error code.
 */
ErrorCode delete_metadata_object(
    boost::property_tree::ptree& objects, 
    std::string_view key, std::string_view value, 
    ObjectId& object_id) {
  
  ErrorCode error = ErrorCode::UNKNOWN;

  // Initialize the error code.
  error = ErrorCode::NOT_FOUND;

  ptree& node = objects.get_child(IndexDaoJson::kRootNode);
  for (ptree::iterator ite = node.begin(); ite != node.end();) {
    const auto& temp_obj = ite->second;
    auto id = temp_obj.get_optional<std::string>(Object::ID);
    if (!id) {
      error = ErrorCode::INTERNAL_ERROR;
      break;
    }

    if (key == Object::ID) {
      if (id.get() == value) {
        // find the object.
        ite = node.erase(ite);
        object_id = std::stoul(id.get());     
        error = ErrorCode::OK;
        break;
      }
    } else if (key == Object::NAME) {
      auto name = temp_obj.get_optional<std::string>(Object::NAME);
      if (name && (name.get() == value)) {
        // find the object.
        ite = node.erase(ite);
        object_id = std::stoul(id.get());
        error = ErrorCode::OK;
        break;
      }
    } else {
      // Unsupported keys.
      error = ErrorCode::NOT_SUPPORTED;
      break;
    }
    ++ite;
  }

  return error;
}

ErrorCode IndexDaoJson::prepare() {
  ErrorCode error = ErrorCode::UNKNOWN;

  boost::format file_path = boost::format("%s/%s.json") %
                            Config::get_storage_dir_path() % kIndexMetadataName;

  error = session_->connect(file_path.str(), kRootNode);

  oid_generator = std::make_unique<ObjectIdGenerator>();

  return error;
}

manager::metadata::ErrorCode IndexDaoJson::insert(
    const boost::property_tree::ptree& object,
    ObjectId& object_id) const {

  ErrorCode error = ErrorCode::UNKNOWN;

  // Check if the object is already exists.
  if (this->exists(object)) {
    return ErrorCode::ALREADY_EXISTS;
  }

  error = session_->load_contents();
  if (error != ErrorCode::OK) {
    return error;
  }

  // Get a metadata contents.
  ptree* contents = session_->get_contents();

  // Copy to the temporary area.
  ptree temp_obj = object;

  // Generate the object ID of the metadata object to be added.
  object_id = oid_generator->generate(kOidKeyNameIndex);

  // Generate management metadata.
  temp_obj.put<int64_t>(Object::FORMAT_VERSION, Indexes::format_version());
  temp_obj.put<int64_t>(Object::GENERATION, Indexes::generation());
  temp_obj.put<ObjectId>(Object::ID, object_id);

  ptree root = contents->get_child(kRootNode);
  root.push_back(std::make_pair("", temp_obj));
  contents->put_child(kRootNode, root);

  // The metadata object is NOT yet persisted, persisted on commit.

  return  ErrorCode::OK;
}

manager::metadata::ErrorCode IndexDaoJson::select_all(
    std::vector<boost::property_tree::ptree>& objects) const {
  
  ErrorCode error = ErrorCode::UNKNOWN;

  error = session_->load_contents();
  if (error != ErrorCode::OK) {
    return error;
  }

  ptree* contents = session_->get_contents();

  // Convert from ptree structure type to vector<ptree>.
  auto node = contents->get_child(kRootNode);
  std::transform(node.begin(), node.end(), std::back_inserter(objects),
                 [](ptree::value_type v) { return v.second; });

  return error;
}

manager::metadata::ErrorCode IndexDaoJson::select(
    std::string_view key, const std::vector<std::string_view>& values,
    boost::property_tree::ptree& object) const {
  ErrorCode error = ErrorCode::UNKNOWN;

  if (values.size() == 0) {
    LOG_ERROR << Message::PARAMETER_FAILED << "Key value is unspecified.";
    error = ErrorCode::INVALID_PARAMETER;
    return error;
  }

  error = session_->load_contents();
  if (error != ErrorCode::OK) {
    return error;
  }
  ptree* metadata_table = session_->get_contents();

  error = find_metadata_object(*metadata_table, key, values[0], object);
  if (error == ErrorCode::NOT_FOUND) {
    error = get_not_found_error_code(key);
  }
  
  return error;
}

/**
 * @brief
 */
manager::metadata::ErrorCode IndexDaoJson::update(
    std::string_view key, const std::vector<std::string_view>& values,
    const boost::property_tree::ptree& object) const {
  ErrorCode error = ErrorCode::UNKNOWN;

  if (values.size() == 0) {
    LOG_ERROR << Message::PARAMETER_FAILED << "Key value is unspecified.";
    error = ErrorCode::INVALID_PARAMETER;
    return error;
  }

  error = session_->load_contents();
  if (error != ErrorCode::OK) {
    return error;
  }
  ptree* metadata_table = session_->get_contents();

  ptree temp_obj;
  error = find_metadata_object(*metadata_table, key, values[0], temp_obj);
  if (error == ErrorCode::NOT_FOUND) {
    error = get_not_found_error_code(key);
  }

  // copy management metadata.
  ObjectId object_id;
  this->remove(key, values, object_id);
  auto format_version = temp_obj.get_optional<int64_t>(Object::FORMAT_VERSION)
                            .value_or(INVALID_VALUE);
  auto generation = temp_obj.get_optional<int64_t>(Object::GENERATION)
                        .value_or(INVALID_VALUE);
  auto id =
      temp_obj.get_optional<ObjectId>(Object::ID).value_or(INVALID_OBJECT_ID);

  temp_obj = object;
  temp_obj.put<int64_t>(Object::FORMAT_VERSION, format_version);
  temp_obj.put<int64_t>(Object::GENERATION, generation);
  temp_obj.put<ObjectId>(Object::ID, id);

  ptree root = metadata_table->get_child(kRootNode);
  root.push_back(std::make_pair("", temp_obj));
  metadata_table->put_child(kRootNode, root);

  return error;
}

manager::metadata::ErrorCode IndexDaoJson::remove(
    std::string_view key, const std::vector<std::string_view>& values,
    ObjectId& object_id) const {
  ErrorCode error = ErrorCode::UNKNOWN;

  if (values.size() == 0) {
    LOG_ERROR << Message::PARAMETER_FAILED << "Key value is unspecified.";
    error = ErrorCode::INVALID_PARAMETER;
    return error;
  }

  error = session_->load_contents();
  if (error != ErrorCode::OK) {
    return error;
  }
  ptree* contents = session_->get_contents();

  error = delete_metadata_object(*contents, key, values[0], object_id);
  if (error == ErrorCode::NOT_FOUND) {
    // Convert the error code.
    error = get_not_found_error_code(key);
  }

  return error;
}

}  // namespace manager::metadata::db
