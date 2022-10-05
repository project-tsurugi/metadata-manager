/*
 * Copyright 2021 tsurugi project.
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
#include "manager/metadata/dao/json/db_session_manager_json.h"

#include <fstream>
#include <iostream>

#include <boost/property_tree/json_parser.hpp>
#include <boost/property_tree/ptree.hpp>

#include "manager/metadata/common/message.h"
#include "manager/metadata/dao/json/columns_dao_json.h"
#include "manager/metadata/dao/json/datatypes_dao_json.h"
#include "manager/metadata/dao/json/tables_dao_json.h"
#include "manager/metadata/helper/logging_helper.h"
#include "manager/metadata/dao/dao.h"
#include "manager/metadata/dao/json/index_dao_json.h"

namespace manager::metadata::db {

namespace json_parser = boost::property_tree::json_parser;
using boost::property_tree::json_parser_error;

// =============================================================================
// DbSessionManagerJson class methods.

std::shared_ptr<Dao> DbSessionManagerJson::get_index_dao() {
  return std::make_shared<IndexDaoJson>(this);
}

/**
 * @brief Sets the file name of the metadata.
 * @param file_name [in] file name.
 * @param root_node [in] root node name.
 * @return ErrorCode::OK if success, otherwise an error code.
 */
ErrorCode DbSessionManagerJson::connect(std::string_view file_name,
                                        std::string_view root_node) {
  ErrorCode error = ErrorCode::UNKNOWN;

  database_ = std::string(file_name);
  std::ifstream file(database_);
  if (file) {
    // open a metadata-table file.
    error = ErrorCode::OK;
  } else {
    // create a metadata-table file and initialize.
    clear_contents();
    contents_->put(root_node.data(), "");
    error = this->save_contents();
    if (error != ErrorCode::OK) {
      database_.clear();
    }
  }

  return error;
}

/**
 * @brief Load the metadata from the file.
 * @return ErrorCode::OK if success, otherwise an error code.
 */
ErrorCode DbSessionManagerJson::load_contents() const {

  ErrorCode error = ErrorCode::UNKNOWN;

  if (database_.empty()) {
    LOG_ERROR << Message::NOT_INITIALIZED;
    error = ErrorCode::NOT_INITIALIZED;
    return error;
  }

  try {
    json_parser::read_json(database_, *(contents_.get()));
  } catch (json_parser_error& e) {
    LOG_ERROR << Message::READ_JSON_FILE_FAILURE << database_ << "\n  "
              << e.what();
    error = ErrorCode::INTERNAL_ERROR;
    return error;
  } catch (...) {
    LOG_ERROR << Message::READ_JSON_FILE_FAILURE << database_;
    error = ErrorCode::INTERNAL_ERROR;
    return error;
  }

  error = ErrorCode::OK;
  return error;
}

/**
 * @brief Save the metadata to a file.
 * @return ErrorCode::OK if success, otherwise an error code.
 */
ErrorCode DbSessionManagerJson::save_contents() const {

  ErrorCode error = ErrorCode::UNKNOWN;

  if (database_.empty()) {
    LOG_ERROR << Message::NOT_INITIALIZED;
    error = ErrorCode::NOT_INITIALIZED;
    return error;
  }

  try {
    json_parser::write_json(database_, *(contents_.get()));
  } catch (json_parser_error& e) {
    LOG_ERROR << Message::WRITE_JSON_FAILURE << e.what();
    error = ErrorCode::INTERNAL_ERROR;
    return error;
  } catch (...) {
    LOG_ERROR << Message::WRITE_JSON_FAILURE;
    error = ErrorCode::INTERNAL_ERROR;
    return error;
  }
  error = ErrorCode::OK;

  return error;
}

/**
 * @brief Starts a transaction scope managed by this DBSessionManager.
 * @return ErrorCode::OK if success, otherwise an error code.
 */
ErrorCode DbSessionManagerJson::start_transaction() {
  ErrorCode error = ErrorCode::UNKNOWN;

  if (database_.empty()) {
    LOG_ERROR << Message::NOT_INITIALIZED;
    error = ErrorCode::NOT_INITIALIZED;
    return error;
  }

  clear_contents();

  error = ErrorCode::OK;
  return error;
}

/**
 * @brief Commits all transactions currently started for all DAO contexts
 *   managed by this DBSessionManager.
 * @return ErrorCode::OK if success, otherwise an error code.
 */
ErrorCode DbSessionManagerJson::commit() {
  ErrorCode error = ErrorCode::UNKNOWN;

  if (database_.empty()) {
    LOG_ERROR << Message::NOT_INITIALIZED;
    error = ErrorCode::NOT_INITIALIZED;
    return error;
  }

  this->save_contents();
  this->clear_contents();

  error = ErrorCode::OK;
  return error;
}

/**
 * @brief Rollbacks all transactions currently started for all DAO contexts
 *   managed by this DBSessionManager.
 * @return ErrorCode::OK if success, otherwise an error code.
 */
ErrorCode DbSessionManagerJson::rollback() {
  ErrorCode error = ErrorCode::UNKNOWN;

  if (database_.empty()) {
    LOG_ERROR << Message::NOT_INITIALIZED;
    error = ErrorCode::NOT_INITIALIZED;
    return error;
  }

  clear_contents();

  error = ErrorCode::OK;
  return error;
}

} // manager::metadata::db

// ==========================================================================
//  namespace manager::metadata::db::json

namespace manager::metadata::db::json {

namespace json_parser = boost::property_tree::json_parser;
using boost::property_tree::json_parser_error;

/**
 * @brief Gets Dao instance for the requested table name
 * @param table_name  [in]  unique id for the Dao.
 * @param gdao        [out] Dao instance if success.
 *   for the requested table name.
 * @return ErrorCode::OK if success, otherwise an error code.
 */
ErrorCode DBSessionManager::get_dao(const GenericDAO::TableName table_name,
                                    std::shared_ptr<GenericDAO>& gdao) {
  ErrorCode error = ErrorCode::UNKNOWN;

  error = create_dao(table_name, (manager::metadata::db::DBSessionManager*)this,
                     gdao);

  return error;
}

/**
 * @brief Starts a transaction scope managed by this DBSessionManager.
 * @return ErrorCode::OK if success, otherwise an error code.
 */
ErrorCode DBSessionManager::start_transaction() {
  ErrorCode error = ErrorCode::UNKNOWN;

  if (file_name_.empty()) {
    LOG_ERROR << Message::NOT_INITIALIZED;
    error = ErrorCode::NOT_INITIALIZED;
    return error;
  }

  init_meta_data();

  // Load the meta data from the JSON file.
  error = load_object();

  return error;
}

/**
 * @brief Commits all transactions currently started for all DAO contexts
 *   managed by this DBSessionManager.
 * @return ErrorCode::OK if success, otherwise an error code.
 */
ErrorCode DBSessionManager::commit() {
  ErrorCode error = ErrorCode::UNKNOWN;

  if (file_name_.empty()) {
    LOG_ERROR << Message::NOT_INITIALIZED;
    error = ErrorCode::NOT_INITIALIZED;
    return error;
  }

  save_object();
  init_meta_data();

  error = ErrorCode::OK;
  return error;
}

/**
 * @brief Rollbacks all transactions currently started for all DAO contexts
 *   managed by this DBSessionManager.
 * @return ErrorCode::OK if success, otherwise an error code.
 */
ErrorCode DBSessionManager::rollback() {
  ErrorCode error = ErrorCode::UNKNOWN;

  if (file_name_.empty()) {
    LOG_ERROR << Message::NOT_INITIALIZED;
    error = ErrorCode::NOT_INITIALIZED;
    return error;
  }

  init_meta_data();

  error = ErrorCode::OK;
  return error;
}

/**
 * @brief Sets the file name of the metadata.
 * @param (file_name)  [in] file name.
 * @param (initial_node)  [in] Node name when initializing.
 * @return ErrorCode::OK if success, otherwise an error code.
 */
ErrorCode DBSessionManager::connect(std::string_view file_name,
                                    std::string_view initial_node) {
  ErrorCode error = ErrorCode::UNKNOWN;

  if (!file_name_.empty()) {
    error = ErrorCode::OK;
  } else {
    file_name_ = std::string(file_name);

    std::ifstream file(file_name_);
    if (file) {
      // open metadata-table
      error = ErrorCode::OK;
    } else {
      // create metadata-table
      init_meta_data();
      meta_object_->put(initial_node.data(), "");
      error = save_object();

      if (error != ErrorCode::OK) {
        file_name_.clear();
      }
    }
  }

  return error;
}

/**
 * @brief Gets the metadata object.
 * @return metadata object.
 */
boost::property_tree::ptree* DBSessionManager::get_container() const {
  return meta_object_.get();
}

/**
 * @brief Load the metadata from the file.
 * @return ErrorCode::OK if success, otherwise an error code.
 */
ErrorCode DBSessionManager::load_object() const {
  ErrorCode error = ErrorCode::UNKNOWN;

  if (file_name_.empty()) {
    LOG_ERROR << Message::NOT_INITIALIZED;
    error = ErrorCode::NOT_INITIALIZED;
    return error;
  }

  if (!meta_object_->empty()) {
    LOG_DEBUG << "Metadata is already loaded.";
    error = ErrorCode::OK;
    return error;
  }
  LOG_DEBUG << "Loading Metadata.";

  try {
    json_parser::read_json(file_name_, *(meta_object_.get()));
  } catch (json_parser_error& e) {
    LOG_ERROR << Message::READ_JSON_FILE_FAILURE << file_name_ << "\n  "
              << e.what();
    error = ErrorCode::INTERNAL_ERROR;
    return error;
  } catch (...) {
    LOG_ERROR << Message::READ_JSON_FILE_FAILURE << file_name_;
    error = ErrorCode::INTERNAL_ERROR;
    return error;
  }

  error = ErrorCode::OK;
  return error;
}

/* =============================================================================
 * Private method area
 */

/**
 * @brief Initialize the metadata object.
 *   access private.
 * @return none.
 */
void DBSessionManager::init_meta_data() { meta_object_->clear(); }

/**
 * @brief Save the metadata to a file.
 * @return ErrorCode::OK if success, otherwise an error code.
 */
ErrorCode DBSessionManager::save_object() const {
  ErrorCode error = ErrorCode::UNKNOWN;

  if (file_name_.empty()) {
    LOG_ERROR << Message::NOT_INITIALIZED;
    error = ErrorCode::NOT_INITIALIZED;
    return error;
  }

  try {
    json_parser::write_json(file_name_, *(meta_object_.get()));
  } catch (json_parser_error& e) {
    LOG_ERROR << Message::WRITE_JSON_FAILURE << e.what();
    error = ErrorCode::INTERNAL_ERROR;
    return error;
  } catch (...) {
    LOG_ERROR << Message::WRITE_JSON_FAILURE;
    error = ErrorCode::INTERNAL_ERROR;
    return error;
  }

  error = ErrorCode::OK;
  return error;
}

}  // namespace manager::metadata::db::json
