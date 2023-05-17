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

#include <boost/property_tree/json_parser.hpp>

#include "manager/metadata/common/message.h"
#include "manager/metadata/dao/json/columns_dao_json.h"
#include "manager/metadata/dao/json/constraints_dao_json.h"
#include "manager/metadata/dao/json/datatypes_dao_json.h"
#include "manager/metadata/dao/json/index_dao_json.h"
#include "manager/metadata/dao/json/privileges_dao_json.h"
#include "manager/metadata/dao/json/roles_dao_json.h"
#include "manager/metadata/dao/json/statistics_dao_json.h"
#include "manager/metadata/dao/json/tables_dao_json.h"
#include "manager/metadata/helper/logging_helper.h"

namespace manager::metadata::db {

namespace json_parser = boost::property_tree::json_parser;
using boost::property_tree::json_parser_error;

// =============================================================================
// DbSessionManagerJson class methods.

/**
 * @brief Get an instance of a DAO for table metadata.
 * @return Instance of a DAO.
 */
std::shared_ptr<Dao> DbSessionManagerJson::get_tables_dao() {
  return std::make_shared<TablesDaoJson>(this);
}

/**
 * @brief Get an instance of a DAO for column metadata.
 * @return Instance of a DAO.
 */
std::shared_ptr<Dao> DbSessionManagerJson::get_columns_dao() {
  return std::make_shared<ColumnsDaoJson>(this);
}

/**
 * @brief Get an instance of a DAO for index metadata.
 * @return Instance of a DAO.
 */
std::shared_ptr<Dao> DbSessionManagerJson::get_indexes_dao() {
  return std::make_shared<IndexDaoJson>(this);
}

/**
 * @brief Get an instance of a DAO for constraint metadata.
 * @return Instance of a DAO.
 */
std::shared_ptr<Dao> DbSessionManagerJson::get_constraints_dao() {
  return std::make_shared<ConstraintsDaoJson>(this);
}

/**
 * @brief Get an instance of a DAO for data-type metadata.
 * @return Instance of a DAO.
 */
std::shared_ptr<Dao> DbSessionManagerJson::get_datatypes_dao() {
  return std::make_shared<DataTypesDaoJson>(this);
}

/**
 * @brief Get an instance of a DAO for role metadata.
 * @return Instance of a DAO.
 */
std::shared_ptr<Dao> DbSessionManagerJson::get_roles_dao() {
  return std::make_shared<RolesDaoJson>(this);
}

/**
 * @brief Get an instance of a DAO for privilege metadata.
 * @return Instance of a DAO.
 */
std::shared_ptr<Dao> DbSessionManagerJson::get_privileges_dao() {
  return std::make_shared<PrivilegesDaoJson>(this);
}

/**
 * @brief Get an instance of a DAO for statistic metadata.
 * @return Instance of a DAO.
 */
std::shared_ptr<Dao> DbSessionManagerJson::get_statistics_dao() {
  // Create an instance of DAO.
  return std::make_shared<StatisticsDaoJson>(this);
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
    this->clear_contents();
    error = ErrorCode::OK;
  } else {
    // create a metadata-table file and initialize.
    this->clear_contents();
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

  if (!contents_->empty()) {
    LOG_DEBUG << "Metadata is already loaded.";
    error = ErrorCode::OK;
    return error;
  }
  LOG_DEBUG << "Loading Metadata.";

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
 * @brief Starts a transaction scope managed by this DbSessionManager.
 * @return ErrorCode::OK if success, otherwise an error code.
 */
ErrorCode DbSessionManagerJson::start_transaction() {
  ErrorCode error = ErrorCode::UNKNOWN;

  if (database_.empty()) {
    LOG_ERROR << Message::NOT_INITIALIZED;
    error = ErrorCode::NOT_INITIALIZED;
    return error;
  }

  this->clear_contents();

  error = ErrorCode::OK;
  return error;
}

/**
 * @brief Commits all transactions currently started for all DAO contexts
 *   managed by this DbSessionManager.
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
 *   managed by this DbSessionManager.
 * @return ErrorCode::OK if success, otherwise an error code.
 */
ErrorCode DbSessionManagerJson::rollback() {
  ErrorCode error = ErrorCode::UNKNOWN;

  if (database_.empty()) {
    LOG_ERROR << Message::NOT_INITIALIZED;
    error = ErrorCode::NOT_INITIALIZED;
    return error;
  }

  this->clear_contents();

  error = ErrorCode::OK;
  return error;
}

}  // namespace manager::metadata::db
