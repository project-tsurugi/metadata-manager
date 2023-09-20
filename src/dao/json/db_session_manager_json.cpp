/*
 * Copyright 2021-2023 tsurugi project.
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

#include <filesystem>

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
#include "manager/metadata/helper/ptree_helper.h"

namespace manager::metadata::db {

namespace json_parser = boost::property_tree::json_parser;
using boost::property_tree::json_parser_error;
using boost::property_tree::ptree;

// =============================================================================
// DbSessionManagerJson class methods.

ErrorCode DbSessionManagerJson::get_tables_dao(std::shared_ptr<Dao>& dao) {
  // Generate an instance of tables DAO.
  return this->create_dao_instance<TablesDaoJson>(dao);
}

ErrorCode DbSessionManagerJson::get_columns_dao(std::shared_ptr<Dao>& dao) {
  // Generate an instance of columns DAO.
  return this->create_dao_instance<ColumnsDaoJson>(dao);
}

ErrorCode DbSessionManagerJson::get_indexes_dao(std::shared_ptr<Dao>& dao) {
  // Generate an instance of indexes DAO.
  return this->create_dao_instance<IndexDaoJson>(dao);
}

ErrorCode DbSessionManagerJson::get_constraints_dao(std::shared_ptr<Dao>& dao) {
  // Generate an instance of constraints DAO.
  return this->create_dao_instance<ConstraintsDaoJson>(dao);
}

ErrorCode DbSessionManagerJson::get_datatypes_dao(std::shared_ptr<Dao>& dao) {
  // Generate an instance of datatypes DAO.
  return this->create_dao_instance<DataTypesDaoJson>(dao);
}

ErrorCode DbSessionManagerJson::get_roles_dao(std::shared_ptr<Dao>& dao) {
  // Generate an instance of roles DAO.
  return this->create_dao_instance<RolesDaoJson>(dao);
}

ErrorCode DbSessionManagerJson::get_privileges_dao(std::shared_ptr<Dao>& dao) {
  // Generate an instance of privileges DAO.
  return this->create_dao_instance<PrivilegesDaoJson>(dao);
}

ErrorCode DbSessionManagerJson::get_statistics_dao(std::shared_ptr<Dao>& dao) {
  // Generate an instance of statistics DAO.
  return this->create_dao_instance<StatisticsDaoJson>(dao);
}

/**
 * @brief Starts a transaction scope managed by this DbSessionManager.
 * @return ErrorCode::OK if success, otherwise an error code.
 */
ErrorCode DbSessionManagerJson::start_transaction() {
  ErrorCode error = ErrorCode::UNKNOWN;

  // Locking within the transaction scope.
  transaction_lock.lock();

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

  if (!transaction_lock.is_lock()) {
    LOG_ERROR << Message::TRANSACTION_NOT_START;
    error = ErrorCode::INTERNAL_ERROR;
    return error;
  }

  this->save_contents();
  this->clear_contents();

  // Unlocks a lock in the transaction scope.
  transaction_lock.unlock();

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

  if (!transaction_lock.is_lock()) {
    LOG_ERROR << Message::TRANSACTION_NOT_START;
    error = ErrorCode::INTERNAL_ERROR;
    return error;
  }

  this->clear_contents();

  // Unlocks a lock in the transaction scope.
  transaction_lock.unlock();

  error = ErrorCode::OK;
  return error;
}

ErrorCode DbSessionManagerJson::load_contents(
    std::string_view database, std::string_view root_node,
    boost::property_tree::ptree& object) {
  ErrorCode error = ErrorCode::UNKNOWN;

  if (contents_map_.count(std::string(database)) == 0) {
    LOG_DEBUG << "Loading Metadata.: " << database;

    ptree contents;
    if (std::filesystem::exists(database)) {
      try {
        json_parser::read_json(std::string(database), contents);
        error = ErrorCode::OK;
      } catch (json_parser_error& e) {
        LOG_ERROR << Message::READ_JSON_FILE_FAILURE << database << "\n  "
                  << e.what();
        error = ErrorCode::INTERNAL_ERROR;
      } catch (...) {
        LOG_ERROR << Message::READ_JSON_FILE_FAILURE << database;
        error = ErrorCode::INTERNAL_ERROR;
      }
    } else {
      contents.put(root_node.data(), "");
      error = ErrorCode::OK;
    }

    if (error == ErrorCode::OK) {
#ifndef NDEBUG
      LOG_DEBUG << "[" << database << "]"
                << ptree_helper::ptree_to_json(contents);
#endif

      object = contents;
      if (transaction_lock.is_lock()) {
        contents_map_[std::string(database)] = contents;
      }
    }
  } else {
    LOG_DEBUG << "Metadata is already loaded.: " << database;

    object = contents_map_[std::string(database)].data();
    error  = ErrorCode::OK;
  }

  return error;
}

void DbSessionManagerJson::set_contents(
    std::string_view database, const boost::property_tree::ptree& object) {
  contents_map_[std::string(database)] = object;
}

ErrorCode DbSessionManagerJson::save_contents() const {
  ErrorCode error = ErrorCode::UNKNOWN;

  if (contents_map_.size() == 0) {
    LOG_WARNING << "No content has been set.";
    error = ErrorCode::OK;
    return error;
  }

  error = ErrorCode::OK;
  for (auto& element : contents_map_) {
    const auto& database = element.first;
    const auto& content  = element.second;

    if (content.is_modified()) {
      try {
        LOG_INFO << "Metadata has been written.: " << database;
#ifndef NDEBUG
        LOG_DEBUG << "[" << database << "]"
                  << ptree_helper::ptree_to_json(content.data());
#endif

        json_parser::write_json(database, content.data());
      } catch (json_parser_error& e) {
        LOG_ERROR << Message::WRITE_JSON_FAILURE << e.what();

        error = ErrorCode::INTERNAL_ERROR;
        break;
      } catch (...) {
        LOG_ERROR << Message::WRITE_JSON_FAILURE;

        error = ErrorCode::INTERNAL_ERROR;
        break;
      }
    }
  }

  return error;
}

}  // namespace manager::metadata::db
