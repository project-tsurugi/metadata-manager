/*
 * Copyright 2021-2023 Project Tsurugi.
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
#pragma once

#include <memory>
#include <mutex>
#include <string>
#include <string_view>
#include <unordered_map>

#include <boost/property_tree/ptree.hpp>

#include "manager/metadata/dao/db_session_manager.h"
#include "manager/metadata/error_code.h"

#include "manager/metadata/helper/ptree_helper.h"

namespace manager::metadata::db {

/**
 * @brief Class for managing sessions with JSON files.
 */
class DbSessionManagerJson : public DbSessionManager {
 public:
  /**
   * @brief Function defined for compatibility.
   * @return Always ErrorCode::OK.
   */
  ErrorCode connect() override { return ErrorCode::OK; }

  /**
   * @brief Get an instance of a DAO for table metadata.
   * @param dao  [out] DAO instance.
   * @return ErrorCode::OK if success, otherwise an error code.
   */
  ErrorCode get_tables_dao(std::shared_ptr<Dao>& dao) override;

  /**
   * @brief Get an instance of a DAO for column metadata.
   * @param dao  [out] DAO instance.
   * @return ErrorCode::OK if success, otherwise an error code.
   */
  ErrorCode get_columns_dao(std::shared_ptr<Dao>& dao) override;

  /**
   * @brief Get an instance of a DAO for index metadata.
   * @param dao  [out] DAO instance.
   * @return ErrorCode::OK if success, otherwise an error code.
   */
  ErrorCode get_indexes_dao(std::shared_ptr<Dao>& dao) override;

  /**
   * @brief Get an instance of a DAO for constraint metadata.
   * @param dao  [out] DAO instance or nullptr.
   * @return DAO instance ErrorCode::OK if success, otherwise an error code.
   */
  ErrorCode get_constraints_dao(std::shared_ptr<Dao>& dao) override;

  /**
   * @brief Get an instance of a DAO for data-type metadata.
   * @param dao  [out] DAO instance.
   * @return ErrorCode::OK if success, otherwise an error code.
   */
  ErrorCode get_datatypes_dao(std::shared_ptr<Dao>& dao) override;

  /**
   * @brief Get an instance of a DAO for role metadata.
   * @param dao  [out] DAO instance.
   * @return ErrorCode::OK if success, otherwise an error code.
   */
  ErrorCode get_roles_dao(std::shared_ptr<Dao>& dao) override;

  /**
   * @brief Get an instance of a DAO for privilege metadata.
   * @param dao  [out] DAO instance.
   * @return ErrorCode::OK if success, otherwise an error code.
   */
  ErrorCode get_privileges_dao(std::shared_ptr<Dao>& dao) override;

  /**
   * @brief Get an instance of a DAO for statistic metadata.
   * @param dao  [out] DAO instance.
   * @return ErrorCode::OK if success, otherwise an error code.
   */
  ErrorCode get_statistics_dao(std::shared_ptr<Dao>& dao) override;

  /**
   * @brief Starts a transaction scope managed by this DBSessionManager.
   * @return ErrorCode::OK if success, otherwise an error code.
   */
  ErrorCode start_transaction() override;

  /**
   * @brief Commits all transactions currently started for all DAO contexts
   *   managed by this DBSessionManager.
   * @return ErrorCode::OK if success, otherwise an error code.
   */
  ErrorCode commit() override;

  /**
   * @brief Rollbacks all transactions currently started for all DAO contexts
   *   managed by this DBSessionManager.
   * @return ErrorCode::OK if success, otherwise an error code.
   */
  ErrorCode rollback() override;

  /**
   * @brief Loads a metadata object from a metadata table file.
   * @param database  [in]  path to the JSON file.
   * @param root_node [in]  root node name.
   * @param object    [out] metadata object.
   * @return ErrorCode::OK if success, otherwise an error code.
   */
  ErrorCode load_contents(std::string_view database, std::string_view root_node,
                          boost::property_tree::ptree& object);

  /**
   * @brief Set the contents object.
   *   To make it persistent, perform transaction control
   *   (start_transaction(), commit()).
   * @param database  [in]  path to the JSON file.
   * @param object    [in]  metadata object.
   */
  void set_contents(std::string_view database,
                    const boost::property_tree::ptree& object);

 private:
  class Content {
   public:
    Content() : pre_hash(0), cur_hash(0) {}
    Content& operator=(const boost::property_tree::ptree& content) {
      this->data_ = content;

      auto hash_value =
          std::hash<std::string>()(ptree_helper::ptree_to_json(content));
      if (this->pre_hash == 0) {
        this->pre_hash = hash_value;
      } else {
        this->cur_hash = hash_value;
      }

      return *this;
    }

    const boost::property_tree::ptree& data() const { return this->data_; }
    boost::property_tree::ptree* data_ptr() { return &this->data_; }
    bool is_modified() const { return (this->pre_hash != this->cur_hash); }

   private:
    std::size_t pre_hash;
    std::size_t cur_hash;
    boost::property_tree::ptree data_;
  };

  class MutexWrapper {
   public:
    MutexWrapper() : locking_(false) {}

    void lock() {
      mutex_lock_.lock();
      locking_ = true;
    }
    void unlock() {
      mutex_lock_.unlock();
      locking_ = false;
    }
    bool is_lock() { return locking_; }

   private:
    std::mutex mutex_lock_;
    bool locking_;
  };

  MutexWrapper transaction_lock;
  std::unordered_map<std::string, Content> contents_map_ = {};

  /**
   * @brief Save the metadata to a file.
   * @param database  [in]  path to the JSON file.
   * @return ErrorCode::OK if success, otherwise an error code.
   */
  ErrorCode save_contents() const;

  /**
   * @brief Clear the content data.
   */
  void clear_contents() { contents_map_.clear(); }

  /**
   * @brief Create and initialize an instance of the DAO.
   * @param dao  [in/out] DAO of the metadata.
   * @return ErrorCode::OK if success, otherwise an error code.
   */
  template <typename T, typename = std::enable_if_t<std::is_base_of_v<Dao, T>>>
  ErrorCode create_dao_instance(std::shared_ptr<Dao>& dao) {
    return DbSessionManager::create_dao_instance<T>(dao, this);
  }
};  // class DbSessionManagerJson

}  // namespace manager::metadata::db
