/*
 * Copyright 2022 tsurugi project.
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
#ifndef MANAGER_METADATA_DAO_COMMON_STATEMENTS_H_
#define MANAGER_METADATA_DAO_COMMON_STATEMENTS_H_

#include <string>
#include <string_view>
#include <typeinfo>
#include <unordered_map>

#include "manager/metadata/dao/dao.h"
#include "manager/metadata/dao/db_session_manager.h"

namespace manager::metadata::db {

/**
 * @brief Base class for managing SQL statements.
 */
class Statement {
 public:
  static constexpr const char* const kDefaultKey = "DefaultStatementKey";

  Statement() {}
  explicit Statement(std::string_view table_name, std::string_view statement)
      : table_name_(table_name), statement_(statement), key_(kDefaultKey) {}
  explicit Statement(std::string_view table_name, std::string_view statement,
                     std::string_view key)
      : table_name_(table_name), statement_(statement), key_(key) {}

  virtual ~Statement() {}

  void set(std::string_view table_name, std::string_view statement,
           std::string_view key) {
    table_name_ = table_name;
    statement_  = statement;
    key_        = key;
  }

  std::string table_name() const { return table_name_; }
  std::string statement() const { return statement_; }
  std::string key() const { return key_; }
  std::string name() const {
    return table_name_ + ':' + this->get_base_name() + '-' + key_.data();
  }

 protected:
  virtual std::string get_base_name() const {
    const auto& id = typeid(this);
    return id.name();
  }

 private:
  std::string table_name_;
  std::string statement_;
  std::string key_;
};

using StatementMap = std::unordered_map<std::string_view, Statement>;

// ==========================================================================
// Concrete classes.

/**
 * @brief Class for managing insert statements.
 */
class InsertStatement : public Statement {
 public:
  // Inheritance constructor.
  using Statement::Statement;

 private:
  std::string get_base_name() const override {
    return std::to_string(__LINE__);
  }
};

/**
 * @brief Class for managing select statements.
 */
class SelectStatement : public Statement {
 public:
  // Inheritance constructor.
  using Statement::Statement;

 private:
  std::string get_base_name() const override {
    return std::to_string(__LINE__);
  }
};

/**
 * @brief Class for managing update statements.
 */
class UpdateStatement : public Statement {
 public:
  // Inheritance constructor.
  using Statement::Statement;

 private:
  std::string get_base_name() const override {
    return std::to_string(__LINE__);
  }
};

/**
 * @brief Class for managing delete statements.
 */
class DeleteStatement : public Statement {
 public:
  // Inheritance constructor.
  using Statement::Statement;

 private:
  std::string get_base_name() const override {
    return std::to_string(__LINE__);
  }
};

}  // namespace manager::metadata::db

#endif  // MANAGER_METADATA_DAO_COMMON_STATEMENTS_H_
