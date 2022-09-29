#include <string>
#include <string_view>
#include <typeinfo>
#include <unordered_map>
#include "manager/metadata/dao/dao.h"
#include "manager/metadata/dao/db_session_manager.h"

namespace manager::metadata::db {

/**
 * @brief
 */
class Statement {
 public:
  Statement() {}
  explicit Statement(std::string_view table_name, std::string_view statement) 
      : table_name_(table_name), statement_(statement) {}
//  Statement(const Statement&) = delete;
//  Statement& operator=(const Statement&) = delete;
  virtual ~Statement() {}

  void set(std::string_view table_name, std::string_view statement) {
    table_name_ = table_name;
    statement_ = statement;
  }
  std::string table_name() const { return table_name_; }
  std::string statement() const { return statement_; }
  virtual std::string name() const {
    return table_name_ + ':' + this->get_base_name();
  }

 protected:
  virtual std::string get_base_name() const {
    const auto& id = typeid(this);
    return id.name();
  }

 private:
  std::string table_name_;
  std::string statement_;
};

/**
 * @brief
 */
class StatementWithKey : public Statement {
 public:
  StatementWithKey() : Statement() {}
  explicit StatementWithKey(std::string_view table_name, 
                            std::string_view statement, 
                            std::string_view key) 
      : Statement(table_name, statement), key_(key) {}
//  StatementWithKey(const StatementWithKey&) = delete;
//  StatementWithKey& operator=(const StatementWithKey&) = delete;
  virtual ~StatementWithKey() {}

  void set(std::string_view table_name, 
          std::string_view statement, 
          std::string_view key) 
  {
    Statement::set(table_name, statement);
    key_ = key;
  }
  virtual std::string name() const override {
    return table_name() + ':' + this->get_base_name() + '-' + key_.data();
  }

 private:
  std::string key_;
};

using StatementMap = std::unordered_map<std::string_view, Statement>;
#if 0
static ErrorCode find_statement(
    const std::unordered_map<std::string_view, Statement>& statements, 
    std::string_view key,
    Statement& statement) {

  Statement statement;
  try {
    statement = statements.at(key.data());
  }
  catch (std::out_of_range& e) {
    return ErrorCode::NOT_FOUND;
  }
  catch (...) {
    return ErrorCode::INTERNAL_ERROR;
  }

  return ErrorCode::OK;
}
#endif
// ==========================================================================
// Concreate classes.
/**
 * @brief
 */
class InsertStatement : public Statement {
 public:
  InsertStatement() : Statement() {}
  InsertStatement(std::string_view table_name, std::string_view statement) 
      : Statement(table_name, statement) {}
};

/**
 * @brief
 */
class SelectAllStatement : public Statement {
 public:
  SelectAllStatement() : Statement() {}
  SelectAllStatement(std::string_view table_name, std::string_view statement)
      : Statement(table_name, statement) {}
};

/**
 * @brief
 */
class SelectStatement : public StatementWithKey {
 public:
  SelectStatement() {}
  SelectStatement(std::string_view table_name, 
                  std::string_view statement, 
                  std::string_view key)
      : StatementWithKey(table_name, statement, key) {}
};

/**
 * @brief
 */
class UpdateStatement : public StatementWithKey {
 public:
  UpdateStatement() {}
  UpdateStatement(std::string_view table_name, 
                  std::string_view statement, 
                  std::string_view key)
      : StatementWithKey(table_name, statement, key) {}
};

/**
 * @brief
 */
class DeleteStatement : public StatementWithKey {
 public:
  DeleteStatement() {}
  DeleteStatement(std::string_view table_name, 
                  std::string_view statement, 
                  std::string_view key)
      : StatementWithKey(table_name, statement, key) {}
};

} // namespace manager::metadata::db
