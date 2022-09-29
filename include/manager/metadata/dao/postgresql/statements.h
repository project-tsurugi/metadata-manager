#include <string>
#include <string_view>
#include "manager/metadata/dao/dao.h"
#include "manager/metadata/dao/db_session_manager.h"

namespace manager::metadata::db {

/**
 * @brief
 */
class Statement {
 public:
  explicit Statement(std::string_view table_name, std::string_view statement) 
      : table_name_(table_name), statement_(statement) {}

  std::string table_name() const { return table_name_; }
  std::string statement() const { return statement_; }

  virtual std::string name() const {
    return table_name_ + ':' + this->get_base_name();
  }

 protected:
  virtual std::string get_base_name() const = 0;

 private:
  std::string table_name_;
  std::string statement_;
  std::string name_;
};

/**
 * @brief
 */
class StatementWithKey : public Statement {
 public:
  explicit StatementWithKey(std::string_view table_name, 
                            std::string_view statement, 
                            std::string_view key)
      : Statement(table_name, statement), key_(key) {}

  std::string key() { return key_; }

  virtual std::string name() const override  {
    return table_name() + ':' + this->get_base_name() + '-' + key_.data();
  }

protected:
  virtual std::string get_base_name() const { ""; }

 private:
  std::string key_;
};

// ==========================================================================
// Concreate classes.

/**
 * @brief
 */
class InsertStatement : public Statement {
 public:
  explicit InsertStatement(std::string_view table_name, std::string_view statement)
      : Statement(table_name, statement) {}

 protected:
  static constexpr const char* const BASE_NAME = "insert_statement";
  std::string get_base_name() const override { return BASE_NAME; }
};

/**
 * @brief
 */
class SelectAllStatement : public Statement {
 public:
  explicit SelectAllStatement(std::string_view table_name, std::string_view statement) 
      : Statement(table_name, statement) {}

 protected:
  static constexpr const char* const BASE_NAME = "select_all_statement";
  std::string get_base_name() const override { return BASE_NAME; }
};

/**
 * @brief
 */
class SelectStatement : public StatementWithKey {
 public:
  explicit SelectStatement(std::string_view table_name, 
                          std::string_view statement, 
                          std::string_view key)
      : StatementWithKey(table_name, statement, key) {}

 protected:
  static constexpr const char* const BASE_NAME = "select_statement";
  std::string get_base_name() const override { return BASE_NAME; }
};

} // namespace manager::metadata::db
