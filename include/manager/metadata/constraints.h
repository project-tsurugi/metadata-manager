/*
 * Copyright 2022 Project Tsurugi.
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
#ifndef MANAGER_METADATA_CONSTRAINTS_H_
#define MANAGER_METADATA_CONSTRAINTS_H_

#include <string>
#include <vector>

#include <boost/property_tree/ptree.hpp>

#include "manager/metadata/metadata.h"

namespace manager::metadata {

/**
 * @brief Constraint metadata object.
 */
struct Constraint : public Object {
  /**
   * @brief Field name constant indicating the table id of the metadata.
   */
  static constexpr const char* const TABLE_ID = "tableId";
  /**
   * @brief Field name constant indicating the constraint TYPE of the
   *   metadata.
   */
  static constexpr const char* const TYPE = "type";
  /**
   * @brief Field name constant indicating the list of column numbers subject to constraints.
   */
  static constexpr const char* const COLUMNS = "columns";
  /**
   * @brief Field name constant indicating the list of column IDs subject to constraints.
   */
  static constexpr const char* const COLUMNS_ID = "columnsId";
  /**
   * @brief Field name constant indicating the index ID.
   */
  static constexpr const char* const INDEX_ID = "indexId";
  /**
   * @brief Field name constant indicating the constraints with expressions (CHECK) of
   *   the metadata.
   */
  static constexpr const char* const EXPRESSION = "expression";
  /**
   * @brief Field name constant indicating the referenced table name of the foreign key constraint.
   */
  static constexpr const char* const PK_TABLE = "pkTable";
  /**
   * @brief Field name constant indicating the list of referenced column numbers for foreign key constraint.
   */
  static constexpr const char* const PK_COLUMNS = "pkColumns";
  /**
   * @brief Field name constant indicating the list of referenced column IDs for foreign key constraint.
   */
  static constexpr const char* const PK_COLUMNS_ID = "pkColumnsId";
  /**
   * @brief Field name constant indicating the match type for referenced rows in foreign key constraint.
   */
  static constexpr const char* const FK_MATCH_TYPE = "fkMatchType";
  /**
   * @brief Field name constant indicating the delete action of referenced row in foreign key constraint.
   */
  static constexpr const char* const FK_DELETE_ACTION = "fkDeleteAction";
  /**
   * @brief Field name constant indicating the update action of referenced row in foreign key constraint.
   */
  static constexpr const char* const FK_UPDATE_ACTION = "fkUpdateAction";

  /**
   * @brief Represents the type of constraint.
   */
  enum class ConstraintType : int64_t {
    PRIMARY_KEY = 0,          //!< @brief Primary Key Constraints.
    UNIQUE,                   //!< @brief Uniqueness Constraints.
    CHECK,                    //!< @brief Check Constraints.
    FOREIGN_KEY,              //!< @brief Foreign Key Constraints.
    TRIGGER,                  //!< @brief Constraint Triggers. (Not supported)
    EXCLUDE,                  //!< @brief Exclusive Constraints. (Not supported)
    UNKNOWN = INVALID_VALUE,  //!< @brief Unknown Constraints.
  };

  /**
   * @brief Represents the match type for referenced rows.
   */
  enum class MatchType : int64_t {
    SIMPLE = 0,               //!< @brief MATCH SIMPLE.
    FULL,                     //!< @brief MATCH FULL.
    PARTIAL,                  //!< @brief MATCH PARTIAL.
    UNKNOWN = INVALID_VALUE,  //!< @brief Unknown.
  };

  /**
   * @brief Represents the match type for referenced rows.
   */
  enum class ActionType : int64_t {
    NO_ACTION = 0,            //!< @brief NO ACTION.
    RESTRICT,                 //!< @brief RESTRICT.
    CASCADE,                  //!< @brief CASCADE.
    SET_NULL,                 //!< @brief SET NULL.
    SET_DEFAULT,              //!< @brief SET DEFAULT.
    UNKNOWN = INVALID_VALUE,  //!< @brief Unknown.
  };

  Constraint()
      : Object(),
        table_id(INVALID_OBJECT_ID),
        type(ConstraintType::UNKNOWN),
        columns({}),
        columns_id({}),
        index_id(INVALID_VALUE),
        expression(""),
        pk_table(""),
        pk_columns({}),
        pk_columns_id({}),
        fk_match_type(MatchType::UNKNOWN),
        fk_delete_action(ActionType::UNKNOWN),
        fk_update_action(ActionType::UNKNOWN) {}

  ObjectId table_id;                    //!< @brief Table id of the metadata.
  ConstraintType type;                  //!< @brief Constraint TYPE of the metadata.
  std::vector<int64_t> columns;         //!< @brief List of column numbers subject to constraints.
  std::vector<ObjectId> columns_id;     //!< @brief Column IDs subject to constraints.
  int64_t index_id;                     //!< @brief Index ID.
  std::string expression;               //!< @brief Expression of constraint (CHECK).
  std::string pk_table;                 //!< @brief Referenced table name.
  std::vector<int64_t> pk_columns;      //!< @brief List of referenced column numbers.
  std::vector<ObjectId> pk_columns_id;  //!< @brief List of referenced column IDs.
  MatchType fk_match_type;              //!< @brief Match type for referenced rows.
  ActionType fk_delete_action;          //!< @brief Delete action of referenced row.
  ActionType fk_update_action;          //!< @brief Update action of referenced row.

  boost::property_tree::ptree convert_to_ptree() const override;
  void convert_from_ptree(const boost::property_tree::ptree& ptree) override;
};

/**
 * @brief Container of constraint metadata objects.
 */
class Constraints : public Metadata {
 public:
  explicit Constraints(std::string_view database)
      : Constraints(database, kDefaultComponent) {}
  Constraints(std::string_view database, std::string_view component)
      : Metadata(database, component) {}

  Constraints(const Constraints&)            = delete;
  Constraints& operator=(const Constraints&) = delete;
  virtual ~Constraints() {}

  ErrorCode init() const override;

  ErrorCode add(const boost::property_tree::ptree& object) const override;
  ErrorCode add(const boost::property_tree::ptree& object, ObjectId* object_id) const override;

  ErrorCode get(const ObjectId object_id, boost::property_tree::ptree& object) const override;
  ErrorCode get([[maybe_unused]] std::string_view object_name,
                [[maybe_unused]] boost::property_tree::ptree& object) const override {
    return ErrorCode::UNKNOWN;
  }
  ErrorCode get_all(std::vector<boost::property_tree::ptree>& container) const override;

  ErrorCode update([[maybe_unused]] const ObjectIdType object_id,
                   [[maybe_unused]] const boost::property_tree::ptree& object) const override {
    return ErrorCode::UNKNOWN;
  }

  ErrorCode remove(const ObjectId object_id) const override;
  ErrorCode remove([[maybe_unused]] std::string_view object_name,
                   [[maybe_unused]] ObjectId* object_id) const override {
    return ErrorCode::UNKNOWN;
  }

 private:
  ErrorCode param_check_metadata_add(
      const boost::property_tree::ptree& object) const;
};

}  // namespace manager::metadata

#endif  // MANAGER_METADATA_CONSTRAINTS_H_
