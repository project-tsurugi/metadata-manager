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
#ifndef MANAGER_METADATA_CONSTRAINTS_H_
#define MANAGER_METADATA_CONSTRAINTS_H_

#include <string>
#include <vector>
#include <memory>
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
   * @brief Represents the type of constraint.
   */
  enum class ConstraintType {
    PRIMARY_KEY = 0,          //!< @brief Primary Key Constraints.
    UNIQUE,                   //!< @brief Uniqueness Constraints.
    CHECK,                    //!< @brief Check Constraints.
    FOREIGN_KEY,              //!< @brief Foreign Key Constraints.
    TRIGGER,                  //!< @brief Constraint Triggers. (Not supported)
    EXCLUDE,                  //!< @brief Exclusive Constraints. (Not supported)
    UNKNOWN = INVALID_VALUE,  //!< @brief Unknown Constraints.
  };

  Constraint() {}
  ObjectId table_id;                 //!< @brief Table id of the metadata.
  ConstraintType type;               //!< @brief Constraint TYPE of the metadata.
  std::vector<int64_t> columns;      //!< @brief List of column numbers subject to constraints.
  std::vector<ObjectId> columns_id;  //!< @brief Column IDs subject to constraints.
  int64_t index_id;                  //!< @brief Index ID.
  std::string expression;            //!< @brief Expression of constraint (CHECK).

  boost::property_tree::ptree convert_to_ptree() const override;
  void convert_from_ptree(const boost::property_tree::ptree& ptree) override;
};

/**
 * @brief Container of constraint metadata objects.
 */
class Constraints : public Metadata {
 public:
  explicit Constraints(std::string_view database) : Constraints(database, kDefaultComponent) {}
  Constraints(std::string_view database, std::string_view component);

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

  std::shared_ptr<Object> create_object() const override;

 private:
  manager::metadata::ErrorCode param_check_metadata_add(
      const boost::property_tree::ptree& object) const;
};

}  // namespace manager::metadata

#endif  // MANAGER_METADATA_CONSTRAINTS_H_
