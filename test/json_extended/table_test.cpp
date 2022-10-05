/*
 * Copyright 2020-2022 tsurugi project.
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
#include <iostream>
#include <string>
#include <string_view>
#include <vector>

#include <boost/foreach.hpp>
#include <boost/optional.hpp>
#include <boost/property_tree/exceptions.hpp>
#include <boost/property_tree/json_parser.hpp>
#include <boost/property_tree/ptree.hpp>

#include "manager/metadata/dao/json/object_id_json.h"
#include "manager/metadata/datatypes.h"
#include "manager/metadata/tables.h"

#define ERROR(error) print_error(error, __FILE__, __LINE__);

using boost::property_tree::ptree;
using boost::property_tree::ptree_error;
using manager::metadata::Constraint;
using manager::metadata::DataTypes;
using manager::metadata::ErrorCode;
using manager::metadata::ObjectIdType;
using manager::metadata::Tables;
using manager::metadata::db::json::ObjectId;

const char* const TEST_DB = "test_DB";

std::vector<std::pair<ObjectIdType, std::string>> datatypes_list = {
    { 4,   "INT32"},
    { 6,   "INT64"},
    { 8, "FLOAT32"},
    { 9, "FLOAT64"},
    {13,    "CHAR"},
    {14, "VARCHAR"}
};

/*
 * @brief print error code and line number.
 */
void print_error(ErrorCode error, const char* file, uint64_t line) {
  std::cout << std::endl
            << "error occurred at " << file << ":" << line << ", errorno: " << (uint64_t)error
            << std::endl;
}

/*
 * @brief generate new table name.
 */
const std::string get_table_name() {
  auto oid_manager = std::make_unique<ObjectId>();

  ObjectIdType number = oid_manager->current("tables") + 1;
  std::string name    = "table_" + std::to_string(number);

  return name;
}

/**
 *  @brief Output for object.
 */
template <typename T>
ErrorCode check_object(std::string_view key, bool required,
                       const ptree& object) {
  ErrorCode error = ErrorCode::OK;

  boost::optional<T> optional_object = object.get_optional<T>(key.data());

  std::cout << " " << std::right << std::setw(10) << key.substr(0, 10) << ": ";
  if (required && !optional_object) {
    std::cout << "Required fields are undefined.";
    error = ErrorCode::NOT_FOUND;
  } else {
    if (optional_object) {
      std::cout << "[" << optional_object.value() << "]";
    } else {
      std::cout << "[--]";
    }
  }
  std::cout << std::endl;

  return error;
}

/**
 *  @brief Output for difference.
 */
template <typename T>
ErrorCode output_object_diff(std::string_view key, const ptree& before,
                             const ptree& after) {
  ErrorCode error = ErrorCode::OK;

  boost::optional<T> optional_before = before.get_optional<T>(key.data());
  boost::optional<T> optional_after = after.get_optional<T>(key.data());

  std::cout << " " << std::right << std::setw(10) << key.substr(0, 10) << ": ";
  if (optional_before) {
    std::cout << "[" << optional_before.value() << "]";
  } else {
    std::cout << "[--]";
  }
  std::cout << " --> ";
  if (optional_after) {
    std::cout << "[" << optional_after.value() << "]";
  } else {
    std::cout << "[--]";
  }
  std::cout << std::endl;

  return error;
}

/**
 *  @brief Output for object.
 */
template <typename T>
ErrorCode check_object(std::string_view key, bool required, const ptree& object) {
  ErrorCode error = ErrorCode::OK;

  boost::optional<T> optional_object = object.get_optional<T>(key.data());

  std::cout << " " << std::right << std::setw(10) << key.substr(0, 10) << ": ";
  if (required && !optional_object) {
    std::cout << "Required fields are undefined.";
    error = ErrorCode::NOT_FOUND;
  } else {
    if (optional_object) {
      std::cout << "[" << optional_object.value() << "]";
    } else {
      std::cout << "[--]";
    }
  }
  std::cout << std::endl;

  return error;
}

/**
 *  @brief Output for difference.
 */
template <typename T>
ErrorCode output_object_diff(std::string_view key, const ptree& before, const ptree& after) {
  ErrorCode error = ErrorCode::OK;

  std::ios::fmtflags original_flags = std::cout.flags();
  std::cout.fill(' ');

  boost::optional<T> optional_before = before.get_optional<T>(key.data());
  boost::optional<T> optional_after  = after.get_optional<T>(key.data());

  std::cout << " " << std::right << std::setw(10) << key.substr(0, 10) << ": ";
  if (optional_before) {
    std::cout << "[" << optional_before.value() << "]";
  } else {
    std::cout << "[--]";
  }
  std::cout << " --> ";
  if (optional_after) {
    std::cout << "[" << optional_after.value() << "]";
  } else {
    std::cout << "[--]";
  }
  std::cout << std::endl;

  std::cout.flags(original_flags);

  return error;
}

/**
 *  @brief display columns-metadata-object.
 */
ErrorCode display_columns_metadata_object(const ptree& table) {
  ErrorCode error = ErrorCode::OK;

  std::ios::fmtflags original_flags = std::cout.flags();
  std::cout.fill(' ');

  // column metadata
  std::cout << "--- columns metadata ---" << std::endl;
  BOOST_FOREACH (const ptree::value_type& node, table.get_child(Tables::COLUMNS_NODE)) {
    const ptree& column = node.second;

    // id
    error = check_object<ObjectIdType>(Tables::Column::ID, true, column);
    if (error != ErrorCode::OK) {
      ERROR(error);
      return error;
    }

    // tableId
    error = check_object<ObjectIdType>(Tables::Column::TABLE_ID, true, column);
    if (error != ErrorCode::OK) {
      ERROR(error);
      return error;
    }

    // ordinalPosition
    error = check_object<uint64_t>(Tables::Column::ORDINAL_POSITION, true, column);
    if (error != ErrorCode::OK) {
      ERROR(error);
      return error;
    }

    // dataTypeId
    error = check_object<ObjectIdType>(Tables::Column::DATA_TYPE_ID, true, column);
    if (error != ErrorCode::OK) {
      ERROR(error);
      return error;
    }

    // dataLength
    error = check_object<uint64_t>(Tables::Column::DATA_LENGTH, false, column);
    if (error != ErrorCode::OK) {
      ERROR(error);
      return error;
    }

    // varying
    error = check_object<bool>(Tables::Column::VARYING, false, column);
    if (error != ErrorCode::OK) {
      ERROR(error);
      return error;
    }

    // nullable
    error = check_object<bool>(Tables::Column::NULLABLE, true, column);
    if (error != ErrorCode::OK) {
      ERROR(error);
      return error;
    }

    // defaultExpr
    error = check_object<std::string>(Tables::Column::DEFAULT, false, column);
    if (error != ErrorCode::OK) {
      ERROR(error);
      return error;
    }

    // direction
    error = check_object<uint64_t>(Tables::Column::DIRECTION, false, column);
    if (error != ErrorCode::OK) {
      ERROR(error);
      return error;
    }

    std::cout << "  ------------------" << std::endl;
  }
  std::cout.flags(original_flags);

  return ErrorCode::OK;
}

/**
 *  @brief display constraints-metadata-object.
 */
ErrorCode display_constraint_metadata_object(const ptree& table) {
  ErrorCode error = ErrorCode::OK;

  std::ios::fmtflags original_flags = std::cout.flags();
  std::cout.fill(' ');

  // column metadata
  std::cout << "--- constraints metadata ---" << std::endl;
  BOOST_FOREACH (const ptree::value_type& node, table.get_child(Tables::CONSTRAINTS_NODE)) {
    const ptree& constraint = node.second;

    // id
    error = check_object<ObjectIdType>(Constraint::ID, true, constraint);
    if (error != ErrorCode::OK) {
      ERROR(error);
      return error;
    }

    // name
    error = check_object<std::string>(Constraint::NAME, false, constraint);
    if (error != ErrorCode::OK) {
      ERROR(error);
      return error;
    }

    // tableId
    error = check_object<ObjectIdType>(Constraint::TABLE_ID, true, constraint);
    if (error != ErrorCode::OK) {
      ERROR(error);
      return error;
    }

    // type
    error = check_object<int64_t>(Constraint::TYPE, true, constraint);
    if (error != ErrorCode::OK) {
      ERROR(error);
      return error;
    }

    // columns
    ptree columns = constraint.get_child(Constraint::COLUMNS);
    std::string columns_string;
    std::for_each(columns.begin(), columns.end(), [&columns_string](ptree::value_type v) mutable {
      columns_string = columns_string + (columns_string.empty() ? "" : ",") + v.second.data();
    });
    std::cout << " " << std::right << std::setw(10) << Constraint::COLUMNS << ": ["
              << columns_string << "]" << std::endl;

    // columnsId
    ptree columns_id = constraint.get_child(Constraint::COLUMNS_ID);
    std::string columns_id_string;
    std::for_each(
        columns_id.begin(), columns_id.end(), [&columns_id_string](ptree::value_type v) mutable {
          columns_id_string =
              columns_id_string + (columns_id_string.empty() ? "" : ",") + v.second.data();
        });
    std::cout << " " << std::right << std::setw(10) << Constraint::COLUMNS_ID << ": ["
              << columns_id_string << "]" << std::endl;

    // indexId
    error = check_object<int64_t>(Constraint::INDEX_ID, false, constraint);
    if (error != ErrorCode::OK) {
      ERROR(error);
      return error;
    }

    // expression
    error = check_object<std::string>(Constraint::EXPRESSION, false, constraint);
    if (error != ErrorCode::OK) {
      ERROR(error);
      return error;
    }

    std::cout << "  ------------------" << std::endl;
  }
  std::cout.flags(original_flags);

  return ErrorCode::OK;
}

/**
 *  @brief display table-metadata-object.
 */
template <typename T>
ErrorCode check_object(std::string_view key, bool required, const ptree& object) {
  ErrorCode error = ErrorCode::OK;

  std::ios::fmtflags original_flags = std::cout.flags();
  std::cout.fill(' ');

  auto datatypes = std::make_unique<DataTypes>(TEST_DB);
  ptree datatype;

  // table metadata
  std::cout << "--- table metadata ---" << std::endl;

  // id
  error = check_object<ObjectIdType>(Tables::ID, true, table);
  if (error != ErrorCode::OK) {
    ERROR(error);
    return error;
  }

  // name
  error = check_object<std::string>(Tables::NAME, true, table);
  if (error != ErrorCode::OK) {
    ERROR(error);
    return error;
  }

  // namespace
  error = check_object<std::string>(Tables::NAMESPACE, false, table);
  if (error != ErrorCode::OK) {
    ERROR(error);
    return error;
  }
  std::cout << " --> ";
  if (optional_after) {
    std::cout << "[" << optional_after.value() << "]";
  } else {
    std::cout << "[--]";
  }
  std::cout << std::endl;

  std::cout.flags(original_flags);

  return error;
}

/**
 *  @brief display columns-metadata-object.
 */
ErrorCode display_columns_metadata_object(const ptree& table) {
  ErrorCode error = ErrorCode::OK;

  std::ios::fmtflags original_flags = std::cout.flags();
  std::cout.fill(' ');

  // primaryKey
  ptree primary_keys = table.get_child(Tables::PRIMARY_KEY_NODE);
  std::string primary_keys_string;
  std::for_each(primary_keys.begin(), primary_keys.end(),
                [&primary_keys_string](ptree::value_type v) mutable {
                  primary_keys_string = primary_keys_string +
                                        (primary_keys_string.empty() ? "" : ",") + v.second.data();
                });
  std::cout << " " << std::right << std::setw(10) << Tables::PRIMARY_KEY_NODE << ": ["
            << primary_keys_string << "]" << std::endl;

  // tuples
  error = check_object<ObjectIdType>(Tables::TUPLES, false, table);
  if (error != ErrorCode::OK) {
    ERROR(error);
    return error;
  }

  // columns node.
  error = display_columns_metadata_object(table);
  if (error != ErrorCode::OK) {
    ERROR(error);
    return error;
  }

  // constraint node.
  error = display_constraint_metadata_object(table);
  if (error != ErrorCode::OK) {
    ERROR(error);
    return error;
  }

  std::cout.flags(original_flags);

  return ErrorCode::OK;
}

/**
 *  @brief display table-metadata-object.
 */
ErrorCode display_table_metadata_object(const ptree& before, const ptree& after) {
  ErrorCode error = ErrorCode::OK;

  std::ios::fmtflags original_flags = std::cout.flags();
  std::cout.fill(' ');

  auto datatypes = std::make_unique<DataTypes>(TEST_DB);
  ptree datatype;

  // table metadata
  std::cout << " --- table metadata ---" << std::endl;

  // id
  output_object_diff<ObjectIdType>(Tables::ID, before, after);
  if (after.find(Tables::ID) == after.not_found()) {
    error = ErrorCode::NOT_FOUND;
    ERROR(error);
    return error;
  }

  // name
  output_object_diff<std::string>(Tables::NAME, before, after);
  if (after.find(Tables::NAME) == after.not_found()) {
    error = ErrorCode::NOT_FOUND;
    ERROR(error);
    return error;
  }

  // namespace
  output_object_diff<std::string>(Tables::NAMESPACE, before, after);

  // primaryKey
  ptree pk_node_before = before.get_child(Tables::PRIMARY_KEY_NODE);
  ptree pk_node_after  = after.get_child(Tables::PRIMARY_KEY_NODE);
  std::string primary_keys_before;
  std::for_each(pk_node_before.begin(), pk_node_before.end(),
                [&primary_keys_before](ptree::value_type v) mutable {
                  primary_keys_before = primary_keys_before +
                                        (primary_keys_before.empty() ? "" : ",") + v.second.data();
                });
  std::string primary_keys_after;
  std::for_each(pk_node_after.begin(), pk_node_after.end(),
                [&primary_keys_after](ptree::value_type v) mutable {
                  primary_keys_after = primary_keys_after +
                                       (primary_keys_after.empty() ? "" : ",") + v.second.data();
                });
  std::cout << " " << std::right << std::setw(10) << Tables::PRIMARY_KEY_NODE << ": ["
            << primary_keys_before << "] --> [" << primary_keys_after << "]" << std::endl;

  // tuples
  output_object_diff<float>(Tables::TUPLES, before, after);

  // column metadata
  std::cout << "--- columns metadata ---" << std::endl;
  {
    auto columns_node_before = before.get_child(Tables::COLUMNS_NODE);
    auto columns_node_after  = after.get_child(Tables::COLUMNS_NODE);
    auto columns_before      = columns_node_before.begin();
    auto columns_after       = columns_node_after.begin();

    // Inspection to see if the required fields are set.
    const std::vector<std::string> required_keys = {
        Tables::Column::ID,           Tables::Column::TABLE_ID,
        Tables::Column::NAME,         Tables::Column::ORDINAL_POSITION,
        Tables::Column::DATA_TYPE_ID, Tables::Column::NULLABLE};
    BOOST_FOREACH (const ptree::value_type& node, columns_node_after) {
      const ptree& column = node.second;

      for (std::string key : required_keys) {
        if (column.find(key) == column.not_found()) {
          std::cout << "Required fields are not set: \"" << key << "\"" << std::endl;
          error = ErrorCode::NOT_FOUND;
          ERROR(error);
          return error;
        }
      }
    }

    // before-metadata loop.
    for (auto it_before = columns_node_before.begin(); it_before != columns_node_before.end();
         it_before++) {
      boost::optional<manager::metadata::ObjectIdType> opt_id_before;
      auto before_id = it_before->second.get_optional<ObjectIdType>(Tables::Column::ID).value();

      ptree temp_after;
      temp_after.clear();
      // Extract updated metadata.
      for (auto it_after = columns_node_after.begin(); it_after != columns_node_after.end();
           it_after++) {
        auto opt_id_after = it_after->second.get_optional<ObjectIdType>(Tables::Column::ID);
        if (opt_id_after && (opt_id_after.value() == before_id)) {
          temp_after = it_after->second;
          it_after->second.erase(Tables::Column::ID);
          break;
        }
      }

      // id
      output_object_diff<ObjectIdType>(Tables::Column::ID, it_before->second, temp_after);
      // tableId
      output_object_diff<ObjectIdType>(Tables::Column::TABLE_ID, it_before->second, temp_after);
      // name
      output_object_diff<std::string>(Tables::Column::NAME, it_before->second, temp_after);
      // ordinalPosition
      output_object_diff<uint64_t>(Tables::Column::ORDINAL_POSITION, it_before->second, temp_after);
      // dataTypeId
      output_object_diff<ObjectIdType>(Tables::Column::DATA_TYPE_ID, it_before->second, temp_after);
      // dataLength
      output_object_diff<uint64_t>(Tables::Column::DATA_LENGTH, it_before->second, temp_after);
      // varying
      output_object_diff<bool>(Tables::Column::VARYING, it_before->second, temp_after);
      // nullable
      output_object_diff<bool>(Tables::Column::NULLABLE, it_before->second, temp_after);
      // defaultExpr
      output_object_diff<std::string>(Tables::Column::DEFAULT, it_before->second, temp_after);
      // direction
      output_object_diff<uint64_t>(Tables::Column::DIRECTION, it_before->second, temp_after);

      std::cout << " ------------------" << std::endl;
    }

    // Outputs on added metadata.
    const ptree dummy;
    BOOST_FOREACH (const ptree::value_type& node, columns_node_after) {
      const ptree& column = node.second;

      auto optional_object = column.get_optional<ObjectIdType>(Tables::Column::ID);
      if (optional_object) {
        // id
        output_object_diff<ObjectIdType>(Tables::Column::ID, dummy, column);
        // tableId
        output_object_diff<ObjectIdType>(Tables::Column::TABLE_ID, dummy, column);
        // name
        output_object_diff<std::string>(Tables::Column::NAME, dummy, column);
        // ordinalPosition
        output_object_diff<uint64_t>(Tables::Column::ORDINAL_POSITION, dummy, column);
        // dataTypeId
        output_object_diff<ObjectIdType>(Tables::Column::DATA_TYPE_ID, dummy, column);
        // dataLength
        output_object_diff<uint64_t>(Tables::Column::DATA_LENGTH, dummy, column);
        // varying
        output_object_diff<bool>(Tables::Column::VARYING, dummy, column);
        // nullable
        output_object_diff<bool>(Tables::Column::NULLABLE, dummy, column);
        // defaultExpr
        output_object_diff<std::string>(Tables::Column::DEFAULT, dummy, column);
        // direction
        output_object_diff<uint64_t>(Tables::Column::DIRECTION, dummy, column);

        std::cout << " ------------------" << std::endl;
      }
    }
  }

  // constraint metadata
  std::cout << "--- constraints metadata ---" << std::endl;
  {
    auto constraints_node_before = before.get_child(Tables::CONSTRAINTS_NODE);
    auto constraints_node_after  = after.get_child(Tables::CONSTRAINTS_NODE);
    auto constraints_before      = constraints_node_before.begin();
    auto constraints_after       = constraints_node_after.begin();

    // Inspection to see if the required fields are set.
    const std::vector<std::string> required_keys = {Constraint::ID, Constraint::TABLE_ID,
                                                    Constraint::TYPE};
    BOOST_FOREACH (const ptree::value_type& node, constraints_node_after) {
      const ptree& constraint = node.second;

      for (std::string key : required_keys) {
        if (constraint.find(key) == constraint.not_found()) {
          std::cout << "Required fields are not set: \"" << key << "\"" << std::endl;
          error = ErrorCode::NOT_FOUND;
          ERROR(error);
          return error;
        }
      }
    }

    // before-metadata loop.
    for (auto it_before = constraints_node_before.begin();
         it_before != constraints_node_before.end(); it_before++) {
      boost::optional<manager::metadata::ObjectIdType> opt_id_before;
      auto before_id = it_before->second.get_optional<ObjectIdType>(Constraint::ID).value();

      ptree temp_after;
      temp_after.clear();
      // Extract updated metadata.
      for (auto it_after = constraints_node_after.begin(); it_after != constraints_node_after.end();
           it_after++) {
        auto opt_id_after = it_after->second.get_optional<ObjectIdType>(Constraint::ID);
        if (opt_id_after && (opt_id_after.value() == before_id)) {
          temp_after = it_after->second;
          it_after->second.erase(Constraint::ID);
          break;
        }
      }

      // id
      output_object_diff<ObjectIdType>(Constraint::ID, it_before->second, temp_after);
      // tableId
      output_object_diff<ObjectIdType>(Constraint::TABLE_ID, it_before->second, temp_after);
      // name
      output_object_diff<std::string>(Constraint::NAME, it_before->second, temp_after);
      // type
      output_object_diff<uint64_t>(Constraint::TYPE, it_before->second, temp_after);
      // indexId
      output_object_diff<int64_t>(Constraint::INDEX_ID, it_before->second, temp_after);
      // expression
      output_object_diff<std::string>(Constraint::EXPRESSION, it_before->second, temp_after);
      // columns
      auto columns_node_before = it_before->second.get_child_optional(Constraint::COLUMNS);
      std::string columns_before;
      if (columns_node_before) {
        std::for_each(columns_node_before.get().begin(), columns_node_before.get().end(),
                      [&columns_before](ptree::value_type v) mutable {
                        columns_before =
                            columns_before + (columns_before.empty() ? "" : ",") + v.second.data();
                      });
      } else {
        columns_before = "--";
      }
      auto columns_node_after = temp_after.get_child_optional(Constraint::COLUMNS);
      std::string columns_after;
      if (columns_node_after) {
        std::for_each(columns_node_after.get().begin(), columns_node_after.get().end(),
                      [&columns_after](ptree::value_type v) mutable {
                        columns_after =
                            columns_after + (columns_after.empty() ? "" : ",") + v.second.data();
                      });
      } else {
        columns_after = "--";
      }
      std::cout << " " << std::right << std::setw(10) << Constraint::COLUMNS << ": ["
                << columns_before << "] --> [" << columns_after << "]" << std::endl;

      // columnsId
      auto columns_id_node_before = it_before->second.get_child_optional(Constraint::COLUMNS_ID);
      std::string columns_id_before;
      if (columns_node_before) {
        std::for_each(columns_id_node_before.get().begin(), columns_id_node_before.get().end(),
                      [&columns_id_before](ptree::value_type v) mutable {
                        columns_id_before = columns_id_before +
                                            (columns_id_before.empty() ? "" : ",") +
                                            v.second.data();
                      });
      } else {
        columns_id_before = "--";
      }
      auto columns_id_node_after = temp_after.get_child_optional(Constraint::COLUMNS_ID);
      std::string columns_id_after;
      if (columns_id_node_after) {
        std::for_each(columns_id_node_after.get().begin(), columns_id_node_after.get().end(),
                      [&columns_id_after](ptree::value_type v) mutable {
                        columns_id_after = columns_id_after +
                                           (columns_id_after.empty() ? "" : ",") + v.second.data();
                      });
      } else {
        columns_id_after = "--";
      }
      std::cout << " " << std::right << std::setw(10) << Constraint::COLUMNS_ID << ": ["
                << columns_id_before << "] --> [" << columns_id_after << "]" << std::endl;

      std::cout << " ------------------" << std::endl;
    }

    // Outputs on added metadata.
    const ptree dummy;
    BOOST_FOREACH (const ptree::value_type& node, constraints_node_after) {
      const ptree& constraint = node.second;

      auto optional_object = constraint.get_optional<ObjectIdType>(Constraint::ID);
      if (optional_object) {
        // id
        output_object_diff<ObjectIdType>(Constraint::ID, dummy, constraint);
        // tableId
        output_object_diff<ObjectIdType>(Constraint::TABLE_ID, dummy, constraint);
        // name
        output_object_diff<std::string>(Constraint::NAME, dummy, constraint);
        // type
        output_object_diff<uint64_t>(Constraint::TYPE, dummy, constraint);
        // indexId
        output_object_diff<int64_t>(Constraint::INDEX_ID, dummy, constraint);
        // expression
        output_object_diff<std::string>(Constraint::EXPRESSION, dummy, constraint);
        // columns
        auto columns_node = constraint.get_child_optional(Constraint::COLUMNS);
        std::string columns;
        if (columns_node) {
          std::for_each(columns_node.get().begin(), columns_node.get().end(),
                        [&columns](ptree::value_type v) mutable {
                          columns = columns + (columns.empty() ? "" : ",") + v.second.data();
                        });
        } else {
          columns = "--";
        }
        std::cout << " " << std::right << std::setw(10) << Constraint::COLUMNS << ": [--] --> ["
                  << columns << "]" << std::endl;
        // columnsId
        auto columns_id_node = constraint.get_child_optional(Constraint::COLUMNS_ID);
        std::string columns_id;
        if (columns_node) {
          std::for_each(columns_id_node.get().begin(), columns_id_node.get().end(),
                        [&columns_id](ptree::value_type v) mutable {
                          columns_id =
                              columns_id + (columns_id.empty() ? "" : ",") + v.second.data();
                        });
        } else {
          columns_id = "--";
        }
        std::cout << " " << std::right << std::setw(10) << Constraint::COLUMNS_ID << ": [--] --> ["
                  << columns_id << "]" << std::endl;

        std::cout << " ------------------" << std::endl;
      }
    }
  }
  std::cout.flags(original_flags);

    // Outputs on added metadata.
    const ptree dummy;
    BOOST_FOREACH (const ptree::value_type& node, constraints_node_after) {
      const ptree& constraint = node.second;

      auto optional_object = constraint.get_optional<ObjectIdType>(Constraint::ID);
      if (optional_object) {
        // id
        output_object_diff<ObjectIdType>(Constraint::ID, dummy, constraint);
        // tableId
        output_object_diff<ObjectIdType>(Constraint::TABLE_ID, dummy, constraint);
        // name
        output_object_diff<std::string>(Constraint::NAME, dummy, constraint);
        // type
        output_object_diff<uint64_t>(Constraint::TYPE, dummy, constraint);
        // indexId
        output_object_diff<int64_t>(Constraint::INDEX_ID, dummy, constraint);
        // expression
        output_object_diff<std::string>(Constraint::EXPRESSION, dummy, constraint);
        // columns
        auto columns_node = constraint.get_child_optional(Constraint::COLUMNS);
        std::string columns;
        if (columns_node) {
          std::for_each(columns_node.get().begin(), columns_node.get().end(),
                        [&columns](ptree::value_type v) mutable {
                          columns = columns + (columns.empty() ? "" : ",") + v.second.data();
                        });
        } else {
          columns = "--";
        }
        std::cout << " " << std::right << std::setw(10) << Constraint::COLUMNS << ": [--] --> ["
                  << columns << "]" << std::endl;
        // columnsId
        auto columns_id_node = constraint.get_child_optional(Constraint::COLUMNS_ID);
        std::string columns_id;
        if (columns_node) {
          std::for_each(columns_id_node.get().begin(), columns_id_node.get().end(),
                        [&columns_id](ptree::value_type v) mutable {
                          columns_id =
                              columns_id + (columns_id.empty() ? "" : ",") + v.second.data();
                        });
        } else {
          columns_id = "--";
        }
        std::cout << " " << std::right << std::setw(10) << Constraint::COLUMNS_ID << ": [--] --> ["
                  << columns_id << "]" << std::endl;

        std::cout << " ------------------" << std::endl;
      }
    }
  }
  std::cout.flags(original_flags);

  return ErrorCode::OK;
}

/**
 *  @brief Add table-metadata to the metadata-table.
 */
ErrorCode add_table_metadata() {
  ErrorCode error = ErrorCode::UNKNOWN;

  ptree datatype_metadata;
  ptree new_table_metadata;
  auto tables    = std::make_unique<Tables>(TEST_DB);  // use Template-Method.
  auto datatypes = std::make_unique<DataTypes>(TEST_DB);

  //
  // table-metadata
  //
  new_table_metadata.put(Tables::FORMAT_VERSION, Tables::format_version());
  new_table_metadata.put(Tables::GENERATION, Tables::generation());
  new_table_metadata.put(Tables::NAME, get_table_name());
  new_table_metadata.put(Tables::NAMESPACE, "public");
  new_table_metadata.put(Tables::TUPLES, "1.23");

  ptree primary_key;
  ptree primary_keys;
  enum class ordinal_position {
    column_1 = 1,
    column_2,
    column_3,
  };
  std::vector<std::string> column_name = {"column_1", "column_2", "column_3"};
  primary_key.put("", static_cast<int>(ordinal_position::column_1));
  primary_keys.push_back(std::make_pair("", primary_key));
  primary_key.put("", static_cast<int>(ordinal_position::column_2));
  primary_keys.push_back(std::make_pair("", primary_key));
  new_table_metadata.add_child(Tables::PRIMARY_KEY_NODE, primary_keys);

  //
  // column-metadata
  //
  ptree columns_metadata;
  {
    ptree column;
    // column #1
    column.clear();
    column.put(Tables::Column::NAME, column_name[0]);
    column.put(Tables::Column::ORDINAL_POSITION, static_cast<int>(ordinal_position::column_1));
    datatypes->get(DataTypes::PG_DATA_TYPE_QUALIFIED_NAME, "float4", datatype_metadata);
    ObjectIdType data_type_id = datatype_metadata.get<ObjectIdType>(DataTypes::ID);
    if (!data_type_id) {
      return ErrorCode::NOT_FOUND;
    } else {
      if (DataTypes::DataTypesId::FLOAT32 != static_cast<DataTypes::DataTypesId>(data_type_id)) {
        return ErrorCode::UNKNOWN;
      }
    }
    column.put<ObjectIdType>(Tables::Column::DATA_TYPE_ID, data_type_id);
    column.put<bool>(Tables::Column::NULLABLE, false);
    column.put(Tables::Column::DIRECTION, static_cast<int>(Tables::Column::Direction::ASCENDANT));
    columns_metadata.push_back(std::make_pair("", column));

    // column #2
    column.clear();
    column.put(Tables::Column::NAME, column_name[1]);
    column.put(Tables::Column::ORDINAL_POSITION, static_cast<int>(ordinal_position::column_2));
    datatypes->get("VARCHAR", datatype_metadata);
    data_type_id = datatype_metadata.get<ObjectIdType>(DataTypes::ID);
    if (!data_type_id) {
      return ErrorCode::NOT_FOUND;
    } else {
      if (DataTypes::DataTypesId::VARCHAR != static_cast<DataTypes::DataTypesId>(data_type_id)) {
        return ErrorCode::UNKNOWN;
      }
    }
    column.put(Tables::Column::DATA_TYPE_ID, data_type_id);
    column.put<uint64_t>(Tables::Column::DATA_LENGTH, 8);
    column.put<bool>(Tables::Column::VARYING, true);
    column.put<bool>(Tables::Column::NULLABLE, false);
    column.put(Tables::Column::DIRECTION, static_cast<int>(Tables::Column::Direction::DEFAULT));
    columns_metadata.push_back(std::make_pair("", column));

    // column #3
    column.clear();
    column.put(Tables::Column::NAME, column_name[2]);
    column.put(Tables::Column::ORDINAL_POSITION, static_cast<int>(ordinal_position::column_3));
    datatypes->get("CHAR", datatype_metadata);
    data_type_id = datatype_metadata.get<ObjectIdType>(DataTypes::ID);
    if (!data_type_id) {
      return ErrorCode::NOT_FOUND;
    } else {
      if (DataTypes::DataTypesId::CHAR != static_cast<DataTypes::DataTypesId>(data_type_id)) {
        return ErrorCode::UNKNOWN;
      }
    }
    column.put(Tables::Column::DATA_TYPE_ID, data_type_id);
    column.put<uint64_t>(Tables::Column::DATA_LENGTH, 1);
    column.put<bool>(Tables::Column::VARYING, false);
    column.put<bool>(Tables::Column::NULLABLE, true);
    column.put(Tables::Column::DIRECTION, static_cast<int>(Tables::Column::Direction::DEFAULT));
    columns_metadata.push_back(std::make_pair("", column));
  }
  new_table_metadata.add_child(Tables::COLUMNS_NODE, columns_metadata);

  //
  // constraints-metadata
  //
  ptree constraints;
  {
    // Set the value of the constraints to ptree.
    ptree constraint;
    ptree columns_num;
    ptree columns_num_value;
    ptree columns_id;
    ptree columns_id_value;

    constraint.put(Constraint::TYPE, static_cast<int32_t>(Constraint::ConstraintType::UNIQUE));
    constraint.put(Constraint::NAME, "unique constraint");
    // constraint.add_child(Constraint::COLUMNS, columns_num);
    // constraint.add_child(Constraint::COLUMNS_ID, columns_id);
    // constraints
    constraints.push_back(std::make_pair("", constraint));

    constraint.clear();
    columns_num.clear();
    columns_num_value.clear();
    columns_id.clear();
    columns_id_value.clear();
    // type
    constraint.put(Constraint::TYPE, static_cast<int32_t>(Constraint::ConstraintType::CHECK));
    // columns
    columns_num_value.put("", 1);
    columns_num.push_back(std::make_pair("", columns_num_value));
    columns_num_value.put("", 2);
    columns_num.push_back(std::make_pair("", columns_num_value));
    constraint.add_child(Constraint::COLUMNS, columns_num);
    // columns id
    columns_id_value.put("", 1234);
    columns_id.push_back(std::make_pair("", columns_id_value));
    columns_id_value.put("", 5678);
    columns_id.push_back(std::make_pair("", columns_id_value));
    constraint.add_child(Constraint::COLUMNS_ID, columns_id);
    // expression
    constraint.put(Constraint::EXPRESSION, "expression text");
    // constraints
    constraints.push_back(std::make_pair("", constraint));
  }
  new_table_metadata.add_child(Tables::CONSTRAINTS_NODE, constraints);

  //
  // add table-metadata object
  //
  error = tables->add(new_table_metadata);
  if (error != ErrorCode::OK) {
    ERROR(error);
  }
  }

  if (error == ErrorCode::OK) {
    error = display_table_metadata_object(table_metadata_before,
                                          table_metadata_after);
  }

  // Remove metadata used in testing.
  tables->remove(table_id);

  return error;
}

/**
 *  @brief Test to add table-metadata to the metadata-table and retrieve it.
 */
ErrorCode test_tables_add_get() {
  ErrorCode error = ErrorCode::UNKNOWN;

  try {
    error = add_table_metadata();
  } catch (const ptree_error& e) {
    std::cerr << e.what() << std::endl;
    ERROR(error);
    return error;
  }
  if (error != ErrorCode::OK) {
    ERROR(error);
    return error;
  }

  ptree table_metadata;
  auto tables      = std::make_unique<Tables>(TEST_DB);  // use Template-Method.
  auto oid_manager = std::make_unique<ObjectId>();
  auto table_id    = oid_manager->current("tables");

  std::string table_name = "table_" + std::to_string(table_id);
  if (error == ErrorCode::OK) {
    std::cout << "--- get table metadata by table name. ---" << std::endl;
    error = tables->get(table_name, table_metadata);
    if (error != ErrorCode::OK) {
      ERROR(error);
    }
  }
  if (error == ErrorCode::OK) {
    error = display_table_metadata_object(table_metadata);
    if (error != ErrorCode::OK) {
      ERROR(error);
    }
  }

  if (error == ErrorCode::OK) {
    std::cout << "--- get table metadata by table id. ---" << std::endl;
    table_metadata.clear();
    error = tables->get(table_id, table_metadata);
    if (error != ErrorCode::OK) {
      ERROR(error);
    }
  }
  if (error == ErrorCode::OK) {
    error = display_table_metadata_object(table_metadata);
    if (error != ErrorCode::OK) {
      ERROR(error);
    }
  }

  // Remove metadata used in testing.
  tables->remove(table_id);

  return error;
}

/**
 *  @brief Test to update table-metadata in the metadata-table.
 */
ErrorCode test_tables_update() {
  ErrorCode error = ErrorCode::UNKNOWN;

  auto tables      = std::make_unique<Tables>(TEST_DB);  // use Template-Method.
  auto datatypes   = std::make_unique<DataTypes>(TEST_DB);
  auto oid_manager = std::make_unique<ObjectId>();

  try {
    error = add_table_metadata();
  } catch (const ptree_error& e) {
    std::cerr << e.what() << std::endl;
    ERROR(error);
  }
  if (error != ErrorCode::OK) {
    ERROR(error);
  }

  auto table_id = oid_manager->current("tables");
  ptree table_metadata_before;
  if (error == ErrorCode::OK) {
    error = tables->get(table_id, table_metadata_before);
    if (error != ErrorCode::OK) {
      ERROR(error);
    }
  }

  ptree table_metadata;
  if (error == ErrorCode::OK) {
    table_metadata = table_metadata_before;

    auto optional_name = table_metadata_before.get_optional<std::string>(Tables::NAME);
    table_metadata.put(Tables::NAME, optional_name.value_or("unknown-name") + "-update");

    auto optional_namespace = table_metadata_before.get_optional<std::string>(Tables::NAMESPACE);
    table_metadata.put(Tables::NAMESPACE,
                       optional_namespace.value_or("unknown-namespace") + "-update");

    table_metadata.erase(Tables::PRIMARY_KEY_NODE);
    ptree primary_key;
    ptree primary_keys;
    primary_key.put("", 3);
    primary_keys.push_back(std::make_pair("", primary_key));
    table_metadata.add_child(Tables::PRIMARY_KEY_NODE, primary_keys);

    auto optional_tuples = table_metadata_before.get_optional<float>(Tables::TUPLES);
    table_metadata.put(Tables::TUPLES, optional_tuples.value_or(-1.0f) + 1.23f);

    //
    // column-metadata
    //
    table_metadata.erase(Tables::COLUMNS_NODE);
    ptree columns;
    {
      ptree column;
      ptree datatype;
      auto columns_node = table_metadata_before.get_child(Tables::COLUMNS_NODE);

      auto it = columns_node.begin();
      // 1 item skip.
      // 2 item update.
      column = (++it)->second;
      column.put(Tables::Column::NAME,
                 it->second.get_optional<std::string>(Tables::Column::NAME).value_or("unknown-1") +
                     "-update");
      column.put(Tables::Column::ORDINAL_POSITION, 1);
      datatypes->get("INT64", datatype);
      column.put(Tables::Column::DATA_TYPE_ID, datatype.get<ObjectIdType>(DataTypes::ID));
      column.erase(Tables::Column::DATA_LENGTH);
      column.put<bool>(Tables::Column::VARYING, false);
      column.put<bool>(Tables::Column::NULLABLE, true);
      column.put(Tables::Column::DEFAULT, -1);
      column.put(Tables::Column::DIRECTION, static_cast<int>(Tables::Column::Direction::ASCENDANT));
      columns.push_back(std::make_pair("", column));

      // 3 item update.
      column = (++it)->second;
      column.put(Tables::Column::NAME,
                 it->second.get_optional<std::string>(Tables::Column::NAME).value_or("unknown-2") +
                     "-update");
      column.put(Tables::Column::ORDINAL_POSITION, 2);
      datatypes->get("VARCHAR", datatype);
      column.put(Tables::Column::DATA_TYPE_ID, datatype.get<ObjectIdType>(DataTypes::ID));
      column.put(Tables::Column::DATA_LENGTH, 123);
      column.put<bool>(Tables::Column::VARYING, true);
      column.put<bool>(Tables::Column::NULLABLE, false);
      column.put(Tables::Column::DEFAULT, "default-string");
      column.put(Tables::Column::DIRECTION,
                 static_cast<int>(Tables::Column::Direction::DESCENDANT));
      columns.push_back(std::make_pair("", column));

      // 4 item add.
      column.clear();
      column.put(Tables::Column::NAME, "new-col");
      column.put(Tables::Column::ORDINAL_POSITION, 3);
      datatypes->get("INT32", datatype);
      column.put(Tables::Column::DATA_TYPE_ID, datatype.get<ObjectIdType>(DataTypes::ID));
      column.put<bool>(Tables::Column::VARYING, false);
      column.put<bool>(Tables::Column::NULLABLE, false);
      column.put(Tables::Column::DEFAULT, 9999);
      column.put(Tables::Column::DIRECTION, static_cast<int>(Tables::Column::Direction::DEFAULT));
      columns.push_back(std::make_pair("", column));
    }
    table_metadata.add_child(Tables::COLUMNS_NODE, columns);

    //
    // constraints-metadata
    //
    table_metadata.erase(Tables::CONSTRAINTS_NODE);
      ptree constraints;
    {
      // Set the value of the constraints to ptree.
      ptree constraint;
      ptree columns_num;
      ptree columns_num_value;
      ptree columns_id;
      ptree columns_id_value;

      auto constraints_node = table_metadata_before.get_child(Tables::CONSTRAINTS_NODE);

      auto it = constraints_node.begin();
      // 1 item skip.
      // 2 item update.
      constraint = (++it)->second;
      // type
      constraint.put(Constraint::TYPE, static_cast<int32_t>(Constraint::ConstraintType::CHECK));
      // columns
      constraint.erase(Constraint::COLUMNS);
      columns_num_value.put("", 5);
      columns_num.push_back(std::make_pair("", columns_num_value));
      constraint.add_child(Constraint::COLUMNS, columns_num);

      // columns id
      constraint.erase(Constraint::COLUMNS_ID);
      columns_id_value.put("", 9999);
      columns_id.push_back(std::make_pair("", columns_id_value));
      constraint.add_child(Constraint::COLUMNS_ID, columns_id);
      // expression
      constraint.put(Constraint::EXPRESSION, "expression text-update");
      // constraints
      constraints.push_back(std::make_pair("", constraint));

      // 3 item new.
      constraint.clear();
      columns_num.clear();
      columns_id.clear();
      // type
      constraint.put(Constraint::TYPE, static_cast<int32_t>(Constraint::ConstraintType::UNIQUE));
      // name
      constraint.put(Constraint::NAME, "new-constraint");
      // columns
      columns_num_value.put("", 10);
      columns_num.push_back(std::make_pair("", columns_num_value));
      constraint.add_child(Constraint::COLUMNS, columns_num);
      // columns id
      columns_id_value.put("", 1001);
      columns_id.push_back(std::make_pair("", columns_id_value));
      constraint.add_child(Constraint::COLUMNS_ID, columns_id);
      // index id
      constraint.put(Constraint::INDEX_ID, 11);
      // expression
      constraint.put(Constraint::EXPRESSION, "none");
      // constraints
      constraints.push_back(std::make_pair("", constraint));
    }
    table_metadata.add_child(Tables::CONSTRAINTS_NODE, constraints);

    //
    // update table-metadata object
    //
    error = tables->update(table_id, table_metadata);
    if (error != ErrorCode::OK) {
      ERROR(error);
    }
  }

  ptree table_metadata_after;
  if (error == ErrorCode::OK) {
    error = tables->get(table_id, table_metadata_after);
    if (error != ErrorCode::OK) {
      ERROR(error);
    }
  }

  if (error == ErrorCode::OK) {
    error = display_table_metadata_object(table_metadata_before, table_metadata_after);
  }

  // Remove metadata used in testing.
  tables->remove(table_id);

  return error;
}

/**
 *  @brief Test to remove a table-metadata to metadata-table.
 */
ErrorCode tables_remove_test() {
  ErrorCode error     = ErrorCode::UNKNOWN;
  int TABLE_NUM_ADDED = 4;

  for (int num = 0; num < TABLE_NUM_ADDED; num++) {
    error = add_table_metadata();
    if (error != ErrorCode::OK) {
      ERROR(error);
      return error;
    }
  }

  auto tables = std::make_unique<Tables>(TEST_DB);  // use Template-Method.

  //
  // remove table-metadata object
  //
  auto oid_manager = std::make_unique<ObjectId>();

  ObjectIdType number                  = oid_manager->current("tables");
  std::vector<std::string> table_names = {
      "table_" + std::to_string(number - 3), "table_" + std::to_string(number - 1),
      "table_" + std::to_string(number - 0), "table_" + std::to_string(number - 2)};

  for (std::string name : table_names) {
    ObjectIdType object_id = 0;
    error                  = tables->remove(name.c_str(), &object_id);
    if (error != ErrorCode::OK) {
      ERROR(error);
      return error;
    } else {
      std::cout << "remove table name :" << name << ", id:" << object_id << std::endl;
    }
  }

  const char* const table_name_not_exists = "table_name_not_exists";
  ObjectIdType ret_object_id              = 0;
  error                                   = tables->remove(table_name_not_exists, &ret_object_id);
  if (error == ErrorCode::OK) {
    ERROR(error);
    return error;
  } else {
    std::cout << "can't remove table name not exists :" << table_name_not_exists << std::endl;
  }

  for (int num = 0; num < TABLE_NUM_ADDED; num++) {
    error = add_table_metadata();
    if (error != ErrorCode::OK) {
      ERROR(error);
      return error;
    }
  }

  //
  // remove table-metadata object
  //

  number                               = oid_manager->current("tables");
  std::vector<ObjectIdType> object_ids = {number - 3, number - 1, number - 0, number - 2};

  for (uint64_t object_id : object_ids) {
    error = tables->remove(object_id);
    if (error != ErrorCode::OK) {
      ERROR(error);
      return error;
    } else {
      std::cout << "remove table id:" << object_id << std::endl;
    }
  }

  uint64_t table_id_not_exists = 0;
  error                        = tables->remove(table_id_not_exists);
  if (error == ErrorCode::OK) {
    ERROR(error);
    return error;
  } else {
    std::cout << "can't remove table id not exists :" << table_id_not_exists << std::endl;
  }

  error = ErrorCode::OK;

  return error;
}

/**
 *  @brief Test to get datatypes-metadata in the metadata-table.
 */
ErrorCode datatypes_test() {
  ErrorCode error = ErrorCode::UNKNOWN;

  auto datatypes = std::make_unique<DataTypes>(TEST_DB);
  ptree datatype_id;
  ptree datatype_name;

  try {
    for (std::pair<ObjectIdType, std::string> datatype : datatypes_list) {
      error = datatypes->get(datatype.first, datatype_id);
      if (error != ErrorCode::OK) {
        std::cout << "DataTypes does not exist. [" << datatype.first << "]" << std::endl;
        return error;
      }

      error = datatypes->get(datatype.second, datatype_name);
      if (error != ErrorCode::OK) {
        std::cout << "DataTypes does not exist. [" << datatype.second << "]" << std::endl;
        return error;
      }

      std::string data_type_name = datatype_id.get<std::string>(DataTypes::NAME);
      if (data_type_name != datatype.second) {
        std::cout << "DataTypes Name error. [" << datatype.first << "] expected:["
                  << datatype.second << "], actual:[" << data_type_name << "]" << std::endl;
        return ErrorCode::INTERNAL_ERROR;
      }

      ObjectIdType data_type_id = datatype_name.get<ObjectIdType>(DataTypes::ID);
      if (data_type_id != datatype.first) {
        std::cout << "DataTypes ID error. [" << datatype.second << "] expected:[" << datatype.first
                  << "], actual:[" << data_type_id << "]" << std::endl;
        return ErrorCode::INTERNAL_ERROR;
      }

      uint16_t format_version = datatype_name.get<uint16_t>(DataTypes::FORMAT_VERSION);
      uint32_t generation     = datatype_name.get<uint32_t>(DataTypes::GENERATION);

      std::cout << "DataTypes -> FORMAT_VERSION:[" << format_version << "] / GENERATION:["
                << generation << "] / ID:[" << datatype.first << "] / NAME:[" << datatype.second
                << "]" << std::endl;
    }
  } catch (const ptree_error& e) {
    std::cerr << e.what() << std::endl;
    ERROR(error);
    return error;
  }

  if (error != ErrorCode::OK) {
    ERROR(error);
  }
  return error;
}

/**
 *  @brief main function.
 */
int main(void) {
  std::cout << "*** TableMetadata test start. ***" << std::endl << std::endl;
  std::cout << "=== Start test of add and get of Tables class. ===" << std::endl;
  ErrorCode tables_add_get_test_error = test_tables_add_get();
  std::cout << "=== Done test of add and get of Tables class. ===" << std::endl;
  std::cout << std::endl;

  std::cout << "=== Start test of update of Tables class. ===" << std::endl;
  ErrorCode tables_update_test_error = test_tables_update();
  std::cout << "=== Done test of update of Tables class. ===" << std::endl;
  std::cout << std::endl;

  std::cout << "=== Start test of remove of Tables class. ===" << std::endl;
  std::cout << "=== Done test of remove of Tables class. ===" << std::endl;
  ErrorCode tables_remove_test_error = tables_remove_test();
  std::cout << "=== remove table functions test done. ===" << std::endl;
  std::cout << std::endl;

  std::cout << "=== Start test of get of DataTypes class. ===" << std::endl;
  ErrorCode datatypes_test_error = datatypes_test();
  std::cout << "=== Done test of get of DataTypes class. ===" << std::endl;

  std::cout << std::endl;
  std::cout << "Tables add and get functions test: "
            << (tables_add_get_test_error == ErrorCode::OK ? "Success" : "*** Failure ***")
            << std::endl;
  std::cout << "Tables update functions test     : "
            << (tables_update_test_error == ErrorCode::OK ? "Success" : "*** Failure ***")
            << std::endl;
  std::cout << "Tables remove functions test     : "
            << (tables_remove_test_error == ErrorCode::OK ? "Success" : "*** Failure ***")
            << std::endl;
  std::cout << "DataTypes get functions test     : "
            << (datatypes_test_error == ErrorCode::OK ? "Success" : "*** Failure ***") << std::endl;
  std::cout << std::endl;

  std::cout << "*** TableMetadata test completed. ***" << std::endl;

  return 0;
}
