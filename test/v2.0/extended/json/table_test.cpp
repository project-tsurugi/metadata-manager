/*
 * Copyright 2020-2023 tsurugi project.
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
#include "manager/metadata/metadata_factory.h"

#define ERROR(error) print_error(error, __FILE__, __LINE__);

using boost::property_tree::ptree;
using boost::property_tree::ptree_error;

using manager::metadata::Column;
using manager::metadata::DataType;
using manager::metadata::ErrorCode;
using manager::metadata::ObjectIdType;
using manager::metadata::Table;
using manager::metadata::Tables;
using manager::metadata::db::ObjectIdGenerator;

const char* const TEST_DB = "test_DB";

std::vector<std::pair<ObjectIdType, std::string>> datatypes_list = {
    {23,         "INT32"},
    {20,         "INT64"},
    {700,      "FLOAT32"},
    {701,      "FLOAT64"},
    {1042,        "CHAR"},
    {1043,     "VARCHAR"},
    {1700,     "NUMERIC"},
    {1082,        "DATE"},
    {1083,        "TIME"},
    {1266,      "TIMETZ"},
    {1114,   "TIMESTAMP"},
    {1184, "TIMESTAMPTZ"},
    {1186,    "INTERVAL"}
};

/*
 * @brief print error code and line number.
 */
void print_error(ErrorCode error, const char* file, uint64_t line) {
  std::cout << std::endl
            << "error occurred at " << file << ":" << line
            << ", errorno: " << (uint64_t)error << std::endl;
}

/*
 * @brief generate new table name.
 */
const std::string get_table_name() {
  auto oid_generator = std::make_unique<ObjectIdGenerator>();

  ObjectIdType number = oid_generator->current("tables") + 1;
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

  return error;
}

/**
 *  @brief display table-metadata-object.
 */
ErrorCode display_table_metadata_object(const ptree& table) {
  ErrorCode error = ErrorCode::OK;

  auto datatypes = manager::metadata::get_datatypes_ptr(TEST_DB);
  ptree datatype;

  // table metadata
  std::cout << "--- table metadata ---" << std::endl;

  // id
  error = check_object<ObjectIdType>(Table::ID, true, table);
  if (error != ErrorCode::OK) {
    ERROR(error);
    return error;
  }

  // name
  error = check_object<std::string>(Table::NAME, true, table);
  if (error != ErrorCode::OK) {
    ERROR(error);
    return error;
  }

  // namespace
  error = check_object<std::string>(Table::NAMESPACE, false, table);
  if (error != ErrorCode::OK) {
    ERROR(error);
    return error;
  }

  // tuples
  error = check_object<ObjectIdType>(Table::NUMBER_OF_TUPLES, false, table);
  if (error != ErrorCode::OK) {
    ERROR(error);
    return error;
  }

  // column metadata
  std::cout << "--- columns metadata ---" << std::endl;
  BOOST_FOREACH (const ptree::value_type& node,
                 table.get_child(Table::COLUMNS_NODE)) {
    const ptree& column = node.second;

    // id
    error = check_object<ObjectIdType>(Column::ID, true, column);
    if (error != ErrorCode::OK) {
      ERROR(error);
      return error;
    }

    // tableId
    error = check_object<ObjectIdType>(Column::TABLE_ID, true, column);
    if (error != ErrorCode::OK) {
      ERROR(error);
      return error;
    }

    // name
    error = check_object<std::string>(Column::NAME, true, column);
    if (error != ErrorCode::OK) {
      ERROR(error);
      return error;
    }

    // columnNumber
    error = check_object<uint64_t>(Column::COLUMN_NUMBER, true, column);
    if (error != ErrorCode::OK) {
      ERROR(error);
      return error;
    }

    // dataTypeId
    error = check_object<ObjectIdType>(Column::DATA_TYPE_ID, true, column);
    if (error != ErrorCode::OK) {
      ERROR(error);
      return error;
    }

    // dataLength
    error = check_object<uint64_t>(Column::DATA_LENGTH, false, column);
    if (error != ErrorCode::OK) {
      ERROR(error);
      return error;
    }

    // varying
    error = check_object<bool>(Column::VARYING, false, column);
    if (error != ErrorCode::OK) {
      ERROR(error);
      return error;
    }

    // isNotNull
    error = check_object<bool>(Column::IS_NOT_NULL, true, column);
    if (error != ErrorCode::OK) {
      ERROR(error);
      return error;
    }

    // defaultExpr
    error = check_object<std::string>(Column::DEFAULT_EXPR, false, column);
    if (error != ErrorCode::OK) {
      ERROR(error);
      return error;
    }

    std::cout << "  ------------------" << std::endl;
  }

  return ErrorCode::OK;
}

/**
 *  @brief display table-metadata-object.
 */
ErrorCode display_table_metadata_object(const ptree& before,
                                        const ptree& after) {
  ErrorCode error = ErrorCode::OK;

  auto datatypes = manager::metadata::get_datatypes_ptr(TEST_DB);
  ptree datatype;

  // table metadata
  std::cout << " --- table metadata ---" << std::endl;

  // id
  output_object_diff<ObjectIdType>(Table::ID, before, after);
  if (after.find(Table::ID) == after.not_found()) {
    error = ErrorCode::NOT_FOUND;
    ERROR(error);
    return error;
  }

  // name
  output_object_diff<std::string>(Table::NAME, before, after);
  if (after.find(Table::NAME) == after.not_found()) {
    error = ErrorCode::NOT_FOUND;
    ERROR(error);
    return error;
  }

  // namespace
  output_object_diff<std::string>(Table::NAMESPACE, before, after);

  // number of tuples
  output_object_diff<int64_t>(Table::NUMBER_OF_TUPLES, before, after);

  std::cout << "--- columns metadata ---" << std::endl;

  // column metadata
  auto columns_node_before = before.get_child(Table::COLUMNS_NODE);
  auto columns_node_after  = after.get_child(Table::COLUMNS_NODE);
  auto columns_before      = columns_node_before.begin();
  auto columns_after       = columns_node_after.begin();

  // Inspection to see if the required fields are set.
  const std::vector<std::string> required_keys = {
      Column::ID,           Column::TABLE_ID,
      Column::NAME,         Column::COLUMN_NUMBER,
      Column::DATA_TYPE_ID, Column::IS_NOT_NULL};
  BOOST_FOREACH (const ptree::value_type& node, columns_node_after) {
    const ptree& column = node.second;

    for (std::string key : required_keys) {
      if (column.find(key) == column.not_found()) {
        std::cout << "Required fields are not set: \"" << key << "\""
                  << std::endl;
        error = ErrorCode::NOT_FOUND;
        ERROR(error);
        return error;
      }
    }
  }

  // before-metadata loop.
  for (auto it_before = columns_node_before.begin();
       it_before != columns_node_before.end(); it_before++) {
    boost::optional<manager::metadata::ObjectIdType> opt_id_before;
    auto before_id =
        it_before->second.get_optional<ObjectIdType>(Column::ID).value();

    ptree temp_after;
    temp_after.clear();
    // Extract updated metadata.
    for (auto it_after = columns_node_after.begin();
         it_after != columns_node_after.end(); it_after++) {
      auto opt_id_after =
          it_after->second.get_optional<ObjectIdType>(Column::ID);
      if (opt_id_after && (opt_id_after.value() == before_id)) {
        temp_after = it_after->second;
        it_after->second.erase(Column::ID);
        break;
      }
    }

    // id
    output_object_diff<ObjectIdType>(Column::ID, it_before->second, temp_after);
    // tableId
    output_object_diff<ObjectIdType>(Column::TABLE_ID, it_before->second,
                                     temp_after);
    // name
    output_object_diff<std::string>(Column::NAME, it_before->second,
                                    temp_after);
    // ordinalPosition
    output_object_diff<uint64_t>(Column::COLUMN_NUMBER, it_before->second,
                                 temp_after);
    // dataTypeId
    output_object_diff<ObjectIdType>(Column::DATA_TYPE_ID, it_before->second,
                                     temp_after);
    // dataLength
    output_object_diff<uint64_t>(Column::DATA_LENGTH, it_before->second,
                                 temp_after);
    // varying
    output_object_diff<bool>(Column::VARYING, it_before->second, temp_after);
    // isNotNull
    output_object_diff<bool>(Column::IS_NOT_NULL, it_before->second,
                             temp_after);
    // defaultExpr
    output_object_diff<std::string>(Column::DEFAULT_EXPR, it_before->second,
                                    temp_after);

    std::cout << " ------------------" << std::endl;
  }

  // Outputs on added metadata.
  const ptree dummy;
  BOOST_FOREACH (const ptree::value_type& node, columns_node_after) {
    const ptree& column = node.second;

    auto optional_object = column.get_optional<ObjectIdType>(Column::ID);
    if (optional_object) {
      // id
      output_object_diff<ObjectIdType>(Column::ID, dummy, column);
      // tableId
      output_object_diff<ObjectIdType>(Column::TABLE_ID, dummy, column);
      // name
      output_object_diff<std::string>(Column::NAME, dummy, column);
      // ordinalPosition
      output_object_diff<uint64_t>(Column::COLUMN_NUMBER, dummy, column);
      // dataTypeId
      output_object_diff<ObjectIdType>(Column::DATA_TYPE_ID, dummy, column);
      // dataLength
      output_object_diff<uint64_t>(Column::DATA_LENGTH, dummy, column);
      // varying
      output_object_diff<bool>(Column::VARYING, dummy, column);
      // isNotNull
      output_object_diff<bool>(Column::IS_NOT_NULL, dummy, column);
      // defaultExpr
      output_object_diff<std::string>(Column::DEFAULT_EXPR, dummy, column);

      std::cout << " ------------------" << std::endl;
    }
  }

  return ErrorCode::OK;
}

/**
 *  @brief Add table-metadata to the metadata-table.
 */
ErrorCode add_table_metadata() {
  ErrorCode error = ErrorCode::UNKNOWN;

  ptree datatype_metadata;
  ptree new_table_metadata;
  auto tables = manager::metadata::get_tables_ptr(TEST_DB);  // use Template-Method.
  // TODO(future): Change when changing Metadata class.
  auto datatypes_tmp = manager::metadata::get_datatypes_ptr(TEST_DB);
  auto datatypes = static_cast<manager::metadata::DataTypes*>(datatypes_tmp.get());

  //
  // table-metadata
  //
  new_table_metadata.put(Table::FORMAT_VERSION, Tables::format_version());
  new_table_metadata.put(Table::GENERATION, Tables::generation());
  new_table_metadata.put(Table::NAME, get_table_name());
  new_table_metadata.put(Table::NAMESPACE, "public");
  new_table_metadata.put(Table::NUMBER_OF_TUPLES, "123");

  ptree primary_key;
  ptree primary_keys;
  enum class ordinal_position {
    column_1 = 1,
    column_2,
    column_3,
  };
  std::vector<std::string> column_name = {"column_1", "column_2", "column_3"};

  //
  // column-metadata
  //
  ptree columns_metadata;
  {
    ptree column;
    // column #1
    column.clear();
    column.put(Column::NAME, column_name[0]);
    column.put(Column::COLUMN_NUMBER,
               static_cast<int>(ordinal_position::column_1));
    datatypes->get(DataType::PG_DATA_TYPE_QUALIFIED_NAME, "float4",
                   datatype_metadata);
    ObjectIdType data_type_id =
        datatype_metadata.get<ObjectIdType>(DataType::ID);
    if (!data_type_id) {
      return ErrorCode::NOT_FOUND;
    } else {
      if (DataType::DataTypeId::FLOAT32 !=
          static_cast<DataType::DataTypeId>(data_type_id)) {
        return ErrorCode::UNKNOWN;
      }
    }
    column.put<ObjectIdType>(Column::DATA_TYPE_ID, data_type_id);
    column.put<bool>(Column::IS_NOT_NULL, false);
    columns_metadata.push_back(std::make_pair("", column));

    // column #2
    column.clear();
    column.put(Column::NAME, column_name[1]);
    column.put(Column::COLUMN_NUMBER,
               static_cast<int>(ordinal_position::column_2));
    datatypes->get("VARCHAR", datatype_metadata);
    data_type_id = datatype_metadata.get<ObjectIdType>(DataType::ID);
    if (!data_type_id) {
      return ErrorCode::NOT_FOUND;
    } else {
      if (DataType::DataTypeId::VARCHAR !=
          static_cast<DataType::DataTypeId>(data_type_id)) {
        return ErrorCode::UNKNOWN;
      }
    }
    column.put(Column::DATA_TYPE_ID, data_type_id);
    column.put<uint64_t>(Column::DATA_LENGTH, 8);
    column.put<bool>(Column::VARYING, true);
    column.put<bool>(Column::IS_NOT_NULL, false);
    columns_metadata.push_back(std::make_pair("", column));

    // column #3
    column.clear();
    column.put(Column::NAME, column_name[2]);
    column.put(Column::COLUMN_NUMBER,
               static_cast<int>(ordinal_position::column_3));
    datatypes->get("CHAR", datatype_metadata);
    data_type_id = datatype_metadata.get<ObjectIdType>(DataType::ID);
    if (!data_type_id) {
      return ErrorCode::NOT_FOUND;
    } else {
      if (DataType::DataTypeId::CHAR !=
          static_cast<DataType::DataTypeId>(data_type_id)) {
        return ErrorCode::UNKNOWN;
      }
    }
    column.put(Column::DATA_TYPE_ID, data_type_id);
    column.put<uint64_t>(Column::DATA_LENGTH, 1);
    column.put<bool>(Column::VARYING, false);
    column.put<bool>(Column::IS_NOT_NULL, true);
    columns_metadata.push_back(std::make_pair("", column));
  }
  new_table_metadata.add_child(Table::COLUMNS_NODE, columns_metadata);

  //
  // add table-metadata object
  //
  error = tables->add(new_table_metadata);
  if (error != ErrorCode::OK) {
    ERROR(error);
  }

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
  auto tables        = manager::metadata::get_tables_ptr(TEST_DB);  // use Template-Method.
  auto oid_generator = std::make_unique<ObjectIdGenerator>();
  auto table_id      = oid_generator->current("tables");

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

  auto tables        = manager::metadata::get_tables_ptr(TEST_DB);  // use Template-Method.
  auto datatypes     = manager::metadata::get_datatypes_ptr(TEST_DB);
  auto oid_generator = std::make_unique<ObjectIdGenerator>();

  try {
    error = add_table_metadata();
  } catch (const ptree_error& e) {
    std::cerr << e.what() << std::endl;
    ERROR(error);
  }
  if (error != ErrorCode::OK) {
    ERROR(error);
  }

  auto table_id = oid_generator->current("tables");
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

    auto optional_name =
        table_metadata_before.get_optional<std::string>(Table::NAME);
    table_metadata.put(Table::NAME,
                       optional_name.value_or("unknown-name") + "-update");

    auto optional_namespace =
        table_metadata_before.get_optional<std::string>(Table::NAMESPACE);
    table_metadata.put(
        Table::NAMESPACE,
        optional_namespace.value_or("unknown-namespace") + "-update");

    auto optional_tuples =
        table_metadata_before.get_optional<int64_t>(Table::NUMBER_OF_TUPLES);
    table_metadata.put(Table::NUMBER_OF_TUPLES,
                       optional_tuples.value_or(-1) + 123);

    //
    // column-metadata
    //
    table_metadata.erase(Table::COLUMNS_NODE);
    ptree columns;
    {
      ptree column;
      ptree datatype;
      auto columns_node = table_metadata_before.get_child(Table::COLUMNS_NODE);

      auto it = columns_node.begin();
      // 1 item skip.
      // 2 item update.
      column = (++it)->second;
      column.put(Column::NAME,
                 it->second.get_optional<std::string>(Column::NAME)
                         .value_or("unknown-1") +
                     "-update");
      column.put(Column::COLUMN_NUMBER, 1);
      datatypes->get("INT64", datatype);
      column.put(Column::DATA_TYPE_ID,
                 datatype.get<ObjectIdType>(DataType::ID));
      column.erase(Column::DATA_LENGTH);
      column.put<bool>(Column::VARYING, false);
      column.put<bool>(Column::IS_NOT_NULL, true);
      column.put(Column::DEFAULT_EXPR, -1);
      columns.push_back(std::make_pair("", column));

      // 3 item update.
      column = (++it)->second;
      column.put(Column::NAME,
                 it->second.get_optional<std::string>(Column::NAME)
                         .value_or("unknown-2") +
                     "-update");
      column.put(Column::COLUMN_NUMBER, 2);
      datatypes->get("VARCHAR", datatype);
      column.put(Column::DATA_TYPE_ID,
                 datatype.get<ObjectIdType>(DataType::ID));
      column.put(Column::DATA_LENGTH, 123);
      column.put<bool>(Column::VARYING, true);
      column.put<bool>(Column::IS_NOT_NULL, false);
      column.put(Column::DEFAULT_EXPR, "default-string");
      columns.push_back(std::make_pair("", column));

      // 4 item add.
      column.clear();
      column.put(Column::NAME, "new-col");
      column.put(Column::COLUMN_NUMBER, 3);
      datatypes->get("INT32", datatype);
      column.put(Column::DATA_TYPE_ID,
                 datatype.get<ObjectIdType>(DataType::ID));
      column.put<bool>(Column::VARYING, false);
      column.put<bool>(Column::IS_NOT_NULL, false);
      column.put(Column::DEFAULT_EXPR, 9999);
      columns.push_back(std::make_pair("", column));
    }
    table_metadata.add_child(Table::COLUMNS_NODE, columns);

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
    error = display_table_metadata_object(table_metadata_before,
                                          table_metadata_after);
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

  auto tables = manager::metadata::get_tables_ptr(TEST_DB);  // use Template-Method.

  //
  // remove table-metadata object
  //
  auto oid_generator = std::make_unique<ObjectIdGenerator>();

  ObjectIdType number = oid_generator->current("tables");

  std::vector<std::string> table_names = {
      "table_" + std::to_string(number - 3),
      "table_" + std::to_string(number - 1),
      "table_" + std::to_string(number - 0),
      "table_" + std::to_string(number - 2)};

  for (std::string name : table_names) {
    ObjectIdType object_id = 0;
    error                  = tables->remove(name.c_str(), &object_id);
    if (error != ErrorCode::OK) {
      ERROR(error);
      return error;
    } else {
      std::cout << "remove table name :" << name << ", id:" << object_id
                << std::endl;
    }
  }

  const char* const table_name_not_exists = "table_name_not_exists";
  ObjectIdType ret_object_id              = 0;
  error = tables->remove(table_name_not_exists, &ret_object_id);
  if (error == ErrorCode::OK) {
    ERROR(error);
    return error;
  } else {
    std::cout << "can't remove table name not exists :" << table_name_not_exists
              << std::endl;
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
  number = oid_generator->current("tables");

  std::vector<ObjectIdType> object_ids = {number - 3, number - 1, number - 0,
                                          number - 2};

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
    std::cout << "can't remove table id not exists :" << table_id_not_exists
              << std::endl;
  }

  error = ErrorCode::OK;

  return error;
}

/**
 *  @brief Test to get datatypes-metadata in the metadata-table.
 */
ErrorCode datatypes_test() {
  ErrorCode error = ErrorCode::UNKNOWN;

  auto datatypes = manager::metadata::get_datatypes_ptr(TEST_DB);
  ptree datatype_id;
  ptree datatype_name;

  try {
    for (std::pair<ObjectIdType, std::string> datatype : datatypes_list) {
      error = datatypes->get(datatype.first, datatype_id);
      if (error != ErrorCode::OK) {
        std::cout << "DataTypes does not exist. [" << datatype.first << "]"
                  << std::endl;
        return error;
      }

      error = datatypes->get(datatype.second, datatype_name);
      if (error != ErrorCode::OK) {
        std::cout << "DataTypes does not exist. [" << datatype.second << "]"
                  << std::endl;
        return error;
      }

      std::string data_type_name =
          datatype_id.get<std::string>(DataType::NAME);
      if (data_type_name != datatype.second) {
        std::cout << "DataTypes Name error. [" << datatype.first
                  << "] expected:[" << datatype.second << "], actual:["
                  << data_type_name << "]" << std::endl;
        return ErrorCode::INTERNAL_ERROR;
      }

      ObjectIdType data_type_id =
          datatype_name.get<ObjectIdType>(DataType::ID);
      if (data_type_id != datatype.first) {
        std::cout << "DataTypes ID error. [" << datatype.second
                  << "] expected:[" << datatype.first << "], actual:["
                  << data_type_id << "]" << std::endl;
        return ErrorCode::INTERNAL_ERROR;
      }

      uint16_t format_version =
          datatype_name.get<uint16_t>(DataType::FORMAT_VERSION);
      uint32_t generation = datatype_name.get<uint32_t>(DataType::GENERATION);

      std::cout << "DataTypes -> FORMAT_VERSION:[" << format_version
                << "] / GENERATION:[" << generation << "] / ID:["
                << datatype.first << "] / NAME:[" << datatype.second << "]"
                << std::endl;
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
  std::cout << "=== Start test of add and get of Tables class. ==="
            << std::endl;
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
            << (tables_add_get_test_error == ErrorCode::OK ? "Success"
                                                           : "*** Failure ***")
            << std::endl;
  std::cout << "Tables update functions test     : "
            << (tables_update_test_error == ErrorCode::OK ? "Success"
                                                          : "*** Failure ***")
            << std::endl;
  std::cout << "Tables remove functions test     : "
            << (tables_remove_test_error == ErrorCode::OK ? "Success"
                                                          : "*** Failure ***")
            << std::endl;
  std::cout << "DataTypes get functions test     : "
            << (datatypes_test_error == ErrorCode::OK ? "Success"
                                                      : "*** Failure ***")
            << std::endl;
  std::cout << std::endl;

  std::cout << "*** TableMetadata test completed. ***" << std::endl;

  return 0;
}
