/*
 * Copyright 2020-2021 tsurugi project.
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

#include "manager/metadata/dao/json/object_id.h"
#include "manager/metadata/datatypes.h"
#include "manager/metadata/error_code.h"
#include "manager/metadata/metadata.h"
#include "manager/metadata/tables.h"

using boost::property_tree::ptree;
using boost::property_tree::ptree_error;
using manager::metadata::DataTypes;
using manager::metadata::ErrorCode;
using manager::metadata::ObjectIdType;
using manager::metadata::Tables;
using manager::metadata::db::json::ObjectId;

const char* const TEST_DB = "test_DB";

std::vector<std::pair<ObjectIdType, std::string>> datatypes_list = {
    {4, "INT32"},   {6, "INT64"}, {8, "FLOAT32"},
    {9, "FLOAT64"}, {13, "CHAR"}, {14, "VARCHAR"}};

/*
 * @brief print error code and line number.
 */
void print_error(ErrorCode error, uint64_t line) {
  std::cout << std::endl
            << "error occurred at line " << line
            << ", errorno: " << (uint64_t)error << std::endl;
}

/*
 * @brief generate new table name.
 */
const std::string get_table_name() {
  std::unique_ptr<ObjectId> oid_manager = std::make_unique<ObjectId>();

  ObjectIdType number = oid_manager->current("tables") + 1;
  std::string name = "table_" + std::to_string(number);

  return name;
}

/**
 *  @brief display table-metadata-object.
 */
ErrorCode display_table_metadata_object(const ptree& table) {
  ErrorCode error = ErrorCode::OK;

  std::unique_ptr<DataTypes> datatypes(new DataTypes(TEST_DB));
  ptree datatype;

  // table metadata
  std::cout << "--- table ---" << std::endl;
  boost::optional<ObjectIdType> optional_id =
      table.get_optional<ObjectIdType>(Tables::ID);
  if (!optional_id) {
    error = ErrorCode::NOT_FOUND;
    print_error(error, __LINE__);
    return error;
  }
  std::cout << "id : " << optional_id.get() << std::endl;

  boost::optional<std::string> optional_name =
      table.get_optional<std::string>(Tables::NAME);
  if (!optional_name) {
    error = ErrorCode::NOT_FOUND;
    print_error(error, __LINE__);
    return error;
  }
  std::cout << "name : " << optional_name << std::endl;

  ptree primary_keys = table.get_child(Tables::PRIMARY_KEY_NODE);
  BOOST_FOREACH (const ptree::value_type& node, primary_keys) {
    std::cout << "primary_key : " << node.second.data() << std::endl;
  }

  // column metadata
  std::cout << "--- columns ---" << std::endl;
  BOOST_FOREACH (const ptree::value_type& node,
                 table.get_child(Tables::COLUMNS_NODE)) {
    const ptree& column = node.second;

    boost::optional<ObjectIdType> id =
        column.get_optional<ObjectIdType>(Tables::Column::ID);
    if (!id) {
      error = ErrorCode::NOT_FOUND;
      print_error(error, __LINE__);
      return error;
    }
    std::cout << "id : " << id << std::endl;

    boost::optional<ObjectIdType> table_id =
        column.get_optional<ObjectIdType>(Tables::Column::TABLE_ID);
    if (!table_id) {
      error = ErrorCode::NOT_FOUND;
      print_error(error, __LINE__);
      return error;
    }
    std::cout << "table id : " << table_id << std::endl;

    boost::optional<std::string> name =
        column.get_optional<std::string>(Tables::Column::NAME);
    if (!name) {
      error = ErrorCode::NOT_FOUND;
      print_error(error, __LINE__);
      return error;
    }
    std::cout << "name : " << name << std::endl;

    boost::optional<uint64_t> ordinal_position =
        column.get_optional<uint64_t>(Tables::Column::ORDINAL_POSITION);
    if (!ordinal_position) {
      error = ErrorCode::NOT_FOUND;
      print_error(error, __LINE__);
      return error;
    }
    std::cout << "ordinal position : " << ordinal_position << std::endl;

    boost::optional<ObjectIdType> data_type_id =
        column.get_optional<ObjectIdType>(Tables::Column::DATA_TYPE_ID);
    if (!data_type_id) {
      error = ErrorCode::NOT_FOUND;
      print_error(error, __LINE__);
      return error;
    }
    std::cout << "datatype id : " << data_type_id << std::endl;
    datatypes->get(data_type_id.get(), datatype);
    std::cout << "datatype name : "
              << datatype.get<std::string>(DataTypes::NAME) << std::endl;

    boost::optional<uint64_t> data_length =
        column.get_optional<uint64_t>(Tables::Column::DATA_LENGTH);
    if (data_length) {
      std::cout << "data length : " << data_length << std::endl;
    }

    boost::optional<bool> varying =
        column.get_optional<bool>(Tables::Column::VARYING);
    if (varying) {
      std::cout << "varying : " << varying << std::endl;
    }

    boost::optional<bool> nullable =
        column.get_optional<bool>(Tables::Column::NULLABLE);
    if (!nullable) {
      error = ErrorCode::NOT_FOUND;
      print_error(error, __LINE__);
      return error;
    }
    std::cout << "nullable : " << nullable << std::endl;

    boost::optional<std::string> default_expr =
        column.get_optional<std::string>(Tables::Column::DEFAULT);
    if (default_expr) {
      std::cout << "default : " << default_expr << std::endl;
    }

    boost::optional<uint64_t> direction =
        column.get_optional<uint64_t>(Tables::Column::DIRECTION);
    if (direction) {
      std::cout << "direction : " << direction << std::endl;
      switch (static_cast<Tables::Column::Direction>(direction.get())) {
        case Tables::Column::Direction::ASCENDANT:
          std::cout << "direction : ASCENDANT" << std::endl;
          break;
        case Tables::Column::Direction::DESCENDANT:
          std::cout << "direction : DESCENDANT" << std::endl;
          break;
        case Tables::Column::Direction::DEFAULT:
          std::cout << "direction : DEFAULT" << std::endl;
          break;
      }
    }

    std::cout << "---------------" << std::endl;
  }

  return ErrorCode::OK;
}

/**
 *  @brief add a table-metadata to metadata-table.
 */
ErrorCode add_table_metadata() {
  ErrorCode error = ErrorCode::UNKNOWN;

  std::unique_ptr<Tables> tables(new Tables(TEST_DB));  // use Template-Method.
  // error = tables->load();
  // if (error != ErrorCode::OK) {
  //   print_error(error, __LINE__);
  //   return error;
  // }

  std::unique_ptr<DataTypes> datatypes(new DataTypes(TEST_DB));
  // error = datatypes->load();
  // if (error != ErrorCode::OK) {
  //   print_error(error, __LINE__);
  //   return error;
  // }

  ptree datatype;
  ptree new_table;

  //
  // table-metadata
  //
  new_table.put(Tables::FORMAT_VERSION, Tables::format_version());
  new_table.put(Tables::GENERATION, Tables::generation());
  new_table.put(Tables::NAME, get_table_name());

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
  new_table.add_child(Tables::PRIMARY_KEY_NODE, primary_keys);
  //
  // column-metadata
  //
  ptree columns;
  {
    ptree column;
    // column #1
    column.clear();
    column.put(Tables::Column::NAME, column_name[0]);
    column.put(Tables::Column::ORDINAL_POSITION,
               static_cast<int>(ordinal_position::column_1));
    datatypes->get(DataTypes::PG_DATA_TYPE_QUALIFIED_NAME, "float4", datatype);
    ObjectIdType data_type_id = datatype.get<ObjectIdType>(DataTypes::ID);
    if (!data_type_id) {
      return ErrorCode::NOT_FOUND;
    } else {
      if (DataTypes::DataTypesId::FLOAT32 !=
          static_cast<DataTypes::DataTypesId>(data_type_id)) {
        return ErrorCode::UNKNOWN;
      }
    }
    column.put<ObjectIdType>(Tables::Column::DATA_TYPE_ID, data_type_id);
    column.put<bool>(Tables::Column::NULLABLE, false);
    column.put(Tables::Column::DIRECTION,
               static_cast<int>(Tables::Column::Direction::ASCENDANT));
    columns.push_back(std::make_pair("", column));

    // column #2
    column.clear();
    column.put(Tables::Column::NAME, column_name[1]);
    column.put(Tables::Column::ORDINAL_POSITION,
               static_cast<int>(ordinal_position::column_2));
    datatypes->get("VARCHAR", datatype);
    data_type_id = datatype.get<ObjectIdType>(DataTypes::ID);
    if (!data_type_id) {
      return ErrorCode::NOT_FOUND;
    } else {
      if (DataTypes::DataTypesId::VARCHAR !=
          static_cast<DataTypes::DataTypesId>(data_type_id)) {
        return ErrorCode::UNKNOWN;
      }
    }
    column.put(Tables::Column::DATA_TYPE_ID, data_type_id);
    column.put<uint64_t>(Tables::Column::DATA_LENGTH, 8);
    column.put<bool>(Tables::Column::VARYING, true);
    column.put<bool>(Tables::Column::NULLABLE, false);
    column.put(Tables::Column::DIRECTION,
               static_cast<int>(Tables::Column::Direction::DEFAULT));
    columns.push_back(std::make_pair("", column));

    // column #3
    column.clear();
    column.put(Tables::Column::NAME, column_name[2]);
    column.put(Tables::Column::ORDINAL_POSITION,
               static_cast<int>(ordinal_position::column_3));
    datatypes->get("CHAR", datatype);
    data_type_id = datatype.get<ObjectIdType>(DataTypes::ID);
    if (!data_type_id) {
      return ErrorCode::NOT_FOUND;
    } else {
      if (DataTypes::DataTypesId::CHAR !=
          static_cast<DataTypes::DataTypesId>(data_type_id)) {
        return ErrorCode::UNKNOWN;
      }
    }
    column.put(Tables::Column::DATA_TYPE_ID, data_type_id);
    column.put<uint64_t>(Tables::Column::DATA_LENGTH, 1);
    column.put<bool>(Tables::Column::VARYING, false);
    column.put<bool>(Tables::Column::NULLABLE, true);
    column.put(Tables::Column::DIRECTION,
               static_cast<int>(Tables::Column::Direction::DEFAULT));
    columns.push_back(std::make_pair("", column));
  }
  new_table.add_child(Tables::COLUMNS_NODE, columns);

  //
  // add table-metadata object
  //
  error = tables->add(new_table);
  if (error != ErrorCode::OK) {
    print_error(error, __LINE__);
  }

  return error;
}

/**
 *  @brief remove a table-metadata to metadata-table.
 */
ErrorCode tables_remove_test() {
  ErrorCode error = ErrorCode::UNKNOWN;
  int TABLE_NUM_ADDED = 4;

  for (int num = 0; num < TABLE_NUM_ADDED; num++) {
    error = add_table_metadata();
    if (error != ErrorCode::OK) {
      print_error(error, __LINE__);
      return error;
    }
  }

  std::unique_ptr<Tables> tables(new Tables(TEST_DB));  // use Template-Method.
  // error = tables->load();
  // if (error != ErrorCode::OK) {
  //   print_error(error, __LINE__);
  //   return error;
  // }

  //
  // remove table-metadata object
  //
  std::unique_ptr<ObjectId> oid_manager = std::make_unique<ObjectId>();

  ObjectIdType number = oid_manager->current("tables");
  std::vector<std::string> table_names = {
      "table_" + std::to_string(number - 3),
      "table_" + std::to_string(number - 1),
      "table_" + std::to_string(number - 4),
      "table_" + std::to_string(number - 0),
      "table_" + std::to_string(number - 2)};

  for (std::string name : table_names) {
    ObjectIdType object_id = 0;
    error = tables->remove(name.c_str(), &object_id);
    if (error != ErrorCode::OK) {
      print_error(error, __LINE__);
      return error;
    } else {
      std::cout << "remove table name :" << name << ", id:" << object_id
                << std::endl;
    }
  }

  const char* const table_name_not_exists = "table_name_not_exists";
  ObjectIdType ret_object_id = 0;
  error = tables->remove(table_name_not_exists, &ret_object_id);

  if (error == ErrorCode::OK) {
    print_error(error, __LINE__);
    return error;
  } else {
    std::cout << "can't remove table name not exists :" << table_name_not_exists
              << std::endl;
  }

  for (int num = 0; num < TABLE_NUM_ADDED + 1; num++) {
    error = add_table_metadata();
    if (error != ErrorCode::OK) {
      print_error(error, __LINE__);
      return error;
    }
  }

  // error = tables->load();
  // if (error != ErrorCode::OK) {
  //   print_error(error, __LINE__);
  //   return error;
  // }

  //
  // remove table-metadata object
  //

  number = oid_manager->current("tables");
  std::vector<ObjectIdType> object_ids = {number - 3, number - 1, number - 4,
                                          number - 0, number - 2};

  for (uint64_t object_id : object_ids) {
    error = tables->remove(object_id);
    if (error != ErrorCode::OK) {
      print_error(error, __LINE__);
      return error;
    } else {
      std::cout << "remove table id:" << object_id << std::endl;
    }
  }

  uint64_t table_id_not_exists = 0;
  error = tables->remove(table_id_not_exists);

  if (error == ErrorCode::OK) {
    print_error(error, __LINE__);
    return error;
  } else {
    std::cout << "can't remove table id not exists :" << table_id_not_exists
              << std::endl;
  }

  error = add_table_metadata();
  if (error != ErrorCode::OK) {
    print_error(error, __LINE__);
    return error;
  }

  return error;
}

/**
 *  @brief read table-metadata from metadata-table.
 */
ErrorCode read_table_metadata() {
  ErrorCode error = ErrorCode::UNKNOWN;

  std::unique_ptr<Tables> tables(new Tables(TEST_DB));  // use Template-Method.
  std::unique_ptr<ObjectId> oid_manager = std::make_unique<ObjectId>();
  ptree table;

  std::cout << "--- get table metadata by table name. ---" << std::endl;
  table.clear();
  std::string table_name =
      "table_" + std::to_string(oid_manager->current("tables"));
  error = tables->get(table_name, table);
  if (error == ErrorCode::OK) {
    display_table_metadata_object(table);
  }

  std::cout << "--- get table metadata by table id. ---" << std::endl;
  table.clear();
  error = tables->get(oid_manager->current("tables"), table);
  if (error == ErrorCode::OK) {
    display_table_metadata_object(table);
  }

  return error;
}

/**
 *  @brief test for Tables class object.
 */
ErrorCode tables_test() {
  ErrorCode error = ErrorCode::UNKNOWN;

  try {
    error = add_table_metadata();
  } catch (const ptree_error& e) {
    std::cerr << e.what() << std::endl;
    print_error(error, __LINE__);
    return error;
  }
  if (error != ErrorCode::OK) {
    print_error(error, __LINE__);
    return error;
  }

  try {
    error = read_table_metadata();
  } catch (const ptree_error& e) {
    std::cerr << e.what() << std::endl;
    print_error(error, __LINE__);
    return error;
  }
  if (error != ErrorCode::OK) {
    print_error(error, __LINE__);
    return error;
  }

  error = ErrorCode::OK;

  return error;
}

/**
 *  @brief test for DataTypes class object.
 */
ErrorCode datatypes_test() {
  ErrorCode error = ErrorCode::UNKNOWN;

  std::unique_ptr<DataTypes> datatypes(new DataTypes(TEST_DB));
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
          datatype_id.get<std::string>(DataTypes::NAME);
      if (data_type_name != datatype.second) {
        std::cout << "DataTypes Name error. [" << datatype.first
                  << "] expected:[" << datatype.second << "], actual:["
                  << data_type_name << "]" << std::endl;
        return ErrorCode::INTERNAL_ERROR;
      }

      ObjectIdType data_type_id =
          datatype_name.get<ObjectIdType>(DataTypes::ID);
      if (data_type_id != datatype.first) {
        std::cout << "DataTypes ID error. [" << datatype.second
                  << "] expected:[" << datatype.first << "], actual:["
                  << data_type_id << "]" << std::endl;
        return ErrorCode::INTERNAL_ERROR;
      }

      uint16_t format_version =
          datatype_name.get<uint16_t>(DataTypes::FORMAT_VERSION);
      uint32_t generation = datatype_name.get<uint32_t>(DataTypes::GENERATION);

      std::cout << "DataTypes -> FORMAT_VERSION:[" << format_version
                << "] / GENERATION:[" << generation << "] / ID:["
                << datatype.first << "] / NAME:[" << datatype.second << "]"
                << std::endl;
    }
  } catch (const ptree_error& e) {
    std::cerr << e.what() << std::endl;
    print_error(error, __LINE__);
    return error;
  }

  if (error != ErrorCode::OK) {
    print_error(error, __LINE__);
  }
  return error;
}

/**
 *  @brief main function.
 */
int main(void) {
  std::cout << "*** TableMetadata test start. ***" << std::endl << std::endl;
  std::cout << "=== class object test start. ===" << std::endl;
  ErrorCode tables_test_error = tables_test();
  std::cout << "=== class object test done. ===" << std::endl;
  std::cout << std::endl;

  std::cout << "=== remove table functions test start. ===" << std::endl;
  ErrorCode tables_remove_test_error = tables_remove_test();
  std::cout << "=== remove table functions test done. ===" << std::endl;
  std::cout << std::endl;

  std::cout << "=== datatypes object test start. ===" << std::endl;
  ErrorCode datatypes_test_error = datatypes_test();
  std::cout << "=== datatypes object test done. ===" << std::endl;
  std::cout << std::endl;

  std::cout << "tables test                   : ";
  if (tables_test_error == ErrorCode::OK) {
    std::cout << "Success" << std::endl;
  } else {
    std::cout << "*** Failure ***" << std::endl;
  }

  std::cout << "tables remove  functions test : ";
  if (tables_remove_test_error == ErrorCode::OK) {
    std::cout << "Success" << std::endl;
  } else {
    std::cout << "*** Failure ***" << std::endl;
  }

  std::cout << "datatypes test                : ";
  if (datatypes_test_error == ErrorCode::OK) {
    std::cout << "Success" << std::endl;
  } else {
    std::cout << "*** Failure ***" << std::endl;
  }

  std::cout << std::endl;

  std::cout << "*** TableMetadata test completed. ***" << std::endl;

  return 0;
}
