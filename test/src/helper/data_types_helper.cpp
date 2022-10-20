/*
 * Copyright 2021 tsurugi project.
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
#include "test/helper/data_types_helper.h"

#include <gtest/gtest.h>

#include <string>
#include <vector>

#include "manager/metadata/datatypes.h"
#include "test/common/ut_utils.h"

namespace manager::metadata::testing {

using boost::property_tree::ptree;

// tsurugi data type name
struct DataTypesName {
  static constexpr char INT32[]   = "INT32";
  static constexpr char INT64[]   = "INT64";
  static constexpr char FLOAT32[] = "FLOAT32";
  static constexpr char FLOAT64[] = "FLOAT64";
  static constexpr char CHAR[]    = "CHAR";
  static constexpr char VARCHAR[] = "VARCHAR";
};
// PostgreSQL data type oid
struct PgDataType {
  static constexpr char INT32[]   = "23";
  static constexpr char INT64[]   = "20";
  static constexpr char FLOAT32[] = "700";
  static constexpr char FLOAT64[] = "701";
  static constexpr char CHAR[]    = "1042";
  static constexpr char VARCHAR[] = "1043";
};
// PostgreSQL data type name
struct PgDataTypeName {
  static constexpr char INT32[]   = "integer";
  static constexpr char INT64[]   = "bigint";
  static constexpr char FLOAT32[] = "real";
  static constexpr char FLOAT64[] = "double precision";
  static constexpr char CHAR[]    = "char";
  static constexpr char VARCHAR[] = "varchar";
};
// PostgreSQL internal qualified data type name
struct PgDataTypeQualifiedName {
  static constexpr char INT32[]   = "int4";
  static constexpr char INT64[]   = "int8";
  static constexpr char FLOAT32[] = "float4";
  static constexpr char FLOAT64[] = "float8";
  static constexpr char CHAR[]    = "bpchar";
  static constexpr char VARCHAR[] = "varchar";
};

// a list of tsurugi data type id
std::vector<std::string> DataTypesIdList = {
    std::to_string(static_cast<ObjectIdType>(DataTypes::DataTypesId::INT32)),
    std::to_string(static_cast<ObjectIdType>(DataTypes::DataTypesId::INT64)),
    std::to_string(static_cast<ObjectIdType>(DataTypes::DataTypesId::FLOAT32)),
    std::to_string(static_cast<ObjectIdType>(DataTypes::DataTypesId::FLOAT64)),
    std::to_string(static_cast<ObjectIdType>(DataTypes::DataTypesId::CHAR)),
    std::to_string(static_cast<ObjectIdType>(DataTypes::DataTypesId::VARCHAR)),
};
// a list of tsurugi data type name
std::vector<std::string> DataTypesNameList = {
    DataTypesName::INT32,   DataTypesName::INT64, DataTypesName::FLOAT32,
    DataTypesName::FLOAT64, DataTypesName::CHAR,  DataTypesName::VARCHAR};
// a list of PostgreSQL data type oid
std::vector<std::string> PgDataTypeList = {
    PgDataType::INT32,   PgDataType::INT64, PgDataType::FLOAT32,
    PgDataType::FLOAT64, PgDataType::CHAR,  PgDataType::VARCHAR};
// a list of PostgreSQL data type name
std::vector<std::string> PgDataTypeNameList = {
    PgDataTypeName::INT32,   PgDataTypeName::INT64, PgDataTypeName::FLOAT32,
    PgDataTypeName::FLOAT64, PgDataTypeName::CHAR,  PgDataTypeName::VARCHAR};
// a list of PostgreSQL qualified data type name
std::vector<std::string> PgDataTypeQualifiedNameList = {
    PgDataTypeQualifiedName::INT32,   PgDataTypeQualifiedName::INT64,
    PgDataTypeQualifiedName::FLOAT32, PgDataTypeQualifiedName::FLOAT64,
    PgDataTypeQualifiedName::CHAR,    PgDataTypeQualifiedName::VARCHAR};

/**
 * @brief Make a list of key/value pair about data types metadata.
 * @return a list of key/value pair about data types metadata.
 *   For example, if key = DataTypes::NAME, values are "INT32", "INT64", and
 *   "FLOAT32" etc.
 */
std::vector<DataTypesHelper::BasicTestParameter>
DataTypesHelper::make_datatypes_tuple() {
  std::vector<BasicTestParameter> v;

  make_datatypes_tuple(DataTypes::ID, DataTypesIdList, v);
  make_datatypes_tuple(DataTypes::NAME, DataTypesNameList, v);
  make_datatypes_tuple(DataTypes::PG_DATA_TYPE, PgDataTypeList, v);
  make_datatypes_tuple(DataTypes::PG_DATA_TYPE_NAME, PgDataTypeNameList, v);
  make_datatypes_tuple(DataTypes::PG_DATA_TYPE_QUALIFIED_NAME,
                       PgDataTypeQualifiedNameList, v);
  return v;
}

/**
 * @brief Make a list of list about data type names.
 * @return data type names.
 */
std::vector<std::string> DataTypesHelper::make_datatype_names() {
  return DataTypesNameList;
}

/**
 * @brief Verifies that returned data type metadata equals expected one.
 * @param (datatype)  [in] data type metadata returned from api to get
 * data type metadata
 */
void DataTypesHelper::check_datatype_metadata_expected(
    const boost::property_tree::ptree& datatype) {
  // tsurugi data type id
  auto data_type_id = datatype.get<ObjectIdType>(DataTypes::ID);

  // tsurugi data type name
  auto datatype_name_got = datatype.get<std::string>(DataTypes::NAME);

  // PostgreSQL data type oid
  auto pg_data_type = datatype.get<std::string>(DataTypes::PG_DATA_TYPE);

  // PostgreSQL data type name
  auto pg_data_type_name =
      datatype.get<std::string>(DataTypes::PG_DATA_TYPE_NAME);

  // PostgreSQL data type qualified name
  auto pg_data_type_qualified_name =
      datatype.get<std::string>(DataTypes::PG_DATA_TYPE_QUALIFIED_NAME);

  switch (data_type_id) {
    case static_cast<ObjectIdType>(DataTypes::DataTypesId::INT32): {
      EXPECT_EQ(DataTypesName::INT32, datatype_name_got);
      EXPECT_EQ(PgDataType::INT32, pg_data_type);
      EXPECT_EQ(PgDataTypeName::INT32, pg_data_type_name);
      EXPECT_EQ(PgDataTypeQualifiedName::INT32, pg_data_type_qualified_name);
    } break;
    case static_cast<ObjectIdType>(DataTypes::DataTypesId::INT64): {
      EXPECT_EQ(DataTypesName::INT64, datatype_name_got);
      EXPECT_EQ(PgDataType::INT64, pg_data_type);
      EXPECT_EQ(PgDataTypeName::INT64, pg_data_type_name);
      EXPECT_EQ(PgDataTypeQualifiedName::INT64, pg_data_type_qualified_name);
    } break;
    case static_cast<ObjectIdType>(DataTypes::DataTypesId::FLOAT32): {
      EXPECT_EQ(DataTypesName::FLOAT32, datatype_name_got);
      EXPECT_EQ(PgDataType::FLOAT32, pg_data_type);
      EXPECT_EQ(PgDataTypeName::FLOAT32, pg_data_type_name);
      EXPECT_EQ(PgDataTypeQualifiedName::FLOAT32, pg_data_type_qualified_name);
    } break;
    case static_cast<ObjectIdType>(DataTypes::DataTypesId::FLOAT64): {
      EXPECT_EQ(DataTypesName::FLOAT64, datatype_name_got);
      EXPECT_EQ(PgDataType::FLOAT64, pg_data_type);
      EXPECT_EQ(PgDataTypeName::FLOAT64, pg_data_type_name);
      EXPECT_EQ(PgDataTypeQualifiedName::FLOAT64, pg_data_type_qualified_name);
    } break;
    case static_cast<ObjectIdType>(DataTypes::DataTypesId::CHAR): {
      EXPECT_EQ(DataTypesName::CHAR, datatype_name_got);
      EXPECT_EQ(PgDataType::CHAR, pg_data_type);
      EXPECT_EQ(PgDataTypeName::CHAR, pg_data_type_name);
      EXPECT_EQ(PgDataTypeQualifiedName::CHAR, pg_data_type_qualified_name);
    } break;
    case static_cast<ObjectIdType>(DataTypes::DataTypesId::VARCHAR): {
      EXPECT_EQ(DataTypesName::VARCHAR, datatype_name_got);
      EXPECT_EQ(PgDataType::VARCHAR, pg_data_type);
      EXPECT_EQ(PgDataTypeName::VARCHAR, pg_data_type_name);
      EXPECT_EQ(PgDataTypeQualifiedName::VARCHAR, pg_data_type_qualified_name);
    } break;
    default: {
      UTUtils::print("datatypes id not exists");
      ASSERT_TRUE(false);
    } break;
  }
}

/**
 * @brief internal function of make_datatypes_tuple.
 * @param (key)      [in]  a data types key.
 * @param (values)   [in]  data types values.
 * @param (v)        [out] a list of key/value pair about data types metadata.
 * @return none.
 */
void DataTypesHelper::make_datatypes_tuple(
    std::string_view key, const std::vector<std::string> values,
    std::vector<BasicTestParameter>& v) {
  for (auto value : values) {
    v.emplace_back(key, value);
  }
}

}  // namespace manager::metadata::testing
