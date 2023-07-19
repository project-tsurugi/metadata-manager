/*
 * Copyright 2022-2023 tsurugi project.
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
#include "test/metadata/ut_datatypes_metadata.h"

#include "manager/metadata/datatype.h"
#include "manager/metadata/helper/ptree_helper.h"
#include "test/common/ut_utils.h"

namespace {

using manager::metadata::DataType;
using manager::metadata::ObjectId;

// tsurugi data type name
struct DataTypesName {
  static constexpr char INT32[]       = "INT32";
  static constexpr char INT64[]       = "INT64";
  static constexpr char FLOAT32[]     = "FLOAT32";
  static constexpr char FLOAT64[]     = "FLOAT64";
  static constexpr char CHAR[]        = "CHAR";
  static constexpr char VARCHAR[]     = "VARCHAR";
  static constexpr char NUMERIC[]     = "NUMERIC";
  static constexpr char DATE[]        = "DATE";
  static constexpr char TIME[]        = "TIME";
  static constexpr char TIMETZ[]      = "TIMETZ";
  static constexpr char TIMESTAMP[]   = "TIMESTAMP";
  static constexpr char TIMESTAMPTZ[] = "TIMESTAMPTZ";
  static constexpr char INTERVAL[]    = "INTERVAL";
};
// PostgreSQL data type oid
struct PgDataType {
  static constexpr char INT32[]       = "23";
  static constexpr char INT64[]       = "20";
  static constexpr char FLOAT32[]     = "700";
  static constexpr char FLOAT64[]     = "701";
  static constexpr char CHAR[]        = "1042";
  static constexpr char VARCHAR[]     = "1043";
  static constexpr char NUMERIC[]     = "1700";
  static constexpr char DATE[]        = "1082";
  static constexpr char TIME[]        = "1083";
  static constexpr char TIMETZ[]      = "1266";
  static constexpr char TIMESTAMP[]   = "1114";
  static constexpr char TIMESTAMPTZ[] = "1184";
  static constexpr char INTERVAL[]    = "1186";
};
// PostgreSQL data type name
struct PgDataTypeName {
  static constexpr char INT32[]       = "integer";
  static constexpr char INT64[]       = "bigint";
  static constexpr char FLOAT32[]     = "real";
  static constexpr char FLOAT64[]     = "double precision";
  static constexpr char CHAR[]        = "char";
  static constexpr char VARCHAR[]     = "varchar";
  static constexpr char NUMERIC[]     = "numeric";
  static constexpr char DATE[]        = "date";
  static constexpr char TIME[]        = "time";
  static constexpr char TIMETZ[]      = "timetz";
  static constexpr char TIMESTAMP[]   = "timestamp";
  static constexpr char TIMESTAMPTZ[] = "timestamptz";
  static constexpr char INTERVAL[]    = "interval";
};
// PostgreSQL internal qualified data type name
struct PgDataTypeQualifiedName {
  static constexpr char INT32[]       = "int4";
  static constexpr char INT64[]       = "int8";
  static constexpr char FLOAT32[]     = "float4";
  static constexpr char FLOAT64[]     = "float8";
  static constexpr char CHAR[]        = "bpchar";
  static constexpr char VARCHAR[]     = "varchar";
  static constexpr char NUMERIC[]     = "numeric";
  static constexpr char DATE[]        = "date";
  static constexpr char TIME[]        = "time";
  static constexpr char TIMETZ[]      = "timetz";
  static constexpr char TIMESTAMP[]   = "timestamp";
  static constexpr char TIMESTAMPTZ[] = "timestamptz";
  static constexpr char INTERVAL[]    = "interval";
};

// a list of tsurugi data type id
std::vector<std::string> DataTypeIdList = {
    std::to_string(static_cast<ObjectId>(DataType::DataTypeId::INT32)),
    std::to_string(static_cast<ObjectId>(DataType::DataTypeId::INT64)),
    std::to_string(static_cast<ObjectId>(DataType::DataTypeId::FLOAT32)),
    std::to_string(static_cast<ObjectId>(DataType::DataTypeId::FLOAT64)),
    std::to_string(static_cast<ObjectId>(DataType::DataTypeId::CHAR)),
    std::to_string(static_cast<ObjectId>(DataType::DataTypeId::VARCHAR)),
    std::to_string(static_cast<ObjectId>(DataType::DataTypeId::NUMERIC)),
    std::to_string(static_cast<ObjectId>(DataType::DataTypeId::DATE)),
    std::to_string(static_cast<ObjectId>(DataType::DataTypeId::TIME)),
    std::to_string(static_cast<ObjectId>(DataType::DataTypeId::TIMETZ)),
    std::to_string(static_cast<ObjectId>(DataType::DataTypeId::TIMESTAMP)),
    std::to_string(static_cast<ObjectId>(DataType::DataTypeId::TIMESTAMPTZ)),
    std::to_string(static_cast<ObjectId>(DataType::DataTypeId::INTERVAL)),
};
// a list of tsurugi data type name
std::vector<std::string> DataTypesNameList = {
    DataTypesName::INT32,     DataTypesName::INT64,
    DataTypesName::FLOAT32,   DataTypesName::FLOAT64,
    DataTypesName::CHAR,      DataTypesName::VARCHAR,
    DataTypesName::NUMERIC,   DataTypesName::DATE,
    DataTypesName::TIME,      DataTypesName::TIMETZ,
    DataTypesName::TIMESTAMP, DataTypesName::TIMESTAMPTZ,
    DataTypesName::INTERVAL};
// a list of PostgreSQL data type oid
std::vector<std::string> PgDataTypeList = {
    PgDataType::INT32,   PgDataType::INT64,     PgDataType::FLOAT32,
    PgDataType::FLOAT64, PgDataType::CHAR,      PgDataType::VARCHAR,
    PgDataType::NUMERIC, PgDataType::DATE,      PgDataType::TIME,
    PgDataType::TIMETZ,  PgDataType::TIMESTAMP, PgDataType::TIMESTAMPTZ,
    PgDataType::INTERVAL};
// a list of PostgreSQL data type name
std::vector<std::string> PgDataTypeNameList = {
    PgDataTypeName::INT32,     PgDataTypeName::INT64,
    PgDataTypeName::FLOAT32,   PgDataTypeName::FLOAT64,
    PgDataTypeName::CHAR,      PgDataTypeName::VARCHAR,
    PgDataTypeName::NUMERIC,   PgDataTypeName::DATE,
    PgDataTypeName::TIME,      PgDataTypeName::TIMETZ,
    PgDataTypeName::TIMESTAMP, PgDataTypeName::TIMESTAMPTZ,
    PgDataTypeName::INTERVAL};
// a list of PostgreSQL qualified data type name
std::vector<std::string> PgDataTypeQualifiedNameList = {
    PgDataTypeQualifiedName::INT32,     PgDataTypeQualifiedName::INT64,
    PgDataTypeQualifiedName::FLOAT32,   PgDataTypeQualifiedName::FLOAT64,
    PgDataTypeQualifiedName::CHAR,      PgDataTypeQualifiedName::VARCHAR,
    PgDataTypeQualifiedName::NUMERIC,   PgDataTypeQualifiedName::DATE,
    PgDataTypeQualifiedName::TIME,      PgDataTypeQualifiedName::TIMETZ,
    PgDataTypeQualifiedName::TIMESTAMP, PgDataTypeQualifiedName::TIMESTAMPTZ,
    PgDataTypeQualifiedName::INTERVAL};

}  // namespace

namespace manager::metadata::testing {

using boost::property_tree::ptree;

UtDataTypesMetadata::UtDataTypesMetadata() {
  // INT32
  {
    ptree values;
    values.put(DataType::ID,
               static_cast<int64_t>(DataType::DataTypeId::INT32));
    values.put(DataType::NAME, DataTypesName::INT32);
    values.put(DataType::PG_DATA_TYPE, PgDataType::INT32);
    values.put(DataType::PG_DATA_TYPE_NAME, PgDataTypeName::INT32);
    values.put(DataType::PG_DATA_TYPE_QUALIFIED_NAME,
               PgDataTypeQualifiedName::INT32);
    metadata_ptree_.add_child(DataTypesName::INT32, values);
  }
  // INT64
  {
    ptree values;
    values.put(DataType::ID,
               static_cast<int64_t>(DataType::DataTypeId::INT64));
    values.put(DataType::NAME, DataTypesName::INT64);
    values.put(DataType::PG_DATA_TYPE, PgDataType::INT64);
    values.put(DataType::PG_DATA_TYPE_NAME, PgDataTypeName::INT64);
    values.put(DataType::PG_DATA_TYPE_QUALIFIED_NAME,
               PgDataTypeQualifiedName::INT64);
    metadata_ptree_.add_child(DataTypesName::INT64, values);
  }
  // FLOAT32
  {
    ptree values;
    values.put(DataType::ID,
               static_cast<int64_t>(DataType::DataTypeId::FLOAT32));
    values.put(DataType::NAME, DataTypesName::FLOAT32);
    values.put(DataType::PG_DATA_TYPE, PgDataType::FLOAT32);
    values.put(DataType::PG_DATA_TYPE_NAME, PgDataTypeName::FLOAT32);
    values.put(DataType::PG_DATA_TYPE_QUALIFIED_NAME,
               PgDataTypeQualifiedName::FLOAT32);
    metadata_ptree_.add_child(DataTypesName::FLOAT32, values);
  }
  // FLOAT64
  {
    ptree values;
    values.put(DataType::ID,
               static_cast<int64_t>(DataType::DataTypeId::FLOAT64));
    values.put(DataType::NAME, DataTypesName::FLOAT64);
    values.put(DataType::PG_DATA_TYPE, PgDataType::FLOAT64);
    values.put(DataType::PG_DATA_TYPE_NAME, PgDataTypeName::FLOAT64);
    values.put(DataType::PG_DATA_TYPE_QUALIFIED_NAME,
               PgDataTypeQualifiedName::FLOAT64);
    metadata_ptree_.add_child(DataTypesName::FLOAT64, values);
  }
  // CHAR
  {
    ptree values;
    values.put(DataType::ID,
               static_cast<int64_t>(DataType::DataTypeId::CHAR));
    values.put(DataType::NAME, DataTypesName::CHAR);
    values.put(DataType::PG_DATA_TYPE, PgDataType::CHAR);
    values.put(DataType::PG_DATA_TYPE_NAME, PgDataTypeName::CHAR);
    values.put(DataType::PG_DATA_TYPE_QUALIFIED_NAME,
               PgDataTypeQualifiedName::CHAR);
    metadata_ptree_.add_child(DataTypesName::CHAR, values);
  }
  // VARCHAR
  {
    ptree values;
    values.put(DataType::ID,
               static_cast<int64_t>(DataType::DataTypeId::VARCHAR));
    values.put(DataType::NAME, DataTypesName::VARCHAR);
    values.put(DataType::PG_DATA_TYPE, PgDataType::VARCHAR);
    values.put(DataType::PG_DATA_TYPE_NAME, PgDataTypeName::VARCHAR);
    values.put(DataType::PG_DATA_TYPE_QUALIFIED_NAME,
               PgDataTypeQualifiedName::VARCHAR);
    metadata_ptree_.add_child(DataTypesName::VARCHAR, values);
  }
  // NUMERIC
  {
    ptree values;
    values.put(DataType::ID,
               static_cast<int64_t>(DataType::DataTypeId::NUMERIC));
    values.put(DataType::NAME, DataTypesName::NUMERIC);
    values.put(DataType::PG_DATA_TYPE, PgDataType::NUMERIC);
    values.put(DataType::PG_DATA_TYPE_NAME, PgDataTypeName::NUMERIC);
    values.put(DataType::PG_DATA_TYPE_QUALIFIED_NAME,
               PgDataTypeQualifiedName::NUMERIC);
    metadata_ptree_.add_child(DataTypesName::NUMERIC, values);
  }
  // DATE
  {
    ptree values;
    values.put(DataType::ID,
               static_cast<int64_t>(DataType::DataTypeId::DATE));
    values.put(DataType::NAME, DataTypesName::DATE);
    values.put(DataType::PG_DATA_TYPE, PgDataType::DATE);
    values.put(DataType::PG_DATA_TYPE_NAME, PgDataTypeName::DATE);
    values.put(DataType::PG_DATA_TYPE_QUALIFIED_NAME,
               PgDataTypeQualifiedName::DATE);
    metadata_ptree_.add_child(DataTypesName::DATE, values);
  }
  // TIME
  {
    ptree values;
    values.put(DataType::ID,
               static_cast<int64_t>(DataType::DataTypeId::TIME));
    values.put(DataType::NAME, DataTypesName::TIME);
    values.put(DataType::PG_DATA_TYPE, PgDataType::TIME);
    values.put(DataType::PG_DATA_TYPE_NAME, PgDataTypeName::TIME);
    values.put(DataType::PG_DATA_TYPE_QUALIFIED_NAME,
               PgDataTypeQualifiedName::TIME);
    metadata_ptree_.add_child(DataTypesName::TIME, values);
  }
  // TIMETZ
  {
    ptree values;
    values.put(DataType::ID,
               static_cast<int64_t>(DataType::DataTypeId::TIMETZ));
    values.put(DataType::NAME, DataTypesName::TIMETZ);
    values.put(DataType::PG_DATA_TYPE, PgDataType::TIMETZ);
    values.put(DataType::PG_DATA_TYPE_NAME, PgDataTypeName::TIMETZ);
    values.put(DataType::PG_DATA_TYPE_QUALIFIED_NAME,
               PgDataTypeQualifiedName::TIMETZ);
    metadata_ptree_.add_child(DataTypesName::TIMETZ, values);
  }
  // TIMESTAMP
  {
    ptree values;
    values.put(DataType::ID,
               static_cast<int64_t>(DataType::DataTypeId::TIMESTAMP));
    values.put(DataType::NAME, DataTypesName::TIMESTAMP);
    values.put(DataType::PG_DATA_TYPE, PgDataType::TIMESTAMP);
    values.put(DataType::PG_DATA_TYPE_NAME, PgDataTypeName::TIMESTAMP);
    values.put(DataType::PG_DATA_TYPE_QUALIFIED_NAME,
               PgDataTypeQualifiedName::TIMESTAMP);
    metadata_ptree_.add_child(DataTypesName::TIMESTAMP, values);
  }
  // TIMESTAMPTZ
  {
    ptree values;
    values.put(DataType::ID,
               static_cast<int64_t>(DataType::DataTypeId::TIMESTAMPTZ));
    values.put(DataType::NAME, DataTypesName::TIMESTAMPTZ);
    values.put(DataType::PG_DATA_TYPE, PgDataType::TIMESTAMPTZ);
    values.put(DataType::PG_DATA_TYPE_NAME, PgDataTypeName::TIMESTAMPTZ);
    values.put(DataType::PG_DATA_TYPE_QUALIFIED_NAME,
               PgDataTypeQualifiedName::TIMESTAMPTZ);
    metadata_ptree_.add_child(DataTypesName::TIMESTAMPTZ, values);
  }
  // INTERVAL
  {
    ptree values;
    values.put(DataType::ID,
               static_cast<int64_t>(DataType::DataTypeId::INTERVAL));
    values.put(DataType::NAME, DataTypesName::INTERVAL);
    values.put(DataType::PG_DATA_TYPE, PgDataType::INTERVAL);
    values.put(DataType::PG_DATA_TYPE_NAME, PgDataTypeName::INTERVAL);
    values.put(DataType::PG_DATA_TYPE_QUALIFIED_NAME,
               PgDataTypeQualifiedName::INTERVAL);
    metadata_ptree_.add_child(DataTypesName::INTERVAL, values);
  }
}

/**
 * @brief Get a list of Tsurugi data type IDs.
 * @return Tsurugi data type IDs.
 */
std::vector<std::string> UtDataTypesMetadata::get_datatype_ids() const {
  return DataTypeIdList;
}

/**
 * @brief Get a list of Tsurugi data type names.
 * @return Tsurugi data type names.
 */
std::vector<std::string> UtDataTypesMetadata::get_datatype_names() const {
  return DataTypesNameList;
}

/**
 * @brief Get a list of PostgreSQL data type ids.
 * @return PostgreSQL data type ids.
 */
std::vector<std::string> UtDataTypesMetadata::get_pg_datatype_ids() const {
  return PgDataTypeList;
}

/**
 * @brief Get a list of PostgreSQL data type names.
 * @return PostgreSQL data type names.
 */
std::vector<std::string> UtDataTypesMetadata::get_pg_datatype_names() const {
  return PgDataTypeNameList;
}

/**
 * @brief Get a list of PostgreSQL qualified data type names.
 * @return qualified data type names.
 */
std::vector<std::string> UtDataTypesMetadata::get_pg_datatype_qualified_names()
    const {
  return PgDataTypeQualifiedNameList;
}

/**
 * @brief Verifies that the actual table metadata equals expected one.
 * @param actual    [in]  actual table metadata.
 * @param file      [in]  file name of the caller.
 * @param line      [in]  line number of the caller.
 * @return none.
 */
void UtDataTypesMetadata::check_metadata_expected(
    const boost::property_tree::ptree& actual, const char* file,
    const int64_t line) const {
  auto datatype_name = actual.get_optional<std::string>(DataType::NAME);
  auto expected_datatype =
      metadata_ptree_.get_child_optional(datatype_name.value_or(""));
  if (!expected_datatype) {
    FAIL() << "Unknown data types";
  }

  this->check_metadata_expected(expected_datatype.get(), actual, file, line);
}

/**
 * @brief Verifies that the actual table metadata equals expected one.
 * @param expected  [in]  expected table metadata.
 * @param actual    [in]  actual table metadata.
 * @param file      [in]  file name of the caller.
 * @param line      [in]  line number of the caller.
 * @return none.
 */
void UtDataTypesMetadata::check_metadata_expected(
    const boost::property_tree::ptree& expected,
    const boost::property_tree::ptree& actual, const char* file,
    const int64_t line) const {
  // tsurugi data type id
  check_expected<ObjectId>(expected, actual, DataType::ID, file, line);

  // tsurugi data type name
  check_expected<std::string>(expected, actual, DataType::NAME, file, line);

  // PostgreSQL data type oid
  check_expected<ObjectId>(expected, actual, DataType::PG_DATA_TYPE, file,
                           line);

  // PostgreSQL data type name
  check_expected<std::string>(expected, actual, DataType::PG_DATA_TYPE_NAME,
                              file, line);

  // PostgreSQL data type qualified name
  check_expected<std::string>(
      expected, actual, DataType::PG_DATA_TYPE_QUALIFIED_NAME, file, line);
}

}  // namespace manager::metadata::testing
