/*
 * Copyright 2021-2023 tsurugi project.
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
#include "manager/metadata/dao/json/datatypes_dao_json.h"

#include <boost/foreach.hpp>

#include "manager/metadata/common/message.h"
#include "manager/metadata/dao/common/pg_type.h"
#include "manager/metadata/helper/logging_helper.h"

// =============================================================================
namespace manager::metadata::db {

using boost::property_tree::ptree;

ErrorCode DataTypesDaoJson::prepare() {
  ErrorCode error = ErrorCode::UNKNOWN;

  ptree datatypes;
  {
    ptree datatype;

    // INT32 :
    datatype.put(DataTypes::FORMAT_VERSION, DataTypes::format_version());
    datatype.put(DataTypes::GENERATION, DataTypes::generation());
    datatype.put(DataTypes::ID,
                 static_cast<ObjectId>(DataTypes::DataTypesId::INT32));
    datatype.put(DataTypes::NAME, "INT32");
    datatype.put(DataTypes::PG_DATA_TYPE, PgType::TypeOid::kInt4);
    datatype.put(DataTypes::PG_DATA_TYPE_NAME, "integer");
    datatype.put(DataTypes::PG_DATA_TYPE_QUALIFIED_NAME,
                 PgType::TypeName::kInt4);
    datatypes.push_back(std::make_pair("", datatype));

    // INT64 :
    datatype.put(DataTypes::FORMAT_VERSION, DataTypes::format_version());
    datatype.put(DataTypes::GENERATION, DataTypes::generation());
    datatype.put(DataTypes::ID,
                 static_cast<ObjectId>(DataTypes::DataTypesId::INT64));
    datatype.put(DataTypes::NAME, "INT64");
    datatype.put(DataTypes::PG_DATA_TYPE, PgType::TypeOid::kInt8);
    datatype.put(DataTypes::PG_DATA_TYPE_NAME, "bigint");
    datatype.put(DataTypes::PG_DATA_TYPE_QUALIFIED_NAME,
                 PgType::TypeName::kInt8);
    datatypes.push_back(std::make_pair("", datatype));

    // FLOAT32 :
    datatype.put(DataTypes::FORMAT_VERSION, DataTypes::format_version());
    datatype.put(DataTypes::GENERATION, DataTypes::generation());
    datatype.put(DataTypes::ID,
                 static_cast<ObjectId>(DataTypes::DataTypesId::FLOAT32));
    datatype.put(DataTypes::NAME, "FLOAT32");
    datatype.put(DataTypes::PG_DATA_TYPE, PgType::TypeOid::kFloat4);
    datatype.put(DataTypes::PG_DATA_TYPE_NAME, "real");
    datatype.put(DataTypes::PG_DATA_TYPE_QUALIFIED_NAME,
                 PgType::TypeName::kFloat4);
    datatypes.push_back(std::make_pair("", datatype));

    // FLOAT64 :
    datatype.put(DataTypes::FORMAT_VERSION, DataTypes::format_version());
    datatype.put(DataTypes::GENERATION, DataTypes::generation());
    datatype.put(DataTypes::ID,
                 static_cast<ObjectId>(DataTypes::DataTypesId::FLOAT64));
    datatype.put(DataTypes::NAME, "FLOAT64");
    datatype.put(DataTypes::PG_DATA_TYPE, PgType::TypeOid::kFloat8);
    datatype.put(DataTypes::PG_DATA_TYPE_NAME, "double precision");
    datatype.put(DataTypes::PG_DATA_TYPE_QUALIFIED_NAME,
                 PgType::TypeName::kFloat8);
    datatypes.push_back(std::make_pair("", datatype));

    // CHAR : character, char
    datatype.put(DataTypes::FORMAT_VERSION, DataTypes::format_version());
    datatype.put(DataTypes::GENERATION, DataTypes::generation());
    datatype.put(DataTypes::ID,
                 static_cast<ObjectId>(DataTypes::DataTypesId::CHAR));
    datatype.put(DataTypes::NAME, "CHAR");
    datatype.put(DataTypes::PG_DATA_TYPE, PgType::TypeOid::kBpchar);
    datatype.put(DataTypes::PG_DATA_TYPE_NAME, "char");
    datatype.put(DataTypes::PG_DATA_TYPE_QUALIFIED_NAME,
                 PgType::TypeName::kBpchar);
    datatypes.push_back(std::make_pair("", datatype));

    // VARCHAR : character varying, varchar
    datatype.put(DataTypes::FORMAT_VERSION, DataTypes::format_version());
    datatype.put(DataTypes::GENERATION, DataTypes::generation());
    datatype.put(DataTypes::ID,
                 static_cast<ObjectId>(DataTypes::DataTypesId::VARCHAR));
    datatype.put(DataTypes::NAME, "VARCHAR");
    datatype.put(DataTypes::PG_DATA_TYPE, PgType::TypeOid::kVarchar);
    datatype.put(DataTypes::PG_DATA_TYPE_NAME, "varchar");
    datatype.put(DataTypes::PG_DATA_TYPE_QUALIFIED_NAME,
                 PgType::TypeName::kVarchar);
    datatypes.push_back(std::make_pair("", datatype));

    // NUMERIC : numeric, decimal
    datatype.put(DataTypes::FORMAT_VERSION, DataTypes::format_version());
    datatype.put(DataTypes::GENERATION, DataTypes::generation());
    datatype.put(DataTypes::ID,
                 static_cast<ObjectIdType>(DataTypes::DataTypesId::NUMERIC));
    datatype.put(DataTypes::NAME, "NUMERIC");
    datatype.put(DataTypes::PG_DATA_TYPE, PgType::TypeOid::kNumeric);
    datatype.put(DataTypes::PG_DATA_TYPE_NAME, "numeric");
    datatype.put(DataTypes::PG_DATA_TYPE_QUALIFIED_NAME,
                 PgType::TypeName::kNumeric);
    datatypes.push_back(std::make_pair("", datatype));

    // DATE : 
    datatype.put(DataTypes::FORMAT_VERSION, DataTypes::format_version());
    datatype.put(DataTypes::GENERATION, DataTypes::generation());
    datatype.put(DataTypes::ID,
                 static_cast<ObjectIdType>(DataTypes::DataTypesId::DATE));
    datatype.put(DataTypes::NAME, "DATE");
    datatype.put(DataTypes::PG_DATA_TYPE, PgType::TypeOid::kDate);
    datatype.put(DataTypes::PG_DATA_TYPE_NAME, "date");
    datatype.put(DataTypes::PG_DATA_TYPE_QUALIFIED_NAME,
                 PgType::TypeName::kDate);
    datatypes.push_back(std::make_pair("", datatype));

    // TIME : 
    datatype.put(DataTypes::FORMAT_VERSION, DataTypes::format_version());
    datatype.put(DataTypes::GENERATION, DataTypes::generation());
    datatype.put(DataTypes::ID,
                 static_cast<ObjectIdType>(DataTypes::DataTypesId::TIME));
    datatype.put(DataTypes::NAME, "TIME");
    datatype.put(DataTypes::PG_DATA_TYPE, PgType::TypeOid::kTime);
    datatype.put(DataTypes::PG_DATA_TYPE_NAME, "time");
    datatype.put(DataTypes::PG_DATA_TYPE_QUALIFIED_NAME,
                 PgType::TypeName::kTime);
    datatypes.push_back(std::make_pair("", datatype));

    // TIMETZ : 
    datatype.put(DataTypes::FORMAT_VERSION, DataTypes::format_version());
    datatype.put(DataTypes::GENERATION, DataTypes::generation());
    datatype.put(DataTypes::ID,
                 static_cast<ObjectIdType>(DataTypes::DataTypesId::TIMETZ));
    datatype.put(DataTypes::NAME, "TIMETZ");
    datatype.put(DataTypes::PG_DATA_TYPE, PgType::TypeOid::kTimetz);
    datatype.put(DataTypes::PG_DATA_TYPE_NAME, "timetz");
    datatype.put(DataTypes::PG_DATA_TYPE_QUALIFIED_NAME,
                 PgType::TypeName::kTimetz);
    datatypes.push_back(std::make_pair("", datatype));

    // TIMESTAMP : 
    datatype.put(DataTypes::FORMAT_VERSION, DataTypes::format_version());
    datatype.put(DataTypes::GENERATION, DataTypes::generation());
    datatype.put(DataTypes::ID,
                 static_cast<ObjectIdType>(DataTypes::DataTypesId::TIMESTAMP));
    datatype.put(DataTypes::NAME, "TIMESTAMP");
    datatype.put(DataTypes::PG_DATA_TYPE, PgType::TypeOid::kTimestamp);
    datatype.put(DataTypes::PG_DATA_TYPE_NAME, "timestamp");
    datatype.put(DataTypes::PG_DATA_TYPE_QUALIFIED_NAME,
                 PgType::TypeName::kTimestamp);
    datatypes.push_back(std::make_pair("", datatype));

    // TIMESTAMPTZ : 
    datatype.put(DataTypes::FORMAT_VERSION, DataTypes::format_version());
    datatype.put(DataTypes::GENERATION, DataTypes::generation());
    datatype.put(DataTypes::ID,
                 static_cast<ObjectIdType>(DataTypes::DataTypesId::TIMESTAMPTZ));
    datatype.put(DataTypes::NAME, "TIMESTAMPTZ");
    datatype.put(DataTypes::PG_DATA_TYPE, PgType::TypeOid::kTimestamptz);
    datatype.put(DataTypes::PG_DATA_TYPE_NAME, "timestamptz");
    datatype.put(DataTypes::PG_DATA_TYPE_QUALIFIED_NAME,
                 PgType::TypeName::kTimestamptz);
    datatypes.push_back(std::make_pair("", datatype));

    // INTERVAL : 
    datatype.put(DataTypes::FORMAT_VERSION, DataTypes::format_version());
    datatype.put(DataTypes::GENERATION, DataTypes::generation());
    datatype.put(DataTypes::ID,
                 static_cast<ObjectIdType>(DataTypes::DataTypesId::INTERVAL));
    datatype.put(DataTypes::NAME, "INTERVAL");
    datatype.put(DataTypes::PG_DATA_TYPE, PgType::TypeOid::kInterval);
    datatype.put(DataTypes::PG_DATA_TYPE_NAME, "interval");
    datatype.put(DataTypes::PG_DATA_TYPE_QUALIFIED_NAME,
                 PgType::TypeName::kInterval);
    datatypes.push_back(std::make_pair("", datatype));
  }

  datatype_contents_.clear();
  datatype_contents_.add_child(kRootNode, datatypes);

  error = ErrorCode::OK;
  return error;
}

ErrorCode DataTypesDaoJson::select_all(
    std::vector<boost::property_tree::ptree>& objects) const {
  ErrorCode error = ErrorCode::UNKNOWN;

  // Getting a metadata contents.
  const ptree& contents = datatype_contents_;

  // Convert from ptree structure type to vector<ptree>.
  const auto& node = contents.get_child(kRootNode);
  std::transform(node.begin(), node.end(), std::back_inserter(objects),
                 [](ptree::value_type v) { return v.second; });

  return error;
}

ErrorCode DataTypesDaoJson::select(std::string_view key,
                                   const std::vector<std::string_view>& values,
                                   boost::property_tree::ptree& object) const {
  ErrorCode error = ErrorCode::UNKNOWN;

  if (values.empty()) {
    LOG_ERROR << Message::PARAMETER_FAILED << "Key value is unspecified.";
    error = ErrorCode::INVALID_PARAMETER;
    return error;
  }

  // Getting a metadata contents.
  const ptree& contents = datatype_contents_;

  // Initialize the error code.
  error = get_not_found_error_code(key);

  object.clear();
  BOOST_FOREACH (const auto& node, contents.get_child(kRootNode)) {
    const ptree& temp_obj = node.second;

    // Get the value of the key.
    auto data_value = temp_obj.get_optional<std::string>(key.data());
    if (data_value) {
      // If the key value matches, the metadata is added.
      if (data_value.value() == values[0]) {
        object.push_back(std::make_pair("", temp_obj));
        error = ErrorCode::OK;
      }
    } else {
      object.clear();
      error = ErrorCode::INVALID_PARAMETER;
      break;
    }
  }

  return error;
}

}  // namespace manager::metadata::db
