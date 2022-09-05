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
#include "manager/metadata/dao/json/datatypes_dao_json.h"

#include <boost/foreach.hpp>
#include <boost/property_tree/ptree.hpp>

#include "manager/metadata/dao/common/pg_type.h"
#include "manager/metadata/error_code.h"

// =============================================================================
namespace manager::metadata::db::json {

using boost::property_tree::ptree;

/**
 * @brief Defines all prepared data types metadata.
 * @param none.
 * @return ErrorCode::OK if success, otherwise an error code.
 */
ErrorCode DataTypesDAO::prepare() const {
  ErrorCode error = ErrorCode::UNKNOWN;

  ptree datatypes;
  {
    ptree datatype;

    // INT32 :
    datatype.put(DataTypes::FORMAT_VERSION, DataTypes::format_version());
    datatype.put(DataTypes::GENERATION, DataTypes::generation());
    datatype.put(DataTypes::ID,
                 static_cast<ObjectIdType>(DataTypes::DataTypesId::INT32));
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
                 static_cast<ObjectIdType>(DataTypes::DataTypesId::INT64));
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
                 static_cast<ObjectIdType>(DataTypes::DataTypesId::FLOAT32));
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
                 static_cast<ObjectIdType>(DataTypes::DataTypesId::FLOAT64));
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
                 static_cast<ObjectIdType>(DataTypes::DataTypesId::CHAR));
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
                 static_cast<ObjectIdType>(DataTypes::DataTypesId::VARCHAR));
    datatype.put(DataTypes::NAME, "VARCHAR");
    datatype.put(DataTypes::PG_DATA_TYPE, PgType::TypeOid::kVarchar);
    datatype.put(DataTypes::PG_DATA_TYPE_NAME, "varchar");
    datatype.put(DataTypes::PG_DATA_TYPE_QUALIFIED_NAME,
                 PgType::TypeName::kVarchar);
    datatypes.push_back(std::make_pair("", datatype));
  }

  ptree* meta_object = session_manager_->get_container();
  meta_object->add_child(DataTypesDAO::DATATYPES_NODE, datatypes);

  error = ErrorCode::OK;
  return error;
}

/**
 * @brief Get one data type metadata from the data types metadata table,
 *   where the given key equals the given value.
 * @param (object_key)    [in]  metadata-object key.
 * @param (object_value)  [in]  metadata-object value.
 * @param (object)        [out] metadata-object with the specified name.
 * @retval ErrorCode::OK if success.
 * @retval ErrorCode::NOT_FOUND if the data type id or data type name
 *   does not exist.
 * @retval otherwise an error code.
 */
ErrorCode DataTypesDAO::select_one_data_type_metadata(
    std::string_view object_key, std::string_view object_value,
    boost::property_tree::ptree& object) const {
  ErrorCode error = ErrorCode::UNKNOWN;

  ptree* meta_object = session_manager_->get_container();

  // Initialize the error code.
  if (object_key == DataTypes::ID) {
    error = ErrorCode::ID_NOT_FOUND;
  } else if (object_key == DataTypes::NAME) {
    error = ErrorCode::NAME_NOT_FOUND;
  } else {
    error = ErrorCode::NOT_FOUND;
  }

  BOOST_FOREACH (const ptree::value_type& node,
                 meta_object->get_child(DataTypesDAO::DATATYPES_NODE)) {
    const ptree& temp_obj = node.second;

    boost::optional<std::string> value =
        temp_obj.get_optional<std::string>(object_key.data());
    if (!value) {
      error = ErrorCode::INVALID_PARAMETER;
      break;
    }
    if (value.get() == object_value) {
      object = temp_obj;
      error = ErrorCode::OK;
      break;
    }
  }

  return error;
}

}  // namespace manager::metadata::db::json
