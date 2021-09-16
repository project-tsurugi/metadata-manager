/*
 * Copyright 2020 tsurugi project.
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
#include "manager/metadata/dao/json/datatypes_dao.h"

#include <boost/foreach.hpp>
#include <boost/property_tree/ptree.hpp>

#include "manager/metadata/datatypes.h"
#include "manager/metadata/error_code.h"
#include "manager/metadata/metadata.h"

// =============================================================================
namespace manager::metadata::db::json {

using boost::property_tree::ptree;
using manager::metadata::ErrorCode;

/**
 * @brief Defines all prepared data types metadata.
 * @param none.
 * @return ErrorCode::OK if success, otherwise an error code.
 */
ErrorCode DataTypesDAO::prepare() const {
  ptree datatypes;
  {
    ptree datatype;

    // INT32 :
    datatype.put(DataTypes::ID,
                 static_cast<ObjectIdType>(DataTypes::DataTypesId::INT32));
    datatype.put(DataTypes::NAME, "INT32");
    datatype.put(DataTypes::PG_DATA_TYPE, 23);
    datatype.put(DataTypes::PG_DATA_TYPE_NAME, "integer");
    datatype.put(DataTypes::PG_DATA_TYPE_QUALIFIED_NAME, "int4");
    datatypes.push_back(std::make_pair("", datatype));

    // INT64 :
    datatype.put(DataTypes::ID,
                 static_cast<ObjectIdType>(DataTypes::DataTypesId::INT64));
    datatype.put(DataTypes::NAME, "INT64");
    datatype.put(DataTypes::PG_DATA_TYPE, 20);
    datatype.put(DataTypes::PG_DATA_TYPE_NAME, "bigint");
    datatype.put(DataTypes::PG_DATA_TYPE_QUALIFIED_NAME, "int8");
    datatypes.push_back(std::make_pair("", datatype));

    // FLOAT32 :
    datatype.put(DataTypes::ID,
                 static_cast<ObjectIdType>(DataTypes::DataTypesId::FLOAT32));
    datatype.put(DataTypes::NAME, "FLOAT32");
    datatype.put(DataTypes::PG_DATA_TYPE, 700);
    datatype.put(DataTypes::PG_DATA_TYPE_NAME, "real");
    datatype.put(DataTypes::PG_DATA_TYPE_QUALIFIED_NAME, "float4");
    datatypes.push_back(std::make_pair("", datatype));

    // FLOAT64 :
    datatype.put(DataTypes::ID,
                 static_cast<ObjectIdType>(DataTypes::DataTypesId::FLOAT64));
    datatype.put(DataTypes::NAME, "FLOAT64");
    datatype.put(DataTypes::PG_DATA_TYPE, 701);
    datatype.put(DataTypes::PG_DATA_TYPE_NAME, "double precision");
    datatype.put(DataTypes::PG_DATA_TYPE_QUALIFIED_NAME, "float8");
    datatypes.push_back(std::make_pair("", datatype));

    // CHAR : character, char
    datatype.put(DataTypes::ID,
                 static_cast<ObjectIdType>(DataTypes::DataTypesId::CHAR));
    datatype.put(DataTypes::NAME, "CHAR");
    datatype.put(DataTypes::PG_DATA_TYPE, 1042);
    datatype.put(DataTypes::PG_DATA_TYPE_NAME, "char");
    datatype.put(DataTypes::PG_DATA_TYPE_QUALIFIED_NAME, "bpchar");
    datatypes.push_back(std::make_pair("", datatype));

    // VARCHAR : character varying, varchar
    datatype.put(DataTypes::ID,
                 static_cast<ObjectIdType>(DataTypes::DataTypesId::VARCHAR));
    datatype.put(DataTypes::NAME, "VARCHAR");
    datatype.put(DataTypes::PG_DATA_TYPE, 1043);
    datatype.put(DataTypes::PG_DATA_TYPE_NAME, "varchar");
    datatype.put(DataTypes::PG_DATA_TYPE_QUALIFIED_NAME, "varchar");
    datatypes.push_back(std::make_pair("", datatype));
  }

  ptree* meta_object = session_manager_->get_container();
  meta_object->add_child(DataTypes::DATATYPES_NODE, datatypes);

  return ErrorCode::OK;
}

/**
 * @brief Get one data type metadata from the data types metadata table,
 *   where the given key equals the given value.
 * @param (object_key)    [in]  metadata-object key.
 * @param (object_value)  [in]  metadata-object value.
 * @param (object)        [out] metadata-object with the specified name.
 * @return ErrorCode::OK if success, otherwise an error code.
 */
ErrorCode DataTypesDAO::select_one_data_type_metadata(
    std::string_view object_key, std::string_view object_value,
    boost::property_tree::ptree& object) const {
  // Initialization of return value.
  ErrorCode error = ErrorCode::NOT_FOUND;

  ptree* meta_object = session_manager_->get_container();

  BOOST_FOREACH (const ptree::value_type& node,
                 meta_object->get_child(DataTypes::DATATYPES_NODE)) {
    const ptree& temp_obj = node.second;

    boost::optional<std::string> value =
        temp_obj.get_optional<std::string>(object_key.data());
    if (!value) {
      error = ErrorCode::INVALID_PARAMETER;
      break;
    }
    if (!value.get().compare(object_value)) {
      object = temp_obj;
      error = ErrorCode::OK;
      break;
    }
  }

  return error;
}

}  // namespace manager::metadata::db::json
