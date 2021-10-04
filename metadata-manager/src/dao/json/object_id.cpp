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
#include "manager/metadata/dao/json/object_id.h"

#include <fstream>
#include <iostream>

#include <boost/format.hpp>
#include <boost/optional.hpp>
#include <boost/property_tree/ini_parser.hpp>
#include <boost/property_tree/ptree.hpp>

#include "manager/metadata/dao/common/config.h"

// =============================================================================
namespace {

std::string oid_file_name;

}  // namespace

// =============================================================================
namespace manager::metadata::db::json {

namespace ini_parser = boost::property_tree::ini_parser;
using boost::property_tree::ini_parser_error;
using boost::property_tree::ptree;
using manager::metadata::ErrorCode;

static ObjectIdType INVALID_OID = 0;

/**
 * @brief initialize object-ID metadata-table.
 */
ErrorCode ObjectId::init() {
  ErrorCode error = ErrorCode::UNKNOWN;

  // Filename of the table metadata.
  boost::format filename = boost::format("%s/%s") %
                           Config::get_storage_dir_path() %
                           std::string(ObjectId::OID_NAME);

  oid_file_name = filename.str();
  std::ifstream file(oid_file_name);

  try {
    if (!file) {
      // create oid-metadata-table.
      ptree root;
      ini_parser::write_ini(oid_file_name, root);
    }
    error = ErrorCode::OK;
  } catch (...) {
    error = ErrorCode::INTERNAL_ERROR;
  }

  return error;
}

/**
 * @brief current object-ID.
 * @return ErrorCode::OK if success, otherwise an error code.
 */
ObjectIdType ObjectId::current(const std::string table_name) {
  if (ObjectId::init() != ErrorCode::OK) {
    return INVALID_OID;
  }

  ptree pt;
  try {
    ini_parser::read_ini(oid_file_name, pt);
  } catch (ini_parser_error& e) {
    std::wcout << "read_ini() error. " << e.what() << std::endl;
    return INVALID_OID;
  } catch (...) {
    std::cout << "read_ini() error." << std::endl;
    return INVALID_OID;
  }

  boost::optional<ObjectIdType> oid = pt.get_optional<ObjectIdType>(table_name);
  if (!oid) {
    // create OID key for specified metadata.
    pt.put(table_name, 0);
    oid = pt.get_optional<ObjectIdType>(table_name);
  }

  return oid.get();
}

/**
 * @brief generate new object-ID.
 * @return ErrorCode::OK if success, otherwise an error code.
 */
ObjectIdType ObjectId::generate(const std::string table_name) {
  if (ObjectId::init() != ErrorCode::OK) {
    return INVALID_OID;
  }

  ptree pt;
  try {
    ini_parser::read_ini(oid_file_name, pt);
  } catch (ini_parser_error& e) {
    std::wcout << "read_ini() error. " << e.what() << std::endl;
    return INVALID_OID;
  } catch (...) {
    std::cout << "read_ini() error." << std::endl;
    return INVALID_OID;
  }

  boost::optional<ObjectIdType> oid = pt.get_optional<ObjectIdType>(table_name);
  if (!oid) {
    // create OID key for specified metadata.
    pt.put(table_name, 0);
    oid = pt.get_optional<ObjectIdType>(table_name);
  }

  // generate new OID
  pt.put(table_name, ++oid.get());

  try {
    ini_parser::write_ini(oid_file_name, pt);
  } catch (ini_parser_error& e) {
    std::wcout << "write_ini() error. " << e.what() << std::endl;
    return INVALID_OID;
  } catch (...) {
    std::cout << "read_ini() error." << std::endl;
    return INVALID_OID;
  }

  return oid.get();
}

}  // namespace manager::metadata::db::json
