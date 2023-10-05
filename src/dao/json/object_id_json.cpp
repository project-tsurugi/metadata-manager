/*
 * Copyright 2021 Project Tsurugi.
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
#include "manager/metadata/dao/json/object_id_json.h"

#include <fstream>

#include <boost/format.hpp>
#include <boost/property_tree/ini_parser.hpp>

#include "manager/metadata/common/config.h"
#include "manager/metadata/common/message.h"
#include "manager/metadata/helper/logging_helper.h"

// =============================================================================
namespace manager::metadata::db {

namespace ini_parser = boost::property_tree::ini_parser;
using boost::property_tree::ini_parser_error;
using boost::property_tree::ptree;

static ObjectId INVALID_OID = 0;
static ObjectId OID_INITIAL_VALUE = 100001;

/**
 * @brief Contractor.
 */
ObjectIdGenerator::ObjectIdGenerator() {
  boost::format file_path = boost::format("%s/%s") %
                            Config::get_storage_dir_path() %
                            std::string(ObjectIdGenerator::FILE_NAME);
  oid_file_name_ = file_path.str();
}

/**
 * @brief initialize object-ID metadata-table.
 */
ErrorCode ObjectIdGenerator::init() {
  ErrorCode error = ErrorCode::UNKNOWN;

  std::ifstream file(oid_file_name_);
  try {
    if (!file) {
      // create oid-metadata-table.
      ptree root;
      ini_parser::write_ini(oid_file_name_, root);
    }
    error = ErrorCode::OK;
  } catch (ini_parser_error& e) {
    LOG_ERROR << Message::WRITE_INI_FILE_FAILURE << oid_file_name_ << "\n  "
              << e.what();
    error = ErrorCode::INTERNAL_ERROR;
  } catch (...) {
    LOG_ERROR << Message::WRITE_INI_FILE_FAILURE << oid_file_name_;
    error = ErrorCode::INTERNAL_ERROR;
  }

  return error;
}

/**
 * @brief current object-ID.
 * @param (table_name) [in]  OID table name.
 * @return Returns the current OID. Returns 0 if an error occurred.
 */
ObjectId ObjectIdGenerator::current(std::string_view metadata_name) {
  ErrorCode error = ErrorCode::UNKNOWN;

  error = init();
  if (error != ErrorCode::OK) {
    return INVALID_OID;
  }

  ptree oid_data;
  error = this->read(oid_data);
  if (error != ErrorCode::OK) {
    return INVALID_OID;
  }

  auto oid = oid_data.get_optional<ObjectId>(metadata_name.data());

  return oid.value_or(OID_INITIAL_VALUE);
}

/**
 * @brief generate new object-ID.
 * @param metadata_name [in]  OID table name.
 * @return Returns the generated OID. Returns 0 if an error occurred.
 */
ObjectId ObjectIdGenerator::generate(std::string_view metadata_name) {
  ErrorCode error = ErrorCode::UNKNOWN;

  error = init();
  if (error != ErrorCode::OK) {
    return INVALID_OID;
  }

  ptree oid_data;
  error = this->read(oid_data);
  if (error != ErrorCode::OK) {
    return INVALID_OID;
  }

  boost::optional<ObjectId> oid =
      oid_data.get_optional<ObjectId>(metadata_name.data());
  ObjectId object_id = oid.value_or(OID_INITIAL_VALUE);

  // Generate next OID.
  oid_data.put(metadata_name.data(), ++object_id);

  error = this->write(oid_data);
  if (error != ErrorCode::OK) {
    return INVALID_OID;
  }

  return object_id;
}

/**
 * @brief If greater than the current OID, the OID is updated.
 * @param metadata_name)  [in]  OID table name.
 * @param new_oid         [in]  new OID.
 * @return Returns the next OID. Returns 0 if an error occurred.
 */
ObjectId ObjectIdGenerator::update(std::string_view metadata_name,
                                   ObjectId new_oid) {
  ErrorCode error = ErrorCode::UNKNOWN;

  error = init();
  if (error != ErrorCode::OK) {
    return INVALID_OID;
  }

  ptree oid_data;
  error = this->read(oid_data);
  if (error != ErrorCode::OK) {
    return INVALID_OID;
  }

  boost::optional<ObjectId> oid =
      oid_data.get_optional<ObjectId>(metadata_name.data());
  ObjectId current_oid = oid.value_or(OID_INITIAL_VALUE);

  // If the specified OID exceeds the current OID,
  // the OID management file is updated.
  ObjectId oid_value = INVALID_OID;
  if (new_oid > current_oid) {
    oid_data.put(metadata_name.data(), new_oid);
    error     = this->write(oid_data);
    oid_value = (error == ErrorCode::OK ? new_oid : INVALID_OID);
  } else {
    oid_value = current_oid;
  }

  return oid_value;
}

/**
 * @brief Reads the OID management file.
 * @param oid_data [in]  OID management data.
 * @return ErrorCode if success, otherwise an error code.
 */
ErrorCode ObjectIdGenerator::read(boost::property_tree::ptree& oid_data) const {
  ErrorCode error = ErrorCode::UNKNOWN;

  try {
    ini_parser::read_ini(oid_file_name_, oid_data);
    error = ErrorCode::OK;
  } catch (ini_parser_error& e) {
    LOG_ERROR << Message::READ_INI_FILE_FAILURE << oid_file_name_ << "\n  "
              << e.what();
    error = ErrorCode::INTERNAL_ERROR;
  } catch (...) {
    LOG_ERROR << Message::READ_INI_FILE_FAILURE << oid_file_name_ << "\n  "
              << "Unknown exception";
    error = ErrorCode::INTERNAL_ERROR;
  }

  return error;
}

/**
 * @brief Writes to the OID management file.
 * @param oid_data [in]  OID management data.
 * @return ErrorCode if success, otherwise an error code.
 */
ErrorCode ObjectIdGenerator::write(
    const boost::property_tree::ptree& oid_data) const {
  ErrorCode error = ErrorCode::UNKNOWN;

  try {
    ini_parser::write_ini(oid_file_name_, oid_data);
    error = ErrorCode::OK;
  } catch (ini_parser_error& e) {
    LOG_ERROR << Message::WRITE_INI_FILE_FAILURE << oid_file_name_ << "\n  "
              << e.what();
    error = ErrorCode::INTERNAL_ERROR;
  } catch (...) {
    LOG_ERROR << Message::WRITE_INI_FILE_FAILURE << oid_file_name_ << "\n  "
              << "Unknown exception";
    error = ErrorCode::INTERNAL_ERROR;
  }

  return error;
}

}  // namespace manager::metadata::db
