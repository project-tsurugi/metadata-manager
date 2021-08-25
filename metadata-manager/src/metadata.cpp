/*
 * Copyright 2020 tsurugi project.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
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
#include "manager/metadata/metadata.h"

#include <memory>

namespace manager::metadata {

/**
 *  @brief  Read latest table-metadata from metadata-table.
 *  @return ErrorCode::OK if success, otherwise an error code.
 */
[[deprecated("don't use load() function.")]] ErrorCode Metadata::load() {
  return ErrorCode::OK;
}

/**
 *  @brief  Load metadata from metadata-table.
 *  @param  (database)   [in]  database name
 *  @param  (pt)         [out] property_tree object to populating metadata.
 *  @param  (generation) [in]  metadata generation to load. load latest
 * generation if NOT provided.
 *  @return ErrorCode::OK if success, otherwise an error code.
 */
[[deprecated("don't use load() function.")]] ErrorCode Metadata::load(
    std::string_view database, boost::property_tree::ptree& object,
    const GenerationType generation) {
  return ErrorCode::OK;
}

/**
 *  @brief  Converts error codes according to the specified conditions.
 *  @param  (error_code)  [in] error code.
 *  @param  (key_name)    [in] key.
 *  @return  Converted error code.
 */
ErrorCode Metadata::code_converter(const ErrorCode& error_code, std::string_view key_name) {
  ErrorCode result_code = error_code;
  // Find of error code.
  auto find_error_code = code_convert_list_.find(error_code);
  if (find_error_code != code_convert_list_.end()) {
    // Find of key.
    auto find_convert_code = find_error_code->second.find(key_name.data());
    if (find_convert_code != find_error_code->second.end()) {
      result_code = find_convert_code->second;
    }
  }
  return result_code;
}

}  // namespace manager::metadata
