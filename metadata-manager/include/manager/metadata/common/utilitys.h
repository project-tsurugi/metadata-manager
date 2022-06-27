/*
 * Copyright 2020-2021 tsurugi project.
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
#ifndef MANAGER_METADATA_MANAGER_INCLUDE_MANAGER_METADATA_COMMON_UTILITYS_H_
#define MANAGER_METADATA_MANAGER_INCLUDE_MANAGER_METADATA_COMMON_UTILITYS_H_

#include <boost/property_tree/ptree.hpp>
#include <string>
#include <string_view>

#include "manager/metadata/error_code.h"

namespace manager::metadata {

class Utilitys {
 public:
  template <typename T>
  static manager::metadata::ErrorCode str_to_numeric(std::string_view str,
                                                     T& return_value);

  static manager::metadata::ErrorCode json_to_ptree(
      std::string_view json, boost::property_tree::ptree& ptree);
  static manager::metadata::ErrorCode ptree_to_json(
      const boost::property_tree::ptree& ptree, std::string& json);

 private:
  template <typename T>
  [[nodiscard]] static T convert_to_numeric(std::string_view str);
};  // class Utilitys

}  // namespace manager::metadata

#endif  // MANAGER_METADATA_MANAGER_INCLUDE_MANAGER_METADATA_COMMON_UTILITYS_H_
