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
#include "manager/metadata/helper/table_metadata_helper.h"

#include <iostream>
#include <regex>

#include <boost/foreach.hpp>

namespace manager::metadata::helper {

using boost::property_tree::ptree;

/**
 * @brief Extracts and returns ACL information for a role name
 *   from the ACL list.
 * @param (role_name)  [in]  role name.
 * @param (acl_list)   [in]  ACL list.
 * @return ACLs string.
 */
std::string TableMetadataHelper::get_table_acl(std::string_view role_name,
                                               ptree acl_list) {
  std::string result_acl = "";

  BOOST_FOREACH (const auto& node, acl_list) {
    std::string acl_data =
        std::regex_replace(node.second.data(), std::regex(R"((^"|"$))"), "");

    std::smatch regex_results;
    std::regex_match(
        acl_data, regex_results,
        std::regex(R"((\\".+\\"|[^\\"]*)=([arwdDxt]+)(/(\\".+\\"|.+)|$))"));
    if (regex_results.empty()) {
      // Skip because the ACL format is invalid.
      continue;
    }
    std::string acl_user_name = regex_results[1].str();
    std::string acl_permission = regex_results[2].str();

    // Conversion of record value (\\ -> \).
    acl_user_name =
        std::regex_replace(acl_user_name, std::regex(R"(\\\\)"), R"(\)");
    // Conversion of record value (\" or \"\" -> ").
    acl_user_name =
        std::regex_replace(acl_user_name, std::regex(R"((\\"){1,2})"), R"(")");
    // Conversion of record value (^" or "$ -> ).
    acl_user_name =
        std::regex_replace(acl_user_name, std::regex(R"((^"|"$))"), "");
    if ((acl_user_name.empty()) ||
        (std::regex_match(acl_user_name, std::regex(std::string(role_name))))) {
      result_acl = acl_permission;
      break;
    }
  }

  return result_acl;
}

}  // namespace manager::metadata::helper