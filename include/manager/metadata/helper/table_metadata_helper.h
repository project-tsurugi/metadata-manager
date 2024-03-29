/*
 * Copyright 2021-2023 Project Tsurugi.
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
#ifndef MANAGER_METADATA_HELPER_TABLE_METADATA_HELPER_H_
#define MANAGER_METADATA_HELPER_TABLE_METADATA_HELPER_H_

#include <string>
#include <string_view>

#include <boost/property_tree/ptree.hpp>

namespace table_metadata_helper {

std::string get_table_acl(std::string_view user_name,
                          boost::property_tree::ptree acl_list);

}  // namespace table_metadata_helper

#endif  // MANAGER_METADATA_HELPER_TABLE_METADATA_HELPER_H_
