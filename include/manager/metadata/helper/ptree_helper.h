/*
 * Copyright 2022 tsurugi project.
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
#pragma once

#include <string_view>
#include <vector>

#include <boost/property_tree/ptree.hpp>

namespace ptree_helper {

boost::property_tree::ptree make_array_ptree(const std::vector<int64_t>& v);
std::vector<int64_t> make_vector_int(const boost::property_tree::ptree& pt,
                                     std::string_view key);

}  // namespace ptree_helper
