/*
 * Copyright 2022 Project Tsurugi.
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
#ifndef TEST_INCLUDE_TEST_HELPER_JSON_METADATA_HELPER_JSON_H_
#define TEST_INCLUDE_TEST_HELPER_JSON_METADATA_HELPER_JSON_H_

#include <string>
#include <string_view>

#include <boost/property_tree/ptree.hpp>

#include "test/helper/metadata_helper.h"

namespace manager::metadata::testing {

class MetadataHelperJson : public MetadataHelper {
 public:
  /**
   * @param metadata_name  metadata name (file name without extension).
   * @param root_node_name root node name of the tree.
   */
  MetadataHelperJson(std::string_view metadata_name,
                     std::string_view root_node_name)
      : metadata_name_(metadata_name),
        root_node_name_(root_node_name),
        sub_node_name_("") {}
  /**
   * @param metadata_name  metadata name (file name without extension).
   * @param root_node_name root node name of the tree.
   * @param sub_node_name  sub node name of the tree.
   */
  MetadataHelperJson(std::string_view metadata_name,
                     std::string_view root_node_name,
                     std::string_view sub_node_name)
      : metadata_name_(metadata_name),
        root_node_name_(root_node_name),
        sub_node_name_(sub_node_name) {}
  MetadataHelperJson() = delete;

  int64_t get_record_count() const override;

 private:
  std::string metadata_name_;
  std::string root_node_name_;
  std::string sub_node_name_;

  boost::property_tree::ptree load_contents() const;
};

}  // namespace manager::metadata::testing

#endif  // TEST_INCLUDE_TEST_HELPER_JSON_METADATA_HELPER_JSON_H_
