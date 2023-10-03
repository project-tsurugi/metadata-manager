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
#include "test/helper/json/metadata_helper_json.h"

#include <fstream>

#include <boost/foreach.hpp>
#include <boost/format.hpp>
#include <boost/property_tree/ptree.hpp>
#define BOOST_BIND_GLOBAL_PLACEHOLDERS
#include <boost/property_tree/json_parser.hpp>

#include "manager/metadata/common/config.h"

namespace manager::metadata::testing {

using boost::property_tree::ptree;

/**
 * @brief Get the number of records in the current constraint metadata.
 * @return int32_t - current number of records.
 */
int64_t MetadataHelperJson::get_record_count() const {
  int64_t record_count = 0;

  // load root metadata.
  auto metadata = load_contents();

  // root
  auto root_node = metadata.get_child_optional(this->root_node_name_.data());
  if (root_node) {
    if (this->sub_node_name_.empty()) {
      record_count = root_node.get().size();
    } else {
      BOOST_FOREACH (const auto& child_node, root_node.get()) {
        auto child_contents =
            child_node.second.get_child_optional(this->sub_node_name_.data());
        record_count += (child_contents ? child_contents.get().size() : 0);
      }
    }
  }

  return record_count;
}

/**
 * @brief Load root metadata from a metadata file.
 * @return boost::property_tree::ptree - metadata
 */
boost::property_tree::ptree MetadataHelperJson::load_contents() const {
  // Filename of the constraint metadata.
  boost::format filename = boost::format("%s/%s.json") %
                           Config::get_storage_dir_path() %
                           this->metadata_name_;

  ptree contents;

  std::ifstream file_stream(filename.str());
  if (file_stream) {
    try {
      boost::property_tree::json_parser::read_json(file_stream, contents);
      file_stream.close();
    } catch (...) {
    }
  }
  return contents;
}

}  // namespace manager::metadata::testing
