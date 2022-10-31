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
#include "test/helper/json/constraint_metadata_helper_json.h"

#include <fstream>

#include <boost/foreach.hpp>
#include <boost/format.hpp>
#include <boost/property_tree/json_parser.hpp>
#include <boost/property_tree/ptree.hpp>

#include "manager/metadata/common/config.h"
#include "manager/metadata/dao/json/constraints_dao_json.h"

namespace {

static constexpr const char* const kConstraintsMetadataName = "tables";
static constexpr const char* const kRootNode                = "tables";
static constexpr const char* const kConstraintsNode         = "constraints";

}  // namespace

namespace manager::metadata::testing {

using boost::property_tree::ptree;

/**
 * @brief Get the number of records in the current constraint metadata.
 * @return Current number of records.
 */
int32_t ConstraintMetadataHelperJson::get_record_count() const {
  int32_t record_count = 0;

  // load constraint metadata.
  auto metadata = load_contents();

  // constraint metadata
  BOOST_FOREACH (const auto& root_node, metadata.get_child(kRootNode)) {
    auto& tables = root_node.second;

    auto constraints = tables.get_child_optional(kConstraintsNode);
    record_count += (constraints ? constraints.get().size() : 0);
  }

  return record_count;
}

/**
 * @brief Load root metadata from a metadata file.
 * @return boost::property_tree::ptree - metadata
 */
boost::property_tree::ptree ConstraintMetadataHelperJson::load_contents()
    const {
  // Filename of the constraint metadata.
  boost::format filename = boost::format("%s/%s.json") %
                           Config::get_storage_dir_path() %
                           std::string(kConstraintsMetadataName);

  ptree contents;

  std::ifstream file_stream(filename.str());
  if (file_stream) {
    try {
      boost::property_tree::json_parser::read_json(file_stream, contents);
    } catch (...) {
    }
  }
  return contents;
}

}  // namespace manager::metadata::testing
