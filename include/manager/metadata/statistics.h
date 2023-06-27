/*
 * Copyright 2020-2023 tsurugi project.
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
#ifndef MANAGER_METADATA_STATISTICS_H_
#define MANAGER_METADATA_STATISTICS_H_

#include <string_view>
#include <vector>

#include <boost/property_tree/ptree.hpp>

#include "manager/metadata/error_code.h"
#include "manager/metadata/metadata.h"
#include "manager/metadata/statistic.h"

namespace manager::metadata {

class Statistics : public Metadata {
 public:
  explicit Statistics(std::string_view database)
      : Statistics(database, kDefaultComponent) {}
  Statistics(std::string_view database, std::string_view component);

  Statistics(const Statistics&)            = delete;
  Statistics& operator=(const Statistics&) = delete;

  ErrorCode init() const override;

  ErrorCode add(const boost::property_tree::ptree& object) const override;
  ErrorCode add(const boost::property_tree::ptree& object,
                ObjectIdType* object_id) const override;

  ErrorCode get(const ObjectIdType object_id,
                boost::property_tree::ptree& object) const override;
  ErrorCode get(std::string_view object_name,
                boost::property_tree::ptree& object) const override;
  ErrorCode get_by_column_id(const ObjectIdType column_id,
                             boost::property_tree::ptree& object) const;
  ErrorCode get_by_column_number(const ObjectIdType table_id,
                                 const int64_t ordinal_position,
                                 boost::property_tree::ptree& object) const;
  ErrorCode get_by_column_name(const ObjectIdType table_id,
                               std::string_view column_name,
                               boost::property_tree::ptree& object) const;

  ErrorCode get_all(
      std::vector<boost::property_tree::ptree>& container) const override;
  ErrorCode get_all(const ObjectIdType table_id,
                    std::vector<boost::property_tree::ptree>& container) const;

  ErrorCode update([[maybe_unused]] const ObjectIdType object_id,
                   [[maybe_unused]] const boost::property_tree::ptree& object)
      const override {
    return ErrorCode::UNKNOWN;
  }

  ErrorCode remove(const ObjectIdType object_id) const override;
  ErrorCode remove(std::string_view object_name,
                   ObjectIdType* object_id) const override;
  ErrorCode remove_by_table_id(const ObjectIdType table_id) const;
  ErrorCode remove_by_column_id(const ObjectIdType column_id) const;
  ErrorCode remove_by_column_number(const ObjectIdType table_id,
                                    const int64_t ordinal_position) const;
  ErrorCode remove_by_column_name(const ObjectIdType table_id,
                                  std::string_view column_name) const;

 private:
  ErrorCode param_check_statistics_add(
      const boost::property_tree::ptree& object) const;
};  // class Statistics

}  // namespace manager::metadata

#endif  // MANAGER_METADATA_STATISTICS_H_
