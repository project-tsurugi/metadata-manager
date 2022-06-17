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
#ifndef MANAGER_METADATA_MANAGER_INCLUDE_MANAGER_METADATA_ROLES_H_
#define MANAGER_METADATA_MANAGER_INCLUDE_MANAGER_METADATA_ROLES_H_

#include <boost/property_tree/ptree.hpp>
#include <string_view>
#include <vector>

#include "manager/metadata/error_code.h"
#include "manager/metadata/metadata.h"

namespace manager::metadata {

class Roles : public Metadata {
 public:
  // role metadata-object.
  // FORMAT_VERSION is defined in base class.
  // GENERATION is defined in base class.

  /**
   * @brief Field name constant indicating the role id of the metadata.
   */
  static constexpr const char* const ROLE_OID = Metadata::ID;
  /**
   * @brief Field name constant indicating the role name of the metadata.
   */
  static constexpr const char* const ROLE_ROLNAME = Metadata::NAME;
  /**
   * @brief Field name constant indicating the super of the metadata.
   */
  static constexpr const char* const ROLE_ROLSUPER = "super";
  /**
   * @brief Field name constant indicating the inherit of the metadata.
   */
  static constexpr const char* const ROLE_ROLINHERIT = "inherit";
  /**
   * @brief Field name constant indicating the createrole of the metadata.
   */
  static constexpr const char* const ROLE_ROLCREATEROLE = "createrole";
  /**
   * @brief Field name constant indicating the createdb of the metadata.
   */
  static constexpr const char* const ROLE_ROLCREATEDB = "createdb";
  /**
   * @brief Field name constant indicating the canlogin of the metadata.
   */
  static constexpr const char* const ROLE_ROLCANLOGIN = "canlogin";
  /**
   * @brief Field name constant indicating the replication of the metadata.
   */
  static constexpr const char* const ROLE_ROLREPLICATION = "replication";
  /**
   * @brief Field name constant indicating the bypassrls of the metadata.
   */
  static constexpr const char* const ROLE_ROLBYPASSRLS = "bypassrls";
  /**
   * @brief Field name constant indicating the connlimit of the metadata.
   */
  static constexpr const char* const ROLE_ROLCONNLIMIT = "connlimit";
  /**
   * @brief Field name constant indicating the password of the metadata.
   */
  static constexpr const char* const ROLE_ROLPASSWORD = "password";
  /**
   * @brief Field name constant indicating the validuntil of the metadata.
   */
  static constexpr const char* const ROLE_ROLVALIDUNTIL = "validuntil";

  explicit Roles(std::string_view database)
      : Roles(database, kDefaultComponent) {}
  Roles(std::string_view database, std::string_view component);

  Roles(const Roles&) = delete;
  Roles& operator=(const Roles&) = delete;

  ErrorCode init() const override;

  ErrorCode add([[maybe_unused]] const boost::property_tree::ptree& object)
      const override {
    return ErrorCode::UNKNOWN;
  }
  ErrorCode add([[maybe_unused]]const boost::property_tree::ptree& object,
                [[maybe_unused]]ObjectIdType* object_id) const override {
    return ErrorCode::UNKNOWN;
  }

  ErrorCode get(const ObjectIdType object_id,
                boost::property_tree::ptree& object) const override;
  ErrorCode get(std::string_view object_name,
                boost::property_tree::ptree& object) const override;
  ErrorCode get_all([[maybe_unused]] std::vector<boost::property_tree::ptree>&
                        container) const override {
    return ErrorCode::UNKNOWN;
  }

  ErrorCode remove(
      [[maybe_unused]] const ObjectIdType object_id) const override {
    return ErrorCode::UNKNOWN;
  }
  ErrorCode remove([[maybe_unused]] std::string_view object_name,
                   [[maybe_unused]] ObjectIdType* object_id) const override {
    return ErrorCode::UNKNOWN;
  }
};  // class Roles

}  // namespace manager::metadata

#endif  // MANAGER_METADATA_MANAGER_INCLUDE_MANAGER_METADATA_ROLES_H_
