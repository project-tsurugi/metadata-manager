/*
 * Copyright 2021-2023 tsurugi project.
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
#ifndef MANAGER_METADATA_ROLES_H_
#define MANAGER_METADATA_ROLES_H_

#include <string_view>
#include <vector>

#include <boost/property_tree/ptree.hpp>

#include "manager/metadata/metadata.h"
#include "manager/metadata/role.h"

namespace manager::metadata {

class Roles : public Metadata {
 public:
  /**
   * @brief Field name constant indicating the role id of the metadata.
   * @deprecated Deprecated in the future. Please use Role::ROLE_OID.
   */
  static constexpr const char* const ROLE_OID = Role::ROLE_OID;
  /**
   * @brief Field name constant indicating the role name of the metadata.
   * @deprecated Deprecated in the future. Please use Role::ROLE_ROLNAME.
   */
  static constexpr const char* const ROLE_ROLNAME = Role::ROLE_ROLNAME;
  /**
   * @brief Field name constant indicating the super of the metadata.
   * @deprecated Deprecated in the future. Please use Role::ROLE_ROLSUPER.
   */
  static constexpr const char* const ROLE_ROLSUPER = Role::ROLE_ROLSUPER;
  /**
   * @brief Field name constant indicating the inherit of the metadata.
   * @deprecated Deprecated in the future. Please use Role::ROLE_ROLINHERIT.
   */
  static constexpr const char* const ROLE_ROLINHERIT = Role::ROLE_ROLINHERIT;
  /**
   * @brief Field name constant indicating the createrole of the metadata.
   * @deprecated Deprecated in the future. Please use Role::ROLE_ROLCREATEROLE.
   */
  static constexpr const char* const ROLE_ROLCREATEROLE =
      Role::ROLE_ROLCREATEROLE;
  /**
   * @brief Field name constant indicating the createdb of the metadata.
   * @deprecated Deprecated in the future. Please use Role::ROLE_ROLCREATEDB.
   */
  static constexpr const char* const ROLE_ROLCREATEDB = Role::ROLE_ROLCREATEDB;
  /**
   * @brief Field name constant indicating the canlogin of the metadata.
   * @deprecated Deprecated in the future. Please use Role::ROLE_ROLCANLOGIN.
   */
  static constexpr const char* const ROLE_ROLCANLOGIN = Role::ROLE_ROLCANLOGIN;
  /**
   * @brief Field name constant indicating the replication of the metadata.
   * @deprecated Deprecated in the future. Please use Role::ROLE_ROLREPLICATION.
   */
  static constexpr const char* const ROLE_ROLREPLICATION =
      Role::ROLE_ROLREPLICATION;
  /**
   * @brief Field name constant indicating the bypassrls of the metadata.
   * @deprecated Deprecated in the future. Please use Role::ROLE_ROLBYPASSRLS.
   */
  static constexpr const char* const ROLE_ROLBYPASSRLS =
      Role::ROLE_ROLBYPASSRLS;
  /**
   * @brief Field name constant indicating the connlimit of the metadata.
   * @deprecated Deprecated in the future. Please use Role::ROLE_ROLCONNLIMIT.
   */
  static constexpr const char* const ROLE_ROLCONNLIMIT =
      Role::ROLE_ROLCONNLIMIT;
  /**
   * @brief Field name constant indicating the password of the metadata.
   * @deprecated Deprecated in the future. Please use Role::ROLE_ROLPASSWORD.
   */
  static constexpr const char* const ROLE_ROLPASSWORD = Role::ROLE_ROLPASSWORD;
  /**
   * @brief Field name constant indicating the validuntil of the metadata.
   * @deprecated Deprecated in the future. Please use Role::ROLE_ROLVALIDUNTIL.
   */
  static constexpr const char* const ROLE_ROLVALIDUNTIL =
      Role::ROLE_ROLVALIDUNTIL;

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

  ErrorCode update([[maybe_unused]] const ObjectIdType object_id,
                   [[maybe_unused]] const boost::property_tree::ptree& object) const override {
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

#endif  // MANAGER_METADATA_ROLES_H_
