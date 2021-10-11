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
#ifndef MANAGER_METADATA_ROLES_H_
#define MANAGER_METADATA_ROLES_H_

#include "manager/metadata/metadata.h"

namespace manager::metadata {

class Roles : public Metadata {
 public:
  // role metadata-object.
  // FORMAT_VERSION is defined in base class.
  // GENERATION is defined in base class.
  static constexpr const char* const ROLE_OID = Metadata::ID;
  static constexpr const char* const ROLE_ROLNAME = Metadata::NAME;
  static constexpr const char* const ROLE_ROLSUPER = "super";
  static constexpr const char* const ROLE_ROLINHERIT = "inherit";
  static constexpr const char* const ROLE_ROLCREATEROLE = "createrole";
  static constexpr const char* const ROLE_ROLCREATEDB = "createdb";
  static constexpr const char* const ROLE_ROLCANLOGIN = "canlogin";
  static constexpr const char* const ROLE_ROLREPLICATION = "replication";
  static constexpr const char* const ROLE_ROLBYPASSRLS = "bypassrls";
  static constexpr const char* const ROLE_ROLCONNLIMIT = "connlimit";
  static constexpr const char* const ROLE_ROLPASSWORD = "password";
  static constexpr const char* const ROLE_ROLVALIDUNTIL = "validuntil";

  Roles(std::string_view database, std::string_view component = "visitor");

  Roles(const Roles&) = delete;
  Roles& operator=(const Roles&) = delete;

  ErrorCode init() const override;

  ErrorCode add(boost::property_tree::ptree& object
                __attribute__((unused))) const override {
    return ErrorCode::UNKNOWN;
  }
  ErrorCode add(boost::property_tree::ptree& object __attribute__((unused)),
                ObjectIdType* object_id
                __attribute__((unused))) const override {
    return ErrorCode::UNKNOWN;
  }

  ErrorCode get(const ObjectIdType object_id,
                boost::property_tree::ptree& object) const override;
  ErrorCode get(std::string_view object_name,
                boost::property_tree::ptree& object) const override;
  ErrorCode get_all(std::vector<boost::property_tree::ptree>& container
                    __attribute__((unused))) const override {
    return ErrorCode::UNKNOWN;
  }

  ErrorCode remove(const ObjectIdType object_id
                   __attribute__((unused))) const override {
    return ErrorCode::UNKNOWN;
  }
  ErrorCode remove(std::string_view object_name __attribute__((unused)),
                   ObjectIdType* object_id
                   __attribute__((unused))) const override {
    return ErrorCode::UNKNOWN;
  }

};  // class Roles

}  // namespace manager::metadata

#endif  // MANAGER_METADATA_ROLES_H_
