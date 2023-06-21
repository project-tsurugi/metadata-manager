/*
 * Copyright 2023 tsurugi project.
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
#ifndef MANAGER_METADATA_ROLE_H_
#define MANAGER_METADATA_ROLE_H_

#include <string>

#include <boost/property_tree/ptree.hpp>

#include "manager/metadata/object.h"

namespace manager::metadata {

/**
 * @brief Role metadata object.
 */
class Role : public Object {
 public:
  /**
   * @brief Field name constant indicating the role id of the metadata.
   */
  static constexpr const char* const ROLE_OID = Object::ID;
  /**
   * @brief Field name constant indicating the role name of the metadata.
   */
  static constexpr const char* const ROLE_ROLNAME = Object::NAME;
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

  /**
   * @brief Super user.
   */
  bool super;
  /**
   * @brief Inherit.
   */
  bool inherit;
  /**
   * @brief Create role.
   */
  bool createrole;
  /**
   * @brief Create DB.
   */
  bool createdb;
  /**
   * @brief Can login.
   */
  bool canlogin;
  /**
   * @brief Replication.
   */
  bool replication;
  /**
   * @brief Bypass RLS.
   */
  bool bypassrls;
  /**
   * @brief Connection limit.
   */
  int32_t connlimit;
  /**
   * @brief Password.
   */
  std::string password;
  /**
   * @brief Valid until.
   */
  std::string validuntil;

  Role()
      : Object(),
        super(false),
        inherit(false),
        createrole(false),
        createdb(false),
        canlogin(false),
        replication(false),
        bypassrls(false),
        connlimit(INVALID_VALUE) {}

  /**
   * @brief Transform role metadata from structure object to ptree object.
   * @return ptree object.
   */
  boost::property_tree::ptree convert_to_ptree() const override;

  /**
   * @brief Transform role metadata from ptree object to structure object.
   * @param pt  [in]  ptree object of metadata.
   * @return structure object of metadata.
   */
  void convert_from_ptree(const boost::property_tree::ptree& pt) override;
};  // class Role

}  // namespace manager::metadata

#endif  // MANAGER_METADATA_ROLE_H_
