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
#include "manager/metadata/role.h"

// =============================================================================
namespace manager::metadata {

boost::property_tree::ptree Role::convert_to_ptree() const {
  auto pt = Object::convert_to_ptree();

  // super
  pt.put(ROLE_ROLSUPER, this->super);
  // inherit
  pt.put(ROLE_ROLINHERIT, this->inherit);
  // createrole
  pt.put(ROLE_ROLCREATEROLE, this->createrole);
  // createdb
  pt.put(ROLE_ROLCREATEDB, this->createdb);
  // canlogin
  pt.put(ROLE_ROLCANLOGIN, this->canlogin);
  // replication
  pt.put(ROLE_ROLREPLICATION, this->replication);
  // bypassrls
  pt.put(ROLE_ROLBYPASSRLS, this->bypassrls);
  // connlimit
  pt.put(ROLE_ROLCONNLIMIT, this->connlimit);
  // password
  pt.put(ROLE_ROLPASSWORD, this->password);
  // validuntil
  pt.put(ROLE_ROLVALIDUNTIL, this->validuntil);

  return pt;
}

void Role::convert_from_ptree(const boost::property_tree::ptree& pt) {
  Object::convert_from_ptree(pt);

  // super
  this->super = pt.get_optional<bool>(ROLE_ROLSUPER).get_value_or(false);
  // inherit
  this->inherit = pt.get_optional<bool>(ROLE_ROLINHERIT).get_value_or(false);
  // createrole
  this->createrole =
      pt.get_optional<bool>(ROLE_ROLCREATEROLE).get_value_or(false);
  // createdb
  this->createdb = pt.get_optional<bool>(ROLE_ROLCREATEDB).get_value_or(false);
  // canlogin
  this->canlogin = pt.get_optional<bool>(ROLE_ROLCANLOGIN).get_value_or(false);
  // replication
  this->replication =
      pt.get_optional<bool>(ROLE_ROLREPLICATION).get_value_or(false);
  // bypassrls
  this->bypassrls =
      pt.get_optional<bool>(ROLE_ROLBYPASSRLS).get_value_or(false);
  // connlimit
  this->connlimit =
      pt.get_optional<int32_t>(ROLE_ROLCONNLIMIT).get_value_or(INVALID_VALUE);
  // password
  this->password =
      pt.get_optional<std::string>(ROLE_ROLPASSWORD).get_value_or("");
  // validuntil
  this->validuntil =
      pt.get_optional<std::string>(ROLE_ROLVALIDUNTIL).get_value_or("");
}

}  // namespace manager::metadata
