/*
 * Copyright 2021 tsurugi project.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
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
#ifndef MANAGER_AUTHENTICATION_MANAGER_INCLUDE_MANAGER_AUTHENTICATION_PROVIDER_AUTHENTICATION_PROVIDER_H_
#define MANAGER_AUTHENTICATION_MANAGER_INCLUDE_MANAGER_AUTHENTICATION_PROVIDER_AUTHENTICATION_PROVIDER_H_

#include <boost/property_tree/ptree.hpp>

#include "manager/authentication/error_code.h"

namespace manager::authentication::db {

class AuthenticationProvider {
 public:
  static manager::authentication::ErrorCode auth_user(
      const boost::property_tree::ptree& params);
  static manager::authentication::ErrorCode auth_user(
      std::string_view conninfo);
};  // class RolesProvider

}  // namespace manager::authentication::db

#endif  // MANAGER_AUTHENTICATION_MANAGER_INCLUDE_MANAGER_AUTHENTICATION_PROVIDER_AUTHENTICATION_PROVIDER_H_
