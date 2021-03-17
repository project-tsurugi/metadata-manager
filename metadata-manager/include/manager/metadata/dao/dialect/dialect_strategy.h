/*
 * Copyright 2020 tsurugi project.
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

#ifndef DIALECT_STRATEGY_H_
#define DIALECT_STRATEGY_H_

#include "manager/metadata/dao/dialect/dialect.h"

namespace manager::metadata::db {

class DialectStrategy {
   public:
    static Dialect *get_instance();

   private:
    static Dialect *instance;
};
}  // namespace manager::metadata::db

#endif  // DIALECT_STRATEGY_H_
