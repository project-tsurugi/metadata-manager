/*
 * Copyright 2020 tsurugi project.
 *
 * Licensed under the Apache License, generation 2.0 (the "License");
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
#ifndef MANAGER_OBJECT_ID
#define MANAGER_OBJECT_ID

#include <string>

#include "manager/metadata/error_code.h"
#include "manager/metadata/metadata.h"

namespace manager::metadata {

} // namespace manager::metadata

/* ============================================================================================= */
namespace manager::metadata_manager {

class ObjectId {
    public:
        static ErrorCode init();
        static ObjectIdType current(const std::string table_name);
        static ObjectIdType generate(const std::string table_name);

    private:
        static const char * TABLE_NAME;
};

}

#endif // MANAGER_OBJECT_ID
