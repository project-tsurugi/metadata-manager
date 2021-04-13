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

#ifndef DBC_UTILS_H_
#define DBC_UTILS_H_

extern "C" {
typedef struct pg_conn PGconn;
typedef struct pg_result PGresult;
}

#include <functional>
#include <memory>
#include <string>

#include "manager/metadata/error_code.h"

namespace manager::metadata::db {

typedef std::shared_ptr<PGconn> ConnectionSPtr;
typedef std::unique_ptr<PGresult, std::function<void(PGresult *)>> ResultUPtr;

class DbcUtils {
   public:
    static bool is_open(const ConnectionSPtr &connection);
    static std::string convert_boolean_expression(const char *string);

    template <typename T>
    static manager::metadata::ErrorCode str_to_floating_point(const char *input,
                                                              T &return_value);
    template <typename T>
    static manager::metadata::ErrorCode str_to_integral(const char *input,
                                                        T &return_value);

    static manager::metadata::ErrorCode get_number_of_rows_affected(
        PGresult *&res, uint64_t &return_value);

    static ConnectionSPtr make_connection_sptr(PGconn *pgconn);
    static ResultUPtr make_result_uptr(PGresult *pgres);

   private:
    static constexpr int BASE_10 = 10;

    template <typename T>
    [[nodiscard]] static T call_floating_point(const char *nptr, char **endptr);

    template <typename T>
    [[nodiscard]] static T call_integral(const char *nptr, char **endptr,
                                         int base);
};

}  // namespace manager::metadata::db

#endif  // DBC_UTILS_H_
