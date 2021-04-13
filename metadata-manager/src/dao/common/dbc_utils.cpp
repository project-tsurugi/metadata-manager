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

#include "manager/metadata/dao/common/dbc_utils.h"

#include <cassert>
#include <cerrno>
#include <cstdlib>
#include <cstring>
#include <iostream>
#include <string>

#include <libpq-fe.h>

#include "manager/metadata/dao/common/message.h"

namespace manager::metadata::db {

/**
 * @brief Is this connection open?
 * @param  (connection)   [in]  a connection.
 * @return true if this connection is open, otherwise false.
 */
bool DbcUtils::is_open(const ConnectionSPtr &connection) {
    return PQstatus(connection.get()) == CONNECTION_OK;
}

/**
 * @brief Converts boolean expression ("t" or "f") in metadata repository
 * to "true" or "false" in application.
 * @param  (string)   [in]  "t" , "f" or an other string.
 * @return "true" if "t" is input,
 * "false" if "f" is input,
 * otherwise an empty string.
 */
std::string DbcUtils::convert_boolean_expression(const char *string) {
    if (!string) {
        return "";
    }

    if (strcmp(string, "t") == 0) {
        return "true";
    } else if (strcmp(string, "f") == 0) {
        return "false";
    } else {
        return "";
    }
}

/**
 * @brief call standard library function to convert string to float.
 * @param  (nptr)    [in]  C-string beginning with the representation of a
 * floating-point number.
 * @param  (endptr)   [in]  Reference to an already allocated object of type
 * char*, whose value is set by the function to the next character in nptr
 * after the numerical value.
 * @return the converted floating point number as a value of type float.
 */
template <>
[[nodiscard]] float DbcUtils::call_floating_point<float>(const char *nptr,
                                                         char **endptr) {
    return std::strtof(nptr, endptr);
}

/**
 * @brief Explicit Template Instantiation for str_to_floating_point(float type).
 */
template ErrorCode DbcUtils::str_to_floating_point(const char *input,
                                                   float &return_value);

/**
 * @brief Convert string to floating point.
 * @param  (input)        [in]   C-string beginning with the representation of a
 * floating-point number.
 * @param  (return_value) [out]  the converted floating point number.
 * @return ErrorCode::OK if success, otherwise an error code.
 */
template <typename T>
[[nodiscard]] ErrorCode DbcUtils::str_to_floating_point(const char *input,
                                                        T &return_value) {
    if (!input || *input == '\0' || std::isspace(*input)) {
        return ErrorCode::INTERNAL_ERROR;
    }
    char *end;
    errno = 0;

    const T result = call_floating_point<T>(input, &end);
    if (errno == 0 && *end == '\0') {
        return_value = result;
        return ErrorCode::OK;
    }
    std::cerr << Message::CONVERT_STRING_TO_FLOAT_FAILURE << std::endl;
    return ErrorCode::INTERNAL_ERROR;
}

/**
 * @brief call standard library function to convert string to unsigned long
 * integer.
 * @param  (nptr)     [in]  C-string containing the representation of an
 * integral number.
 * @param  (endptr)   [in]  Reference to an object of type char*, whose value is
 * set by the function to the next character in nptr after the numerical
 * value.
 * @param  (base)     [in]  Numerical base (radix) that determines the valid
 * characters and their interpretation.
 * @return converted integral number as an unsigned long int value.
 */
template <>
[[nodiscard]] unsigned long DbcUtils::call_integral<unsigned long>(
    const char *nptr, char **endptr, int base) {
    return std::strtoul(nptr, endptr, base);
}

/**
 * @brief call standard library function to convert string to long
 * integer.
 * @param  (nptr)     [in]  C-string containing the representation of an
 * integral number.
 * @param  (endptr)   [in]  Reference to an object of type char*, whose value is
 * set by the function to the next character in nptr after the numerical
 * value.
 * @param  (base)     [in]  Numerical base (radix) that determines the valid
 * characters and their interpretation.
 * @return converted integral number as long int value.
 */
template <>
[[nodiscard]] long DbcUtils::call_integral<long>(const char *nptr,
                                                 char **endptr, int base) {
    return std::strtol(nptr, endptr, base);
}

/**
 * @brief Explicit Template Instantiation for str_to_integral(unsigned long).
 */
template ErrorCode DbcUtils::str_to_integral(const char *str,
                                             unsigned long &return_value);
/**
 * @brief Explicit Template Instantiation for str_to_integral(long).
 */
template ErrorCode DbcUtils::str_to_integral(const char *str,
                                             long &return_value);

/**
 * @brief Convert string to integr.
 * @param  (input)        [in]   C-string containing the representation of
 * decimal integer literal (base 10).
 * @param  (return_value) [out]  the converted integral number.
 * @return ErrorCode::OK if success, otherwise an error code.
 */
template <typename T>
[[nodiscard]] ErrorCode DbcUtils::str_to_integral(const char *input,
                                                  T &return_value) {
    if (!input || *input == '\0' || std::isspace(*input)) {
        return ErrorCode::INTERNAL_ERROR;
    }

    char *end;
    errno = 0;

    const T result = call_integral<T>(input, &end, BASE_10);
    if (errno == 0 && *end == '\0') {
        return_value = result;
        return ErrorCode::OK;
    }
    std::cerr << Message::CONVERT_STRING_TO_INT_FAILURE << std::endl;
    return ErrorCode::INTERNAL_ERROR;
}
/**
 * @brief Gets the number of affected rows
 * if command was INSERT, UPDATE, or DELETE.
 * @param  (res)          [in]  the result of a query.
 * @param  (return_value) [out] the number of affected rows.
 * @return the number of affected rows
 * if last command was INSERT, UPDATE, or
 * DELETE. zero for all other commands.
 */
ErrorCode DbcUtils::get_number_of_rows_affected(PGresult *&res,
                                                uint64_t &return_value) {
    return str_to_integral(PQcmdTuples(res), return_value);
}

/**
 * @brief Makes shared_ptr of PGconn with deleter.
 * @param  (pgconn)   [in]  a connection.
 * @return shared_ptr of PGconn with deleter.
 */
ConnectionSPtr DbcUtils::make_connection_sptr(PGconn *pgconn) {
    ConnectionSPtr conn(pgconn, [](PGconn *c) { ::PQfinish(c); });
    return conn;
}

/**
 * @brief Makes unique_ptr of PGresult with deleter.
 * @param  (pgres)   [in]  the result of a query.
 * @return unique_ptr of PGresult with deleter.
 */
ResultUPtr DbcUtils::make_result_uptr(PGresult *pgres) {
    ResultUPtr res(pgres, [](PGresult *r) { ::PQclear(r); });
    return res;
}

}  // namespace manager::metadata::db
