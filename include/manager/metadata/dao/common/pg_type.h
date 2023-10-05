/*
 * Copyright 2021-2022 Project Tsurugi.
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
#ifndef MANAGER_METADATA_DAO_COMMON_PG_TYPE_H_
#define MANAGER_METADATA_DAO_COMMON_PG_TYPE_H_

#include <cstdint>

namespace manager::metadata::db {

class PgType {
 public:
  /**
   * @brief OID of the data type in the pg_type catalog.
   */
  class TypeOid {
   public:
    static constexpr std::uint32_t kInt4 = 23;
    static constexpr std::uint32_t kInt8 = 20;
    static constexpr std::uint32_t kFloat4 = 700;
    static constexpr std::uint32_t kFloat8 = 701;
    static constexpr std::uint32_t kBpchar = 1042;
    static constexpr std::uint32_t kVarchar = 1043;
    static constexpr std::uint32_t kNumeric = 1700;
    static constexpr std::uint32_t kDate = 1082;
    static constexpr std::uint32_t kTime = 1083;
    static constexpr std::uint32_t kTimetz = 1266;
    static constexpr std::uint32_t kTimestamp = 1114;
    static constexpr std::uint32_t kTimestamptz = 1184;
    static constexpr std::uint32_t kInterval = 1186;
  };

  /**
   * @brief Data type name of the data type in the pg_type catalog.
   */
  class TypeName {
   public:
    static constexpr const char* const kInt4 = "int4";
    static constexpr const char* const kInt8 = "int8";
    static constexpr const char* const kFloat4 = "float4";
    static constexpr const char* const kFloat8 = "float8";
    static constexpr const char* const kBpchar = "bpchar";
    static constexpr const char* const kVarchar = "varchar";
    static constexpr const char* const kNumeric = "numeric";
    static constexpr const char* const kDate = "date";
    static constexpr const char* const kTime = "time";
    static constexpr const char* const kTimetz = "timetz";
    static constexpr const char* const kTimestamp = "timestamp";
    static constexpr const char* const kTimestamptz = "timestamptz";
    static constexpr const char* const kInterval = "interval";
  };
};

}  // namespace manager::metadata::db

#endif  // MANAGER_METADATA_DAO_COMMON_PG_TYPE_H_
