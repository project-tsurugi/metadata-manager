/*
 * Copyright 2022 tsurugi project.
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
#include "test/helper/postgresql/metadata_helper_pg.h"

#include <boost/format.hpp>

#include "manager/metadata/common/config.h"
#include "manager/metadata/dao/postgresql/pg_common.h"
#include "test/common/ut_utils.h"

namespace manager::metadata::testing {

namespace dao = ::manager::metadata::db;

/**
 * @brief Get the number of records in the current constraint metadata.
 * @return Current number of records.
 */
int64_t MetadataHelperPg::get_record_count() const {
  PGconn* connection = PQconnectdb(Config::get_connection_string().c_str());

  boost::format statement = boost::format("SELECT COUNT(*) FROM %s.%s") %
                            dao::kSchemaTsurugiCatalog % this->table_name_;
  PGresult* res = PQexec(connection, statement.str().c_str());

  int64_t res_val = UTUtils::to_integral<int64_t>(PQgetvalue(res, 0, 0));

  PQclear(res);
  PQfinish(connection);

  return res_val;
}

}  // namespace manager::metadata::testing
