/*
 * Copyright 2022 tsurugi project.
 *
 * Licensed under the Apache License, version 2.0 (the "License"); you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */

package com.github.project_tsurugi.manager.authentication.db;

import com.github.project_tsurugi.manager.exceptions.AuthenticationException;
import com.github.project_tsurugi.manager.exceptions.DbAccessException;
import java.sql.DriverManager;
import java.sql.SQLException;

/**
 * This class manages DB sessions.
 */
public class DbSessionManager {
  private static final String JDBC_PREFIX_STRING = "jdbc:";
  private static final String REGEX_AUTH_FAILURE = "28.+";

  private DbSessionManager() {}

  /**
   * Attempt to connect to the database.
   *
   * @param connectionString cnection string in URI format.
   * @param userName user name to authenticate.
   * @param password passward.
   * @throws AuthenticationException if the authentication failed.
   * @throws DbAccessException if the connection to the database failed.
   */
  public static void attemptConnection(final String connectionString, final String userName,
      final String password)
      throws AuthenticationException, DbAccessException {

    String jdbcConnectionString = (connectionString != null ? connectionString : "");
    if (!jdbcConnectionString.matches(JDBC_PREFIX_STRING + ".+")) {
      jdbcConnectionString = JDBC_PREFIX_STRING + jdbcConnectionString;
    }

    try {
      // Connectrion to PostgreSQL
      var conn = DriverManager.getConnection(jdbcConnectionString, userName, password);
      conn.close();
    } catch (SQLException e) {
      String errorState = e.getSQLState();
      if (errorState.matches(REGEX_AUTH_FAILURE)) {
        throw new AuthenticationException(e.getMessage());
      } else {
        throw new DbAccessException(e.getMessage());
      }
    }
  }

}
