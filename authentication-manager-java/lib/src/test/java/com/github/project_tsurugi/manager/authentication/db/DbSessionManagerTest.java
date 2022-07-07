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

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.github.project_tsurugi.manager.exceptions.AuthenticationException;
import com.github.project_tsurugi.manager.exceptions.DbAccessException;
import java.sql.DriverManager;
import java.sql.SQLException;
import org.junit.jupiter.api.Test;

class DbSessionManagerTest {
  /**
   * Perform a test when the parameter is a normal value.
   */
  @Test
  public void base() {
    final String connectionString = "jdbc:postgresql://localhost:5432/tsurugi";
    final String userName = "ut_user_name";
    final String password = "ut_password";

    try (var mockDriverManager = mockStatic(DriverManager.class)) {
      // Mock the Connection class.
      var mockConnection = mock(DummyConnection.class);
      doNothing().when(mockConnection).close();

      // Mock the method.
      mockDriverManager
          .when(() -> DriverManager.getConnection(anyString(), anyString(), anyString()))
          .thenReturn(mockConnection);

      // Run the test.
      assertDoesNotThrow(
          () -> DbSessionManager.attemptConnection(connectionString, userName, password));

      // Verify test results.
      mockDriverManager.verify(
          () -> DriverManager.getConnection(connectionString, userName, password), times(1));
      verify(mockConnection, times(1)).close();
    } catch (SQLException e) {
      e.printStackTrace();
      fail();
    }
  }

  /**
   * Perform a test when the parameter is a normal value.
   */
  @Test
  public void baseNonePrefix() {
    final String connectionString = "postgresql://localhost:5432/tsurugi";
    final String connectionStringJdbc = "jdbc:postgresql://localhost:5432/tsurugi";
    final String userName = "ut_user_name";
    final String password = "ut_password";

    try (var mockDriverManager = mockStatic(DriverManager.class)) {
      // Mock the Connection class.
      var mockConnection = mock(DummyConnection.class);
      doNothing().when(mockConnection).close();

      // Mock the method.
      mockDriverManager
          .when(() -> DriverManager.getConnection(anyString(), anyString(), anyString()))
          .thenReturn(mockConnection);

      // Run the test.
      assertDoesNotThrow(
          () -> DbSessionManager.attemptConnection(connectionString, userName, password));

      // Verify test results.
      mockDriverManager.verify(
          () -> DriverManager.getConnection(connectionStringJdbc, userName, password), times(1));
      verify(mockConnection, times(1)).close();
    } catch (SQLException e) {
      e.printStackTrace();
      fail();
    }
  }

  /**
   * Test for DB connection failure.
   */
  @Test
  public void dbConnectionFailure() {
    final String connectionString = "jdbc:postgresql://localhost:5432/tsurugi";
    final String userName = "ut_user_name";
    final String password = "ut_password";

    try (var mockDriverManager = mockStatic(DriverManager.class)) {
      // Mock the SQLException class.
      var mockSqlException = mock(SQLException.class);

      // Test for PostgreSQL error code.
      when(mockSqlException.getSQLState())
          .thenReturn("08000")
          .thenReturn("08003")
          .thenReturn("08006");

      // Mock the method.
      mockDriverManager
          .when(() -> DriverManager.getConnection(anyString(), anyString(), anyString()))
          .thenThrow(mockSqlException);

      // Run the test for PostgreSQL error code 08000.
      assertThrows(DbAccessException.class,
          () -> DbSessionManager.attemptConnection(connectionString, userName, password));
      // Run the test for PostgreSQL error code 08003.
      assertThrows(DbAccessException.class,
          () -> DbSessionManager.attemptConnection(connectionString, userName, password));
      // Run the test for PostgreSQL error code 08006.
      assertThrows(DbAccessException.class,
          () -> DbSessionManager.attemptConnection(connectionString, userName, password));

      // Verify test results.
      mockDriverManager.verify(
          () -> DriverManager.getConnection(connectionString, userName, password), times(3));
    }
  }

  /**
   * Test for authentication failure.
   */
  @Test
  public void authenticationFailure() {
    final String connectionString = "jdbc:postgresql://localhost:5432/tsurugi";
    final String userName = "ut_user_name";
    final String password = "ut_password";

    try (var mockDriverManager = mockStatic(DriverManager.class)) {
      // Mock the SQLException class.
      var mockSqlException = mock(SQLException.class);

      // Test for PostgreSQL error code.
      when(mockSqlException.getSQLState())
          .thenReturn("28000")
          .thenReturn("28P01");

      // Mock the method.
      mockDriverManager
          .when(() -> DriverManager.getConnection(anyString(), anyString(), anyString()))
          .thenThrow(mockSqlException);

      // Run the test for PostgreSQL error code 28000.
      assertThrows(AuthenticationException.class,
          () -> DbSessionManager.attemptConnection(connectionString, userName, password));
      // Run the test for PostgreSQL error code 28P01.
      assertThrows(AuthenticationException.class,
          () -> DbSessionManager.attemptConnection(connectionString, userName, password));

      // Verify test results.
      mockDriverManager.verify(
          () -> DriverManager.getConnection(connectionString, userName, password), times(2));
    }
  }

  /**
   * Test for null.
   */
  @Test
  public void paramNull() {
    final String connectionString = null;
    final String userName = null;
    final String password = null;

    // Run the test.
    assertThrows(DbAccessException.class,
        () -> DbSessionManager.attemptConnection(connectionString, userName, password));
  }
}
