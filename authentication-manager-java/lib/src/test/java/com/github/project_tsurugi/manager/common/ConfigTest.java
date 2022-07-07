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

package com.github.project_tsurugi.manager.common;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.junit.jupiter.api.Test;

class ConfigTest extends ConfigTestBase {
  /**
   * Test of gets connection string for authentication.
   */
  @Test
  public void getConnectionString() {
    final String envKey = Key.CONNECTION_STRING;
    final String expected = "connection-string";
    final String expectedDefault = DefaultValue.CONNECTION_STRING;

    // Backup the values of environment variables.
    final String backupEnvironmentValue = System.getenv(envKey);

    // If environment variables are defined.
    setEnv(envKey, expected);
    // Run tests and verify results.
    assertEquals(expected, Config.getConnectionString());

    // If environment variables are not defined.
    setEnv(envKey, null);
    // Run tests and verify results.
    assertEquals(expectedDefault, Config.getConnectionString());

    // Restore the value of the environment variable.
    setEnv(envKey, backupEnvironmentValue);
  }

  @Test
  public void getJwtIssuer() {
    final String envKey = Key.JWT_CLAIM_ISSUER;
    final String expected = "Jwt-Issuer";
    final String expectedDefault = DefaultValue.JWT_CLAIM_ISSUER;

    // Backup the values of environment variables.
    final String backupEnvironmentValue = System.getenv(envKey);

    // If environment variables are defined.
    setEnv(envKey, expected);
    // Run tests and verify results.
    assertEquals(expected, Config.getJwtIssuer());

    // If environment variables are not defined.
    setEnv(envKey, null);
    // Run tests and verify results.
    assertEquals(expectedDefault, Config.getJwtIssuer());

    // Restore the value of the environment variable.
    setEnv(envKey, backupEnvironmentValue);
  }

  @Test
  public void getJwtAudience() {
    final String envKey = Key.JWT_CLAIM_AUDIENCE;
    final String expected = "Jwt-Audience";
    final String expectedDefault = DefaultValue.JWT_CLAIM_AUDIENCE;

    // Backup the values of environment variables.
    final String backupEnvironmentValue = System.getenv(envKey);

    // If environment variables are defined.
    setEnv(envKey, expected);
    // Run tests and verify results.
    assertEquals(expected, Config.getJwtAudience());

    // If environment variables are not defined.
    setEnv(envKey, null);
    // Run tests and verify results.
    assertEquals(expectedDefault, Config.getJwtAudience());

    // Restore the value of the environment variable.
    setEnv(envKey, backupEnvironmentValue);
  }

  @Test
  public void getJwtSubject() {
    final String envKey = Key.JWT_CLAIM_SUBJECT;
    final String expected = "Jwt-Subject";
    final String expectedDefault = DefaultValue.JWT_CLAIM_SUBJECT;

    // Backup the values of environment variables.
    final String backupEnvironmentValue = System.getenv(envKey);

    // If environment variables are defined.
    setEnv(envKey, expected);
    // Run tests and verify results.
    assertEquals(expected, Config.getJwtSubject());

    // If environment variables are not defined.
    setEnv(envKey, null);
    // Run tests and verify results.
    assertEquals(expectedDefault, Config.getJwtSubject());

    // Restore the value of the environment variable.
    setEnv(envKey, backupEnvironmentValue);
  }

  @Test
  public void getJwtSecretKey() {
    final String envKey = Key.JWT_SECRET_KEY;
    final String expected = "Jwt-SecretKey";
    final String expectedDefault = DefaultValue.JWT_SECRET_KEY;

    // Backup the values of environment variables.
    final String backupEnvironmentValue = System.getenv(envKey);

    // If environment variables are defined.
    setEnv(envKey, expected);
    // Run tests and verify results.
    assertEquals(expected, Config.getJwtSecretKey());

    // If environment variables are not defined.
    setEnv(envKey, null);
    // Run tests and verify results.
    assertEquals(expectedDefault, Config.getJwtSecretKey());

    // Restore the value of the environment variable.
    setEnv(envKey, backupEnvironmentValue);
  }

  @Test
  public void getJwtExpiration() {
    final String envKey = Key.JWT_EXPIRATION;
    final String envValue = "1";
    final int expectedSec = 1;
    final int expectedMin = 60;
    final int expectedHour = 3600;
    final int expectedDay = 86400;
    final int expectedDefault = DefaultValue.JWT_EXPIRATION;

    // Backup the values of environment variables.
    final String backupEnvironmentValue = System.getenv(envKey);

    // If environment variables are defined. (no unit)
    setEnv(envKey, envValue);
    // Run tests and verify results.
    assertEquals(expectedSec, Config.getJwtExpiration());

    // If environment variables are defined. (unit is "s")
    setEnv(envKey, envValue + "s");
    // Run tests and verify results.
    assertEquals(expectedSec, Config.getJwtExpiration());

    // If environment variables are defined. (unit is "min")
    setEnv(envKey, envValue + "min");
    // Run tests and verify results.
    assertEquals(expectedMin, Config.getJwtExpiration());

    // If environment variables are defined. (unit is "h")
    setEnv(envKey, envValue + "h");
    // Run tests and verify results.
    assertEquals(expectedHour, Config.getJwtExpiration());

    // If environment variables are defined. (unit is "d")
    setEnv(envKey, envValue + "d");
    // Run tests and verify results.
    assertEquals(expectedDay, Config.getJwtExpiration());

    // If environment variables are not defined.
    setEnv(envKey, null);
    // Run tests and verify results.
    assertEquals(expectedDefault, Config.getJwtExpiration());

    // Restore the value of the environment variable.
    setEnv(envKey, backupEnvironmentValue);
  }

  @Test
  public void getJwtExpirationRefresh() {
    final String envKey = Key.JWT_REFRESH_EXPIRATION;
    final String envValue = "1";
    final int expectedSec = 1;
    final int expectedMin = 60;
    final int expectedHour = 3600;
    final int expectedDay = 86400;
    final int expectedDefault = DefaultValue.JWT_REFRESH_EXPIRATION;

    // Backup the values of environment variables.
    final String backupEnvironmentValue = System.getenv(envKey);

    // If environment variables are defined. (no unit)
    setEnv(envKey, envValue);
    // Run tests and verify results.
    assertEquals(expectedSec, Config.getJwtExpirationRefresh());

    // If environment variables are defined. (unit is "s")
    setEnv(envKey, envValue + "s");
    // Run tests and verify results.
    assertEquals(expectedSec, Config.getJwtExpirationRefresh());

    // If environment variables are defined. (unit is "min")
    setEnv(envKey, envValue + "min");
    // Run tests and verify results.
    assertEquals(expectedMin, Config.getJwtExpirationRefresh());

    // If environment variables are defined. (unit is "h")
    setEnv(envKey, envValue + "h");
    // Run tests and verify results.
    assertEquals(expectedHour, Config.getJwtExpirationRefresh());

    // If environment variables are defined. (unit is "d")
    setEnv(envKey, envValue + "d");
    // Run tests and verify results.
    assertEquals(expectedDay, Config.getJwtExpirationRefresh());

    // If environment variables are not defined.
    setEnv(envKey, null);
    // Run tests and verify results.
    assertEquals(expectedDefault, Config.getJwtExpirationRefresh());

    // Restore the value of the environment variable.
    setEnv(envKey, backupEnvironmentValue);
  }

  @Test
  public void getJwtExpirationAvailable() {
    final String envKey = Key.JWT_AVAILABLE_EXPIRATION;
    final String envValue = "1";
    final int expectedSec = 1;
    final int expectedMin = 60;
    final int expectedHour = 3600;
    final int expectedDay = 86400;
    final int expectedDefault = DefaultValue.JWT_AVAILABLE_EXPIRATION;

    // Backup the values of environment variables.
    final String backupEnvironmentValue = System.getenv(envKey);

    // If environment variables are defined. (no unit)
    setEnv(envKey, envValue);
    // Run tests and verify results.
    assertEquals(expectedSec, Config.getJwtExpirationAvailable());

    // If environment variables are defined. (unit is "s")
    setEnv(envKey, envValue + "s");
    // Run tests and verify results.
    assertEquals(expectedSec, Config.getJwtExpirationAvailable());

    // If environment variables are defined. (unit is "min")
    setEnv(envKey, envValue + "min");
    // Run tests and verify results.
    assertEquals(expectedMin, Config.getJwtExpirationAvailable());

    // If environment variables are defined. (unit is "h")
    setEnv(envKey, envValue + "h");
    // Run tests and verify results.
    assertEquals(expectedHour, Config.getJwtExpirationAvailable());

    // If environment variables are defined. (unit is "d")
    setEnv(envKey, envValue + "d");
    // Run tests and verify results.
    assertEquals(expectedDay, Config.getJwtExpirationAvailable());

    // If environment variables are not defined.
    setEnv(envKey, null);
    // Run tests and verify results.
    assertEquals(expectedDefault, Config.getJwtExpirationAvailable());

    // Restore the value of the environment variable.
    setEnv(envKey, backupEnvironmentValue);
  }

  @Test
  public void getJwtExpirationInvalid() {
    final String envKey = Key.JWT_EXPIRATION;
    final String envValue = "1";
    final int expectedDefault = DefaultValue.JWT_EXPIRATION;

    // Backup the values of environment variables.
    final String backupEnvironmentValue = System.getenv(envKey);

    // If environment variables are defined. (unit is "x")
    setEnv(envKey, envValue + "x");
    // Run tests and verify results.
    assertEquals(expectedDefault, Config.getJwtExpiration());

    // Restore the value of the environment variable.
    setEnv(envKey, backupEnvironmentValue);
  }

  @Test
  public void getJwtExpirationRefreshInvalid() {
    final String envKey = Key.JWT_EXPIRATION;
    final String envValue = "1";
    final int expectedDefault = DefaultValue.JWT_REFRESH_EXPIRATION;

    // Backup the values of environment variables.
    final String backupEnvironmentValue = System.getenv(envKey);

    // If environment variables are defined. (unit is "x")
    setEnv(envKey, envValue + "x");
    // Run tests and verify results.
    assertEquals(expectedDefault, Config.getJwtExpirationRefresh());

    // Restore the value of the environment variable.
    setEnv(envKey, backupEnvironmentValue);
  }

  @Test
  public void getJwtExpirationAvailableInvalid() {
    final String envKey = Key.JWT_EXPIRATION;
    final String envValue = "1";
    final int expectedDefault = DefaultValue.JWT_AVAILABLE_EXPIRATION;

    // Backup the values of environment variables.
    final String backupEnvironmentValue = System.getenv(envKey);

    // If environment variables are defined. (unit is "x")
    setEnv(envKey, envValue + "x");
    // Run tests and verify results.
    assertEquals(expectedDefault, Config.getJwtExpirationAvailable());

    // Restore the value of the environment variable.
    setEnv(envKey, backupEnvironmentValue);
  }
}
