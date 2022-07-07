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

package com.github.project_tsurugi.manager.authentication;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.when;

import com.auth0.jwt.JWT;
import com.auth0.jwt.JWTCreator;
import com.auth0.jwt.JWTCreator.Builder;
import com.auth0.jwt.algorithms.Algorithm;
import com.auth0.jwt.exceptions.JWTCreationException;
import com.github.project_tsurugi.manager.authentication.db.DbSessionManager;
import com.github.project_tsurugi.manager.common.Config;
import com.github.project_tsurugi.manager.common.JwtClaims;
import com.github.project_tsurugi.manager.exceptions.AuthenticationException;
import com.github.project_tsurugi.manager.exceptions.DbAccessException;
import com.github.project_tsurugi.manager.exceptions.InternalException;
import com.github.project_tsurugi.manager.exceptions.InvalidTokenException;
import java.time.Duration;
import java.util.Calendar;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;

class AuthenticationTest extends AuthenticationTestBase {
  /**
   * [authUser API] Perform a test when the parameter is a normal value.
   */
  @Test
  public void authUserParamNormal() {
    final String connectionString = "jdbc:postgresql://localhost:5432/tsurugi";
    final String userName = "ut_user_name";
    final String password = "ut_password";
    final String secretKey = "ut_secret-key";
    final String issuer = "ut_issuer";
    final String audience = "ut_audience";
    final String subject = "ut_subject";
    final int expiration = 10;
    final int expirationRefresh = 20;
    final int expirationAvailable = 30;

    try (var mockConfig = mockStatic(Config.class)) {
      // Mock the method.
      mockConfig.when(() -> Config.getConnectionString()).thenReturn(connectionString);
      mockConfig.when(() -> Config.getJwtIssuer()).thenReturn(issuer);
      mockConfig.when(() -> Config.getJwtAudience()).thenReturn(audience);
      mockConfig.when(() -> Config.getJwtSubject()).thenReturn(subject);
      mockConfig.when(() -> Config.getJwtExpiration()).thenReturn(expiration);
      mockConfig.when(() -> Config.getJwtExpirationRefresh()).thenReturn(expirationRefresh);
      mockConfig.when(() -> Config.getJwtExpirationAvailable()).thenReturn(expirationAvailable);
      mockConfig.when(() -> Config.getJwtSecretKey()).thenReturn(secretKey);

      try (var mockDbSessionManager = mockStatic(DbSessionManager.class)) {
        String token = "";

        // Run the test. authUser API that does not specify a connection string.
        token = assertDoesNotThrow(() -> Authentication.authUser(userName, password));
        // Verify test results.
        assertFalse(token.isEmpty());
        // Verify that the token is correct.
        checkJwt(token, userName);

        // Run the test. authUser API that specify a connection string.
        token =
            assertDoesNotThrow(() -> Authentication.authUser(connectionString, userName, password));
        // Verify test results.
        assertFalse(token.isEmpty());
        // Verify that the token is correct.
        checkJwt(token, userName);

        // Verify test results.
        mockDbSessionManager.verify(
            () -> DbSessionManager.attemptConnection(connectionString, userName, password),
            times(2));
      } catch (Exception e) {
        fail(e.getMessage());
      }
    }
  }

  /**
   * [authUser API] Perform a test when the parameter is an empty string.
   */
  @Test
  public void authUserParamEmpty() {
    final String connectionString = "";
    final String userName = "";
    final String password = "";

    try (var mockDbSessionManager = mockStatic(DbSessionManager.class)) {
      String token = "";

      // Run the test. authUser API that does not specify a connection string.
      token = assertDoesNotThrow(() -> Authentication.authUser(userName, password));
      // Verify test results.
      assertFalse(token.isEmpty());
      // Verify that the token is correct.
      checkJwt(token, userName);
      // Verify test results.
      mockDbSessionManager.verify(
          () -> DbSessionManager.attemptConnection(Config.getConnectionString(), userName,
              password),
          times(1));

      // Run the test. authUser API that specify a connection string.
      token =
          assertDoesNotThrow(() -> Authentication.authUser(connectionString, userName, password));
      // Verify test results.
      assertFalse(token.isEmpty());
      // Verify that the token is correct.
      checkJwt(token, userName);
      // Verify test results.
      mockDbSessionManager.verify(
          () -> DbSessionManager.attemptConnection(connectionString, userName, password),
          times(1));
    } catch (Exception e) {
      fail(e.getMessage());
    }
  }

  /**
   * [authUser API] Perform a test when the parameter is a null.
   */
  @Test
  public void authUserParamNull() {
    final String connectionString = null;
    final String userName = null;
    final String password = null;

    try (var mockDbSessionManager = mockStatic(DbSessionManager.class)) {
      String token = "";

      // Run the test. authUser API that does not specify a connection string.
      token = assertDoesNotThrow(() -> Authentication.authUser(userName, password));
      // Verify test results.
      assertFalse(token.isEmpty());
      // Verify that the token is correct.
      checkJwt(token, userName);
      // Verify test results.
      mockDbSessionManager.verify(
          () -> DbSessionManager.attemptConnection(Config.getConnectionString(), userName,
              password),
          times(1));

      // Run the test. authUser API that specify a connection string.
      token =
          assertDoesNotThrow(() -> Authentication.authUser(connectionString, userName, password));
      // Verify test results.
      assertFalse(token.isEmpty());
      // Verify that the token is correct.
      checkJwt(token, userName);
      // Verify test results.
      mockDbSessionManager.verify(
          () -> DbSessionManager.attemptConnection(connectionString, userName, password),
          times(1));
    } catch (Exception e) {
      fail(e.getMessage());
    }
  }

  /**
   * [authUser API] Test if authentication fails.
   */
  @Test
  public void authUserAuthFailure() {
    final String connectionString = Config.getConnectionString();
    final String userName = "ut_user_name";
    final String password = "ut_password";

    try (var mockDbSessionManager = mockStatic(DbSessionManager.class)) {
      // Mock the method.
      mockDbSessionManager.when(() -> DbSessionManager.attemptConnection(
          anyString(), anyString(), anyString())).thenThrow(AuthenticationException.class);

      // Run the test. authUser API that does not specify a connection string.
      assertThrows(AuthenticationException.class,
          () -> Authentication.authUser(userName, password));

      // Run the test. authUser API that specify a connection string.
      assertThrows(AuthenticationException.class,
          () -> Authentication.authUser(connectionString, userName, password));

      // Verify test results.
      mockDbSessionManager.verify(
          () -> DbSessionManager.attemptConnection(connectionString, userName, password),
          times(2));
    } catch (Exception e) {
      fail(e.getMessage());
    }
  }

  /**
   * [authUser API] Test for token creation failure.
   */
  @Test
  public void authUserTokenCreateFaile() {
    final String connectionString = "jdbc:postgresql://localhost:5432/tsurugi";
    final String userName = "ut_user_name";
    final String password = "ut_password";
    final String secretKey = "";

    try (var mockConfig = mockStatic(Config.class)) {
      // Mock the method.
      mockConfig.when(() -> Config.getJwtSecretKey()).thenReturn(secretKey);

      try (var mockDbSessionManager = mockStatic(DbSessionManager.class)) {
        // Run the test. authUser API that does not specify a connection string.
        assertThrows(InternalException.class,
            () -> Authentication.authUser(userName, password));

        // Run the test. authUser API that specify a connection string.
        assertThrows(InternalException.class,
            () -> Authentication.authUser(connectionString, userName, password));
      } catch (Exception e) {
        fail(e.getMessage());
      }
    }
  }

  /**
   * [authUser API] Test for DB connection failure.
   */
  @Test
  public void authUserDbConnFailure() {
    final String connectionString = Config.getConnectionString();
    final String userName = "ut_user_name";
    final String password = "ut_password";

    try (var mockDbSessionManager = mockStatic(DbSessionManager.class)) {
      // Mock the method.
      mockDbSessionManager.when(() -> DbSessionManager.attemptConnection(
          anyString(), anyString(), anyString())).thenThrow(DbAccessException.class);

      // Run the test. authUser API that does not specify a connection string.
      assertThrows(DbAccessException.class,
          () -> Authentication.authUser(userName, password));

      // Run the test. authUser API that specify a connection string.
      assertThrows(DbAccessException.class,
          () -> Authentication.authUser(connectionString, userName, password));

      // Verify test results.
      mockDbSessionManager.verify(
          () -> DbSessionManager.attemptConnection(connectionString, userName, password),
          times(2));
    } catch (Exception e) {
      fail(e.getMessage());
    }
  }

  /**
   * [authUser API] Perform a test when the parameter is a normal value. (Indefinite)
   */
  @Test
  public void authUserIndefinite() {
    final String connectionString = "jdbc:postgresql://localhost:5432/tsurugi";
    final String userName = "ut_user_name";
    final String password = "ut_password";
    final String secretKey = "ut_secret-key";
    final String issuer = "ut_issuer";
    final String audience = "ut_audience";
    final String subject = "ut_subject";
    final int expiration = 300;
    final int expirationRefresh = 600;
    final int expirationAvailable = 0;

    try (var mockConfig = mockStatic(Config.class)) {
      // Mock the method.
      mockConfig.when(() -> Config.getConnectionString()).thenReturn(connectionString);
      mockConfig.when(() -> Config.getJwtIssuer()).thenReturn(issuer);
      mockConfig.when(() -> Config.getJwtAudience()).thenReturn(audience);
      mockConfig.when(() -> Config.getJwtSubject()).thenReturn(subject);
      mockConfig.when(() -> Config.getJwtExpiration()).thenReturn(expiration);
      mockConfig.when(() -> Config.getJwtExpirationRefresh()).thenReturn(expirationRefresh);
      mockConfig.when(() -> Config.getJwtExpirationAvailable()).thenReturn(expirationAvailable);
      mockConfig.when(() -> Config.getJwtSecretKey()).thenReturn(secretKey);

      try (var mockDbSessionManager = mockStatic(DbSessionManager.class)) {
        String tokenString = "";

        // Run the test. authUser API that does not specify a connection string.
        tokenString = assertDoesNotThrow(() -> Authentication.authUser(userName, password));
        // Decode the token.
        var decodedToken = JWT.decode(tokenString);
        // "tsurugi/exp/available" claim
        assertEquals(0, decodedToken.getClaim(JwtClaims.Payload.EXPIRATION_AVAILABLE).asLong());

        tokenString = "";
        // Run the test. authUser API that specify a connection string.
        tokenString = assertDoesNotThrow(() -> Authentication.authUser(userName, password));
        // Decode the token.
        decodedToken = JWT.decode(tokenString);
        // "tsurugi/exp/available" claim
        assertEquals(0, decodedToken.getClaim(JwtClaims.Payload.EXPIRATION_AVAILABLE).asLong());

      }
    } catch (Exception e) {
      e.printStackTrace();
      fail();
    }
  }

  /**
   * [refreshToken API] Perform a test when the parameter is a normal value.
   */
  @Test
  public void refreshTokenParamNormal() {
    // Set the date.
    var expireTime = Calendar.getInstance();
    expireTime.add(Calendar.MINUTE, 1); // Set 1 minuutes after the current time.
    var expRefresh = Calendar.getInstance();
    expRefresh.add(Calendar.MINUTE, 60); // Set 60 minuutes after the current time.
    var expAvailable = Calendar.getInstance();
    expAvailable.add(Calendar.MINUTE, 60); // Set 60 minuutes after the current time.

    // Run the test.
    refreshTokenApiTestBase(expireTime, expRefresh, expAvailable, null);
  }

  /**
   * [refreshToken API] Perform a test when the parameter is a normal value. (expired)
   */
  @Test
  public void refreshTokenExpired() {
    // Set the date.
    var expireTime = Calendar.getInstance();
    expireTime.add(Calendar.MINUTE, -1); // Set 1 minuutes before the current time.
    var expRefresh = Calendar.getInstance();
    expRefresh.add(Calendar.MINUTE, 60); // Set 60 minuutes after the current time.
    var expAvailable = Calendar.getInstance();
    expAvailable.add(Calendar.MINUTE, 60); // Set 60 minuutes after the current time.

    // Run the test.
    refreshTokenApiTestBase(expireTime, expRefresh, expAvailable, null);
  }

  /**
   * [refreshToken API] Perform a test when the parameter is a normal value. (refresh expired)
   */
  @Test
  public void refreshTokenRefreshExpired() {
    // Set the date.
    var expireTime = Calendar.getInstance();
    expireTime.add(Calendar.MINUTE, 60); // Set 60 minuutes after the current time.
    var expRefresh = Calendar.getInstance();
    expRefresh.add(Calendar.MINUTE, -1); // Set 1 minuutes before the current time.
    var expAvailable = Calendar.getInstance();
    expAvailable.add(Calendar.MINUTE, 60); // Set 60 minuutes after the current time.

    // Run the test.
    refreshTokenApiTestBase(expireTime, expRefresh, expAvailable, InvalidTokenException.class);
  }

  /**
   * [refreshToken API] Perform a test when the parameter is a normal value. (expired & refresh
   * expired)
   */
  @Test
  public void refreshTokenExpiredRefreshExpired() {
    // Set the date.
    var expireTime = Calendar.getInstance();
    expireTime.add(Calendar.MINUTE, -1); // Set 1 minuutes before the current time.
    var expRefresh = Calendar.getInstance();
    expRefresh.add(Calendar.MINUTE, -1); // Set 1 minuutes before the current time.
    var expAvailable = Calendar.getInstance();
    expAvailable.add(Calendar.MINUTE, 60); // Set 60 minuutes after the current time.

    // Run the test.
    refreshTokenApiTestBase(expireTime, expRefresh, expAvailable, InvalidTokenException.class);
  }

  /**
   * [refreshToken API] Perform a test when the parameter is a normal value. (available expired)
   */
  @Test
  public void refreshTokenAvailableExpired() {
    // Set the date.
    var expireTime = Calendar.getInstance();
    expireTime.add(Calendar.MINUTE, 60); // Set 60 minuutes after the current time.
    var expRefresh = Calendar.getInstance();
    expRefresh.add(Calendar.MINUTE, 60); // Set 60 minuutes after the current time.
    var expAvailable = Calendar.getInstance();
    expAvailable.add(Calendar.MINUTE, -1); // Set 1 minuutes before the current time.

    // Run the test.
    refreshTokenApiTestBase(expireTime, expRefresh, expAvailable, InvalidTokenException.class);
  }

  /**
   * [refreshToken API] Perform a test when the parameter is a normal value. (all expired)
   */
  @Test
  public void refreshTokenAllExpired() {
    // Set the date.
    var expireTime = Calendar.getInstance();
    expireTime.add(Calendar.MINUTE, -1); // Set 1 minuutes before the current time.
    var expRefresh = Calendar.getInstance();
    expRefresh.add(Calendar.MINUTE, -1); // Set 1 minuutes before the current time.
    var expAvailable = Calendar.getInstance();
    expAvailable.add(Calendar.MINUTE, -1); // Set 1 minuutes before the current time.

    // Run the test.
    refreshTokenApiTestBase(expireTime, expRefresh, expAvailable, InvalidTokenException.class);
  }

  /**
   * [refreshToken API] Perform a test when the parameter is a normal value. (rounding)
   */
  @Test
  public void refreshTokenRounding() {
    final String userName = "ut_user_name";
    final String secretKey = "ut_secret-key";
    final String issuer = "ut_issuer";
    final String audience = "ut_audience";
    final String subject = "ut_subject";
    final int extendedSec = 100;
    final int expiration = 300;
    final int expirationRefresh = 600;
    final int expirationAvailable = 900;
    final int expectedLimit = 60;

    // Set the date.
    var baseTime = Calendar.getInstance();
    var issuedTime = Calendar.getInstance();
    issuedTime.add(Calendar.MINUTE, -1); // Set 1 minuutes before the current time.
    var expireTime = Calendar.getInstance();
    expireTime.add(Calendar.MINUTE, 5); // Set 5 minuutes after the current time.
    var expRefresh = Calendar.getInstance();
    expRefresh.add(Calendar.MINUTE, 5); // Set 5 minuutes after the current time.
    var expAvailable = Calendar.getInstance();
    expAvailable.add(Calendar.SECOND, expectedLimit); // Set 1 minuutes after the current time.

    issuedTime.set(Calendar.MILLISECOND, 0);
    expireTime.set(Calendar.MILLISECOND, 0);
    expRefresh.set(Calendar.MILLISECOND, 0);
    expAvailable.set(Calendar.MILLISECOND, 0);

    try (var mockConfig = mockStatic(Config.class)) {
      // Mock the method.
      mockConfig.when(() -> Config.getJwtIssuer()).thenReturn(issuer);
      mockConfig.when(() -> Config.getJwtAudience()).thenReturn(audience);
      mockConfig.when(() -> Config.getJwtSubject()).thenReturn(subject);
      mockConfig.when(() -> Config.getJwtExpiration()).thenReturn(expiration);
      mockConfig.when(() -> Config.getJwtExpirationRefresh()).thenReturn(expirationRefresh);
      mockConfig.when(() -> Config.getJwtExpirationAvailable()).thenReturn(expirationAvailable);
      mockConfig.when(() -> Config.getJwtSecretKey()).thenReturn(secretKey);

      Builder jwtBuilder = JWT.create()
          .withIssuedAt(issuedTime.getTime())
          .withExpiresAt(expireTime.getTime())
          .withClaim(JwtClaims.Payload.EXPIRATION_REFRESH, expRefresh.getTime())
          .withClaim(JwtClaims.Payload.EXPIRATION_AVAILABLE, expAvailable.getTime())
          .withClaim(JwtClaims.Payload.AUTH_USER_NAME, userName);

      // Cryptographic algorithms.
      var algorithm = Algorithm.HMAC256(Config.getJwtSecretKey());
      // Sign the JWT token.
      String tokenStringOld = jwtBuilder.sign(algorithm);

      String tokenStringNew = "";
      try (var mockDbSessionManager = mockStatic(DbSessionManager.class)) {
        // Run the test.
        tokenStringNew =
            assertDoesNotThrow(
                () -> Authentication.refreshToken(tokenStringOld, extendedSec, TimeUnit.SECONDS));
        // Verify test results.
        assertFalse(tokenStringNew.isEmpty());

        // Decode the new token.
        var decodedNewToken = JWT.decode(tokenStringNew);

        // "exp" claim
        var actualExpiresTime = Duration.between(baseTime.getTime().toInstant(),
            decodedNewToken.getExpiresAt().toInstant());
        assertEquals((double) expectedLimit, (double) actualExpiresTime.getSeconds(), 1.0f);

        // "tsurugi/exp/refresh" claim
        var actualRefreshTime = Duration.between(baseTime.getTime().toInstant(),
            decodedNewToken.getClaim(JwtClaims.Payload.EXPIRATION_REFRESH).asDate().toInstant());
        assertEquals((double) expectedLimit, (double) actualRefreshTime.getSeconds(), 1.0f);

        // "tsurugi/exp/available" claim
        assertEquals((expAvailable.getTimeInMillis() / 1000),
            decodedNewToken.getClaim(JwtClaims.Payload.EXPIRATION_AVAILABLE).asLong());
      }
    } catch (Exception e) {
      e.printStackTrace();
      fail();
    }
  }

  /**
   * [refreshToken API] Test for token creation failure.
   */
  @Test
  public void refreshTokenCreateFaile() {
    final String userName = "ut_user_name";
    final String secretKey = "ut_secret-key";
    final String issuer = "ut_issuer";
    final String audience = "ut_audience";
    final String subject = "ut_subject";
    final int extendedSec = 100;
    final int expiration = 300;
    final int expirationRefresh = 600;
    final int expirationAvailable = 900;
    final int expectedLimit = 60;

    // Set the date.
    var issuedTime = Calendar.getInstance();
    issuedTime.add(Calendar.MINUTE, -1); // Set 1 minuutes before the current time.
    var expireTime = Calendar.getInstance();
    expireTime.add(Calendar.MINUTE, 5); // Set 5 minuutes after the current time.
    var expRefresh = Calendar.getInstance();
    expRefresh.add(Calendar.MINUTE, 5); // Set 5 minuutes after the current time.
    var expAvailable = Calendar.getInstance();
    expAvailable.add(Calendar.SECOND, expectedLimit); // Set 1 minuutes after the current time.

    issuedTime.set(Calendar.MILLISECOND, 0);
    expireTime.set(Calendar.MILLISECOND, 0);
    expRefresh.set(Calendar.MILLISECOND, 0);
    expAvailable.set(Calendar.MILLISECOND, 0);

    try (var mockConfig = mockStatic(Config.class)) {
      // Mock the method.
      mockConfig.when(() -> Config.getJwtIssuer()).thenReturn(issuer);
      mockConfig.when(() -> Config.getJwtAudience()).thenReturn(audience);
      mockConfig.when(() -> Config.getJwtSubject()).thenReturn(subject);
      mockConfig.when(() -> Config.getJwtExpiration()).thenReturn(expiration);
      mockConfig.when(() -> Config.getJwtExpirationRefresh()).thenReturn(expirationRefresh);
      mockConfig.when(() -> Config.getJwtExpirationAvailable()).thenReturn(expirationAvailable);
      mockConfig.when(() -> Config.getJwtSecretKey()).thenReturn(secretKey);

      Builder jwtBuilder = JWT.create()
          .withIssuer(Config.getJwtIssuer())
          .withAudience(Config.getJwtAudience())
          .withSubject(Config.getJwtSubject())
          .withIssuedAt(issuedTime.getTime())
          .withExpiresAt(expireTime.getTime())
          .withClaim(JwtClaims.Payload.EXPIRATION_REFRESH, expRefresh.getTime())
          .withClaim(JwtClaims.Payload.EXPIRATION_AVAILABLE, expAvailable.getTime())
          .withClaim(JwtClaims.Payload.AUTH_USER_NAME, userName);

      // Cryptographic algorithms.
      var algorithm = Algorithm.HMAC256(Config.getJwtSecretKey());
      // Sign the JWT token.
      String tokenStringOld = jwtBuilder.sign(algorithm);

      try (var mockDbSessionManager = mockStatic(DbSessionManager.class)) {
        var mockBuilder = mock(JWTCreator.Builder.class);
        when(mockBuilder.sign(any())).thenThrow(JWTCreationException.class);

        try (var mockJwt = mockStatic(JWT.class)) {
          // Mock the method.
          mockJwt.when(() -> JWT.create()).thenReturn(mockBuilder);
          mockJwt.when(() -> JWT.decode(any())).thenCallRealMethod();
          mockJwt.when(() -> JWT.require(any())).thenCallRealMethod();

          // Run the test.
          assertThrows(InternalException.class,
              () -> Authentication.refreshToken(tokenStringOld, extendedSec, TimeUnit.SECONDS));
        }
      }
    } catch (Exception e) {
      e.printStackTrace();
      fail();
    }
  }

  /**
   * [refreshToken API] Perform a test when the parameter is a normal value. (illegal token)
   */
  @Test
  public void refreshTokenIllegalToken() {
    final String tokenString = "header.payload.signature";

    // Run the test.
    assertThrows(InvalidTokenException.class,
        () -> Authentication.refreshToken(tokenString, 300, TimeUnit.SECONDS));
  }

  /**
   * [refreshToken API] Perform a test when the parameter is an empry value.
   */
  @Test
  public void refreshTokenParamEmpty() {
    final String tokenString = "";

    // Run the test.
    assertThrows(InvalidTokenException.class,
        () -> Authentication.refreshToken(tokenString, 300, TimeUnit.SECONDS));
  }

  /**
   * [refreshToken API] Perform a test when the parameter is a null.
   */
  @Test
  public void refreshTokenParamNull() {
    final String tokenString = null;

    // Run the test.
    assertThrows(InvalidTokenException.class,
        () -> Authentication.refreshToken(tokenString, 300, TimeUnit.SECONDS));
  }
}
