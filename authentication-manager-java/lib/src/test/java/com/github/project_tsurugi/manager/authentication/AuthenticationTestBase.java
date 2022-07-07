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
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.Mockito.mockStatic;

import com.auth0.jwt.JWT;
import com.auth0.jwt.JWTCreator.Builder;
import com.auth0.jwt.JWTVerifier;
import com.auth0.jwt.algorithms.Algorithm;
import com.github.project_tsurugi.manager.authentication.db.DbSessionManager;
import com.github.project_tsurugi.manager.common.Config;
import com.github.project_tsurugi.manager.common.JwtClaims;
import java.time.Duration;
import java.util.Calendar;
import java.util.concurrent.TimeUnit;

class AuthenticationTestBase {
  /**
   * Run a token refresh test.
   *
   * @param expireTime expiration date.
   * @param expRefresh refresh expiration date.
   * @param expAvailable available period.
   * @param extendedException expected exception or null.
   */
  protected <T extends Throwable> void refreshTokenApiTestBase(final Calendar expireTime,
      final Calendar expRefresh,
      final Calendar expAvailable, Class<T> extendedException) {
    final String userName = "ut_user_name";
    final String secretKey = "ut_secret-key";
    final String issuer = "ut_issuer";
    final String audience = "ut_audience";
    final String subject = "ut_subject";
    final int extendedSec = 100;
    final int expiration = 300;
    final int expirationRefresh = 600;
    final int expirationAvailable = 900;

    // Set the date.
    var issuedTime = Calendar.getInstance();
    issuedTime.add(Calendar.MINUTE, -1); // Set 1 minuutes before the current time.

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

      String tokenStringNew = "";
      try (var mockDbSessionManager = mockStatic(DbSessionManager.class)) {
        // Run the test.
        if (extendedException == null) {
          tokenStringNew =
              assertDoesNotThrow(
                  () -> Authentication.refreshToken(tokenStringOld, extendedSec, TimeUnit.SECONDS));
          // Verify test results.
          assertFalse(tokenStringNew.isEmpty());
          // Verify that the token is correct.
          compareJwt(tokenStringNew, tokenStringOld, extendedSec, expirationRefresh);
        } else {
          assertThrows(extendedException,
              () -> Authentication.refreshToken(tokenStringOld, extendedSec, TimeUnit.SECONDS));
        }
      }
    } catch (Exception e) {
      e.printStackTrace();
      fail();
    }
  }

  /**
   * Checks if the values that make up the token are correct.
   *
   * @param token token
   * @param userName user name
   */
  protected void checkJwt(final String token, final String userName) {
    // Decode.
    var decodedToken = JWT.decode(token);

    // Cryptographic algorithms.
    var algorithm = Algorithm.HMAC256(Config.getJwtSecretKey());
    // Setting up data for token.
    JWTVerifier verifier = JWT.require(algorithm).acceptLeeway(10).build();
    // Verify the JWT token.
    assertDoesNotThrow(() -> verifier.verify(token));

    // "typ" claim
    assertEquals("JWT", decodedToken.getType());
    // "iss" claim
    assertEquals(Config.getJwtIssuer(), decodedToken.getIssuer());
    // "aud" claim
    assertEquals(Config.getJwtAudience(), decodedToken.getAudience().get(0));
    // "sub" claim
    assertEquals(Config.getJwtSubject(), decodedToken.getSubject());
    // "userName" claim
    assertEquals(userName, decodedToken.getClaim(JwtClaims.Payload.AUTH_USER_NAME).asString());
    // "iat" claim
    var issuedAt = decodedToken.getIssuedAt();
    assertNotEquals(0, issuedAt.getTime());

    // "exp" claim
    var expectedExpires = Calendar.getInstance();
    expectedExpires.setTime(decodedToken.getIssuedAt());
    expectedExpires.add(Calendar.SECOND, Config.getJwtExpiration());
    assertEquals(expectedExpires.getTime(), decodedToken.getExpiresAt());

    // "tsurugi/exp/refresh" claim
    var expectedRefresh = Calendar.getInstance();
    expectedRefresh.setTime(decodedToken.getIssuedAt());
    expectedRefresh.add(Calendar.SECOND, Config.getJwtExpirationRefresh());
    assertEquals(expectedRefresh.getTime(),
        decodedToken.getClaim(JwtClaims.Payload.EXPIRATION_REFRESH).asDate());

    // "tsurugi/exp/available" claim
    var expectedAvailable = Calendar.getInstance();
    expectedAvailable.setTime(decodedToken.getIssuedAt());
    expectedAvailable.add(Calendar.SECOND, Config.getJwtExpirationAvailable());
    assertEquals(expectedAvailable.getTime(),
        decodedToken.getClaim(JwtClaims.Payload.EXPIRATION_AVAILABLE).asDate());
  }

  /**
   * Compare if the values that make up the token are correct.
   *
   * @param newToken new token.
   * @param oldToken old token.
   * @param extended extended seconds for expiration.
   * @param extendedRefresh extended seconds for expiration refresh.
   */
  protected void compareJwt(final String newToken, final String oldToken, final int extended,
      final int extendedRefresh) {
    // Decode.
    var decodedNewToken = JWT.decode(newToken);
    // Cryptographic algorithms.
    var algorithm = Algorithm.HMAC256(Config.getJwtSecretKey());
    // Setting up data for token.
    JWTVerifier verifier = JWT.require(algorithm).acceptLeeway(10).build();
    // Verify the JWT token.
    assertDoesNotThrow(() -> verifier.verify(decodedNewToken));

    var decodedOldToken = JWT.decode(oldToken);

    // "typ" claim
    assertEquals(decodedOldToken.getType(), decodedNewToken.getType());
    // "iss" claim
    assertEquals(decodedOldToken.getIssuer(), decodedNewToken.getIssuer());
    // "aud" claim
    assertEquals(decodedOldToken.getAudience().get(0), decodedNewToken.getAudience().get(0));
    // "sub" claim
    assertEquals(decodedOldToken.getSubject(), decodedNewToken.getSubject());
    // "iat" claim
    assertEquals(decodedOldToken.getIssuedAt(), decodedNewToken.getIssuedAt());

    var nowDateTime = Calendar.getInstance();
    nowDateTime.set(Calendar.MILLISECOND, 0);

    // "exp" claim
    assertNotEquals(decodedOldToken.getExpiresAt(), decodedNewToken.getExpiresAt());
    var actualExpiresTime = Duration.between(nowDateTime.getTime().toInstant(),
        decodedNewToken.getExpiresAt().toInstant());
    assertEquals((double) extended, (double) actualExpiresTime.getSeconds(), 1.0f);

    // "tsurugi/exp/refresh" claim
    assertNotEquals(decodedOldToken.getClaim(JwtClaims.Payload.EXPIRATION_REFRESH).asInt(),
        decodedNewToken.getClaim(JwtClaims.Payload.EXPIRATION_REFRESH).asInt());
    var actualRefreshTime = Duration.between(nowDateTime.getTime().toInstant(),
        decodedNewToken.getClaim(JwtClaims.Payload.EXPIRATION_REFRESH).asDate().toInstant());
    assertEquals((double) extendedRefresh, (double) actualRefreshTime.getSeconds(), 1.0f);

    // "tsurugi/exp/available" claim
    assertEquals(decodedOldToken.getClaim(JwtClaims.Payload.EXPIRATION_AVAILABLE).asLong(),
        decodedNewToken.getClaim(JwtClaims.Payload.EXPIRATION_AVAILABLE).asLong());

    // "userName" claim
    assertEquals(decodedOldToken.getClaim(JwtClaims.Payload.AUTH_USER_NAME).asString(),
        decodedNewToken.getClaim(JwtClaims.Payload.AUTH_USER_NAME).asString());
  }
}
