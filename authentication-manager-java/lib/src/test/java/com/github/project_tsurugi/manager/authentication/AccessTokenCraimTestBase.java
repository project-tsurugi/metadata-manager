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
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mockStatic;

import com.auth0.jwt.JWT;
import com.auth0.jwt.JWTCreator.Builder;
import com.auth0.jwt.algorithms.Algorithm;
import com.auth0.jwt.interfaces.DecodedJWT;
import com.github.project_tsurugi.manager.common.Config;
import com.github.project_tsurugi.manager.common.JwtClaims;
import java.util.Calendar;
import java.util.Date;
import org.junit.jupiter.api.BeforeAll;

class AccessTokenCraimTestBase {
  protected static Algorithm algorithm;

  protected static class CraimValue {
    protected static final String ISSUER = "ut_issuer";
    protected static final String AUDIENCE = "ut_audience";
    protected static final String SUBJECT = "ut_subject";
    protected static final String USER_NAME = "ut_user_name";

    private static Calendar issuedAt;
    private static Calendar expiresAt;
    private static Calendar expRefreshAt;
    private static Calendar expAvailableAt;

    protected static Date getIssuedAt() {
      return issuedAt.getTime();
    }

    protected static Date getExpiresAt() {
      return expiresAt.getTime();
    }

    protected static Date getExpRefreshAt() {
      return expRefreshAt.getTime();
    }

    protected static Date getExpAvailableAt() {
      return expAvailableAt.getTime();
    }
  }

  @BeforeAll
  public static void setUp() {
    // Cryptographic algorithms.
    algorithm = Algorithm.HMAC256(Config.getJwtSecretKey());

    CraimValue.issuedAt = Calendar.getInstance();

    CraimValue.expiresAt = Calendar.getInstance();
    CraimValue.expiresAt.add(Calendar.MINUTE, 5);

    CraimValue.expRefreshAt = Calendar.getInstance();
    CraimValue.expRefreshAt.add(Calendar.HOUR, 24);

    CraimValue.expAvailableAt = Calendar.getInstance();
    CraimValue.expAvailableAt.add(Calendar.HOUR, 72);
  }

  /**
   * Run a AccessToken test.
   *
   * @param mockDecodedwt mock of DecodedJWT.
   * @param expected expected value.
   * @return access token.
   */
  protected AccessToken accessTokenTestBase(DecodedJWT mockDecodedwt, boolean expected) {
    AccessToken accessToken;

    Builder jwtBuilder = JWT.create()
        .withIssuer(CraimValue.ISSUER)
        .withAudience(CraimValue.AUDIENCE)
        .withSubject(CraimValue.SUBJECT)
        .withIssuedAt(CraimValue.getIssuedAt())
        .withExpiresAt(CraimValue.getExpiresAt())
        .withClaim(JwtClaims.Payload.EXPIRATION_REFRESH, CraimValue.getExpRefreshAt())
        .withClaim(JwtClaims.Payload.EXPIRATION_AVAILABLE, CraimValue.getExpAvailableAt())
        .withClaim(JwtClaims.Payload.AUTH_USER_NAME, CraimValue.USER_NAME);

    try (var mockJwt = mockStatic(JWT.class)) {
      // Mock the method.
      mockJwt.when(() -> JWT.create()).thenCallRealMethod();
      mockJwt.when(() -> JWT.decode(any())).thenReturn(mockDecodedwt);
      mockJwt.when(() -> JWT.require(any())).thenCallRealMethod();

      // Run tests and verify results.
      accessToken = assertDoesNotThrow(() -> new AccessToken(jwtBuilder.sign(algorithm)));
      assertEquals(expected, accessToken.isValid());
      assertEquals(expected, accessToken.isAvailable());
    }

    return accessToken;
  }
}
