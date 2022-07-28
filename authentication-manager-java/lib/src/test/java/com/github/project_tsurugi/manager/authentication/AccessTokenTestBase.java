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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.mockStatic;

import com.auth0.jwt.JWT;
import com.auth0.jwt.JWTCreator.Builder;
import com.auth0.jwt.algorithms.Algorithm;
import com.github.project_tsurugi.manager.common.Config;
import com.github.project_tsurugi.manager.common.JwtClaims;
import java.util.Calendar;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.mockito.MockedStatic;

class AccessTokenTestBase {
  private static MockedStatic<Config> mockConfig = null;

  protected class TestParameters {
    private Calendar issuedAt;
    private Calendar expireAt;
    private Calendar expRefreshAt;
    private Calendar expAvailableAt;
    private String userName;

    public Calendar getIssuedAt() {
      return issuedAt;
    }

    public void setIssuedAt(Calendar value) {
      issuedAt = value;
      issuedAt.set(Calendar.MILLISECOND, 0);
    }

    public Calendar getExpireAt() {
      return expireAt;
    }

    public void setExpireAt(Calendar value) {
      expireAt = value;
      expireAt.set(Calendar.MILLISECOND, 0);
    }

    public Calendar getExpRefreshAt() {
      return expRefreshAt;
    }

    public void setExpRefreshAt(Calendar value) {
      expRefreshAt = value;
      expRefreshAt.set(Calendar.MILLISECOND, 0);
    }

    public Calendar getExpAvailableAt() {
      return expAvailableAt;
    }

    public void setExpAvailableAt(Calendar value) {
      expAvailableAt = value;
      if (expAvailableAt != null) {
        expAvailableAt.set(Calendar.MILLISECOND, 0);
      }
    }

    public String getUserName() {
      return userName;
    }

    public void setUserName(String value) {
      userName = value;
    }
  }

  @BeforeAll
  public static void setUp() {
    mockConfig = mockStatic(Config.class);
    final String secretKey = "ut_secret-key";
    final String issuer = "ut_issuer";
    final String audience = "ut_audience";
    final String subject = "ut_subject";
    final int expiration = 300;
    final int expirationRefresh = 600;
    final int expirationAvailable = 900;

    // Mock the method.
    mockConfig.when(() -> Config.getJwtIssuer()).thenReturn(issuer);
    mockConfig.when(() -> Config.getJwtAudience()).thenReturn(audience);
    mockConfig.when(() -> Config.getJwtSubject()).thenReturn(subject);
    mockConfig.when(() -> Config.getJwtExpiration()).thenReturn(expiration);
    mockConfig.when(() -> Config.getJwtExpirationRefresh()).thenReturn(expirationRefresh);
    mockConfig.when(() -> Config.getJwtExpirationAvailable()).thenReturn(expirationAvailable);
    mockConfig.when(() -> Config.getJwtSecretKey()).thenReturn(secretKey);
  }

  @AfterAll
  public static void tearDown() {
    mockConfig.close();
  }

  /**
   * Generate tokens with specified conditions.
   *
   * @param param test paramaters.
   * @return token.
   */
  protected String generateToken(final TestParameters param) {
    final String issuer = "ut_issuer";
    final String audience = "ut_audience";
    final String subject = "ut_subject";

    Builder jwtBuilder = JWT.create()
        .withIssuer(issuer)
        .withAudience(audience)
        .withSubject(subject)
        .withIssuedAt(param.getIssuedAt().getTime())
        .withExpiresAt(param.getExpireAt().getTime())
        .withClaim(JwtClaims.Payload.EXPIRATION_REFRESH, param.getExpRefreshAt().getTime())
        .withClaim(JwtClaims.Payload.AUTH_USER_NAME, param.getUserName());

    if (param.getExpAvailableAt() != null) {
      jwtBuilder.withClaim(JwtClaims.Payload.EXPIRATION_AVAILABLE,
          param.getExpAvailableAt().getTime());
    } else {
      jwtBuilder.withClaim(JwtClaims.Payload.EXPIRATION_AVAILABLE, 0);
    }

    // Cryptographic algorithms.
    var algorithm = Algorithm.HMAC256(Config.getJwtSecretKey());
    // Sign the JWT token.
    String tokenString = jwtBuilder.sign(algorithm);

    return tokenString;
  }

  /**
   * Checks if the values that make up the token are correct.
   *
   * @param accessToken AccessToken
   * @param tokenString token
   * @param param test paramaters.
   */
  protected void checkJwt(final AccessToken accessToken, final String tokenString,
      final TestParameters param) {
    // token verification.
    assertEquals(tokenString, accessToken.string());

    // "typ" claim verification.
    assertEquals("JWT", accessToken.getType());

    // "iss" claim verification.
    assertEquals(Config.getJwtIssuer(), accessToken.getIssuer());

    // "aud" claim verification.
    assertEquals(Config.getJwtAudience(), accessToken.getAudience().get(0));

    // "sub" claim verification.
    assertEquals(Config.getJwtSubject(), accessToken.getSubject());

    // "iat" claim verification.
    assertEquals(param.getIssuedAt().getTime(), accessToken.getIssuedTime());

    // "exp" claim verification.
    assertEquals(param.getExpireAt().getTime(), accessToken.getExpirationTime());

    // "tsurugi/exp/refresh" claim verification.
    assertEquals(param.getExpRefreshAt().getTime(), accessToken.getRefreshExpirationTime());

    // "tsurugi/exp/available" claim verification.
    assertEquals(param.getExpAvailableAt().getTime(), accessToken.getAvailableExpirationTime());

    // "tsurugi/auth/name" claim verification.
    assertEquals(param.getUserName(), accessToken.getUserName());
  }
}
