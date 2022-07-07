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
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.auth0.jwt.JWT;
import com.auth0.jwt.JWTCreator.Builder;
import com.auth0.jwt.interfaces.DecodedJWT;
import com.github.project_tsurugi.manager.common.JwtClaims;
import org.junit.jupiter.api.Test;

class AccessTokenCraimTest extends AccessTokenCraimTestBase {
  /**
   * Test in the absence of an "alg" claim (required).
   */
  @Test
  public void claimItemsAlg() {
    // Mock the class.
    var mockDecodedwt = mock(DecodedJWT.class);

    // Mock the method.
    when(mockDecodedwt.getAlgorithm()).thenReturn(null);

    // Run tests and verify results.
    accessTokenTestBase(mockDecodedwt, false);
  }

  /**
   * Test in the absence of an "typ" claim (required).
   */
  @Test
  public void claimItemsTyp() {
    // Mock the class.
    var mockDecodedwt = mock(DecodedJWT.class);

    // Mock the method.
    when(mockDecodedwt.getAlgorithm()).thenReturn("HS256");
    when(mockDecodedwt.getType()).thenReturn(null);

    // Run tests and verify results.
    accessTokenTestBase(mockDecodedwt, false);
  }

  /**
   * Test in the absence of an "iss" claim (optional).
   */
  @Test
  public void claimItemsIss() {
    Builder jwtBuilder = JWT.create()
        .withAudience(CraimValue.AUDIENCE)
        .withSubject(CraimValue.SUBJECT)
        .withIssuedAt(CraimValue.getIssuedAt())
        .withExpiresAt(CraimValue.getExpiresAt())
        .withClaim(JwtClaims.Payload.EXPIRATION_REFRESH, CraimValue.getExpRefreshAt())
        .withClaim(JwtClaims.Payload.EXPIRATION_AVAILABLE, CraimValue.getExpAvailableAt())
        .withClaim(JwtClaims.Payload.AUTH_USER_NAME, CraimValue.USER_NAME);

    // Run tests and verify results.
    var accessToken = assertDoesNotThrow(() -> new AccessToken(jwtBuilder.sign(algorithm)));
    assertNull(accessToken.getIssuer());
    assertTrue(accessToken.isValid());
    assertTrue(accessToken.isAvailable());
  }

  /**
   * Test in the absence of an "aud" claim (optional).
   */
  @Test
  public void claimItemsAud() {
    Builder jwtBuilder = JWT.create()
        .withIssuer(CraimValue.ISSUER)
        .withSubject(CraimValue.SUBJECT)
        .withIssuedAt(CraimValue.getIssuedAt())
        .withExpiresAt(CraimValue.getExpiresAt())
        .withClaim(JwtClaims.Payload.EXPIRATION_REFRESH, CraimValue.getExpRefreshAt())
        .withClaim(JwtClaims.Payload.EXPIRATION_AVAILABLE, CraimValue.getExpAvailableAt())
        .withClaim(JwtClaims.Payload.AUTH_USER_NAME, CraimValue.USER_NAME);

    // Run tests and verify results.
    var accessToken = assertDoesNotThrow(() -> new AccessToken(jwtBuilder.sign(algorithm)));
    assertNull(accessToken.getAudience());
    assertTrue(accessToken.isValid());
    assertTrue(accessToken.isAvailable());
  }

  /**
   * Test in the absence of an "sub" claim (optional).
   */
  @Test
  public void claimItemsSub() {
    Builder jwtBuilder = JWT.create()
        .withIssuer(CraimValue.ISSUER)
        .withAudience(CraimValue.AUDIENCE)
        .withIssuedAt(CraimValue.getIssuedAt())
        .withExpiresAt(CraimValue.getExpiresAt())
        .withClaim(JwtClaims.Payload.EXPIRATION_REFRESH, CraimValue.getExpRefreshAt())
        .withClaim(JwtClaims.Payload.EXPIRATION_AVAILABLE, CraimValue.getExpAvailableAt())
        .withClaim(JwtClaims.Payload.AUTH_USER_NAME, CraimValue.USER_NAME);

    // Run tests and verify results.
    var accessToken = assertDoesNotThrow(() -> new AccessToken(jwtBuilder.sign(algorithm)));
    assertNull(accessToken.getSubject());
    assertTrue(accessToken.isValid());
    assertTrue(accessToken.isAvailable());
  }

  /**
   * Test in the absence of an "iat" claim (required).
   */
  @Test
  public void claimItemsIat() {
    Builder jwtBuilder = JWT.create()
        .withIssuer(CraimValue.ISSUER)
        .withAudience(CraimValue.AUDIENCE)
        .withSubject(CraimValue.SUBJECT)
        .withExpiresAt(CraimValue.getExpiresAt())
        .withClaim(JwtClaims.Payload.EXPIRATION_REFRESH, CraimValue.getExpRefreshAt())
        .withClaim(JwtClaims.Payload.EXPIRATION_AVAILABLE, CraimValue.getExpAvailableAt())
        .withClaim(JwtClaims.Payload.AUTH_USER_NAME, CraimValue.USER_NAME);

    // Run tests and verify results.
    var accessToken = assertDoesNotThrow(() -> new AccessToken(jwtBuilder.sign(algorithm)));
    assertNull(accessToken.getIssuedTime());
    assertFalse(accessToken.isValid());
    assertFalse(accessToken.isAvailable());
  }

  /**
   * Test in the absence of an "exp" claim (required).
   */
  @Test
  public void claimItemsExp() {
    Builder jwtBuilder = JWT.create()
        .withIssuer(CraimValue.ISSUER)
        .withAudience(CraimValue.AUDIENCE)
        .withSubject(CraimValue.SUBJECT)
        .withIssuedAt(CraimValue.getIssuedAt())
        .withClaim(JwtClaims.Payload.EXPIRATION_REFRESH, CraimValue.getExpRefreshAt())
        .withClaim(JwtClaims.Payload.EXPIRATION_AVAILABLE, CraimValue.getExpAvailableAt())
        .withClaim(JwtClaims.Payload.AUTH_USER_NAME, CraimValue.USER_NAME);

    // Run tests and verify results.
    var accessToken = assertDoesNotThrow(() -> new AccessToken(jwtBuilder.sign(algorithm)));
    assertNull(accessToken.getExpirationTime());
    assertFalse(accessToken.isValid());
    assertFalse(accessToken.isAvailable());
  }

  /**
   * Test in the absence of an "tsurugi/exp/refresh" claim (required).
   */
  @Test
  public void claimItemsExpRefresh() {
    Builder jwtBuilder = JWT.create()
        .withIssuer(CraimValue.ISSUER)
        .withAudience(CraimValue.AUDIENCE)
        .withSubject(CraimValue.SUBJECT)
        .withIssuedAt(CraimValue.getIssuedAt())
        .withExpiresAt(CraimValue.getExpiresAt())
        .withClaim(JwtClaims.Payload.EXPIRATION_AVAILABLE, CraimValue.getExpAvailableAt())
        .withClaim(JwtClaims.Payload.AUTH_USER_NAME, CraimValue.USER_NAME);

    // Run tests and verify results.
    var accessToken = assertDoesNotThrow(() -> new AccessToken(jwtBuilder.sign(algorithm)));
    assertNull(accessToken.getRefreshExpirationTime());
    assertFalse(accessToken.isValid());
    assertFalse(accessToken.isAvailable());
  }

  /**
   * Test in the absence of an "tsurugi/exp/available" claim (required).
   */
  @Test
  public void claimItemsExpAvailable() {
    Builder jwtBuilder = JWT.create()
        .withIssuer(CraimValue.ISSUER)
        .withAudience(CraimValue.AUDIENCE)
        .withSubject(CraimValue.SUBJECT)
        .withIssuedAt(CraimValue.getIssuedAt())
        .withExpiresAt(CraimValue.getExpiresAt())
        .withClaim(JwtClaims.Payload.EXPIRATION_REFRESH, CraimValue.getExpRefreshAt())
        .withClaim(JwtClaims.Payload.AUTH_USER_NAME, CraimValue.USER_NAME);

    // Run tests and verify results.
    var accessToken = assertDoesNotThrow(() -> new AccessToken(jwtBuilder.sign(algorithm)));
    assertNull(accessToken.getAvailableExpirationTime());
    assertFalse(accessToken.isValid());
    assertFalse(accessToken.isAvailable());
  }

  /**
   * Test in the absence of an "tsurugi/auth/name" claim (required).
   */
  @Test
  public void claimItemsAuthName() {
    Builder jwtBuilder = JWT.create()
        .withIssuer(CraimValue.ISSUER)
        .withAudience(CraimValue.AUDIENCE)
        .withSubject(CraimValue.SUBJECT)
        .withIssuedAt(CraimValue.getIssuedAt())
        .withExpiresAt(CraimValue.getExpiresAt())
        .withClaim(JwtClaims.Payload.EXPIRATION_REFRESH, CraimValue.getExpRefreshAt())
        .withClaim(JwtClaims.Payload.EXPIRATION_AVAILABLE, CraimValue.getExpAvailableAt());

    // Run tests and verify results.
    var accessToken = assertDoesNotThrow(() -> new AccessToken(jwtBuilder.sign(algorithm)));
    assertNull(accessToken.getUserName());
    assertFalse(accessToken.isValid());
    assertFalse(accessToken.isAvailable());
  }
}
