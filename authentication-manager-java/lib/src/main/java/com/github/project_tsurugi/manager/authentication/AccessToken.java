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

import com.auth0.jwt.JWT;
import com.auth0.jwt.algorithms.Algorithm;
import com.auth0.jwt.exceptions.JWTDecodeException;
import com.auth0.jwt.interfaces.DecodedJWT;
import com.auth0.jwt.interfaces.JWTVerifier;
import com.github.project_tsurugi.manager.common.Config;
import com.github.project_tsurugi.manager.common.JwtClaims;
import com.github.project_tsurugi.manager.common.Messages;
import com.github.project_tsurugi.manager.exceptions.InvalidTokenException;
import java.util.Calendar;
import java.util.Date;
import java.util.List;

/**
 * This class analyzes access tokens and provides validation and various information.
 */
public class AccessToken {
  private DecodedJWT decodedToken = null;

  /**
   * Constructor. Initialize with the specified token.
   *
   * @param token access token.
   * @throws InvalidTokenException if any part of the token contained an invalid jwt or JSON format
   *         of each of the jwt parts.
   */
  public AccessToken(final String token) throws InvalidTokenException {
    parse(token);
  }

  /**
   * Parse the specified token and initialize the object.
   *
   * @param token access token.
   * @throws InvalidTokenException if any part of the token contained an invalid jwt or JSON format
   *         of each of the jwt parts.
   */
  public void parse(final String token) throws InvalidTokenException {
    if (token == null) {
      throw new InvalidTokenException(Messages.INVALID_TOKEN_NULL);
    }
    try {
      decodedToken = JWT.decode(token);
    } catch (JWTDecodeException e) {
      throw new InvalidTokenException(e.getLocalizedMessage());
    }
  }

  /**
   * Get an access token.
   *
   * @return access token.
   */
  public String string() {
    return decodedToken.getToken();
  }

  /**
   * Check if the token is valid.
   *
   * @return true if valid. false if invalid.
   */
  public boolean isValid() {
    boolean result = false;
    var nowTime = Calendar.getInstance();

    // Validation of required claims.
    result = validateRequired();
    if (result) {
      try {
        // Cryptographic algorithms.
        var algorithm = Algorithm.HMAC256(Config.getJwtSecretKey());

        // Setting up data for token.
        JWTVerifier verifier = JWT.require(algorithm)
            .acceptIssuedAt(JwtClaims.Leeway.ISSUED)
            .acceptExpiresAt(JwtClaims.Leeway.EXPIRATION)
            .build();

        // Verify the JWT token.
        verifier.verify(this.string());

        result = true;
      } catch (Exception e) {
        result = false;
      }
    }

    if (result) {
      // Extract the available date.
      Date claimDate = decodedToken.getClaim(JwtClaims.Payload.EXPIRATION_AVAILABLE).asDate();
      var availableTime = Calendar.getInstance();
      availableTime.setTime(claimDate);

      // Extract the available dates and allow for leeway.
      availableTime.add(Calendar.SECOND, JwtClaims.Leeway.EXPIRATION_AVAILABLE);

      // Check if it is within the use expiration date.
      result = !availableTime.before(nowTime);
    }

    return result;
  }

  /**
   * Checks if a token is available.
   *
   * @return true if available. false if unavailable.
   */
  public boolean isAvailable() {
    boolean result = false;
    var nowTime = Calendar.getInstance();

    // Validation of required claims.
    result = validateRequired();
    if (result) {
      try {
        // Cryptographic algorithms.
        Algorithm algorithm = Algorithm.HMAC256(Config.getJwtSecretKey());

        // Setting up data for token.
        JWTVerifier verifier = JWT.require(algorithm)
            .acceptIssuedAt(JwtClaims.Leeway.ISSUED)
            .acceptExpiresAt(Integer.MAX_VALUE)
            .build();

        // Verify the JWT token.
        verifier.verify(this.string());

        result = true;
      } catch (Exception e) {
        result = false;
      }
    }

    if (result) {
      // Extract the expiration date.
      var expirationTime = Calendar.getInstance();
      expirationTime.setTime(decodedToken.getExpiresAt());
      // Extract the expiration dates and allow for leeway.
      expirationTime.add(Calendar.SECOND, JwtClaims.Leeway.EXPIRATION);

      // Extract the refresh expiration date.
      var refreshTime = Calendar.getInstance();
      refreshTime.setTime(decodedToken.getClaim(JwtClaims.Payload.EXPIRATION_REFRESH).asDate());
      // Extract the refresh expiration dates and allow for leeway.
      refreshTime.add(Calendar.SECOND, JwtClaims.Leeway.EXPIRATION_REFRESH);

      // Extract the available date.
      var availableTime = Calendar.getInstance();
      var claimValue = decodedToken.getClaim(JwtClaims.Payload.EXPIRATION_AVAILABLE);
      if (claimValue.asInt() != 0) {
        availableTime.setTime(claimValue.asDate());
        // Extract the available dates and allow for leeway.
        availableTime.add(Calendar.SECOND, JwtClaims.Leeway.EXPIRATION_AVAILABLE);
      } else {
        availableTime = nowTime;
      }

      // Extract the available dates and allow for leeway.
      // Check if it is within the use expiration date.
      if (!availableTime.before(nowTime)) {
        // Check if the expiration date or refresh period has expired.
        result = (!expirationTime.before(nowTime)) || (!refreshTime.before(nowTime));
      } else {
        result = false;
      }
    }

    return result;
  }

  /**
   * Get the value of the type claim.
   *
   * @return the Type defined or null.
   */
  public String getType() {
    return decodedToken.getType();
  }

  /**
   * Get a value of the issuer claim.
   *
   * @return the Issuer value or null.
   */
  public String getIssuer() {
    return decodedToken.getIssuer();
  }

  /**
   * Get a value of the audience claim.
   *
   * @return the Audience value or null.
   */
  public List<String> getAudience() {
    return decodedToken.getAudience();
  }

  /**
   * Get a value of the subject claim.
   *
   * @return the Subject value or null.
   */
  public String getSubject() {
    return decodedToken.getSubject();
  }

  /**
   * Get a value of the issued-at claim.
   *
   * @return the Issued At value or null.
   */
  public Date getIssuedTime() {
    return decodedToken.getIssuedAt();
  }

  /**
   * Get a value of the expires-at claim.
   *
   * @return the Expiration Time value or null.
   */
  public Date getExpirationTime() {
    return decodedToken.getExpiresAt();
  }

  /**
   * Get a value of the refresh-expiration claim.
   *
   * @return the Refresh Expiration Time value or null.
   */
  public Date getRefreshExpirationTime() {
    return decodedToken.getClaim(JwtClaims.Payload.EXPIRATION_REFRESH).asDate();
  }

  /**
   * Get a value of the available-expiration claim.
   *
   * @return the Available Expiration Time value or null.
   */
  public Date getAvailableExpirationTime() {
    return decodedToken.getClaim(JwtClaims.Payload.EXPIRATION_AVAILABLE).asDate();
  }

  /**
   * Get a value of the user-name claim.
   *
   * @return the User name value or null.
   */
  public String getUserName() {
    return decodedToken.getClaim(JwtClaims.Payload.AUTH_USER_NAME).asString();
  }

  /**
   * Check if the token is valid.
   *
   * @return true if valid. false if invalid.
   */
  private boolean validateRequired() {
    boolean result = false;

    // Check if algortihm is present ("alg").
    result = (decodedToken.getAlgorithm() != null);
    // Check if type is present ("typ").
    result = (result ? decodedToken.getType() != null : false);
    // Check if issued date is present ("iat").
    result = (result ? decodedToken.getIssuedAt() != null : false);
    // Check if expires date is present ("exp").
    result = (result ? decodedToken.getExpiresAt() != null : false);
    // Check if a payload claim is present (User Name).
    result =
        (result ? !decodedToken.getClaim(JwtClaims.Payload.AUTH_USER_NAME).isMissing() : false);
    // Check if a payload claim is present (Refresh Expiration).
    result =
        (result ? !decodedToken.getClaim(JwtClaims.Payload.EXPIRATION_REFRESH).isMissing() : false);
    // Check if a payload claim is present (Token Use Expiration).
    result = (result ? !decodedToken.getClaim(JwtClaims.Payload.EXPIRATION_AVAILABLE).isMissing()
        : false);

    return result;
  }
}
