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
import com.auth0.jwt.JWTCreator.Builder;
import com.auth0.jwt.algorithms.Algorithm;
import com.github.project_tsurugi.manager.authentication.db.DbSessionManager;
import com.github.project_tsurugi.manager.common.Config;
import com.github.project_tsurugi.manager.common.JwtClaims;
import com.github.project_tsurugi.manager.common.Messages;
import com.github.project_tsurugi.manager.exceptions.AuthenticationException;
import com.github.project_tsurugi.manager.exceptions.DbAccessException;
import com.github.project_tsurugi.manager.exceptions.InternalException;
import com.github.project_tsurugi.manager.exceptions.InvalidTokenException;
import java.util.Calendar;
import java.util.concurrent.TimeUnit;

/**
 * This class provides functions related to user authentication.
 */
public class Authentication {
  private Authentication() {}

  /**
   * Authentication is performed based on the specified user name and password. If authentication is
   * successful, the generated access token is returned.
   *
   * @param userName user name to authenticate.
   * @param password passward.
   * @return generated access token.
   * @throws AuthenticationException if the authentication failed.
   * @throws DbAccessException if the connection to the database failed.
   */
  public static String authUser(final String userName, final String password)
      throws AuthenticationException, DbAccessException {

    // Gets the connection string of the environment variable.
    String connectionString = Config.getConnectionString();

    // Authentication.
    DbSessionManager.attemptConnection(connectionString, userName, password);

    // If no exception is raised, an access token is generated.
    String tokenString = generateToken(userName);

    return tokenString;
  }

  /**
   * Authentication is performed based on the specified user name and password. If authentication is
   * successful, the generated access token is returned.
   *
   * @param userName user name to authenticate.
   * @param password passward.
   * @return generated access token.
   * @throws AuthenticationException if the authentication failed.
   * @throws DbAccessException if an internal error occurs.
   */
  public static String authUser(final String connectionString, final String userName,
      final String password)
      throws AuthenticationException, DbAccessException {

    // Authentication.
    DbSessionManager.attemptConnection(connectionString, userName, password);

    // If no exception is raised, an access token is generated.
    String tokenString = generateToken(userName);

    return tokenString;
  }

  /**
   * Extends the expiration date of the specified token for a specified time. The expiration date
   * that can be extended shall not exceed a certain period of time from the date and time the token
   * is issued, in which case the maximum expiration date shall be used as the expiration date.
   *
   * @param tokenString access token.
   * @param extendTime time to extend.
   * @param timeUnit unit of time to be extended.
   * @return newly generated access token.
   * @throws InvalidTokenException if the connection to the database failed.
   */
  public static String refreshToken(final String tokenString, long extendTime, TimeUnit timeUnit)
      throws InvalidTokenException {
    // Parses tokens.
    var accessToken = new AccessToken(tokenString);

    // Check if the token is available.
    if (!accessToken.isAvailable()) {
      throw new InvalidTokenException(Messages.INVALID_TOKEN_NOT_AVAILABLE);
    }

    var availableTime = Calendar.getInstance();
    availableTime.setTime(accessToken.getAvailableExpirationTime());

    // Check again if the token is within the refresh time limit as the condition is different
    // from is_available().
    var nowTime = Calendar.getInstance();
    var refreshExpirationTime = Calendar.getInstance();
    refreshExpirationTime.setTime(accessToken.getRefreshExpirationTime());
    refreshExpirationTime.add(Calendar.SECOND, JwtClaims.Leeway.EXPIRATION_REFRESH);

    if (refreshExpirationTime.before(nowTime)) {
      // Time limit is over.
      throw new InvalidTokenException(Messages.REFRESH_EXPIRED);
    }

    // Setting up data for token.
    Builder jwtBuilder = JWT.create();

    // Copy the issuer payload claim of the current token.
    if (accessToken.getIssuer() != null) {
      jwtBuilder.withIssuer(accessToken.getIssuer());
    }

    // Copy the audience payload claim of the current token.
    if (accessToken.getAudience() != null) {
      for (String value : accessToken.getAudience()) {
        jwtBuilder.withAudience(value);
      }
    }

    // Copy the subject payload claim of the current token.
    if (accessToken.getSubject() != null) {
      jwtBuilder.withSubject(accessToken.getSubject());
    }

    // Copy the issue date/time payload claim of the current token.
    jwtBuilder.withIssuedAt(accessToken.getIssuedTime());

    // Copy the available date/time payload claim of the current token.
    jwtBuilder.withClaim(JwtClaims.Payload.EXPIRATION_AVAILABLE,
        accessToken.getAvailableExpirationTime());

    // Copy the user name payload claim of the current token.
    jwtBuilder.withClaim(JwtClaims.Payload.AUTH_USER_NAME, accessToken.getUserName());

    // Extension of expiration date.
    var expiresTime = Calendar.getInstance();
    expiresTime.add(Calendar.SECOND, (int) timeUnit.toSeconds(extendTime));

    // Check the extended expiration limit.
    if (expiresTime.after(availableTime)) {
      // If the limit is exceeded, revise to the longest expiration date.
      expiresTime = availableTime;
    }
    jwtBuilder.withExpiresAt(expiresTime.getTime());

    // Reset the refresh expiration date.
    var newRefreshExpirationTime = Calendar.getInstance();
    newRefreshExpirationTime.add(Calendar.SECOND, Config.getJwtExpirationRefresh());

    // Check the extended expiration limit.
    if (newRefreshExpirationTime.after(availableTime)) {
      // If the limit is exceeded, revise to the longest expiration date.
      newRefreshExpirationTime = availableTime;
    }
    jwtBuilder.withClaim(JwtClaims.Payload.EXPIRATION_REFRESH, newRefreshExpirationTime.getTime());

    String token = null;
    try {
      // Cryptographic algorithms.
      var algorithm = Algorithm.HMAC256(Config.getJwtSecretKey());

      // Sign the JWT token.
      token = jwtBuilder.sign(algorithm);
    } catch (Exception e) {
      throw new InternalException(e.getMessage());
    }

    return token;
  }

  /**
   * Generate an access token.
   *
   * @param userName authenticated user name.
   * @return generated access token.
   */
  private static String generateToken(final String userName) {
    var issuedTime = Calendar.getInstance();
    var expireTime = (Calendar) issuedTime.clone();
    var expRefresh = (Calendar) issuedTime.clone();
    var expAvailable = (Calendar) issuedTime.clone();

    // Set the expiration date.
    expireTime.add(Calendar.SECOND, Config.getJwtExpiration());
    expRefresh.add(Calendar.SECOND, Config.getJwtExpirationRefresh());
    expAvailable.add(Calendar.SECOND, Config.getJwtExpirationAvailable());

    Builder jwtBuilder = JWT.create()
        .withIssuer(Config.getJwtIssuer())
        .withAudience(Config.getJwtAudience())
        .withSubject(Config.getJwtSubject())
        .withIssuedAt(issuedTime.getTime())
        .withExpiresAt(expireTime.getTime())
        .withClaim(JwtClaims.Payload.EXPIRATION_REFRESH, expRefresh.getTime())
        .withClaim(JwtClaims.Payload.AUTH_USER_NAME, userName);

    // Setting up available date.
    if (Config.getJwtExpirationAvailable() != 0) {
      jwtBuilder.withClaim(JwtClaims.Payload.EXPIRATION_AVAILABLE, expAvailable.getTime());
    } else {
      jwtBuilder.withClaim(JwtClaims.Payload.EXPIRATION_AVAILABLE, 0);
    }

    String token = null;
    try {
      // Cryptographic algorithms.
      var algorithm = Algorithm.HMAC256(Config.getJwtSecretKey());

      // Sign the JWT token.
      token = jwtBuilder.sign(algorithm);
    } catch (Exception e) {
      throw new InternalException(e.getMessage());
    }

    return token;
  }

} // class Authentication
