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

import java.util.HashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * This class manages configurations.
 */
public class Config {
  private Config() {}

  private static class Key {
    private Key() {}

    /**
     * The name of an OS environment variable for a Connection Strings.
     */
    public static final String CONNECTION_STRING = "TSURUGI_CONNECTION_STRING_AUTH";

    /**
     * The name of an OS environment variable for the JWT issuer claim value.
     */
    public static final String JWT_CLAIM_ISSUER = "TSURUGI_JWT_CLAIM_ISS";

    /**
     * The name of an OS environment variable for the JWT audience claim value.
     */
    public static final String JWT_CLAIM_AUDIENCE = "TSURUGI_JWT_CLAIM_AUD";

    /**
     * The name of an OS environment variable for the JWT subject claim value.
     */
    public static final String JWT_CLAIM_SUBJECT = "TSURUGI_JWT_CLAIM_SUB";

    /**
     * The name of an OS environment variable for the JWT secret key.
     */
    public static final String JWT_SECRET_KEY = "TSURUGI_JWT_SECRET_KEY";

    /**
     * The name of an OS environment variable for the JWT expiration.
     */
    public static final String JWT_EXPIRATION = "TSURUGI_TOKEN_EXPIRATION";

    /**
     * The name of an OS environment variable for the JWT refresh expiration.
     */
    public static final String JWT_REFRESH_EXPIRATION = "TSURUGI_TOKEN_EXPIRATION_REFRESH";

    /**
     * The name of an OS environment variable for the JWT use expiration.
     */
    public static final String JWT_AVAILABLE_EXPIRATION = "TSURUGI_TOKEN_EXPIRATION_AVAILABLE";

  } // class Key

  private static class DefaultValue {
    private DefaultValue() {}

    /**
     * Default Connection Strings. By default, several libpq functions parse this default connection
     * strings to obtain connection parameters.
     */
    public static final String CONNECTION_STRING = "dbname=tsurugi";

    /**
     * Default value of the JWT issuer claim.
     */
    public static final String JWT_CLAIM_ISSUER = "authentication-manager";

    /**
     * Default value of the JWT audience claim.
     */
    public static final String JWT_CLAIM_AUDIENCE = "metadata-manager";

    /**
     * Default value of the JWT subject claim.
     */
    public static final String JWT_CLAIM_SUBJECT = "AuthenticationToken";

    /**
     * Default value of the JWT secret key.
     */
    public static final String JWT_SECRET_KEY = "qiZB8rXTdet7Z3HTaU9t2TtcpmV6FXy7";

    /**
     * Default value of the JWT expiration. (Unit: seconds)
     */
    public static final int JWT_EXPIRATION = 300;

    /**
     * Default value of the JWT refresh expiration. (Unit: seconds)
     */
    public static final int JWT_REFRESH_EXPIRATION = 86400;

    /**
     * Default value of the JWT use expiration. (Unit: seconds)
     */
    public static final int JWT_AVAILABLE_EXPIRATION = (86400 * 7);

  } // class DefaultValue

  private static final String REGEX_TIME = "^(\\d+)(s?|min|h|d)$";
  private static final int REGEX_TIME_VALUE_POS = 1;
  private static final int REGEX_TIME_UNIT_POS = 2;

  /**
   * Information for date/time unit conversion.
   */
  private static final Map<String, Integer> conversionTimeUnit;

  static {
    conversionTimeUnit = new HashMap<>();
    conversionTimeUnit.put("", 1);
    conversionTimeUnit.put("s", 1);
    conversionTimeUnit.put("min", 60);
    conversionTimeUnit.put("h", 3600);
    conversionTimeUnit.put("d", 86400);
  }

  /**
   * Gets connection string for authentication.
   *
   * @return Connection String.
   */
  public static String getConnectionString() {
    String envValue = System.getenv(Key.CONNECTION_STRING);
    return (envValue != null ? envValue : DefaultValue.CONNECTION_STRING);
  }

  /**
   * Gets JWT issuer value.
   *
   * @return JWT issuer value.
   */
  public static String getJwtIssuer() {
    String envValue = System.getenv(Key.JWT_CLAIM_ISSUER);
    return (envValue != null ? envValue : DefaultValue.JWT_CLAIM_ISSUER);
  }

  /**
   * Gets JWT audience value.
   *
   * @return JWT audience value.
   */
  public static String getJwtAudience() {
    String envValue = System.getenv(Key.JWT_CLAIM_AUDIENCE);
    return (envValue != null ? envValue : DefaultValue.JWT_CLAIM_AUDIENCE);
  }

  /**
   * Gets JWT subject value.
   *
   * @return JWT subject value.
   */
  public static String getJwtSubject() {
    String envValue = System.getenv(Key.JWT_CLAIM_SUBJECT);
    return (envValue != null ? envValue : DefaultValue.JWT_CLAIM_SUBJECT);
  }

  /**
   * Gets JWT secret key.
   *
   * @return JWT secret key.
   */
  public static String getJwtSecretKey() {
    String envValue = System.getenv(Key.JWT_SECRET_KEY);
    return (envValue != null ? envValue : DefaultValue.JWT_SECRET_KEY);
  }

  /**
   * Gets JWT expiration value.
   *
   * @return JWT expiration value.
   */
  public static int getJwtExpiration() {
    return getEnvironmentExpiration(Key.JWT_EXPIRATION,
        DefaultValue.JWT_EXPIRATION);
  }

  /**
   * Gets JWT refresh expiration.
   *
   * @return JWT refresh expiration.
   */
  public static int getJwtExpirationRefresh() {
    return getEnvironmentExpiration(Key.JWT_REFRESH_EXPIRATION,
        DefaultValue.JWT_REFRESH_EXPIRATION);
  }

  /**
   * Gets JWT available expiration.
   *
   * @return JWT available expiration.
   */
  public static int getJwtExpirationAvailable() {
    return getEnvironmentExpiration(Key.JWT_AVAILABLE_EXPIRATION,
        DefaultValue.JWT_AVAILABLE_EXPIRATION);
  }

  /**
   * Get the value of the environment variable related to the expiration date.
   *
   * @return Environment variable value.
   */
  private static int getEnvironmentExpiration(final String keyName, final int defaultValue) {
    int resultValue = defaultValue;

    String envValue = System.getenv(keyName);
    if (envValue != null) {
      Pattern pattern = Pattern.compile(REGEX_TIME);
      Matcher matcher = pattern.matcher(envValue);

      // Convert time by dividing values and units.
      if (matcher.find()) {
        // Convert to numeric values.
        Integer envValueNum = null;
        envValueNum = Integer.parseInt(matcher.group(REGEX_TIME_VALUE_POS));

        // Convert units of time.
        Integer conversionValue = conversionTimeUnit.get(matcher.group(REGEX_TIME_UNIT_POS));
        resultValue = envValueNum * conversionValue;
      }
    }

    return resultValue;
  }

} // class Config
