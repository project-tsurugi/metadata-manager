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
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.github.project_tsurugi.manager.exceptions.InvalidTokenException;
import java.util.Calendar;
import org.junit.jupiter.api.Test;

class AccessTokenTest extends AccessTokenTestBase {
  /**
   * Base test of access tokens.
   */
  @Test
  public void base() {
    final String userName = "ut_user_name";

    // Set the date.
    var expireTime = Calendar.getInstance();
    expireTime.add(Calendar.MINUTE, 5); // Set 5 minuutes after the current time.
    var expRefresh = Calendar.getInstance();
    expRefresh.add(Calendar.HOUR, 24); // Set 24 hours after the current time.
    var expAvailable = Calendar.getInstance();
    expAvailable.add(Calendar.HOUR, 72); // Set 72 hours after the current time.

    // Set the TestParameters.
    TestParameters testParameters = new TestParameters();
    testParameters.setIssuedAt(Calendar.getInstance());
    testParameters.setExpireAt(expireTime);
    testParameters.setExpRefreshAt(expRefresh);
    testParameters.setExpAvailableAt(expAvailable);
    testParameters.setUserName(userName);

    // Generate an access token.
    String tokenString = generateToken(testParameters);

    // Run the test.
    var accessToken = assertDoesNotThrow(() -> new AccessToken(tokenString));

    // Verify that the token is correct
    checkJwt(accessToken, tokenString, testParameters);
    assertTrue(accessToken.isValid());
    assertTrue(accessToken.isAvailable());
  }

  /**
   * Parse test of access tokens.
   */
  @Test
  public void parse() {
    final String userName = "ut_user_name";
    final String userNameParse = "ut_user_name_parse";

    // Set the date.
    var expireTime = Calendar.getInstance();
    expireTime.add(Calendar.MINUTE, 5); // Set 5 minuutes after the current time.
    var expRefresh = Calendar.getInstance();
    expRefresh.add(Calendar.HOUR, 24); // Set 24 hours after the current time.
    var expAvailable = Calendar.getInstance();
    expAvailable.add(Calendar.HOUR, 72); // Set 72 hours after the current time.

    // Set the TestParameters.
    TestParameters testParameters = new TestParameters();
    testParameters.setIssuedAt(Calendar.getInstance());
    testParameters.setExpireAt(expireTime);
    testParameters.setExpRefreshAt(expRefresh);
    testParameters.setExpAvailableAt(expAvailable);
    testParameters.setUserName(userName);

    // Generate an access token.
    String tokenString = generateToken(testParameters);
    var accessToken = assertDoesNotThrow(() -> new AccessToken(tokenString));

    testParameters.setUserName(userNameParse);
    // Generate an access token.
    String tokenStringParse = generateToken(testParameters);
    // Run the test.
    assertDoesNotThrow(() -> accessToken.parse(tokenStringParse));

    // Verify that the token is correct.
    checkJwt(accessToken, tokenStringParse, testParameters);
    assertTrue(accessToken.isValid());
    assertTrue(accessToken.isAvailable());
  }

  /**
   * Perform a test when the parameter is a normal value. (illegal token)
   */
  @Test
  public void tokenIllegal() {
    final String tokenString = "header.payload.signature";

    // Run the test.
    assertThrows(InvalidTokenException.class, () -> new AccessToken(tokenString));
  }

  /**
   * Perform a test when the parameter is an empry value.
   */
  @Test
  public void tokenEmpty() {
    final String tokenString = "";

    // Run the test.
    assertThrows(InvalidTokenException.class, () -> new AccessToken(tokenString));
  }

  /**
   * Perform a test when the parameter is a null.
   */
  @Test
  public void tokenNull() {
    final String tokenString = null;

    // Run the test.
    assertThrows(InvalidTokenException.class, () -> new AccessToken(tokenString));
  }

  /**
   * isValid test of access tokens.
   */
  @Test
  public void isValidBase() {
    final String userName = "ut_user_name";

    // Set the date.
    var expireTime = Calendar.getInstance();
    expireTime.add(Calendar.MINUTE, 5); // Set 5 minuutes after the current time.
    var expRefresh = Calendar.getInstance();
    expRefresh.add(Calendar.HOUR, 24); // Set 24 hours after the current time.
    var expAvailable = Calendar.getInstance();
    expAvailable.add(Calendar.HOUR, 72); // Set 72 hours after the current time.

    // Set the TestParameters.
    TestParameters testParameters = new TestParameters();
    testParameters.setIssuedAt(Calendar.getInstance());
    testParameters.setExpireAt(expireTime);
    testParameters.setExpRefreshAt(expRefresh);
    testParameters.setExpAvailableAt(expAvailable);
    testParameters.setUserName(userName);

    // Generate an access token.
    String tokenString = generateToken(testParameters);
    var accessToken = assertDoesNotThrow(() -> new AccessToken(tokenString));

    // Run the test.
    assertTrue(accessToken.isValid());
  }

  /**
   * isValid test of access tokens. (expired)
   */
  @Test
  public void isValidExpired() {
    final String userName = "ut_user_name";

    // Set the date.
    var expireTime = Calendar.getInstance();
    expireTime.add(Calendar.MINUTE, -1); // Set 1 minuutes before the current time.
    var expRefresh = Calendar.getInstance();
    expRefresh.add(Calendar.HOUR, 24); // Set 24 hours after the current time.
    var expAvailable = Calendar.getInstance();
    expAvailable.add(Calendar.HOUR, 72); // Set 72 hours after the current time.

    // Set the TestParameters.
    TestParameters testParameters = new TestParameters();
    testParameters.setIssuedAt(Calendar.getInstance());
    testParameters.setExpireAt(expireTime);
    testParameters.setExpRefreshAt(expRefresh);
    testParameters.setExpAvailableAt(expAvailable);
    testParameters.setUserName(userName);

    // Generate an access token.
    String tokenString = generateToken(testParameters);
    var accessToken = assertDoesNotThrow(() -> new AccessToken(tokenString));

    // Run the test.
    assertFalse(accessToken.isValid());
  }

  /**
   * isValid test of access tokens. (refresh expired)
   */
  @Test
  public void isValidRefreshExpired() {
    final String userName = "ut_user_name";

    // Set the date.
    var expireTime = Calendar.getInstance();
    expireTime.add(Calendar.MINUTE, 5); // Set 5 minuutes after the current time.
    var expRefresh = Calendar.getInstance();
    expRefresh.add(Calendar.MINUTE, -1); // Set 1 minuutes before the current time.
    var expAvailable = Calendar.getInstance();
    expAvailable.add(Calendar.HOUR, 72); // Set 72 hours after the current time.

    // Set the TestParameters.
    TestParameters testParameters = new TestParameters();
    testParameters.setIssuedAt(Calendar.getInstance());
    testParameters.setExpireAt(expireTime);
    testParameters.setExpRefreshAt(expRefresh);
    testParameters.setExpAvailableAt(expAvailable);
    testParameters.setUserName(userName);

    // Generate an access token.
    String tokenString = generateToken(testParameters);
    var accessToken = assertDoesNotThrow(() -> new AccessToken(tokenString));

    // Run the test.
    assertTrue(accessToken.isValid());
  }

  /**
   * isValid test of access tokens. (expired & refresh expired)
   */
  @Test
  public void isValidExpiredRefreshExpired() {
    final String userName = "ut_user_name";

    // Set the date.
    var expireTime = Calendar.getInstance();
    expireTime.add(Calendar.MINUTE, -1); // Set 1 minuutes before the current time.
    var expRefresh = Calendar.getInstance();
    expRefresh.add(Calendar.MINUTE, -1); // Set 1 minuutes before the current time.
    var expAvailable = Calendar.getInstance();
    expAvailable.add(Calendar.HOUR, 72); // Set 72 hours after the current time.

    // Set the TestParameters.
    TestParameters testParameters = new TestParameters();
    testParameters.setIssuedAt(Calendar.getInstance());
    testParameters.setExpireAt(expireTime);
    testParameters.setExpRefreshAt(expRefresh);
    testParameters.setExpAvailableAt(expAvailable);
    testParameters.setUserName(userName);

    // Generate an access token.
    String tokenString = generateToken(testParameters);
    var accessToken = assertDoesNotThrow(() -> new AccessToken(tokenString));

    // Run the test.
    assertFalse(accessToken.isValid());
  }

  /**
   * isValid test of access tokens. (unavailable)
   */
  @Test
  public void isValidUnavailable() {
    final String userName = "ut_user_name";

    // Set the date.
    var expireTime = Calendar.getInstance();
    expireTime.add(Calendar.MINUTE, 5); // Set 5 minuutes after the current time.
    var expRefresh = Calendar.getInstance();
    expRefresh.add(Calendar.HOUR, 24); // Set 24 hours after the current time.
    var expAvailable = Calendar.getInstance();
    expAvailable.add(Calendar.MINUTE, -1); // Set 1 minuutes before the current time.

    // Set the TestParameters.
    TestParameters testParameters = new TestParameters();
    testParameters.setIssuedAt(Calendar.getInstance());
    testParameters.setExpireAt(expireTime);
    testParameters.setExpRefreshAt(expRefresh);
    testParameters.setExpAvailableAt(expAvailable);
    testParameters.setUserName(userName);

    // Generate an access token.
    String tokenString = generateToken(testParameters);
    var accessToken = assertDoesNotThrow(() -> new AccessToken(tokenString));

    // Run the test.
    assertFalse(accessToken.isValid());
  }

  /**
   * isValid test of access tokens. (Illegal issued at.)
   */
  @Test
  public void isValidIllegalIssuedAt() {
    final String userName = "ut_user_name";

    // Set the date.
    var issuedTime = Calendar.getInstance();
    issuedTime.add(Calendar.MINUTE, 1); // Set 1 minuutes after the current time.
    var expireTime = Calendar.getInstance();
    expireTime.add(Calendar.MINUTE, 5); // Set 5 minuutes after the current time.
    var expRefresh = Calendar.getInstance();
    expRefresh.add(Calendar.HOUR, 24); // Set 24 hours after the current time.
    var expAvailable = Calendar.getInstance();
    expAvailable.add(Calendar.HOUR, 72); // Set 72 hours after the current time.

    // Set the TestParameters.
    TestParameters testParameters = new TestParameters();
    testParameters.setIssuedAt(issuedTime);
    testParameters.setExpireAt(expireTime);
    testParameters.setExpRefreshAt(expRefresh);
    testParameters.setExpAvailableAt(expAvailable);
    testParameters.setUserName(userName);

    // Generate an access token.
    String tokenString = generateToken(testParameters);
    var accessToken = assertDoesNotThrow(() -> new AccessToken(tokenString));

    // Run the test.
    assertFalse(accessToken.isValid());
  }

  /**
   * isValid test of access tokens. (all expired)
   */
  @Test
  public void isValidAllExpired() {
    final String userName = "ut_user_name";

    // Set the date.
    var expireTime = Calendar.getInstance();
    expireTime.add(Calendar.MINUTE, -1); // Set 1 minuutes before the current time.
    var expRefresh = Calendar.getInstance();
    expRefresh.add(Calendar.MINUTE, -1); // Set 1 minuutes before the current time.
    var expAvailable = Calendar.getInstance();
    expAvailable.add(Calendar.MINUTE, -1); // Set 1 minuutes before the current time.

    // Set the TestParameters.
    TestParameters testParameters = new TestParameters();
    testParameters.setIssuedAt(Calendar.getInstance());
    testParameters.setExpireAt(expireTime);
    testParameters.setExpRefreshAt(expRefresh);
    testParameters.setExpAvailableAt(expAvailable);
    testParameters.setUserName(userName);

    // Generate an access token.
    String tokenString = generateToken(testParameters);
    var accessToken = assertDoesNotThrow(() -> new AccessToken(tokenString));

    // Run the test.
    assertFalse(accessToken.isValid());
  }

  /**
   * isAvailable test of access tokens.
   */
  @Test
  public void isAvailableBase() {
    final String userName = "ut_user_name";

    // Set the date.
    var expireTime = Calendar.getInstance();
    expireTime.add(Calendar.MINUTE, 5); // Set 5 minuutes after the current time.
    var expRefresh = Calendar.getInstance();
    expRefresh.add(Calendar.HOUR, 24); // Set 24 hours after the current time.
    var expAvailable = Calendar.getInstance();
    expAvailable.add(Calendar.HOUR, 72); // Set 72 hours after the current time.

    // Set the TestParameters.
    TestParameters testParameters = new TestParameters();
    testParameters.setIssuedAt(Calendar.getInstance());
    testParameters.setExpireAt(expireTime);
    testParameters.setExpRefreshAt(expRefresh);
    testParameters.setExpAvailableAt(expAvailable);
    testParameters.setUserName(userName);

    // Generate an access token.
    String tokenString = generateToken(testParameters);
    var accessToken = assertDoesNotThrow(() -> new AccessToken(tokenString));

    // Run the test.
    assertTrue(accessToken.isAvailable());
  }

  /**
   * isAvailable test of access tokens. (expired)
   */
  @Test
  public void isAvailableExpired() {
    final String userName = "ut_user_name";

    // Set the date.
    var expireTime = Calendar.getInstance();
    expireTime.add(Calendar.MINUTE, -1); // Set 1 minuutes before the current time.
    var expRefresh = Calendar.getInstance();
    expRefresh.add(Calendar.HOUR, 24); // Set 24 hours after the current time.
    var expAvailable = Calendar.getInstance();
    expAvailable.add(Calendar.HOUR, 72); // Set 72 hours after the current time.

    // Set the TestParameters.
    TestParameters testParameters = new TestParameters();
    testParameters.setIssuedAt(Calendar.getInstance());
    testParameters.setExpireAt(expireTime);
    testParameters.setExpRefreshAt(expRefresh);
    testParameters.setExpAvailableAt(expAvailable);
    testParameters.setUserName(userName);

    // Generate an access token.
    String tokenString = generateToken(testParameters);
    var accessToken = assertDoesNotThrow(() -> new AccessToken(tokenString));

    // Run the test.
    assertTrue(accessToken.isAvailable());
  }

  /**
   * isAvailable test of access tokens. (refresh expired)
   */
  @Test
  public void isAvailableRefreshExpired() {
    final String userName = "ut_user_name";

    // Set the date.
    var expireTime = Calendar.getInstance();
    expireTime.add(Calendar.MINUTE, 5); // Set 5 minuutes after the current time.
    var expRefresh = Calendar.getInstance();
    expRefresh.add(Calendar.MINUTE, -1); // Set 1 minuutes before the current time.
    var expAvailable = Calendar.getInstance();
    expAvailable.add(Calendar.HOUR, 72); // Set 72 hours after the current time.

    // Set the TestParameters.
    TestParameters testParameters = new TestParameters();
    testParameters.setIssuedAt(Calendar.getInstance());
    testParameters.setExpireAt(expireTime);
    testParameters.setExpRefreshAt(expRefresh);
    testParameters.setExpAvailableAt(expAvailable);
    testParameters.setUserName(userName);

    // Generate an access token.
    String tokenString = generateToken(testParameters);
    var accessToken = assertDoesNotThrow(() -> new AccessToken(tokenString));

    // Run the test.
    assertTrue(accessToken.isAvailable());
  }

  /**
   * isAvailable test of access tokens. (expired & refresh expired)
   */
  @Test
  public void isAvailableExpiredRefreshExpired() {
    final String userName = "ut_user_name";

    // Set the date.
    var expireTime = Calendar.getInstance();
    expireTime.add(Calendar.MINUTE, -1); // Set 1 minuutes before the current time.
    var expRefresh = Calendar.getInstance();
    expRefresh.add(Calendar.MINUTE, -1); // Set 1 minuutes before the current time.
    var expAvailable = Calendar.getInstance();
    expAvailable.add(Calendar.HOUR, 72); // Set 72 hours after the current time.

    // Set the TestParameters.
    TestParameters testParameters = new TestParameters();
    testParameters.setIssuedAt(Calendar.getInstance());
    testParameters.setExpireAt(expireTime);
    testParameters.setExpRefreshAt(expRefresh);
    testParameters.setExpAvailableAt(expAvailable);
    testParameters.setUserName(userName);

    // Generate an access token.
    String tokenString = generateToken(testParameters);
    var accessToken = assertDoesNotThrow(() -> new AccessToken(tokenString));

    // Run the test.
    assertFalse(accessToken.isAvailable());
  }

  /**
   * isAvailable test of access tokens. (Indefinite)
   */
  @Test
  public void isAvailableIndefinite() {
    final String userName = "ut_user_name";

    // Set the date.
    var expireTime = Calendar.getInstance();
    expireTime.add(Calendar.MINUTE, 5); // Set 5 minuutes after the current time.
    var expRefresh = Calendar.getInstance();
    expRefresh.add(Calendar.HOUR, 24); // Set 24 hours after the current time.
    var expAvailable = Calendar.getInstance();
    expAvailable.add(Calendar.HOUR, 72); // Set 72 hours after the current time.

    // Set the TestParameters.
    TestParameters testParameters = new TestParameters();
    testParameters.setIssuedAt(Calendar.getInstance());
    testParameters.setExpireAt(expireTime);
    testParameters.setExpRefreshAt(expRefresh);
    testParameters.setExpAvailableAt(null);
    testParameters.setUserName(userName);

    // Generate an access token.
    String tokenString = generateToken(testParameters);
    var accessToken = assertDoesNotThrow(() -> new AccessToken(tokenString));

    // Run the test.
    assertTrue(accessToken.isAvailable());
  }

  /**
   * isAvailable test of access tokens. (unavailable)
   */
  @Test
  public void isAvailableUnavailable() {
    final String userName = "ut_user_name";

    // Set the date.
    var expireTime = Calendar.getInstance();
    expireTime.add(Calendar.MINUTE, 5); // Set 5 minuutes after the current time.
    var expRefresh = Calendar.getInstance();
    expRefresh.add(Calendar.HOUR, 24); // Set 24 hours after the current time.
    var expAvailable = Calendar.getInstance();
    expAvailable.add(Calendar.MINUTE, -1); // Set 1 minuutes before the current time.

    // Set the TestParameters.
    TestParameters testParameters = new TestParameters();
    testParameters.setIssuedAt(Calendar.getInstance());
    testParameters.setExpireAt(expireTime);
    testParameters.setExpRefreshAt(expRefresh);
    testParameters.setExpAvailableAt(expAvailable);
    testParameters.setUserName(userName);

    // Generate an access token.
    String tokenString = generateToken(testParameters);
    var accessToken = assertDoesNotThrow(() -> new AccessToken(tokenString));

    // Run the test.
    assertFalse(accessToken.isAvailable());
  }

  /**
   * isAvailable test of access tokens. (Illegal issued at.)
   */
  @Test
  public void isAvailableIllegalIssuedAt() {
    final String userName = "ut_user_name";

    // Set the date.
    var issuedTime = Calendar.getInstance();
    issuedTime.add(Calendar.MINUTE, 1); // Set 1 minuutes after the current time.
    var expireTime = Calendar.getInstance();
    expireTime.add(Calendar.MINUTE, 5); // Set 5 minuutes after the current time.
    var expRefresh = Calendar.getInstance();
    expRefresh.add(Calendar.HOUR, 24); // Set 24 hours after the current time.
    var expAvailable = Calendar.getInstance();
    expAvailable.add(Calendar.HOUR, 72); // Set 72 hours after the current time.

    // Set the TestParameters.
    TestParameters testParameters = new TestParameters();
    testParameters.setIssuedAt(issuedTime);
    testParameters.setExpireAt(expireTime);
    testParameters.setExpRefreshAt(expRefresh);
    testParameters.setExpAvailableAt(expAvailable);
    testParameters.setUserName(userName);

    // Generate an access token.
    String tokenString = generateToken(testParameters);
    var accessToken = assertDoesNotThrow(() -> new AccessToken(tokenString));

    // Run the test.
    assertFalse(accessToken.isAvailable());
  }

  /**
   * isAvailable test of access tokens. (all expired)
   */
  @Test
  public void isAvailableAllExpired() {
    final String userName = "ut_user_name";

    // Set the date.
    var expireTime = Calendar.getInstance();
    expireTime.add(Calendar.MINUTE, -1); // Set 1 minuutes before the current time.
    var expRefresh = Calendar.getInstance();
    expRefresh.add(Calendar.MINUTE, -1); // Set 1 minuutes before the current time.
    var expAvailable = Calendar.getInstance();
    expAvailable.add(Calendar.MINUTE, -1); // Set 1 minuutes before the current time.

    // Set the TestParameters.
    TestParameters testParameters = new TestParameters();
    testParameters.setIssuedAt(Calendar.getInstance());
    testParameters.setExpireAt(expireTime);
    testParameters.setExpRefreshAt(expRefresh);
    testParameters.setExpAvailableAt(expAvailable);
    testParameters.setUserName(userName);

    // Generate an access token.
    String tokenString = generateToken(testParameters);
    var accessToken = assertDoesNotThrow(() -> new AccessToken(tokenString));

    // Run the test.
    assertFalse(accessToken.isAvailable());
  }

}
