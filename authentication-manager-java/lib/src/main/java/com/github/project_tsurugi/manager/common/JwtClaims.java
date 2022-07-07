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

/**
 * This class defines data related to JWT claims.
 */
public class JwtClaims {
  private JwtClaims() {}

  /**
   * This class defines data related to the JOSE header of JWT.
   */
  public static class Header {
    private Header() {}

    public static final String TYPE = "JWT";
  } // class Header

  /**
   * This class defines data related to JWT payload claims.
   */
  public static class Payload {
    private Payload() {}

    public static final String AUTH_USER_NAME = "tsurugi/auth/name";
    public static final String EXPIRATION_REFRESH = "tsurugi/exp/refresh";
    public static final String EXPIRATION_AVAILABLE = "tsurugi/exp/available";
  } // class Claim

  /**
   * This class defines leeway values for JWT claims.
   */
  public static class Leeway {
    private Leeway() {}

    public static final int ISSUED = 10;
    public static final int EXPIRATION = 10;
    public static final int EXPIRATION_REFRESH = 10;
    public static final int EXPIRATION_AVAILABLE = 10;
  } // class Leeway

} // class JwtClaims
