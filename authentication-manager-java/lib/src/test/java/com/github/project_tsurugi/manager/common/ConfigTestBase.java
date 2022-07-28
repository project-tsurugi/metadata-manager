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

import java.lang.reflect.Field;
import java.util.Collections;
import java.util.Map;
import org.junit.jupiter.api.BeforeAll;

class ConfigTestBase {
  protected static class Key {
    private Key() {}

    public static final String CONNECTION_STRING = "TSURUGI_CONNECTION_STRING_AUTH";
    public static final String JWT_CLAIM_ISSUER = "TSURUGI_JWT_CLAIM_ISS";
    public static final String JWT_CLAIM_AUDIENCE = "TSURUGI_JWT_CLAIM_AUD";
    public static final String JWT_CLAIM_SUBJECT = "TSURUGI_JWT_CLAIM_SUB";
    public static final String JWT_SECRET_KEY = "TSURUGI_JWT_SECRET_KEY";
    public static final String JWT_EXPIRATION = "TSURUGI_TOKEN_EXPIRATION";
    public static final String JWT_REFRESH_EXPIRATION = "TSURUGI_TOKEN_EXPIRATION_REFRESH";
    public static final String JWT_AVAILABLE_EXPIRATION = "TSURUGI_TOKEN_EXPIRATION_AVAILABLE";
  } // class Key

  protected static class DefaultValue {
    private DefaultValue() {}

    public static final String CONNECTION_STRING = "dbname=tsurugi";
    public static final String JWT_CLAIM_ISSUER = "authentication-manager";
    public static final String JWT_CLAIM_AUDIENCE = "metadata-manager";
    public static final String JWT_CLAIM_SUBJECT = "AuthenticationToken";
    public static final String JWT_SECRET_KEY = "qiZB8rXTdet7Z3HTaU9t2TtcpmV6FXy7";
    public static final int JWT_EXPIRATION = 300;
    public static final int JWT_REFRESH_EXPIRATION = 86400;
    public static final int JWT_AVAILABLE_EXPIRATION = (86400 * 7);
  } // class DefaultValue

  @BeforeAll
  public static void setUp() {
    ignoreWarning();
  }

  /**
   * Set environment variables.
   *
   * @param key name of the environment variable.
   * @param value value of the environment variable.
   */
  protected void setEnv(String key, int value) {
    setEnv(key, String.valueOf(value));
  }

  /**
   * Set environment variables.
   *
   * @param key name of the environment variable.
   * @param value value of the environment variable.
   */
  @SuppressWarnings("unchecked")
  protected void setEnv(String key, String value) {
    try {
      Class<?> processEnvironmentClass = Class.forName("java.lang.ProcessEnvironment");
      Field environmentField = processEnvironmentClass.getDeclaredField("theEnvironment");
      environmentField.setAccessible(true);
      Map<String, String> env = (Map<String, String>) environmentField.get(null);
      if (value != null) {
        env.put(key, value);
      } else {
        env.remove(key);
      }

      Field caseInsensitiveEnvironmentField =
          processEnvironmentClass.getDeclaredField("theCaseInsensitiveEnvironment");
      caseInsensitiveEnvironmentField.setAccessible(true);
      Map<String, String> cienv =
          (Map<String, String>) caseInsensitiveEnvironmentField.get(null);
      if (value != null) {
        cienv.put(key, value);
      } else {
        cienv.remove(key);
      }
    } catch (Exception e) {
      try {
        Class<?>[] classes = Collections.class.getDeclaredClasses();
        Map<String, String> env = System.getenv();
        for (Class<?> cl : classes) {
          if ("java.util.Collections$UnmodifiableMap".equals(cl.getName())) {
            Field field = cl.getDeclaredField("m");
            field.setAccessible(true);
            Object obj = field.get(env);
            Map<String, String> map = (Map<String, String>) obj;
            if (value != null) {
              map.put(key, value);
            } else {
              map.remove(key);
            }
          }
        }
      } catch (Exception ex) {
        // Ignore the exception and continue processing.
      }
    }
  }

  /**
   * Suppresses the appearance of warning messages.
   */
  private static void ignoreWarning() {
    try {
      Field theUnsafe = sun.misc.Unsafe.class.getDeclaredField("theUnsafe");
      theUnsafe.setAccessible(true);
      sun.misc.Unsafe u = (sun.misc.Unsafe) theUnsafe.get(null);
      Class<?> cls = Class.forName("jdk.internal.module.IllegalAccessLogger");
      Field logger = cls.getDeclaredField("logger");
      u.putObjectVolatile(cls, u.staticFieldOffset(logger), null);
    } catch (Exception e) {
      // Ignore the exception and continue processing.
    }
  }
}
