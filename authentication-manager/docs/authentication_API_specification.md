【Project-Tsurugi Internal User Only】

# 認証管理基盤 API仕様書

2022.06.13 KCC 初版

## 目次

- [認証管理基盤 API仕様書](#認証管理基盤-api仕様書)
  - [目次](#目次)
  - [機能概要](#機能概要)
    - [コンポーネント関係図](#コンポーネント関係図)
    - [利用イメージ](#利用イメージ)
      - [C++インタフェース使用](#cインタフェース使用)
      - [Javaインタフェース使用](#javaインタフェース使用)
  - [ユーザ認証機能](#ユーザ認証機能)
    - [C++ I/F](#c-if)
      - [auth_userメソッド](#auth_userメソッド)
    - [Java I/F](#java-if)
      - [authUserメソッド](#authuserメソッド)
  - [トークンリフレッシュ機能](#トークンリフレッシュ機能)
    - [C++ I/F](#c-if-1)
      - [refresh_tokenメソッド](#refresh_tokenメソッド)
    - [Java I/F](#java-if-1)
      - [refreshTokenメソッド](#refreshtokenメソッド)
  - [トークン情報提供機能](#トークン情報提供機能)
    - [C++ I/F](#c-if-2)
      - [showメソッド](#showメソッド)
      - [is_validメソッド](#is_validメソッド)
      - [is_availableメソッド](#is_availableメソッド)
      - [user_nameメソッド](#user_nameメソッド)
      - [issued_timeメソッド](#issued_timeメソッド)
      - [expiration_timeメソッド](#expiration_timeメソッド)
      - [refresh_expiration_timeメソッド](#refresh_expiration_timeメソッド)
      - [use_expiration_timeメソッド](#use_expiration_timeメソッド)
    - [Java I/F](#java-if-2)
      - [showメソッド](#showメソッド-1)
      - [isValidメソッド](#isvalidメソッド)
      - [isAvailableメソッド](#isavailableメソッド)
      - [getUserNameメソッド](#getusernameメソッド)
      - [getIssuedTimeメソッド](#getissuedtimeメソッド)
      - [getExpirationTimeメソッド](#getexpirationtimeメソッド)
      - [getRefreshExpirationTimeメソッド](#getrefreshexpirationtimeメソッド)
      - [getUseExpirationTimeメソッド](#getuseexpirationtimeメソッド)
  - [認可情報提供機能](#認可情報提供機能)
    - [C++ I/F](#c-if-3)
      - [get_aclsメソッド](#get_aclsメソッド)
      - [TABLE_ACL_NODE定数](#table_acl_node定数)
  - [付録](#付録)
    - [コンフィギュレーション](#コンフィギュレーション)
      - [環境変数](#環境変数)
        - [認証管理基盤](#認証管理基盤)
        - [メタデータ管理基盤](#メタデータ管理基盤)
    - [アクセストークン](#アクセストークン)
      - [トークンの有効性検証](#トークンの有効性検証)
      - [トークンの使用可否検証](#トークンの使用可否検証)
      - [トークンの期限](#トークンの期限)
        - [各期限の状態におけるトークンの状態](#各期限の状態におけるトークンの状態)
        - [リフレッシュに伴うトークン状態イメージ](#リフレッシュに伴うトークン状態イメージ)
      - [アクセストークンサンプル](#アクセストークンサンプル)

---

## 機能概要

認証管理基盤は、ユーザ認証に関連する下記機能を提供する。

|#|機能|概要|コンポーネント名|C++ I/F|Java I/F|
|--:|---|---|---|:-:|:-:|
|1.|ユーザ認証機能|ユーザ名およびパスワードを用いた認証を行い、アクセストークンを発行する機能|認証管理基盤|○|○|
|2.|トークンリフレッシュ機能|アクセストークンの有効期限を延長する機能|認証管理基盤|○|○|
|3.|トークン情報提供機能|アクセストークンの有効性検証および各種情報（認証ユーザ名、有効期限など）を提供する機能|認証管理基盤|○|○|
|4.|認可情報提供機能|認証済みユーザのテーブル認可情報を提供する機能|メタデータ管理基盤|○|<span style="color: #bbb">×</span>|

### コンポーネント関係図

```mermaid
graph LR
  classDef request stroke-width:0px, fill:#ecf5e7
  classDef manager stroke-width:2px, stroke:#f66, fill:#fee
  classDef library color:#777, fill:#eee, stroke-dasharray:2 2

  Authentication((認証要求<br>など)):::request
  Authorization(("認可情報<br>要求")):::request
  ComponentAuthJava["User Component (Java)"]
  ComponentAuthCpp["User Component (C++)"]
  ComponentMeta["User Component (C++)"]
  subgraph Manager
    AuthMngJava("認証管理基盤 (Java)"):::manager
    AuthMngCpp("認証管理基盤 (C++)"):::manager
    MetaMng("メタデータ管理基盤 (C++)"):::manager
  end
  DB[("PostgreSQL")]:::library

  Authentication .-> ComponentAuthJava
  ComponentAuthJava --"[Java I/F]"--- AuthMngJava
  AuthMngJava .-  DB

  Authentication .-> ComponentAuthCpp
  ComponentAuthCpp --"[C++ I/F]"--- AuthMngCpp
  AuthMngCpp .- DB

  Authorization .-> ComponentMeta
  ComponentMeta --"[C++ I/F]"--- MetaMng
  MetaMng ...- DB
```

### 利用イメージ

#### C++インタフェース使用

  ```mermaid
  graph LR
    classDef manager stroke-width:2px, stroke:#f66, fill:#fee
    Client["クライアント"]
    subgraph "Frontend Server"
      Tsubakuro["User Component"]
    end
    subgraph "Backend Server"
      Jogasaki["OLTP"]
      AuthMngCpp("認証管理基盤 (C++)"):::manager
      MetaMng("メタデータ管理基盤 (C++)"):::manager
      DB[("PostgreSQL")]
    end

    Client --> Tsubakuro
    Tsubakuro --- Jogasaki
      Jogasaki --"[認証要求]"--> AuthMngCpp
      AuthMngCpp -."[認証]".-> DB
      Jogasaki --"[認可情報要求]"--> MetaMng
      MetaMng -."[データアクセス]".-> DB
  ```

#### Javaインタフェース使用

  ```mermaid
  graph LR
    classDef manager stroke-width:2px, stroke:#f66, fill:#fee
    Client["クライアント"]
    subgraph "Frontend Server"
      Tsubakuro["User Component"]
      AuthMngJava("認証管理基盤 (Java)"):::manager
    end
    subgraph "Backend Server"
      Jogasaki["OLTP"]
      MetaMng("メタデータ管理基盤 (C++)"):::manager
      DB[("PostgreSQL")]
    end

    Client --> Tsubakuro
    Tsubakuro --"[認証要求]"---> AuthMngJava
      AuthMngJava -."[認証]".-> DB
    Tsubakuro --- Jogasaki
      Jogasaki --"[認可情報要求]"--> MetaMng
      MetaMng -."[データアクセス]".-> DB
  ```

---

## ユーザ認証機能

### C++ I/F

#### auth_userメソッド

**ライブラリ**　　：認証管理基盤ライブラリ(`libmanager-authentication.so`)  
**ヘッダファイル**：`include/manager/authentication/authentication.h`  
**名前空間**　　　：`manager::authentication`  
**クラス**　　　　：`Authentication`  

```cpp
namespace manager::authentication {
  class Authentication {
   public:
    static ErrorCode auth_user(std::string_view user_name, std::string_view password, std::string* token);
    static ErrorCode auth_user(std::string_view connection_string, std::string_view user_name, std::string_view password, std::string* token);
  }
}
```

- **概要**  
  指定された認証情報（ユーザ名およびパスワード）を用いて認証を試行する。  
  認証先データベースを指定した場合は、指定された認証先データベースを使用して認証する。  
  アクセストークン(`token`)が指定されている場合(`nullptr`以外の場合)は、アクセストークンを発行し格納する。

- **引数**
  - `connection_string`  
    認証先データベースへの接続文字列。  
  - `userName`  
    認証するユーザ名。
  - `password`  
    パスワード。
  - `token`  
    発行されたアクセストークン（詳細は「[アクセストークン](#アクセストークン)」を参照）を格納するためのポインタ。

- **戻り値**
  - `ErrorCode::OK`  
    入力された接続情報が有効である場合。
  - `ErrorCode::AUTHENTICATION_FAILURE`  
    入力された接続情報が無効である場合。
  - `ErrorCode::CONNECTION_FAILURE`  
    データベースへの接続に失敗した場合。

- **例外**  
  なし

- **使用例**

  ```cpp
  #include <iostream>

  #include "manager/authentication/authentication.h"
  
  using manager::authentication::Authentication;
  using manager::authentication::ErrorCode;
  ```

  ```cpp
  // 接続文字列は認証管理基盤の環境変数を使用するケース
  std::string user_name = "user01";
  std::string password = "1q2w3e4r";
  std::string token_string = "";

  ErrorCode result = Authentication::auth_user(user_name, password, &token_string);
  if (result == ErrorCode::OK) {
    // 認証成功
    std::cout << token_string << std::endl;
  } else if (result == ErrorCode::AUTHENTICATION_FAILURE) {
    // 認証エラー
    std::cout << "認証エラー" << std::endl;
  } else if (result == ErrorCode::CONNECTION_FAILURE) {
    // データベース接続エラー
    std::cout << "データベース接続エラー" << std::endl;
  } 
  ```

  ```cpp
  // 個別に接続文字列を指定するケース
  std::string connection_string = "postgresql://localhost:5432/tsurugi";
  std::string user_name = "user01";
  std::string password = "1q2w3e4r";
  std::string token_string = "";

  ErrorCode result = Authentication::auth_user(connection_string, user_name, password, &token_string);
  if (result == ErrorCode::OK) {
    // 認証成功
    std::cout << token_string << std::endl;
  } else if (result == ErrorCode::AUTHENTICATION_FAILURE) {
    // 認証エラー
    std::cout << "認証エラー" << std::endl;
  } else if (result == ErrorCode::CONNECTION_FAILURE) {
    // データベース接続エラー
    std::cout << "データベース接続エラー" << std::endl;
  } 
  ```

  ```text
  eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJhdXRoZW50aWNhdGlvbi1tYW5hZ2VyIiwiYXVkIjoibWV0YWRhdGEtbWFuYWdlciIsInN1YiI6IkF1dGhlbnRpY2F0aW9uVG9rZW4iLCJpYXQiOjE2NDkwNTA2MzEsImV4cCI6MTY0OTA1MDkzMSwidHN1cnVnaS9leHAvcmVmcmVzaCI6MTY0OTEzNzAzMSwidHN1cnVnaS9leHAvdXNlIjoxNjQ5NjU1NDMxLCJ0c3VydWdpL2F1dGgvbmFtZSI6InRzdXJ1Z2lfdXNlciJ9.YRtavvDPqJ3CaG1ZavXsB4eNi5vdvQkE5-1X2uMfOhk
  ```

### Java I/F

#### authUserメソッド

**ライブラリ**：認証管理基盤ライブラリ(`tsurugi-manager-authentication.jar`)  
**パッケージ**：`com.github.project_tsurugi.manager.authentication`  
**クラス**　　：`Authentication`

```java
package com.github.project_tsurugi.manager.authentication;

public class Authentication {
  public static String authUser(final String userName, final String password)
      throws AuthenticationException, DBAccessException { ... }
  public static String authUser(final String connectionString, final String userName, final String password)
      throws AuthenticationException, DBAccessException { ... }
}
```

- **概要**  
  指定された認証情報（ユーザ名およびパスワード）を用いて認証を試行する。  
  認証先データベースを指定した場合は、指定された認証先データベースを使用して認証する。  
  認証成功した場合はアクセストークンを発行する。

- **引数**
  - `connection_string`  
    認証先データベースへの接続文字列。
  - `userName`  
    認証するユーザ名。
  - `password`  
    パスワード。

- **戻り値**
  - アクセストークン  
    ※詳細は「[アクセストークン](#アクセストークン)」を参照。

- **例外**
  - `AuthenticationException`  
    指定された認証情報（ユーザ名もしくはパスワード）が無効である場合。
  - `DBAccessException`  
    データベースへの接続に失敗した場合。
  - `InternalException`  
    予期しない内部エラーが発生した場合。

- **使用例**

  ```Java
  import com.github.project_tsurugi.manager.authentication.Authentication;
  import com.github.project_tsurugi.manager.exceptions.AuthenticationException;
  import com.github.project_tsurugi.manager.exceptions.DBAccessException;
  ```

  ```Java
  // 接続文字列は認証管理基盤の環境変数を使用するケース
  String userName = "user01";
  String password = "1q2w3e4r";

  String tokenString = "";
  try {
    tokenString = Authentication.authUser(userName, password);
    // 認証成功
    System.out.println(tokenString);
  } catch (AuthenticationException e) {
    // 認証エラー
    System.out.println("認証エラー");
  } catch (DBAccessException e) {
    // データベース接続エラー
    System.out.println("データベース接続エラー");
  }
  ```

  ```Java
  // 個別に接続文字列を指定するケース
  String connectionString = "postgresql://localhost:5432/tsurugi";
  String userName = "user01";
  String password = "1q2w3e4r";

  String tokenString = "";
  try {
    tokenString = Authentication.authUser(connectionString, userName, password);
    // 認証成功
    System.out.println(tokenString);
  } catch (AuthenticationException e) {
    // 認証エラー
    System.out.println("認証エラー");
  } catch (DBAccessException e) {
    // データベース接続エラー
    System.out.println("データベース接続エラー");
  }
  ```

  ```text
  eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJhdXRoZW50aWNhdGlvbi1tYW5hZ2VyIiwiYXVkIjoibWV0YWRhdGEtbWFuYWdlciIsInN1YiI6IkF1dGhlbnRpY2F0aW9uVG9rZW4iLCJpYXQiOjE2NDkwNTA2MzEsImV4cCI6MTY0OTA1MDkzMSwidHN1cnVnaS9leHAvcmVmcmVzaCI6MTY0OTEzNzAzMSwidHN1cnVnaS9leHAvdXNlIjoxNjQ5NjU1NDMxLCJ0c3VydWdpL2F1dGgvbmFtZSI6InRzdXJ1Z2lfdXNlciJ9.YRtavvDPqJ3CaG1ZavXsB4eNi5vdvQkE5-1X2uMfOhk
  ```

---

## トークンリフレッシュ機能

### C++ I/F

#### refresh_tokenメソッド

**ライブラリ**　　：認証管理基盤ライブラリ(`libmanager-authentication.so`)  
**ヘッダファイル**：`include/manager/authentication/authentication.h`  
**名前空間**　　　：`manager::authentication`  
**クラス**　　　　：`Authentication`  

```cpp
namespace manager::authentication {
  class Authentication {
   public:
    static ErrorCode refresh_token(std::string& token, std::chrono::seconds extend_time);
  }
}
```

- **概要**  
  指定されたアクセストークンの有効期限を指定時間延長する。  
  延長可能なアクセストークンは、リフレッシュ期限およびトークン使用期限内のトークンのみとする。  
  有効期限の延長および各種期限に関する詳細は「[トークンの期限](#トークンの期限)」を参照。

- **引数**
  - `token`  
    アクセストークン。
    正常に有効期限が更新された場合は、延長後のアクセストークン（詳細は「[アクセストークン](#アクセストークン)」を参照）が設定される。
  - `extend_time`  
    延長する時間。  
    `std::chrono::seconds`、`std::chrono::minutes`、`std::chrono::hours`等のタイプエイリアスが使用可能。

- **戻り値**
  - `ErrorCode::OK`  
    正常に有効期限が延長された場合。
  - `ErrorCode::INVALID_PARAMETER`  
    無効なトークンが指定された場合。

- **例外**  
  なし

- **使用例**

  ```cpp
  #include <iomanip>
  #include <iostream>

  #include "manager/authentication/access_token.h"
  #include "manager/authentication/authentication.h"

  using manager::authentication::AccessToken;
  using manager::authentication::Authentication;
  using manager::authentication::ErrorCode;
  ```

  ```cpp
  std::string token_string = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJhdXRoZW50aWNhdGlvbi1tYW5hZ2VyIiwiYXVkIjoibWV0YWRhdGEtbWFuYWdlciIsInN1YiI6IkF1dGhlbnRpY2F0aW9uVG9rZW4iLCJpYXQiOjE2NDkwNTA2MzEsImV4cCI6MTY0OTA1MDkzMSwidHN1cnVnaS9leHAvcmVmcmVzaCI6MTY0OTEzNzAzMSwidHN1cnVnaS9leHAvdXNlIjoxNjQ5NjU1NDMxLCJ0c3VydWdpL2F1dGgvbmFtZSI6InRzdXJ1Z2lfdXNlciJ9.YRtavvDPqJ3CaG1ZavXsB4eNi5vdvQkE5-1X2uMfOhk";
  AccessToken access_token(token_string);

  std::time_t expiration_time = access_token.expiration_time();
  std::cout << "Before: " << std::put_time(std::localtime(&expiration_time), "%Y/%m/%d %H:%M:%S") << std::endl;

  // 有効期限を30分延長
  ErrorCode result = Authentication::refresh_token(token_string, std::chrono::minutes{30});
  if (result == ErrorCode::OK) {
    // リフレッシュ成功
    access_token = token_string;
    expiration_time = access_token.expiration_time();
    std::cout << "After: " << std::put_time(std::localtime(&expiration_time), "%Y/%m/%d %H:%M:%S") << std::endl;
  } else if (result == ErrorCode::INVALID_PARAMETER) {
    // 無効なトークン
    std::cout << "無効なトークン" << std::endl;
  } 
  ```

  ```text
  Before: 2022/04/04 14:42:11
  After: 2022/04/05 18:28:51
  ```

### Java I/F

#### refreshTokenメソッド

**ライブラリ**：認証管理基盤ライブラリ(`tsurugi-manager-authentication.jar`)  
**パッケージ**：`com.github.project_tsurugi.manager.authentication`  
**クラス**　　：`Authentication`

```java
package com.github.project_tsurugi.manager.authentication;

public class Authentication {
  public static String refreshToken(final String token, long extendTime, TimeUnit timeUnit) {...}
}
```

- **概要**  
  指定されたアクセストークンの有効期限を指定時間延長する。  
  延長可能なアクセストークンは、リフレッシュ期限およびトークン使用期限内のトークンのみとする。  
  有効期限の延長および各種期限に関する詳細は「[トークンの期限](#トークンの期限)」を参照。

- **引数**
  - `token`  
    アクセストークン。
  - `extendTime`  
    延長する時間。
  - `timeUnit`  
    延長する時間の単位（`HOURS`、`MINUTES`、`SECONDS`等）。

- **戻り値**
  - 期限が延長されたアクセストークン  
    ※詳細は「[アクセストークン](#アクセストークン)」を参照。

- **例外**
  - `InvalidTokenException`  
    無効なトークンが指定された場合。
  - `InternalException`  
    予期しない内部エラーが発生した場合。

- **使用例**

  ```Java
  import com.github.project_tsurugi.manager.authentication.AccessToken;
  import com.github.project_tsurugi.manager.authentication.Authentication;
  import com.github.project_tsurugi.manager.exceptions.InvalidTokenException;

  import java.text.SimpleDateFormat;
  import java.util.Date;
  import java.util.TimeZone;
  ```

  ```Java
  SimpleDateFormat jstSdf = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss");
  jstSdf.setTimeZone(TimeZone.getTimeZone("Asia/Tokyo"));

  String tokenString = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJhdXRoZW50aWNhdGlvbi1tYW5hZ2VyIiwiYXVkIjoibWV0YWRhdGEtbWFuYWdlciIsInN1YiI6IkF1dGhlbnRpY2F0aW9uVG9rZW4iLCJpYXQiOjE2NDkwNTA2MzEsImV4cCI6MTY0OTA1MDkzMSwidHN1cnVnaS9leHAvcmVmcmVzaCI6MTY0OTEzNzAzMSwidHN1cnVnaS9leHAvdXNlIjoxNjQ5NjU1NDMxLCJ0c3VydWdpL2F1dGgvbmFtZSI6InRzdXJ1Z2lfdXNlciJ9.YRtavvDPqJ3CaG1ZavXsB4eNi5vdvQkE5-1X2uMfOhk";

  AccessToken accessToken = new AccessToken(tokenString);
  Date expirationTime = accessToken.getExpirationTime();
  if (!expirationTime) {
    System.out.println("Before: " + jstSdf.format(expirationTime));
  }
 
  try {
    // 有効期限を30分延長
    String newToken = Authentication.refreshToken(tokenString, 30, TimeUnit.MINUTES);

    // リフレッシュ成功
    accessToken = new AccessToken(newToken);
    expirationTime = accessToken.getExpirationTime();
    if (!expirationTime) {
      System.out.println("After: " + jstSdf.format(expirationTime));
    }
  } catch (InvalidTokenException e) {
    // 無効なトークン
    System.out.println("無効なトークン");
  }
  ```

  ```text
  Before: 2022/04/04 14:42:11
  After: 2022/04/05 18:28:51
  ```

---

## トークン情報提供機能

### C++ I/F

#### showメソッド

**ライブラリ**　　：認証管理基盤ライブラリ(`libmanager-authentication.so`)  
**ヘッダファイル**：`include/manager/authentication/access_token.h`  
**名前空間**　　　：`manager::authentication`  
**クラス**　　　　：`AccessToken`  

```cpp
namespace manager::authentication {
  class AccessToken {
   public:
    std::string show();
  }
}
```

- **概要**  
  アクセストークンを返す。

- **引数**
  - なし

- **戻り値**
  - アクセストークン

- **例外**  
  なし

- **使用例**

  ```cpp
  #include <iostream>

  #include "manager/authentication/access_token.h"

  using manager::authentication::AccessToken;
  ```

  ```cpp
  std::string token_string = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJhdXRoZW50aWNhdGlvbi1tYW5hZ2VyIiwiYXVkIjoibWV0YWRhdGEtbWFuYWdlciIsInN1YiI6IkF1dGhlbnRpY2F0aW9uVG9rZW4iLCJpYXQiOjE2NDkwNTA2MzEsImV4cCI6MTY0OTA1MDkzMSwidHN1cnVnaS9leHAvcmVmcmVzaCI6MTY0OTEzNzAzMSwidHN1cnVnaS9leHAvdXNlIjoxNjQ5NjU1NDMxLCJ0c3VydWdpL2F1dGgvbmFtZSI6InRzdXJ1Z2lfdXNlciJ9.YRtavvDPqJ3CaG1ZavXsB4eNi5vdvQkE5-1X2uMfOhk";
  AccessToken access_token(token_string);
 
  std::cout << access_token.show() << std::endl;
  ```

  ```text
  eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJhdXRoZW50aWNhdGlvbi1tYW5hZ2VyIiwiYXVkIjoibWV0YWRhdGEtbWFuYWdlciIsInN1YiI6IkF1dGhlbnRpY2F0aW9uVG9rZW4iLCJpYXQiOjE2NDkwNTA2MzEsImV4cCI6MTY0OTA1MDkzMSwidHN1cnVnaS9leHAvcmVmcmVzaCI6MTY0OTEzNzAzMSwidHN1cnVnaS9leHAvdXNlIjoxNjQ5NjU1NDMxLCJ0c3VydWdpL2F1dGgvbmFtZSI6InRzdXJ1Z2lfdXNlciJ9.YRtavvDPqJ3CaG1ZavXsB4eNi5vdvQkE5-1X2uMfOhk
  ```

#### is_validメソッド

**ライブラリ**　　：認証管理基盤ライブラリ(`libmanager-authentication.so`)  
**ヘッダファイル**：`include/manager/authentication/access_token.h`  
**名前空間**　　　：`manager::authentication`  
**クラス**　　　　：`AccessToken`  

```cpp
namespace manager::authentication {
  class AccessToken {
   public:
    bool is_valid();
  }
}
```

- **概要**  
  アクセストークンの有効性を検証する。  
  検証内容は「[トークンの有効性検証](#トークンの有効性検証)」を参照。

- **引数**
  - なし

- **戻り値**
  - トークン状態
    - `true`  
      アクセストークンが有効である場合。
    - `false`  
      アクセストークンが無効である場合。

- **例外**  
  なし

- **使用例**

  ```cpp
  #include <iostream>

  #include "manager/authentication/access_token.h"

  using manager::authentication::AccessToken;
  ```

  ```cpp
  std::string token_string = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJhdXRoZW50aWNhdGlvbi1tYW5hZ2VyIiwiYXVkIjoibWV0YWRhdGEtbWFuYWdlciIsInN1YiI6IkF1dGhlbnRpY2F0aW9uVG9rZW4iLCJpYXQiOjE2NDkwNTA2MzEsImV4cCI6MTY0OTA1MDkzMSwidHN1cnVnaS9leHAvcmVmcmVzaCI6MTY0OTEzNzAzMSwidHN1cnVnaS9leHAvdXNlIjoxNjQ5NjU1NDMxLCJ0c3VydWdpL2F1dGgvbmFtZSI6InRzdXJ1Z2lfdXNlciJ9.YRtavvDPqJ3CaG1ZavXsB4eNi5vdvQkE5-1X2uMfOhk";
  AccessToken access_token(token_string);

  std::cout << std::boolalpha << access_token.is_valid() << std::endl;
  ```

  ```text
  true
  ```

#### is_availableメソッド

**ライブラリ**　　：認証管理基盤ライブラリ(`libmanager-authentication.so`)  
**ヘッダファイル**：`include/manager/authentication/access_token.h`  
**名前空間**　　　：`manager::authentication`  
**クラス**　　　　：`AccessToken`  

```cpp
namespace manager::authentication {
  class AccessToken {
   public:
    bool is_available();
  }
}
```

- **概要**  
  アクセストークンが使用可能か否かを検証する。  
  検証内容は「[トークンの使用可否検証](#トークンの使用可否検証)」を参照。

- **引数**
  - なし

- **戻り値**
  - トークン状態
    - `true`  
      アクセストークンが使用可能である場合。
    - `false`  
      アクセストークンが使用不可である場合。

- **例外**  
  なし

- **使用例**

  ```cpp
  #include <iostream>

  #include "manager/authentication/access_token.h"

  using manager::authentication::AccessToken;
  ```

  ```cpp
  std::string token_string = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJhdXRoZW50aWNhdGlvbi1tYW5hZ2VyIiwiYXVkIjoibWV0YWRhdGEtbWFuYWdlciIsInN1YiI6IkF1dGhlbnRpY2F0aW9uVG9rZW4iLCJpYXQiOjE2NDkwNTA2MzEsImV4cCI6MTY0OTA1MDkzMSwidHN1cnVnaS9leHAvcmVmcmVzaCI6MTY0OTEzNzAzMSwidHN1cnVnaS9leHAvdXNlIjoxNjQ5NjU1NDMxLCJ0c3VydWdpL2F1dGgvbmFtZSI6InRzdXJ1Z2lfdXNlciJ9.YRtavvDPqJ3CaG1ZavXsB4eNi5vdvQkE5-1X2uMfOhk";
  AccessToken access_token(token_string);

  std::cout << std::boolalpha << access_token.is_available() << std::endl;
  ```

  ```text
  true
  ```

#### user_nameメソッド

**ライブラリ**　　：認証管理基盤ライブラリ(`libmanager-authentication.so`)  
**ヘッダファイル**：`include/manager/authentication/access_token.h`  
**名前空間**　　　：`manager::authentication`  
**クラス**　　　　：`AccessToken`  

```cpp
namespace manager::authentication {
  class AccessToken {
   public:
    std::string user_name();
  }
}
```

- **概要**  
  ユーザ名を返す。  
  未初期化および不正なトークン（有効期限切れ等のデコード可能なトークンは除く）の場合は空文字を返す。

- **引数**
  - なし

- **戻り値**
  - ユーザ名

- **例外**  
  なし

- **使用例**

  ```cpp
  #include <iostream>

  #include "manager/authentication/access_token.h"

  using manager::authentication::AccessToken;
  ```

  ```cpp
  std::string token_string = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJhdXRoZW50aWNhdGlvbi1tYW5hZ2VyIiwiYXVkIjoibWV0YWRhdGEtbWFuYWdlciIsInN1YiI6IkF1dGhlbnRpY2F0aW9uVG9rZW4iLCJpYXQiOjE2NDkwNTA2MzEsImV4cCI6MTY0OTA1MDkzMSwidHN1cnVnaS9leHAvcmVmcmVzaCI6MTY0OTEzNzAzMSwidHN1cnVnaS9leHAvdXNlIjoxNjQ5NjU1NDMxLCJ0c3VydWdpL2F1dGgvbmFtZSI6InRzdXJ1Z2lfdXNlciJ9.YRtavvDPqJ3CaG1ZavXsB4eNi5vdvQkE5-1X2uMfOhk";
  AccessToken access_token(token_string);

  std::string user_name = access_token.user_name();
  if (!user_name.empty()) {
    // ユーザ名の取得成功
    std::cout << user_name << std::endl;
  } else {
    // ユーザ名の取得エラー
    std::cout << "不正なトークン（ユーザ名取得エラー）" << std::endl;
  }
  ```

  ```text
  tsurugi_user
  ```

#### issued_timeメソッド

**ライブラリ**　　：認証管理基盤ライブラリ(`libmanager-authentication.so`)  
**ヘッダファイル**：`include/manager/authentication/access_token.h`  
**名前空間**　　　：`manager::authentication`  
**クラス**　　　　：`AccessToken`  

```cpp
namespace manager::authentication {
  class AccessToken {
   public:
    std::time_t issued_time();
  }
}
```

- **概要**  
  アクセストークンの発行日時を返す。  
  未初期化および不正なトークン（有効期限切れ等のデコード可能なトークンは除く）の場合は`0`を返す。

- **引数**
  - なし

- **戻り値**
  - 発行日時（エポックタイム）

- **例外**  
  なし

- **使用例**

  ```cpp
  #include <iomanip>
  #include <iostream>

  #include "manager/authentication/access_token.h"

  using manager::authentication::AccessToken;
  ```

  ```cpp
  std::string token_string = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJhdXRoZW50aWNhdGlvbi1tYW5hZ2VyIiwiYXVkIjoibWV0YWRhdGEtbWFuYWdlciIsInN1YiI6IkF1dGhlbnRpY2F0aW9uVG9rZW4iLCJpYXQiOjE2NDkwNTA2MzEsImV4cCI6MTY0OTA1MDkzMSwidHN1cnVnaS9leHAvcmVmcmVzaCI6MTY0OTEzNzAzMSwidHN1cnVnaS9leHAvdXNlIjoxNjQ5NjU1NDMxLCJ0c3VydWdpL2F1dGgvbmFtZSI6InRzdXJ1Z2lfdXNlciJ9.YRtavvDPqJ3CaG1ZavXsB4eNi5vdvQkE5-1X2uMfOhk";
  AccessToken access_token(token_string);

  std::time_t issued_time = access_token.issued_time();
  if (issued_time != 0) {
    // 発行日時の取得成功
    std::cout << std::put_time(std::localtime(&issued_time), "%Y/%m/%d %H:%M:%S") << std::endl;
  } else {
    // 発行日時の取得エラー
    std::cout << "不正なトークン（発行日時取得エラー）" << std::endl;
  }
  ```

  ```text
  2022/04/04 14:37:11
  ```

#### expiration_timeメソッド

**ライブラリ**　　：認証管理基盤ライブラリ(`libmanager-authentication.so`)  
**ヘッダファイル**：`include/manager/authentication/access_token.h`  
**名前空間**　　　：`manager::authentication`  
**クラス**　　　　：`AccessToken`  

```cpp
namespace manager::authentication {
  class AccessToken {
   public:
    std::time_t expiration_time();
  }
}
```

- **概要**  
  アクセストークンの有効期限を返す。  
  未初期化および不正なトークン（有効期限切れ等のデコード可能なトークンは除く）の場合は`0`を返す。

- **引数**
  - なし

- **戻り値**
  - 有効期限（エポックタイム）

- **例外**  
  なし

- **使用例**

  ```cpp
  #include <iomanip>
  #include <iostream>

  #include "manager/authentication/access_token.h"

  using manager::authentication::AccessToken;
  ```

  ```cpp
  std::string token_string = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJhdXRoZW50aWNhdGlvbi1tYW5hZ2VyIiwiYXVkIjoibWV0YWRhdGEtbWFuYWdlciIsInN1YiI6IkF1dGhlbnRpY2F0aW9uVG9rZW4iLCJpYXQiOjE2NDkwNTA2MzEsImV4cCI6MTY0OTA1MDkzMSwidHN1cnVnaS9leHAvcmVmcmVzaCI6MTY0OTEzNzAzMSwidHN1cnVnaS9leHAvdXNlIjoxNjQ5NjU1NDMxLCJ0c3VydWdpL2F1dGgvbmFtZSI6InRzdXJ1Z2lfdXNlciJ9.YRtavvDPqJ3CaG1ZavXsB4eNi5vdvQkE5-1X2uMfOhk";
  AccessToken access_token(token_string);

  std::time_t expiration_time = access_token.expiration_time();
  if (expiration_time != 0) {
    // 有効期限の取得成功
    std::cout << std::put_time(std::localtime(&expiration_time), "%Y/%m/%d %H:%M:%S") << std::endl;
  } else {
    // 有効期限の取得エラー
    std::cout << "不正なトークン（有効期限取得エラー）" << std::endl;
  }
  ```

  ```text
  2022/04/04 14:42:11
  ```

#### refresh_expiration_timeメソッド

**ライブラリ**　　：認証管理基盤ライブラリ(`libmanager-authentication.so`)  
**ヘッダファイル**：`include/manager/authentication/access_token.h`  
**名前空間**　　　：`manager::authentication`  
**クラス**　　　　：`AccessToken`  

```cpp
namespace manager::authentication {
  class AccessToken {
   public:
    std::time_t refresh_expiration_time();
  }
}
```

- **概要**  
  アクセストークンのリフレッシュ期限を返す。  
  未初期化および不正なトークン（有効期限切れ等のデコード可能なトークンは除く）の場合は`0`を返す。

- **引数**
  - なし

- **戻り値**
  - リフレッシュ期限（エポックタイム）

- **例外**  
  なし

- **使用例**

  ```cpp
  #include <iomanip>
  #include <iostream>

  #include "manager/authentication/access_token.h"

  using manager::authentication::AccessToken;
  ```

  ```cpp
  std::string token_string = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJhdXRoZW50aWNhdGlvbi1tYW5hZ2VyIiwiYXVkIjoibWV0YWRhdGEtbWFuYWdlciIsInN1YiI6IkF1dGhlbnRpY2F0aW9uVG9rZW4iLCJpYXQiOjE2NDkwNTA2MzEsImV4cCI6MTY0OTA1MDkzMSwidHN1cnVnaS9leHAvcmVmcmVzaCI6MTY0OTEzNzAzMSwidHN1cnVnaS9leHAvdXNlIjoxNjQ5NjU1NDMxLCJ0c3VydWdpL2F1dGgvbmFtZSI6InRzdXJ1Z2lfdXNlciJ9.YRtavvDPqJ3CaG1ZavXsB4eNi5vdvQkE5-1X2uMfOhk";
  AccessToken access_token(token_string);

  std::time_t refresh_expiration_time = access_token.refresh_expiration_time();
  if (refresh_expiration_time != 0) {
    // リフレッシュ期限の取得成功
    std::cout << std::put_time(std::localtime(&refresh_expiration_time), "%Y/%m/%d %H:%M:%S") << std::endl;
  } else {
    // リフレッシュ期限の取得エラー
    std::cout << "不正なトークン（リフレッシュ期限取得エラー）" << std::endl;
  }
  ```

  ```text
  2022-04-05 14:37:11
  ```

#### use_expiration_timeメソッド

**ライブラリ**　　：認証管理基盤ライブラリ(`libmanager-authentication.so`)  
**ヘッダファイル**：`include/manager/authentication/access_token.h`  
**名前空間**　　　：`manager::authentication`  
**クラス**　　　　：`AccessToken`  

```cpp
namespace manager::authentication {
  class AccessToken {
   public:
    std::time_t use_expiration_time();
  }
}
```

- **概要**  
  アクセストークンの使用期限を返す。  
  未初期化および不正なトークン（有効期限切れ等のデコード可能なトークンは除く）の場合は`0`を返す。

- **引数**
  - なし

- **戻り値**
  - トークン使用期限（エポックタイム）

- **例外**  
  なし

- **使用例**

  ```cpp
  #include <iomanip>
  #include <iostream>

  #include "manager/authentication/access_token.h"

  using manager::authentication::AccessToken;
  ```

  ```cpp
  std::string token_string = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJhdXRoZW50aWNhdGlvbi1tYW5hZ2VyIiwiYXVkIjoibWV0YWRhdGEtbWFuYWdlciIsInN1YiI6IkF1dGhlbnRpY2F0aW9uVG9rZW4iLCJpYXQiOjE2NDkwNTA2MzEsImV4cCI6MTY0OTA1MDkzMSwidHN1cnVnaS9leHAvcmVmcmVzaCI6MTY0OTEzNzAzMSwidHN1cnVnaS9leHAvdXNlIjoxNjQ5NjU1NDMxLCJ0c3VydWdpL2F1dGgvbmFtZSI6InRzdXJ1Z2lfdXNlciJ9.YRtavvDPqJ3CaG1ZavXsB4eNi5vdvQkE5-1X2uMfOhk";
  AccessToken access_token(token_string);

  std::time_t use_expiration_time = access_token.use_expiration_time();
  if (use_expiration_time != 0) {
    // トークン使用期限の取得成功
    std::cout << std::put_time(std::localtime(&use_expiration_time), "%Y/%m/%d %H:%M:%S") << std::endl;
  } else {
    // トークン使用期限の取得エラー
    std::cout << "不正なトークン（使用期限取得エラー）" << std::endl;
  }
  ```

  ```text
  2022/04/11 14:37:11
  ```

### Java I/F

#### showメソッド

**ライブラリ**：認証管理基盤ライブラリ(`tsurugi-manager-authentication.jar`)  
**パッケージ**：`com.github.project_tsurugi.manager.authentication`  
**クラス**　　：`AccessToken`

```java
package com.github.project_tsurugi.manager.authentication;

public class AccessToken {
  public String show() {...}
}
```

- **概要**  
  アクセストークンを返す。

- **引数**
  - なし

- **戻り値**
  - アクセストークン

- **例外**  
  なし

- **使用例**

  ```Java
  import com.github.project_tsurugi.manager.authentication.AccessToken;
  ```

  ```Java
  String tokenString = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJhdXRoZW50aWNhdGlvbi1tYW5hZ2VyIiwiYXVkIjoibWV0YWRhdGEtbWFuYWdlciIsInN1YiI6IkF1dGhlbnRpY2F0aW9uVG9rZW4iLCJpYXQiOjE2NDkwNTA2MzEsImV4cCI6MTY0OTA1MDkzMSwidHN1cnVnaS9leHAvcmVmcmVzaCI6MTY0OTEzNzAzMSwidHN1cnVnaS9leHAvdXNlIjoxNjQ5NjU1NDMxLCJ0c3VydWdpL2F1dGgvbmFtZSI6InRzdXJ1Z2lfdXNlciJ9.YRtavvDPqJ3CaG1ZavXsB4eNi5vdvQkE5-1X2uMfOhk";
  AccessToken accessToken = new AccessToken(tokenString);

  System.out.println(accessToken.show());
  ```

  ```text
  eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJhdXRoZW50aWNhdGlvbi1tYW5hZ2VyIiwiYXVkIjoibWV0YWRhdGEtbWFuYWdlciIsInN1YiI6IkF1dGhlbnRpY2F0aW9uVG9rZW4iLCJpYXQiOjE2NDkwNTA2MzEsImV4cCI6MTY0OTA1MDkzMSwidHN1cnVnaS9leHAvcmVmcmVzaCI6MTY0OTEzNzAzMSwidHN1cnVnaS9leHAvdXNlIjoxNjQ5NjU1NDMxLCJ0c3VydWdpL2F1dGgvbmFtZSI6InRzdXJ1Z2lfdXNlciJ9.YRtavvDPqJ3CaG1ZavXsB4eNi5vdvQkE5-1X2uMfOhk
  ```

#### isValidメソッド

**ライブラリ**：認証管理基盤ライブラリ(`tsurugi-manager-authentication.jar`)  
**パッケージ**：`com.github.project_tsurugi.manager.authentication`  
**クラス**　　：`AccessToken`

```java
package com.github.project_tsurugi.manager.authentication;

public class AccessToken {
  public boolean isValid() {...}
}
```

- **概要**  
  アクセストークンの有効性を検証する。  
  検証内容は「[トークンの有効性検証](#トークンの有効性検証)」を参照。

- **引数**
  - なし

- **戻り値**
  - トークン状態
    - `true`  
      アクセストークンが有効である場合。  
    - `false`  
      アクセストークンが無効である場合。

- **例外**  
  なし

- **使用例**

  ```Java
  import com.github.project_tsurugi.manager.authentication.AccessToken;
  ```

  ```Java
  String tokenString = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJhdXRoZW50aWNhdGlvbi1tYW5hZ2VyIiwiYXVkIjoibWV0YWRhdGEtbWFuYWdlciIsInN1YiI6IkF1dGhlbnRpY2F0aW9uVG9rZW4iLCJpYXQiOjE2NDkwNTA2MzEsImV4cCI6MTY0OTA1MDkzMSwidHN1cnVnaS9leHAvcmVmcmVzaCI6MTY0OTEzNzAzMSwidHN1cnVnaS9leHAvdXNlIjoxNjQ5NjU1NDMxLCJ0c3VydWdpL2F1dGgvbmFtZSI6InRzdXJ1Z2lfdXNlciJ9.YRtavvDPqJ3CaG1ZavXsB4eNi5vdvQkE5-1X2uMfOhk";
  AccessToken accessToken = new AccessToken(tokenString);

  System.out.println(accessToken.isValid());
  ```

  ```text
  true
  ```

#### isAvailableメソッド

**ライブラリ**：認証管理基盤ライブラリ(`tsurugi-manager-authentication.jar`)  
**パッケージ**：`com.github.project_tsurugi.manager.authentication`  
**クラス**　　：`AccessToken`

```java
package com.github.project_tsurugi.manager.authentication;

public class AccessToken {
  public boolean isAvailable() {...}
}
```

- **概要**  
  アクセストークンが使用可能か否かを検証する。  
  検証内容は「[トークンの使用可否検証](#トークンの使用可否検証)」を参照。

- **引数**
  - なし

- **戻り値**
  - トークン状態
    - `true`  
      アクセストークンが使用可能である場合。
    - `false`  
      アクセストークンが使用不可である場合。

- **例外**  
  なし

- **使用例**

  ```Java
  import com.github.project_tsurugi.manager.authentication.AccessToken;
  ```

  ```Java
  String tokenString = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJhdXRoZW50aWNhdGlvbi1tYW5hZ2VyIiwiYXVkIjoibWV0YWRhdGEtbWFuYWdlciIsInN1YiI6IkF1dGhlbnRpY2F0aW9uVG9rZW4iLCJpYXQiOjE2NDkwNTA2MzEsImV4cCI6MTY0OTA1MDkzMSwidHN1cnVnaS9leHAvcmVmcmVzaCI6MTY0OTEzNzAzMSwidHN1cnVnaS9leHAvdXNlIjoxNjQ5NjU1NDMxLCJ0c3VydWdpL2F1dGgvbmFtZSI6InRzdXJ1Z2lfdXNlciJ9.YRtavvDPqJ3CaG1ZavXsB4eNi5vdvQkE5-1X2uMfOhk";
  AccessToken accessToken = new AccessToken(tokenString);

  System.out.println(accessToken.isAvailable());
  ```

  ```text
  true
  ```

#### getUserNameメソッド

**ライブラリ**：認証管理基盤ライブラリ(`tsurugi-manager-authentication.jar`)  
**パッケージ**：`com.github.project_tsurugi.manager.authentication`  
**クラス**　　：`AccessToken`

```java
package com.github.project_tsurugi.manager.authentication;

public class AccessToken {
  public String getUserName() {...}
}
```

- **概要**  
  ユーザ名を返す。  
  未初期化および不正なトークン（有効期限切れ等のデコード可能なトークンは除く）の場合は`null`を返す。

- **引数**
  - なし

- **戻り値**
  - ユーザ名 or `null`

- **例外**  
  なし

- **使用例**

  ```Java
  import com.github.project_tsurugi.manager.authentication.AccessToken;
  ```

  ```Java
  String tokenString = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJhdXRoZW50aWNhdGlvbi1tYW5hZ2VyIiwiYXVkIjoibWV0YWRhdGEtbWFuYWdlciIsInN1YiI6IkF1dGhlbnRpY2F0aW9uVG9rZW4iLCJpYXQiOjE2NDkwNTA2MzEsImV4cCI6MTY0OTA1MDkzMSwidHN1cnVnaS9leHAvcmVmcmVzaCI6MTY0OTEzNzAzMSwidHN1cnVnaS9leHAvdXNlIjoxNjQ5NjU1NDMxLCJ0c3VydWdpL2F1dGgvbmFtZSI6InRzdXJ1Z2lfdXNlciJ9.YRtavvDPqJ3CaG1ZavXsB4eNi5vdvQkE5-1X2uMfOhk";
  AccessToken accessToken = new AccessToken(tokenString);

  String user_name = accessToken.getUserName();
  if (!user_name) {
    // ユーザ名の取得成功
    System.out.println(user_name);
  } else {
    // ユーザ名の取得エラー
    System.out.println("不正なトークン（ユーザ名取得エラー）");
  }
  ```

  ```text
  tsurugi_user
  ```

#### getIssuedTimeメソッド

**ライブラリ**：認証管理基盤ライブラリ(`tsurugi-manager-authentication.jar`)  
**パッケージ**：`com.github.project_tsurugi.manager.authentication`  
**クラス**　　：`AccessToken`

```java
package com.github.project_tsurugi.manager.authentication;

public class AccessToken {
  public Date getIssuedTime() {...}
}
```

- **概要**  
  アクセストークンの発行日時を返す。  
  未初期化および不正なトークン（有効期限切れ等のデコード可能なトークンは除く）の場合は`null`を返す。

- **引数**
  - なし

- **戻り値**
  - 発行日時 or `null`

- **例外**  
  なし

- **使用例**

  ```Java
  import com.github.project_tsurugi.manager.authentication.AccessToken;

  import java.text.SimpleDateFormat;
  import java.util.Date;
  import java.util.TimeZone;
  ```

  ```Java
  SimpleDateFormat jstSdf = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss");
  jstSdf.setTimeZone(TimeZone.getTimeZone("Asia/Tokyo"));

  String tokenString = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJhdXRoZW50aWNhdGlvbi1tYW5hZ2VyIiwiYXVkIjoibWV0YWRhdGEtbWFuYWdlciIsInN1YiI6IkF1dGhlbnRpY2F0aW9uVG9rZW4iLCJpYXQiOjE2NDkwNTA2MzEsImV4cCI6MTY0OTA1MDkzMSwidHN1cnVnaS9leHAvcmVmcmVzaCI6MTY0OTEzNzAzMSwidHN1cnVnaS9leHAvdXNlIjoxNjQ5NjU1NDMxLCJ0c3VydWdpL2F1dGgvbmFtZSI6InRzdXJ1Z2lfdXNlciJ9.YRtavvDPqJ3CaG1ZavXsB4eNi5vdvQkE5-1X2uMfOhk";
  AccessToken accessToken = new AccessToken(tokenString);

  Date issuedTime = accessToken.getIssuedTime();
  if (!issuedTime) {
    // 発行日時の取得成功
    System.out.println(jstSdf.format(issuedTime));
  } else {
    // 発行日時の取得エラー
    System.out.println("不正なトークン（発行日時取得エラー）");
  }
  ```

  ```text
  2022/04/04 14:37:11
  ```

#### getExpirationTimeメソッド

**ライブラリ**：認証管理基盤ライブラリ(`tsurugi-manager-authentication.jar`)  
**パッケージ**：`com.github.project_tsurugi.manager.authentication`  
**クラス**　　：`AccessToken`

```java
package com.github.project_tsurugi.manager.authentication;

public class AccessToken {
  public Date getExpirationTime() {...}
}
```

- **概要**  
  アクセストークンの有効期限を返す。  
  未初期化および不正なトークン（有効期限切れ等のデコード可能なトークンは除く）の場合は`null`を返す。

- **引数**
  - なし

- **戻り値**
  - 有効期限 or `null`

- **例外**  
  なし

- **使用例**

  ```Java
  import com.github.project_tsurugi.manager.authentication.AccessToken;

  import java.text.SimpleDateFormat;
  import java.util.Date;
  import java.util.TimeZone;
  ```

  ```Java
  SimpleDateFormat jstSdf = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss");
  jstSdf.setTimeZone(TimeZone.getTimeZone("Asia/Tokyo"));

  String tokenString = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJhdXRoZW50aWNhdGlvbi1tYW5hZ2VyIiwiYXVkIjoibWV0YWRhdGEtbWFuYWdlciIsInN1YiI6IkF1dGhlbnRpY2F0aW9uVG9rZW4iLCJpYXQiOjE2NDkwNTA2MzEsImV4cCI6MTY0OTA1MDkzMSwidHN1cnVnaS9leHAvcmVmcmVzaCI6MTY0OTEzNzAzMSwidHN1cnVnaS9leHAvdXNlIjoxNjQ5NjU1NDMxLCJ0c3VydWdpL2F1dGgvbmFtZSI6InRzdXJ1Z2lfdXNlciJ9.YRtavvDPqJ3CaG1ZavXsB4eNi5vdvQkE5-1X2uMfOhk";
  AccessToken accessToken = new AccessToken(tokenString);

  Date expirationTime = accessToken.getExpirationTime();
  if (!expirationTime) {
    // 有効期限の取得成功
    System.out.println(jstSdf.format(expirationTime));
  } else {
    // 有効期限の取得エラー
    System.out.println("不正なトークン（有効期限取得エラー）");
  }
  ```

  ```text
  2022/04/04 14:42:11
  ```

#### getRefreshExpirationTimeメソッド

**ライブラリ**：認証管理基盤ライブラリ(`tsurugi-manager-authentication.jar`)  
**パッケージ**：`com.github.project_tsurugi.manager.authentication`  
**クラス**　　：`AccessToken`

```java
package com.github.project_tsurugi.manager.authentication;

public class AccessToken {
  public Date getRefreshExpirationTime() {...}
}
```

- **概要**  
  アクセストークンのリフレッシュ期限を返す。  
  未初期化および不正なトークン（有効期限切れ等のデコード可能なトークンは除く）の場合は`null`を返す。

- **引数**
  - なし

- **戻り値**
  - リフレッシュ期限 or `null`

- **例外**  
  なし

- **使用例**

  ```Java
  import com.github.project_tsurugi.manager.authentication.AccessToken;

  import java.text.SimpleDateFormat;
  import java.util.Date;
  import java.util.TimeZone;
  ```

  ```Java
  SimpleDateFormat jstSdf = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss");
  jstSdf.setTimeZone(TimeZone.getTimeZone("Asia/Tokyo"));

  String tokenString = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJhdXRoZW50aWNhdGlvbi1tYW5hZ2VyIiwiYXVkIjoibWV0YWRhdGEtbWFuYWdlciIsInN1YiI6IkF1dGhlbnRpY2F0aW9uVG9rZW4iLCJpYXQiOjE2NDkwNTA2MzEsImV4cCI6MTY0OTA1MDkzMSwidHN1cnVnaS9leHAvcmVmcmVzaCI6MTY0OTEzNzAzMSwidHN1cnVnaS9leHAvdXNlIjoxNjQ5NjU1NDMxLCJ0c3VydWdpL2F1dGgvbmFtZSI6InRzdXJ1Z2lfdXNlciJ9.YRtavvDPqJ3CaG1ZavXsB4eNi5vdvQkE5-1X2uMfOhk";
  AccessToken accessToken = new AccessToken(tokenString);

  Date refreshExpirationTime = accessToken.getRefreshExpirationTime();
  if (!refreshExpirationTime) {
    // リフレッシュ期限の取得成功
    System.out.println(jstSdf.format(refreshExpirationTime));
  } else {
    // リフレッシュ期限の取得エラー
    System.out.println("不正なトークン（リフレッシュ期限取得エラー）");
  }
  ```

  ```text
  2022/04/05 14:37:11
  ```

#### getUseExpirationTimeメソッド

**ライブラリ**：認証管理基盤ライブラリ(`tsurugi-manager-authentication.jar`)  
**パッケージ**：`com.github.project_tsurugi.manager.authentication`  
**クラス**　　：`AccessToken`

```java
package com.github.project_tsurugi.manager.authentication;

public class AccessToken {
  public Date getUseExpirationTime() {...}
}
```

- **概要**  
  アクセストークンの使用期限を返す。  
  未初期化および不正なトークン（有効期限切れ等のデコード可能なトークンは除く）の場合は`null`を返す。

- **引数**
  - なし

- **戻り値**
  - トークン使用期限 or `null`

- **例外**  
  なし

- **使用例**

  ```Java
  import com.github.project_tsurugi.manager.authentication.AccessToken;

  import java.text.SimpleDateFormat;
  import java.util.Date;
  import java.util.TimeZone;
  ```

  ```Java
  SimpleDateFormat jstSdf = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss");
  jstSdf.setTimeZone(TimeZone.getTimeZone("Asia/Tokyo"));

  String tokenString = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJhdXRoZW50aWNhdGlvbi1tYW5hZ2VyIiwiYXVkIjoibWV0YWRhdGEtbWFuYWdlciIsInN1YiI6IkF1dGhlbnRpY2F0aW9uVG9rZW4iLCJpYXQiOjE2NDkwNTA2MzEsImV4cCI6MTY0OTA1MDkzMSwidHN1cnVnaS9leHAvcmVmcmVzaCI6MTY0OTEzNzAzMSwidHN1cnVnaS9leHAvdXNlIjoxNjQ5NjU1NDMxLCJ0c3VydWdpL2F1dGgvbmFtZSI6InRzdXJ1Z2lfdXNlciJ9.YRtavvDPqJ3CaG1ZavXsB4eNi5vdvQkE5-1X2uMfOhk";
  AccessToken accessToken = new AccessToken(tokenString);

  Date useExpirationTime = accessToken.getUseExpirationTime();
  if (!useExpirationTime) {
    // トークン使用期限の取得成功
    System.out.println(jstSdf.format(useExpirationTime));
  } else {
    // トークン使用期限の取得エラー
    System.out.println("不正なトークン（使用期限取得エラー）");
  }
  ```

  ```text
  2022/04/11 14:37:11
  ```

---

## 認可情報提供機能

### C++ I/F

#### get_aclsメソッド

**ライブラリ**　　：メタデータ管理基盤ライブラリ(`libmanager-metadata.so`)  
**ヘッダファイル**：`include/manager/metadata/tables.h`  
**名前空間**　　　：`manager::metadata`  
**クラス**　　　　：`Tables`  

```cpp
namespace manager::metadata {
  class Tables : public Metadata {
   public:
    ErrorCode get_acls(std::string_view token, boost::property_tree::ptree& acls);
  }
}
```

- **概要**  
  全テーブルのACL情報を基に、引数で指定されたアクセストークンより抽出したユーザ名（ロール名）に対する認可情報を生成して返す。

- **引数**
  - `token`  
    検証するアクセストークン。
  - `acls`  
    認証済みユーザに認可された情報。  

    - データ構成(boost::property_tree::ptree)  
      |#|フィールド|データ型|説明|
      |--:|---|---|---|
      |1.|`tables`|object|ACLオブジェクト|
  
      **ACLオブジェクト**  
        |#|フィールド|データ型|説明|
        |--:|---|---|---|
        |1.|*`<テーブル名>`*|std::string|テーブルに対するアクセス権|

    - アクセス権

      |アクセス権<br>(PostgreSQL準拠)|意味|
      |:-:|---|
      |`a`|`INSERT`|
      |`r`|`SELECT`|
      |`w`|`UPDATE`|
      |`d`|`DELETE`|
      |`D`|`TRUNCATE`|
      |`x`|`REFERENCES`|
      |`t`|`TRIGGER`|

    - データイメージ

      ```sql
      table-name | acl
      -----------+--------------------------------------------------
      table_01   | {admin=arwdDxt/admin, tsurugi_user=arwdDxt/admin}    # 全ての権限
      table_02   | {admin=arwdDxt/admin, tsurugi_user=r/admin}          # 読み取りのみ
      table_03   | {admin=arwdDxt/admin, other_user=arwdDxt/admin}      # 権限なし
      table_04   | {admin=arwdDxt/admin, =arwd/admin}                   # PUBLIC
      ```

      ```json
      // ユーザ名：tsurugi_user
      {
        "tables": {
          "table-01": "arwdDxt",
          "table-02": "r",
          "table-04": "arwd"
        }
      }
      ```

- **戻り値**
  - `ErrorCode::OK`  
    正常に終了した場合。
  - `ErrorCode::INVALID_PARAMETER`  
    無効なトークンが指定された場合。
  - `ErrorCode::DATABASE_ACCESS_FAILURE`  
    データベースへのアクセスエラーが発生した場合。
  - `ErrorCode::INTERNAL_ERROR`  
    予期しない内部エラーが発生した場合。

- **例外**  
  なし

- **使用例**

  ```cpp
  #include <boost/foreach.hpp>
  #include <iostream>

  #include "manager/metadata/tables.h"
  
  using manager::metadata::ErrorCode;
  using manager::metadata::Tables;
  ```

  ```cpp
  auto tables = std::make_unique<Tables>("データベース名");

  ErrorCode error = tables->init();
  if (error != ErrorCode::OK) {
    // エラー処理
  }
  ```

  ```cpp
  std::string token_string = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJhdXRoZW50aWNhdGlvbi1tYW5hZ2VyIiwiYXVkIjoibWV0YWRhdGEtbWFuYWdlciIsInN1YiI6IkF1dGhlbnRpY2F0aW9uVG9rZW4iLCJpYXQiOjE2NDkwNTA2MzEsImV4cCI6MTY0OTA1MDkzMSwidHN1cnVnaS9leHAvcmVmcmVzaCI6MTY0OTEzNzAzMSwidHN1cnVnaS9leHAvdXNlIjoxNjQ5NjU1NDMxLCJ0c3VydWdpL2F1dGgvbmFtZSI6InRzdXJ1Z2lfdXNlciJ9.YRtavvDPqJ3CaG1ZavXsB4eNi5vdvQkE5-1X2uMfOhk";
  boost::property_tree::ptree authorization;

  // テーブル認可情報の取得
  error = tables->get_acls(token_string, authorization);
  if (result == ErrorCode::OK) {
    // 認可情報オブジェクト
    auto table_acls = authorization.get_child_optional(Tables::TABLE_ACL_NODE);
    if (table_acls) {
      BOOST_FOREACH (const auto& node, table_acls.get()) {
        std::string table_name = node.first.data();
        std::string table_acl = node.second.data();
        std::cout << table_name  << ": [" << table_acl << "]" << std::endl;
      }
    }
  } else if (result == ErrorCode::INVALID_PARAMETER) {
    // 無効なトークン
    std::cout << "無効なトークン" << std::endl;
  } else if (result == ErrorCode::DATABASE_ACCESS_FAILURE) {
    // データベース接続エラー
    std::cout << "データベース接続エラー" << std::endl;
  } else {
    // その他のエラー
    std::cout << "その他のエラー" << std::endl;
  }
  ```

  ```text
  table_01: [arwdDxt]
  table_02: [r]
  table_04: [arwd]
  ```

#### TABLE_ACL_NODE定数

**ヘッダファイル**：`include/manager/metadata/tables.h`  
**名前空間**　　　：`manager::metadata`  
**クラス**　　　　：`Tables`  

```cpp
namespace manager::metadata {
  class Tables : public Metadata {
   public:
    static constexpr const char* const TABLE_ACL_NODE = "tables";
  }
}
```

- **概要**  
  テーブル認可情報を示すメタデータのフィールド名定数。

---

## 付録

### コンフィギュレーション

#### 環境変数

##### 認証管理基盤

認証管理基盤で使用する環境変数。  
※実効ユーザの環境変数に設定する。

|#|環境変数|説明|設定例|デフォルト値|
|--:|---|---|---|---|
|1.|`TSURUGI_CONNECTION_STRING_AUTH`|ユーザ認証に使用するDBサーバへの接続文字列|`postgresql://localhost:5432/tsurugi`|`dbname=tsurugi`|
|2.|`TSURUGI_JWT_CLAIM_ISS`|アクセストークンの発行者(Issuer)に設定するクレーム値|`authentication-manager`|`authentication-manager`|
|3.|`TSURUGI_JWT_CLAIM_AUD`|アクセストークンの利用者(Audience)に設定するクレーム値|`metadata-manager`|`metadata-manager`|
|4.|`TSURUGI_JWT_CLAIM_SUB`|アクセストークンの用途(Subject)に設定するクレーム値|`AuthenticationToken`|`AuthenticationToken`|
|5.|`TSURUGI_JWT_SECRET_KEY`|アクセストークンの署名に使用する共通キーフレーズ|`tsurugi-256-bit-secret-sample-key`|システム固定値|
|6.|`TSURUGI_TOKEN_EXPIRATION`|アクセストークンの有効期間（※）|`1800`, `1800s`, `30min`|`300`|
|7.|`TSURUGI_TOKEN_EXPIRATION_REFRESH`|アクセストークンをリフレッシュ可能な期間（※）|`48h`, `2d`|`24h`|
|8.|`TSURUGI_TOKEN_EXPIRATION_USE`|アクセストークンが使用可能な期間（※）|`2160h`, `90d`|`7d`|

※指定可能な単位記号は、`s`(秒)、`min`(分)、`h`(時)、`d`(日)とし、未指定時は`s`が指定されたものとして扱う。
※`TSURUGI_TOKEN_EXPIRATION_USE`が`0`である場合は、アクセストークンの使用可能期限は無期限となり、アクセストークンは有効期限およびリフレッシュ期限にのみ準ずる（リフレッシュ期限内にリフレッシュを行うことで、実質的な永久トークンとして使用できる）。

##### メタデータ管理基盤

メタデータ管理基盤で使用する環境変数。  
※実効ユーザの環境変数に設定する。

|#|環境変数|説明|設定例|デフォルト値|
|--:|---|---|---|---|
|1.|`TSURUGI_CONNECTION_STRING`|テーブルメタデータが格納されているDBサーバへの接続文字列|`postgresql://admin:pswd123@localhost:5432/tsurugi`|`dbname=tsurugi`|
|2.|`TSURUGI_JWT_SECRET_KEY`|アクセストークンの署名に使用する共通キーフレーズ|`tsurugi-256-bit-secret-sample-key`|システム固定値|

### アクセストークン

JWT(JWS)に準拠（[RFC7515](https://datatracker.ietf.org/doc/html/rfc7515)参照）したトークン。

> <JOSEヘッダ>.<JWSペイロード>.<JWSシグネチャ>

- JOSEヘッダ  
  下記JSONデータをBase64urlエンコードした文字列
  |#|クレーム名|値|備考|
  |--:|---|---|---|
  |1.|`alg`|`"HS256"`|固定値|
  |2.|`typ`|`"JWT"`|固定値|
- JWSペイロード  
  下記JSONデータをBase64urlエンコードした文字列
  |#|クレーム名|値|備考|
  |--:|---|---|---|
  |1.|`iss`|環境変数値(`TSURUGI_JWT_CLAIM_ISS`)||
  |2.|`aud`|環境変数値(`TSURUGI_JWT_CLAIM_AUD`)||
  |3.|`sub`|環境変数値(`TSURUGI_JWT_CLAIM_SUB`)||
  |4.|`iat`|処理日時(トークン生成日時)|*エポックタイム*|
  |5.|`exp`|処理日時+環境変数値(`TSURUGI_TOKEN_EXPIRATION`)|*エポックタイム*|
  |6.|`tsurugi/exp/refresh`|処理日時+環境変数値(`TSURUGI_TOKEN_EXPIRATION_REFRESH`)|*エポックタイム*|
  |7.|`tsurugi/exp/use`|処理日時+環境変数値(`TSURUGI_TOKEN_EXPIRATION_USE`)|*エポックタイム*|
  |8.|`tsurugi/auth/name`|認証済みユーザ名||
- JWSシグネチャ  
  エンコード済みのヘッダとペイロードを"."で結合、"alg"の暗号アルゴリズムで署名し、Base64urlエンコードした文字列
  - 暗号アルゴリズム
    - HS256(HMAC using SHA-256 hash)固定
  - 共通キー
    - 環境変数値(`TSURUGI_JWT_SECRET_KEY`)

#### トークンの有効性検証

トークンの署名とクレームを検証することで有効性の判定を行う。

- 署名検証
  - データと署名を用いてデータの完全性を検証
- クレーム検証
  - 発行日時(`iat`)が現在日時以前（発行日時≦現在日時）であることを検証
  - 有効期限(`exp`)が現在日時以降（有効期限≧現在日時）であることを検証
  - 使用期限(`tsurugi/exp/use`)が現在日時以降（使用期限≧現在日時）であることを検証

#### トークンの使用可否検証

トークンの署名とクレームを検証することで有効性の判定を行う。

- 署名検証
  - データと署名を用いてデータの完全性を検証
- クレーム検証
  - 発行日時(`iat`)が現在日時以前（発行日時≦現在日時）であることを検証
  - 有効期限(`exp`)もしくはリフレッシュ期限(`tsurugi/exp/refresh`)が現在日時以降（有効期限/リフレッシュ期限≧現在日時）であることを検証
  - 使用期限(`tsurugi/exp/use`)が現在日時以降（使用期限≧現在日時）であることを検証

#### トークンの期限

- **有効期限**
  - 概要
    - トークンの有効期限
    - 発行またはリフレッシュ時に設定される可変の期限
  - 期限切れの扱い
    - 認可情報の取得が行えない無効なトークン
    - リフレッシュ期限内にリフレッシュを行うことで有効化できる（リフレッシュ可能トークン）
- **リフレッシュ期限**
  - 概要
    - トークンリフレッシュ（有効期限の延長）が可能な期限
    - 発行またはリフレッシュ時に設定される可変の期限
  - 期限切れの扱い
    - トークンのリフレッシュが行えないトークン
- **トークン使用期限**
  - 概要
    - トークン自体の使用期限
    - 発行時に確定される不変の期限
  - 期限切れの扱い
    - 利用できない無効なトークン
    - トークン状態  
      無効トークン

##### 各期限の状態におけるトークンの状態

|#|有効期限|リフレッシュ期限|使用期限|トークン状態|`is_valid()`/`isValid()`|`is_available()`/`isAvailable()`|
|--:|:-:|:-:|:-:|---|---|---|
|1.|`○`|`○`|`○`|有効トークン|`true`|`true`|
|2.|`○`|`×`|`○`|有効トークン|`true`|`true`|
|3.|`×`|`○`|`○`|リフレッシュ可能トークン|`false`|`true`|
|4.|`×`|`×`|`○`|無効トークン|`false`|`false`|
|5.|<span style="color: #bbb">－</span>|<span style="color: #bbb">－</span>|`×`|無効トークン|`false`|`false`|

`○`：期限内　`×`：期限切れ

##### リフレッシュに伴うトークン状態イメージ

- 有効期限 : 1時間
- リフレッシュ期限 : 12時間
- トークン使用期限 : 3日

```mermaid
%%{init:{'themeCSS': ".taskText { fill: black; text-anchor: middle;}"}}%%
gantt
  dateFormat DD HH:mm
  axisFormat %d日 %H時
  todayMarker off

  section  
    トークン使用期限（4日 0時） : crit, active, 01 00:00, 04 00:00
  section 発行(認証)
    有効期限（1日 1時） : active, 01 00:00, 1h
    リフレッシュ期限（1日12時） : active, 01 00:00, 12h
  section 1日10時にリフレッシュ
    有効期限（1日11時） : active, 01 10:00, 1h
    リフレッシュ期限（1日22時） : active, 01 10:00, 12h
  section 1日22時にリフレッシュ
    有効期限（1日23時） : active, 01 22:00, 1h
    リフレッシュ期限（2日10時） : active, 01 22:00, 12h
  section ：
    ： : 01 00:00
  section ：  
    ： : 01 00:00
  section 3日22時にリフレッシュ ※4日0時を超えない
    有効期限（3日23時） : active, 03 22:00, 1h
    リフレッシュ期限（4日 0時） : active, 03 22:00, 2h
    無効トークン : done, 04 00:00, 6h
```

#### アクセストークンサンプル

  ```sh
  # JOSEヘッダ
  {
    "alg": "HS256",
    "typ": "JWT"
  }
  # JWSペイロード
  {
    "iss": "authentication-manager",
    "aud": "metadata-manager",
    "sub": "AuthenticationToken",
    "iat": 1649050631,
    "exp": 1649050931,
    "tsurugi/exp/refresh": 1649137031,
    "tsurugi/exp/use": 1649655431,
    "tsurugi/auth/name": "tsurugi_user"
  }
  # 共通キー
  tsurugi-256-bit-secret-sample-key
  ```

  ```text
  eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJhdXRoZW50aWNhdGlvbi1tYW5hZ2VyIiwiYXVkIjoibWV0YWRhdGEtbWFuYWdlciIsInN1YiI6IkF1dGhlbnRpY2F0aW9uVG9rZW4iLCJpYXQiOjE2NDkwNTA2MzEsImV4cCI6MTY0OTA1MDkzMSwidHN1cnVnaS9leHAvcmVmcmVzaCI6MTY0OTEzNzAzMSwidHN1cnVnaS9leHAvdXNlIjoxNjQ5NjU1NDMxLCJ0c3VydWdpL2F1dGgvbmFtZSI6InRzdXJ1Z2lfdXNlciJ9.YRtavvDPqJ3CaG1ZavXsB4eNi5vdvQkE5-1X2uMfOhk
  ```

以上
