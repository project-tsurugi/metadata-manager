【Project-Tsurugi Internal User Only】

# 認証管理基盤 API仕様書

2022.05.27 KCC 初版

## 目次

- [認証管理基盤 API仕様書](#認証管理基盤-api仕様書)
  - [目次](#目次)
  - [機能概要](#機能概要)
    - [コンポーネント関係図](#コンポーネント関係図)
  - [ユーザ認証機能](#ユーザ認証機能)
    - [C++ I/F](#c-if)
      - [auth_userメソッド](#auth_userメソッド)
    - [Java I/F](#java-if)
      - [authUserメソッド](#authuserメソッド)
  - [トークン検証機能](#トークン検証機能)
    - [C++ I/F](#c-if-1)
      - [is_valid_tokenメソッド](#is_valid_tokenメソッド)
    - [Java I/F](#java-if-1)
      - [isValidTokenメソッド](#isvalidtokenメソッド)
  - [認可情報提供機能](#認可情報提供機能)
    - [C++ I/F](#c-if-2)
      - [get_aclsメソッド](#get_aclsメソッド)
      - [TABLE_ACL_NODE定数](#table_acl_node定数)
  - [付録](#付録)
    - [コンフィギュレーション](#コンフィギュレーション)
      - [環境変数](#環境変数)
        - [認証管理基盤](#認証管理基盤)
        - [メタデータ管理基盤](#メタデータ管理基盤)
    - [認証トークン](#認証トークン)
      - [認証トークンサンプル](#認証トークンサンプル)

---

## 機能概要

認証管理基盤は、ユーザ認証に関連する下記機能を提供する。

|#|機能|概要|コンポーネント名|C++ I/F|Java I/F|
|--:|---|---|---|:-:|:-:|
|1.|ユーザ認証機能|ユーザ名およびパスワードを用いた認証を行い、認証トークンを発行する機能|認証管理基盤|○|○|
|2.|トークン検証機能|認証トークンの有効性を検証する機能|認証管理基盤|○|○|
|3.|認可情報提供機能|認証済みユーザのテーブル認可情報を提供する機能|メタデータ管理基盤|○|<span style="color: #bbb">×</span>|

### コンポーネント関係図

```mermaid
graph LR
  classDef request stroke-width:0px, fill:#ecf5e7
  classDef manager stroke-width:2px, stroke:#f66, fill:#fee
  classDef library color:#777, fill:#eee, stroke-dasharray:2 2

  Authentication((認証要求)):::request
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
  AuthMngJava .- AuthMngCpp
  Authentication .-> ComponentAuthCpp
  ComponentAuthCpp --"[C++ I/F]"---- AuthMngCpp
  AuthMngCpp .- DB

  Authorization .-> ComponentMeta
  ComponentMeta --"[C++ I/F]"--- MetaMng
  MetaMng ...- DB
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
  }
}
```

- **概要**  
  指定された認証情報（ユーザ名およびパスワード）を用いて認証を試行する。  
  認証トークン(`token`)が指定されている場合(`nullptr`以外の場合)は、認証トークンを発行し格納する。

- **引数**
  - `userName`  
    認証するユーザ名。
  - `password`  
    パスワード。
  - `token`  
    発行された認証トークン（詳細は「[認証トークン](#認証トークン)」を参照）を格納するためのポインタ。

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
  #include "manager/authentication/authentication.h"
  
  using manager::authentication::Authentication;
  using manager::authentication::ErrorCode;
  ```

  ```cpp
  std::string user_name = "user01";
  std::string password = "1q2w3e4r";
  std::string token = "";

  ErrorCode result = Authentication::auth_user(user_name, password, &token);
  if (result == ErrorCode::OK) {
    // 認証成功
    ...
  } else if (result == ErrorCode::AUTHENTICATION_FAILURE) {
    // 認証エラー
    ...
  } else if (result == ErrorCode::CONNECTION_FAILURE) {
    // データベース接続エラー
    ...
  } 
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
}
```

- **概要**  
  指定された認証情報（ユーザ名およびパスワード）を用いて認証を試行し、認証成功時に認証トークンを発行する。

- **引数**
  - `userName`  
    認証するユーザ名。
  - `password`  
    パスワード。

- **戻り値**
  - 認証トークン  
    ※詳細は「[認証トークン](#認証トークン)」を参照。

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
  String userName = "user01";
  String password = "1q2w3e4r";

  String token = "";
  try {
    token = Authentication.authUser(userName, password);
    // 認証成功
    ...
  } catch (AuthenticationException e) {
    // 認証エラー
    ...
  } catch (DBAccessException e) {
    // データベース接続エラー
    ...
  }
  ```

---

## トークン検証機能

### C++ I/F

#### is_valid_tokenメソッド

**ライブラリ**　　：認証管理基盤ライブラリ(`libmanager-authentication.so`)  
**ヘッダファイル**：`include/manager/authentication/authentication.h`  
**名前空間**　　　：`manager::authentication`  
**クラス**　　　　：`Authentication`  

```cpp
namespace manager::authentication {
  class Authentication {
   public:
    static bool is_valid_token(std::string_view token);
  }
}
```

- **概要**  
  指定された認証トークンの有効性を検証する。

- **引数**
  - `token`  
    検証する認証トークン。

- **戻り値**
  - トークン状態
    - `true`  
      認証トークンが有効である場合。
    - `false`  
      認証トークンが無効である場合。

- **例外**  
  なし

- **使用例**

  ```cpp
  #include "manager/authentication/authentication.h"

  using manager::authentication::Authentication;
  ```

  ```cpp
  std::string token = "Header.Payload.Signature";

  if (Authentication::is_valid_token(token)) {
    // トークン有効
    ...
  } else {
    // トークン無効
    ...
  } 
  ```

### Java I/F

#### isValidTokenメソッド

**ライブラリ**：認証管理基盤ライブラリ(`tsurugi-manager-authentication.jar`)  
**パッケージ**：`com.github.project_tsurugi.manager.authentication`  
**クラス**　　：`Authentication`

```java
package com.github.project_tsurugi.manager.authentication;

public class Authentication {
  public static boolean isValidToken(final String token) {...}
}
```

- **概要**  
  指定された認証トークンの有効性を検証する。

- **引数**
  - `token`  
    検証する認証トークン。

- **戻り値**
  - トークン状態
    - `true`  
      認証トークンが有効である場合。
    - `false`  
      認証トークンが無効である場合。

- **例外**  
  なし

- **使用例**

  ```Java
  import com.github.project_tsurugi.manager.authentication.Authentication;
  ```

  ```Java
  String token = "Header.Payload.Signature";

  if (Authentication.isValidToken(token)) {
    // トークン有効
    ...
  } else {
    // トークン無効
    ...
  }
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
  全テーブルのACL情報を基に、引数で指定された認証トークンより抽出したユーザ名（ロール名）に対する認可情報を生成して返す。

- **引数**
  - `token`  
    検証する認証トークン。
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
  #include "manager/metadata/tables.h"
  
  using manager::metadata::ErrorCode;
  using manager::metadata::Tables;
  ```

  ```cpp
  auto tables = std::make_unique<Tables>("データベース名");
  ```

  ```cpp
  ErrorCode error = tables->init();
  if (error != ErrorCode::OK) {
    // エラー処理
  }
  ```

  ```cpp
  // std::string token = "Header.Payload.Signature";
  boost::property_tree::ptree authorization;

  // テーブル認可情報の取得
  error = tables->get_acls(token, authorization);
  if (result != ErrorCode::OK) {
    // 正常終了
    ...
  } else if (result == ErrorCode::INVALID_PARAMETER) {
    // 無効なトークン
    ...
  } else if (result == ErrorCode::DATABASE_ACCESS_FAILURE) {
    // データベース接続エラー
    ...
  } else {
    // その他のエラー
    ...
  }

  // 認可情報オブジェクト
  auto table_acls = authorization.get_child_optional(Tables::TABLE_ACL_NODE);
  if (!table_acls) {
    // エラー処理
  }
  
  BOOST_FOREACH (const auto& node, table_acls.get()) {
    std::string table_name = node.first.data();
    std::string table_acl = node.second.data();
    std::cout << table_name  << ": [" << table_acl << "]" << std::endl;
  }
  ```

  ```sh
  # 出力例
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
|2.|`TSURUGI_JWT_CLAIM_ISS`|認証トークンの発行者(Issuer)に設定するクレーム値|`authentication-manager`|`authentication-manager`|
|3.|`TSURUGI_JWT_CLAIM_AUD`|認証トークンの利用者(Audience)に設定するクレーム値|`metadata-manager`|`metadata-manager`|
|4.|`TSURUGI_JWT_CLAIM_SUB`|認証トークンの用途(Subject)に設定するクレーム値|`AuthenticationToken`|`AuthenticationToken`|
|5.|`TSURUGI_JWT_CLAIM_EXP`|認証トークンの有効期間（秒）|`300`|`300`|
|6.|`TSURUGI_JWT_SECRET_KEY`|認証トークンの署名に使用する共通キーフレーズ|`tsurugi-256-bit-secret-sample-key`|システム固定値|

##### メタデータ管理基盤

メタデータ管理基盤で使用する環境変数。  
※実効ユーザの環境変数に設定する。

|#|環境変数|説明|設定例|デフォルト値|
|--:|---|---|---|---|
|1.|`TSURUGI_CONNECTION_STRING`|テーブルメタデータが格納されているDBサーバへの接続文字列|`postgresql://admin:pswd123@localhost:5432/tsurugi`|`dbname=tsurugi`|
|2.|`TSURUGI_JWT_SECRET_KEY`|認証トークンの署名に使用する共通キーフレーズ|`tsurugi-256-bit-secret-sample-key`|システム固定値|

### 認証トークン

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
  |4.|`nbf`|*UNIXタイム*|トークン生成日時|
  |5.|`exp`|*UNIXタイム*|トークン生成日時+環境変数値(`TSURUGI_JWT_CLAIM_EXP`)|
  |6.|`userName`|認証済みユーザ名||
- JWSシグネチャ  
  エンコード済みのヘッダとペイロードを"."で結合、"alg"の暗号アルゴリズムで署名し、Base64urlエンコードした文字列
  - 暗号アルゴリズム
    - HS256(HMAC using SHA-256 hash)固定
  - 共通キー
    - 環境変数値(`TSURUGI_JWT_SECRET_KEY`)

#### 認証トークンサンプル

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
    "nbf": 1649050631,
    "exp": 1649050931,
    "userName": "tsurugi_user"
  }
  # 共通キー
  tsurugi-256-bit-secret-sample-key
  ```

  ```sh
  eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJhdXRoZW50aWNhdGlvbi1tYW5hZ2VyIiwiYXVkIjoibWV0YWRhdGEtbWFuYWdlciIsInN1YiI6IkF1dGhlbnRpY2F0aW9uVG9rZW4iLCJuYmYiOjE2NDkwNTA2MzEsImV4cCI6MTY0OTA1MDkzMSwidXNlck5hbWUiOiJ0c3VydWdpX3VzZXIifQ.ryWxKQT54gG0Iso4AC2xJonLOfeoak_Me2FG9ydgb2g
  ```

以上
